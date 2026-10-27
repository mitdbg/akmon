"""Issue #2999: approving a held re-sync deletes what the source removed.

A review-mode (held) source run writes the quarantine branch and waits for an
operator. Accepting it copies the run onto ``default``: its upserts from the
quarantine branch, and its deletions from the run's op='R' rows in
``okg.node_facts`` / ``okg.edge_facts`` (``_load_run_retraction_facts``). The
Chronos runner writes the fact log only when ``write_fact_log`` is on, which it
is not by default, so the approval found no deletions and a record the source
removed stayed on ``default`` for good.

The runner now logs a held run's retractions (and only those) whatever
``write_fact_log`` says. Every database test here drives the real path: a
``run_ingest`` of a registered review-mode source, then
``_run_decide(decision="accept")``, the call behind ``okg admission decide``.
No test writes a fact-log row by hand.
"""

from __future__ import annotations

import json
import sys
import uuid
from pathlib import Path
from typing import Any

import psycopg
import pytest
import yaml

from okg.substrate.admission import QUARANTINE_BRANCH
from tests.pytest.source_jobs_inline import use_inline_source_jobs

postgres = pytest.mark.postgres

_SOURCE = "svc.held"
_OTHER_SOURCE = "svc.other"

_HELD_ADAPTER = '''
import json
from pathlib import Path

from okg.substrate.library.sources.base import EdgeFact, NodeFact, SourceRun


class HeldRunSource:
    name = "held_run"
    profile = "discovery_crawl"
    last_run_stats: dict = {}

    def __init__(self, emission_path, **kwargs):
        self.emission_path = Path(emission_path)

    def run(self, run_id, *, mode="scope_complete", cursor=None,
            sync_scope=None, **kwargs):
        emission = json.loads(self.emission_path.read_text())

        def facts():
            for index in emission["nodes"]:
                yield NodeFact(
                    node_id="svc:held-%d" % index,
                    subtype="document",
                    attrs={"text": emission.get("texts", {}).get(
                        str(index), "held document %d" % index,
                    )},
                    source_record_id={"id": "held-%d" % index},
                    source_revision={"v": 1},
                )
            for src, dst in emission["edges"]:
                yield EdgeFact(
                    src="svc:held-%d" % src,
                    dst="svc:held-%d" % dst,
                    edge_type="references",
                    source_record_id={"id": "edge-%d-%d" % (src, dst)},
                    source_revision={"v": 1},
                )

        # No record_set: the stream is the completed-scope footprint, so a
        # node or edge missing from the next run is retracted.
        return SourceRun(
            facts=facts(),
            completed_scope=True,
            next_cursor={"nodes": len(emission["nodes"])},
            run_mode=mode,
        )
'''

_EDGE_BRIDGE = """\
id: https://example.invalid/okg/fixture/bridges/held_links
name: held_links
classes:
  EdgeNarrowings:
    annotations:
      okg_edge_narrowings: "true"
    attributes:
      document_references_document:
        annotations:
          edge_archetype: references
          src_subtypes: Document
          dst_subtypes: Document
"""

# The shipped review-mode sources (gmail, Slack, calendars, PDF inbox) declare
# no output signature, so their profile is `compat`, and most tests here use
# the same. One test declares this signature so the finalize-time signature
# check reads the logged retractions instead of returning `compat`.
_OUTPUT_SIGNATURE = {
    "nodes": [{"subtype": "document", "clause_id": "held.docs"}],
    "edges": [{
        "src_subtype": "document",
        "edge_type": "references",
        "dst_subtype": "document",
        "clause_id": "held.refs",
    }],
}


def _deployment(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    dsn: str,
    *,
    admission_mode: str = "review",
    declare_signature: bool = False,
) -> tuple[Path, Path]:
    """A deployment whose one source runs in ``admission_mode``."""
    from okg.substrate.runtime import admission_workflows
    from tests.substrate.test_ingest_service import (
        _claim_layered_fixture_catalog,
        _fixture_source_privacy_policy,
        _source,
        _write_layered_fixture_deployment,
    )

    module_dir = tmp_path / "adapters"
    module_dir.mkdir()
    (module_dir / "held_resync_adapter.py").write_text(
        _HELD_ADAPTER, encoding="utf-8",
    )
    monkeypatch.syspath_prepend(str(module_dir))
    sys.modules.pop("held_resync_adapter", None)
    dep = _write_layered_fixture_deployment(tmp_path)
    bridges = dep / "schemas" / "bridges"
    bridges.mkdir()
    (bridges / "held_links.yaml").write_text(_EDGE_BRIDGE)
    # Outside the deployment repository: the tests rewrite it between runs.
    emission_path = tmp_path / "held_emission.json"
    registry_path = dep / "source_registry.yaml"
    data = yaml.safe_load(registry_path.read_text())
    entry = {
        **_source(_SOURCE),
        "module": "held_resync_adapter",
        "class": "HeldRunSource",
        "params": {"emission_path": str(emission_path)},
        "source_policy": _fixture_source_privacy_policy(),
    }
    entry["admission_policy"]["admission_mode"] = admission_mode
    if declare_signature:
        entry["admission_policy"]["allowed_record_kinds"] = ["node", "edge"]
        entry["admission_policy"]["output_signature"] = _OUTPUT_SIGNATURE
    data["sources"] = {"held": entry}
    registry_path.write_text(yaml.safe_dump(data))
    _claim_layered_fixture_catalog(dsn, dep)
    # The tests prove approval and retraction logic, not the source child
    # process; `tests/substrate/ingest/test_source_jobs.py` proves that.
    use_inline_source_jobs(monkeypatch)
    monkeypatch.setattr(admission_workflows, "_resolve_dsn", lambda _name: dsn)
    return dep, emission_path


def _chain(indexes: list[int], *, skip: frozenset[int] = frozenset()) -> list[
    tuple[int, int]
]:
    """Edges held-i -> held-(i+1) between consecutive ``indexes``."""
    ordered = sorted(indexes)
    return [
        (a, b)
        for a, b in zip(ordered, ordered[1:])
        if b == a + 1 and a not in skip
    ]


def _sync(
    dsn: str,
    dep: Path,
    emission_path: Path,
    nodes: list[int],
    edges: list[tuple[int, int]],
    *,
    texts: dict[int, str] | None = None,
) -> dict[str, Any]:
    """Run the source over ``nodes``/``edges``; return its newest envelope.

    ``texts`` replaces the default text of the listed nodes.
    """
    from okg.substrate.ingest import IngestOptions, run_ingest

    emission_path.write_text(json.dumps({
        "nodes": nodes,
        "edges": [list(edge) for edge in edges],
        "texts": {str(index): text for index, text in (texts or {}).items()},
    }))
    outcome = run_ingest(
        IngestOptions(
            dsn=dsn,
            deployment="svc",
            deployment_dir=dep,
            include={"held"},
            include_predecessors=False,
            include_dependents=False,
            publish=False,
        ),
    )
    assert outcome.any_failed is False, outcome.report
    return _latest_envelope(dsn)


def _hold(
    dsn: str,
    dep: Path,
    emission_path: Path,
    nodes: list[int],
    edges: list[tuple[int, int]],
    *,
    texts: dict[int, str] | None = None,
) -> dict[str, Any]:
    envelope = _sync(dsn, dep, emission_path, nodes, edges, texts=texts)
    assert envelope["status"] == "pending", envelope
    return envelope


def _latest_envelope(dsn: str) -> dict[str, Any]:
    with psycopg.connect(dsn, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT admission_id::text, status, run_id::text, verdict
              FROM okg.admission_envelopes
             WHERE deployment_name = 'svc'
               AND intent = 'source_sync'
               AND source = %s
             ORDER BY created_at DESC, admission_id DESC
             LIMIT 1
            """,
            (_SOURCE,),
        )
        admission_id, status, run_id, verdict = cur.fetchone()
    return {
        "admission_id": admission_id,
        "status": status,
        "run_id": run_id,
        "verdict": verdict or {},
    }


def _decide(envelope: dict[str, Any]) -> dict[str, Any]:
    from okg.substrate.runtime import admission_workflows

    return admission_workflows._run_decide(
        deployment="svc",
        admission_id=envelope["admission_id"],
        decision="accept",
        actor="operator",
        reason="issue-2999",
        verdict=None,
    )


def _accept(envelope: dict[str, Any]) -> dict[str, Any]:
    decided = _decide(envelope)
    assert decided["status"] == "accepted", decided
    return decided


def _graph(dsn: str, branch: str) -> tuple[list[str], list[tuple[str, str]]]:
    """(node ids, (src, dst) edges) of the held source on ``branch``."""
    from okg.substrate.graph_store import ChronosGraphStore

    store = ChronosGraphStore.connect(dsn, ensure_schema=False)
    try:
        graph = store.checkout(branch)
        nodes = graph.query(
            "SELECT node_id FROM okg.graph_nodes WHERE node_id LIKE 'svc:held-%'"
            " ORDER BY node_id",
        )
        edges = graph.query(
            "SELECT src, dst FROM okg.graph_edges"
            " WHERE source = :source AND edge_type = 'references'"
            " ORDER BY src, dst",
            {"source": _SOURCE},
        )
    finally:
        store.close()
    return (
        [str(row["node_id"]) for row in nodes],
        [(str(row["src"]), str(row["dst"])) for row in edges],
    )


def _expected(
    nodes: list[int],
    edges: list[tuple[int, int]],
) -> tuple[list[str], list[tuple[str, str]]]:
    return (
        sorted(f"svc:held-{index}" for index in nodes),
        sorted((f"svc:held-{a}", f"svc:held-{b}") for a, b in edges),
    )


def _fact_log(dsn: str) -> dict[tuple[str, str, str], int]:
    """(table, run_id, op) -> rows the held source has in the fact log."""
    counts: dict[tuple[str, str, str], int] = {}
    with psycopg.connect(dsn, autocommit=True) as conn, conn.cursor() as cur:
        for table in ("node_facts", "edge_facts"):
            cur.execute(
                f"SELECT run_id::text, op, count(*) FROM okg.{table}"
                " WHERE source = %s GROUP BY run_id, op",
                (_SOURCE,),
            )
            for run_id, op, count in cur.fetchall():
                counts[(table, str(run_id), str(op))] = int(count)
    return counts


# ---------------------------------------------------------------------------
# No database: which rows the runner keeps for the log
# ---------------------------------------------------------------------------


def _rows_for(branch: str, *, write_fact_log: bool) -> tuple[list, list]:
    from datetime import datetime, timezone

    from okg.substrate.ingest.runner import FactLogRows, SourceRunner
    from okg.substrate.library.sources.base import EdgeFact, NodeFact

    runner = SourceRunner.__new__(SourceRunner)
    runner.source_name = _SOURCE
    runner.graph_branch = branch
    runner.write_fact_log = write_fact_log
    facts = [
        NodeFact("svc:held-1", "document", {"text": "kept"}, {"id": "1"}, {}),
        NodeFact("svc:held-2", "document", {}, {"id": "2"}, {}, op="R"),
        EdgeFact("svc:held-1", "svc:held-3", "references"),
        EdgeFact("svc:held-1", "svc:held-2", "references", op="R"),
    ]
    rows = runner._build_fact_log_rows(
        facts,
        run_id=str(uuid.uuid4()),
        observed_at=datetime.now(timezone.utc),
    )
    assert isinstance(rows, FactLogRows)
    return runner._fact_log_rows_to_persist(rows)


def test_held_run_keeps_only_its_retractions_for_the_log() -> None:
    node_rows, edge_rows = _rows_for(QUARANTINE_BRANCH, write_fact_log=False)
    assert [(row[0], row[9]) for row in node_rows] == [("svc:held-2", "R")]
    assert [(row[1], row[2], row[13]) for row in edge_rows] == [
        ("svc:held-1", "svc:held-2", "R"),
    ]


def test_unheld_run_logs_nothing_unless_the_fact_log_is_on() -> None:
    assert _rows_for("default", write_fact_log=False) == ([], [])
    node_rows, edge_rows = _rows_for("default", write_fact_log=True)
    assert [row[9] for row in node_rows] == ["I", "R"]
    assert [row[13] for row in edge_rows] == ["I", "R"]
    node_rows, edge_rows = _rows_for(QUARANTINE_BRANCH, write_fact_log=True)
    assert len(node_rows) == 2 and len(edge_rows) == 2


# ---------------------------------------------------------------------------
# PostgreSQL: the real runner and the real approval
# ---------------------------------------------------------------------------

_RUN1 = list(range(10))
_RUN1_EDGES = _chain(_RUN1)
# Run 2 drops nodes 0-4 (and every edge touching them) and, separately, the
# edge 6 -> 7 while both of its nodes stay.
_RUN2 = list(range(5, 15))
_RUN2_EDGES = _chain(_RUN2, skip=frozenset({6}))


@postgres
def test_approving_a_held_resync_deletes_removed_nodes_and_edges(
    tmp_path: Path,
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The #2999 reproduction: fails on dev, where default keeps all 15."""
    dep, emission = _deployment(tmp_path, monkeypatch, clean_runtime)
    _accept(_hold(clean_runtime, dep, emission, _RUN1, _RUN1_EDGES))
    assert _graph(clean_runtime, "default") == _expected(_RUN1, _RUN1_EDGES)

    held = _hold(clean_runtime, dep, emission, _RUN2, _RUN2_EDGES)
    assert _graph(clean_runtime, QUARANTINE_BRANCH) == _expected(
        _RUN2, _RUN2_EDGES,
    )
    # Held: default still shows run 1.
    assert _graph(clean_runtime, "default") == _expected(_RUN1, _RUN1_EDGES)
    fact_log = _fact_log(clean_runtime)

    decided = _accept(held)

    # On dev this is where it fails: default still has nodes 0-4 and the
    # six removed edges.
    assert _graph(clean_runtime, "default") == _expected(_RUN2, _RUN2_EDGES)
    assert _graph(clean_runtime, "default") == _graph(
        clean_runtime, QUARANTINE_BRANCH,
    )
    # The runner logged exactly run 2's retractions, under run 2's id, and
    # nothing else (no upserts: the approval reads those from the branch).
    removed_edges = sorted(set(_RUN1_EDGES) - set(_RUN2_EDGES))
    assert removed_edges == [(0, 1), (1, 2), (2, 3), (3, 4), (4, 5), (6, 7)]
    assert fact_log == {
        ("node_facts", held["run_id"], "R"): 5,
        ("edge_facts", held["run_id"], "R"): len(removed_edges),
    }
    emitted = decided["emitted"]
    assert emitted["retractions_applied"] == 5 + len(removed_edges), emitted


@postgres
def test_resync_that_removes_nothing_logs_and_retracts_nothing(
    tmp_path: Path,
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A re-sync with no removals behaves exactly as on dev."""
    dep, emission = _deployment(tmp_path, monkeypatch, clean_runtime)
    _accept(_hold(clean_runtime, dep, emission, _RUN1, _RUN1_EDGES))
    grown = list(range(12))
    held = _hold(clean_runtime, dep, emission, grown, _chain(grown))

    assert _fact_log(clean_runtime) == {}
    decided = _accept(held)

    assert decided["emitted"]["retractions_applied"] == 0
    assert _graph(clean_runtime, "default") == _expected(grown, _chain(grown))


@postgres
def test_later_run_readds_one_record_and_removes_another(
    tmp_path: Path,
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Run 2 removes 4; run 3 brings 4 back and removes 2. Order holds."""
    dep, emission = _deployment(tmp_path, monkeypatch, clean_runtime)
    run1 = [0, 1, 2, 3, 4]
    run2 = [0, 1, 2, 3]
    run3 = [0, 1, 3, 4]
    _accept(_hold(clean_runtime, dep, emission, run1, _chain(run1)))
    _accept(_hold(clean_runtime, dep, emission, run2, _chain(run2)))
    assert _graph(clean_runtime, "default") == _expected(run2, _chain(run2))

    held = _hold(clean_runtime, dep, emission, run3, _chain(run3))
    decided = _accept(held)

    # Run 3 retracts node 2 and the edges 1->2 and 2->3; node 4 and edge
    # 3->4 come back as upserts.
    assert decided["emitted"]["retractions_applied"] == 3, decided["emitted"]
    assert _graph(clean_runtime, "default") == _expected(run3, _chain(run3))


@postgres
def test_approval_after_failure_retries_and_a_second_accept_is_a_no_op(
    tmp_path: Path,
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from okg.substrate.graph_store import ChronosGraphStore

    dep, emission = _deployment(tmp_path, monkeypatch, clean_runtime)
    _accept(_hold(clean_runtime, dep, emission, _RUN1, _RUN1_EDGES))
    held = _hold(clean_runtime, dep, emission, _RUN2, _RUN2_EDGES)

    real_apply = ChronosGraphStore.apply_facts
    failures: list[int] = []

    def failing_apply(self, facts, *args, **kwargs):
        facts = list(facts)
        if kwargs.get("branch") == "default" and not failures:
            failures.append(len(facts))
            raise RuntimeError("injected approval failure")
        return real_apply(self, facts, *args, **kwargs)

    monkeypatch.setattr(ChronosGraphStore, "apply_facts", failing_apply)
    with pytest.raises(RuntimeError, match="injected approval failure"):
        _decide(held)
    assert failures, "the injected failure must fire"
    # Nothing reached default and the run is still waiting.
    assert _latest_envelope(clean_runtime)["status"] == "pending"
    assert _graph(clean_runtime, "default") == _expected(_RUN1, _RUN1_EDGES)

    retried = _accept(held)
    assert retried["emitted"]["retractions_applied"] == 11
    assert _graph(clean_runtime, "default") == _expected(_RUN2, _RUN2_EDGES)

    again = _decide(held)
    assert again.get("already_decided") is True, again
    assert _graph(clean_runtime, "default") == _expected(_RUN2, _RUN2_EDGES)


@postgres
def test_removal_keeps_a_node_another_source_still_declares(
    tmp_path: Path,
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Default moved on: another source declared node 2 after run 1 landed.

    The approval withdraws only the held source's own claim; the other
    source's node and attributes stay.
    """
    from okg.substrate.graph_store import ChronosGraphStore
    from okg.substrate.library.sources.base import NodeFact

    dep, emission = _deployment(tmp_path, monkeypatch, clean_runtime)
    run1 = [0, 1, 2, 3]
    _accept(_hold(clean_runtime, dep, emission, run1, _chain(run1)))
    store = ChronosGraphStore.connect(clean_runtime, ensure_schema=False)
    try:
        store.apply_facts(
            [NodeFact(
                "svc:held-2",
                "document",
                {"text": "edited by another source"},
                {"id": "other-2"},
                {"v": 7},
            )],
            branch="default",
            source=_OTHER_SOURCE,
        )
    finally:
        store.close()

    run2 = [0, 1, 3]
    _accept(_hold(clean_runtime, dep, emission, run2, _chain(run2)))

    store = ChronosGraphStore.connect(clean_runtime, ensure_schema=False)
    try:
        rows = store.checkout("default").query(
            "SELECT node_id, source, attrs FROM okg.graph_nodes"
            " WHERE node_id = 'svc:held-2'",
        )
    finally:
        store.close()
    assert len(rows) == 1, rows
    assert rows[0]["attrs"].get("text") == "edited by another source", rows


@postgres
def test_retractions_logged_while_the_first_run_is_still_pending(
    tmp_path: Path,
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Run 2 removes records run 1 added while run 1 still waits.

    The source declares an output signature, so finalize checks run 2's
    logged retractions against it instead of returning `compat`. The removed
    edges' endpoints are on neither default nor run 2's payload; run 2 must
    still finalize pending, not rejected.
    """
    dep, emission = _deployment(
        tmp_path, monkeypatch, clean_runtime, declare_signature=True,
    )
    first = _hold(clean_runtime, dep, emission, _RUN1, _RUN1_EDGES)
    second = _hold(clean_runtime, dep, emission, _RUN2, _RUN2_EDGES)
    assert second["admission_id"] != first["admission_id"]
    assert _graph(clean_runtime, "default") == ([], [])
    assert _fact_log(clean_runtime) == {
        ("node_facts", second["run_id"], "R"): 5,
        ("edge_facts", second["run_id"], "R"): 6,
    }
    policy = second["verdict"].get("producer_policy") or {}
    assert policy.get("status") != "compat", second["verdict"]
    assert not policy.get("failed_clause_ids"), second["verdict"]


@postgres
def test_two_waiting_runs_accepted_in_order_leave_the_second_footprint(
    tmp_path: Path,
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    dep, emission = _deployment(tmp_path, monkeypatch, clean_runtime)
    first = _hold(clean_runtime, dep, emission, _RUN1, _RUN1_EDGES)
    second = _hold(clean_runtime, dep, emission, _RUN2, _RUN2_EDGES)

    _accept(first)
    _accept(second)

    assert _graph(clean_runtime, "default") == _expected(_RUN2, _RUN2_EDGES)


@postgres
def test_unheld_source_writes_no_fact_log(
    tmp_path: Path,
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A fast-track source's retractions go straight to default, unlogged."""
    dep, emission = _deployment(
        tmp_path, monkeypatch, clean_runtime, admission_mode="fast_track",
    )
    first = _sync(clean_runtime, dep, emission, _RUN1, _RUN1_EDGES)
    assert first["status"] == "accepted", first
    second = _sync(clean_runtime, dep, emission, _RUN2, _RUN2_EDGES)
    assert second["status"] == "accepted", second

    assert _graph(clean_runtime, "default") == _expected(_RUN2, _RUN2_EDGES)
    assert _fact_log(clean_runtime) == {}


# ---------------------------------------------------------------------------
# Accepting held runs out of order (issue #2999 review, round 2)
# ---------------------------------------------------------------------------

_FULL = [0, 1, 2, 3, 4]
_SHORT = [0, 1, 2, 3]


def _reject(envelope: dict[str, Any]) -> dict[str, Any]:
    from okg.substrate.runtime import admission_workflows

    decided = admission_workflows._run_decide(
        deployment="svc",
        admission_id=envelope["admission_id"],
        decision="reject",
        actor="operator",
        reason="issue-2999",
        verdict=None,
    )
    assert decided["status"] == "rejected", decided
    return decided


@postgres
def test_older_removal_accepted_after_a_newer_readd_keeps_the_record(
    tmp_path: Path,
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Run 2 removes node 4, run 3 adds it back; run 3 is accepted first.

    Accepting run 2 afterwards must not delete node 4 or the edge 3->4: the
    source still has both, and the quarantine branch (which the runner
    compares every new run with) still shows them, so nothing would ever
    put them back. Run 4, which sees the same data, changes nothing.
    """
    dep, emission = _deployment(tmp_path, monkeypatch, clean_runtime)
    _accept(_hold(clean_runtime, dep, emission, _FULL, _chain(_FULL)))
    removing = _hold(clean_runtime, dep, emission, _SHORT, _chain(_SHORT))
    readding = _hold(clean_runtime, dep, emission, _FULL, _chain(_FULL))
    # The removal was logged; the branch shows the source's latest state.
    assert _fact_log(clean_runtime) == {
        ("node_facts", removing["run_id"], "R"): 1,
        ("edge_facts", removing["run_id"], "R"): 1,
    }
    assert _graph(clean_runtime, QUARANTINE_BRANCH) == _expected(
        _FULL, _chain(_FULL),
    )

    _accept(readding)
    decided = _accept(removing)

    assert decided["emitted"]["retractions_applied"] == 0, decided["emitted"]
    assert _graph(clean_runtime, "default") == _expected(_FULL, _chain(_FULL))

    later = _hold(clean_runtime, dep, emission, _FULL, _chain(_FULL))
    _accept(later)
    assert _graph(clean_runtime, "default") == _expected(_FULL, _chain(_FULL))
    assert _graph(clean_runtime, "default") == _graph(
        clean_runtime, QUARANTINE_BRANCH,
    )


@postgres
def test_older_removal_applies_when_the_newer_readd_was_rejected(
    tmp_path: Path,
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Rejecting run 3 takes its rows off the branch, so run 2's removal of
    node 4 applies. The next run of the source, which still has node 4,
    writes it again and its approval restores it: nothing is lost for good.
    """
    dep, emission = _deployment(tmp_path, monkeypatch, clean_runtime)
    _accept(_hold(clean_runtime, dep, emission, _FULL, _chain(_FULL)))
    removing = _hold(clean_runtime, dep, emission, _SHORT, _chain(_SHORT))
    readding = _hold(clean_runtime, dep, emission, _FULL, _chain(_FULL))

    _reject(readding)
    assert _graph(clean_runtime, QUARANTINE_BRANCH) == _expected(
        _SHORT, _chain(_SHORT),
    )
    decided = _accept(removing)

    assert decided["emitted"]["retractions_applied"] == 2, decided["emitted"]
    assert _graph(clean_runtime, "default") == _expected(_SHORT, _chain(_SHORT))

    _accept(_hold(clean_runtime, dep, emission, _FULL, _chain(_FULL)))
    assert _graph(clean_runtime, "default") == _expected(_FULL, _chain(_FULL))


@postgres
def test_older_removal_accepted_before_a_pending_readd_is_rejected(
    tmp_path: Path,
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Review round 3 (c1): accept run 2, then reject run 3.

    Run 3's re-added node 4 is live on the branch but still pending when
    run 2 is accepted, so run 2's removal applies. Rejecting run 3 then
    takes node 4 off the branch, and run 4 (which again lacks node 4) has
    nothing to remove: the published graph must already be without it.
    """
    dep, emission = _deployment(tmp_path, monkeypatch, clean_runtime)
    _accept(_hold(clean_runtime, dep, emission, _FULL, _chain(_FULL)))
    removing = _hold(clean_runtime, dep, emission, _SHORT, _chain(_SHORT))
    readding = _hold(clean_runtime, dep, emission, _FULL, _chain(_FULL))

    decided = _accept(removing)
    assert decided["emitted"]["retractions_applied"] == 2, decided["emitted"]
    assert _graph(clean_runtime, "default") == _expected(_SHORT, _chain(_SHORT))

    _reject(readding)
    _accept(_hold(clean_runtime, dep, emission, _SHORT, _chain(_SHORT)))

    assert _graph(clean_runtime, "default") == _expected(_SHORT, _chain(_SHORT))
    assert _graph(clean_runtime, "default") == _graph(
        clean_runtime, QUARANTINE_BRANCH,
    )


@postgres
def test_pending_readd_accepted_after_the_older_removal_restores_it(
    tmp_path: Path,
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Accept run 2 (removal applies), then accept run 3: node 4 is back."""
    dep, emission = _deployment(tmp_path, monkeypatch, clean_runtime)
    _accept(_hold(clean_runtime, dep, emission, _FULL, _chain(_FULL)))
    removing = _hold(clean_runtime, dep, emission, _SHORT, _chain(_SHORT))
    readding = _hold(clean_runtime, dep, emission, _FULL, _chain(_FULL))

    _accept(removing)
    _accept(readding)

    assert _graph(clean_runtime, "default") == _expected(_FULL, _chain(_FULL))
    assert _graph(clean_runtime, "default") == _graph(
        clean_runtime, QUARANTINE_BRANCH,
    )


@postgres
def test_removal_applies_over_a_live_row_whose_writer_is_unknown(
    tmp_path: Path,
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """A live row whose run has no admission envelope does not hold the
    removal back; the approval says so in a warning."""
    import logging

    from okg.substrate.graph_store import ChronosGraphStore
    from okg.substrate.library.sources.base import NodeFact

    dep, emission = _deployment(tmp_path, monkeypatch, clean_runtime)
    _accept(_hold(clean_runtime, dep, emission, _FULL, _chain(_FULL)))
    removing = _hold(clean_runtime, dep, emission, _SHORT, _chain(_SHORT))
    store = ChronosGraphStore.connect(clean_runtime, ensure_schema=False)
    try:
        store.apply_facts(
            [NodeFact(
                "svc:held-4",
                "document",
                {"text": "written outside any admitted run"},
                {"id": "held-4"},
                {"v": 2},
            )],
            branch=QUARANTINE_BRANCH,
            source=_SOURCE,
            run_id=str(uuid.uuid4()),
        )
    finally:
        store.close()

    with caplog.at_level(
        logging.WARNING, logger="okg.substrate.runtime.admission_workflows",
    ):
        decided = _accept(removing)

    assert decided["emitted"]["retractions_applied"] == 2, decided["emitted"]
    assert "svc:held-4" not in _graph(clean_runtime, "default")[0]
    assert any(
        "no admission envelope" in record.getMessage()
        for record in caplog.records
    ), [record.getMessage() for record in caplog.records]


@postgres
def test_removal_applies_when_only_another_source_has_the_node_held(
    tmp_path: Path,
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Another source's live row on the quarantine branch does not hold back
    this source's removal: only this source's own rows are read, so the
    other source's row is not reported as an unidentified writer either."""
    import logging

    from okg.substrate.graph_store import ChronosGraphStore
    from okg.substrate.library.sources.base import NodeFact

    dep, emission = _deployment(tmp_path, monkeypatch, clean_runtime)
    _accept(_hold(clean_runtime, dep, emission, _FULL, _chain(_FULL)))
    removing = _hold(clean_runtime, dep, emission, _SHORT, _chain(_SHORT))
    # After the removal, another (held, never accepted) source declares node 4
    # on the quarantine branch.
    store = ChronosGraphStore.connect(clean_runtime, ensure_schema=False)
    try:
        store.apply_facts(
            [NodeFact(
                "svc:held-4",
                "document",
                {"text": "held by another source"},
                {"id": "other-4"},
                {"v": 1},
            )],
            branch=QUARANTINE_BRANCH,
            source=_OTHER_SOURCE,
        )
        held = store.checkout(QUARANTINE_BRANCH).query(
            "SELECT source FROM okg.graph_nodes WHERE node_id = 'svc:held-4'",
        )
    finally:
        store.close()
    assert [row["source"] for row in held] == [_OTHER_SOURCE]

    with caplog.at_level(
        logging.WARNING, logger="okg.substrate.runtime.admission_workflows",
    ):
        decided = _accept(removing)

    assert decided["emitted"]["retractions_applied"] == 2, decided["emitted"]
    assert _graph(clean_runtime, "default") == _expected(_SHORT, _chain(_SHORT))
    assert not [
        record.getMessage()
        for record in caplog.records
        if "no admission envelope" in record.getMessage()
    ]


@postgres
def test_accepting_a_run_older_than_the_fact_log_retention_is_refused(
    tmp_path: Path,
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Past the fact-log retention its removals may be gone: refuse loudly."""
    from okg.substrate.errors import SubstrateError

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        retention = conn.execute(
            """
            SELECT min((config ->> 'drop_after')::interval)
              FROM timescaledb_information.jobs
             WHERE proc_name = 'policy_retention'
               AND hypertable_schema = 'okg'
               AND hypertable_name IN ('node_facts', 'edge_facts')
            """
        ).fetchone()[0]
    assert retention is not None, "the test database must carry the policy"

    dep, emission = _deployment(tmp_path, monkeypatch, clean_runtime)
    _accept(_hold(clean_runtime, dep, emission, _FULL, _chain(_FULL)))
    held = _hold(clean_runtime, dep, emission, _SHORT, _chain(_SHORT))
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        conn.execute(
            "UPDATE okg.admission_envelopes"
            " SET created_at = now() - %s::interval - interval '1 day'"
            " WHERE admission_id = %s::uuid",
            (retention, held["admission_id"]),
        )

    with pytest.raises(SubstrateError) as refused:
        _decide(held)

    assert refused.value.data["reason"] == (
        "held_run_retractions_may_have_expired"
    )
    assert "re-sync" in (refused.value.fix_hint or "")
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        status = conn.execute(
            "SELECT status FROM okg.admission_envelopes"
            " WHERE admission_id = %s::uuid",
            (held["admission_id"],),
        ).fetchone()[0]
    assert status == "pending"
    assert _graph(clean_runtime, "default") == _expected(_FULL, _chain(_FULL))

    # A held run inside the retention window is accepted as usual.
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        conn.execute(
            "UPDATE okg.admission_envelopes"
            " SET created_at = now() - %s::interval + interval '1 day'"
            " WHERE admission_id = %s::uuid",
            (retention, held["admission_id"]),
        )
    _accept(held)
    assert _graph(clean_runtime, "default") == _expected(_SHORT, _chain(_SHORT))


# ---------------------------------------------------------------------------
# Review round 4: the removal's evidence outlives a later change, decisions
# on one source run one at a time, and any accepted writer counts
# ---------------------------------------------------------------------------


def _envelope(dsn: str, admission_id: str) -> dict[str, Any]:
    with psycopg.connect(dsn, autocommit=True) as conn:
        status, verdict = conn.execute(
            "SELECT status, verdict FROM okg.admission_envelopes"
            " WHERE admission_id = %s::uuid",
            (admission_id,),
        ).fetchone()
    return {"status": status, "verdict": verdict or {}}


def _quarantine_writer(dsn: str, node_id: str) -> str | None:
    from okg.substrate.graph_store import ChronosGraphStore

    store = ChronosGraphStore.connect(dsn, ensure_schema=False)
    try:
        rows = store.checkout(QUARANTINE_BRANCH).query(
            "SELECT run_id FROM okg.graph_nodes"
            " WHERE node_id = :node_id AND source = :source",
            {"node_id": node_id, "source": _SOURCE},
        )
    finally:
        store.close()
    return str(rows[0]["run_id"]) if rows else None


@postgres
def test_older_removal_keeps_a_record_an_accepted_readd_wrote_before_a_later_change(
    tmp_path: Path,
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Review round 4, finding 1.

    Run 2 removes node 4; run 3 adds it back and is accepted; run 4 changes
    node 4's text and waits, so the quarantine row now carries run 4's id.
    Accepting run 2 must still keep node 4: accepted run 3 wrote it after
    run 2 removed it. Rejecting run 4 afterwards leaves node 4 published.
    """
    dep, emission = _deployment(tmp_path, monkeypatch, clean_runtime)
    _accept(_hold(clean_runtime, dep, emission, _FULL, _chain(_FULL)))
    removing = _hold(clean_runtime, dep, emission, _SHORT, _chain(_SHORT))
    readding = _hold(clean_runtime, dep, emission, _FULL, _chain(_FULL))
    _accept(readding)
    changing = _hold(
        clean_runtime, dep, emission, _FULL, _chain(_FULL),
        texts={4: "held document 4, changed"},
    )
    assert _quarantine_writer(clean_runtime, "svc:held-4") == changing["run_id"]

    decided = _accept(removing)

    assert decided["emitted"]["retractions_applied"] == 0, decided["emitted"]
    assert _graph(clean_runtime, "default") == _expected(_FULL, _chain(_FULL))
    _reject(changing)
    assert _graph(clean_runtime, "default") == _expected(_FULL, _chain(_FULL))
    # The evidence: run 3's accepted verdict names the removals it undid.
    assert _envelope(clean_runtime, readding["admission_id"])["verdict"].get(
        "superseded_removals"
    ) == {
        removing["run_id"]: {
            "nodes": ["svc:held-4"],
            "edges": [["svc:held-3", "references", "svc:held-4"]],
        },
    }


@postgres
def test_concurrent_approvals_of_one_source_run_one_at_a_time(
    tmp_path: Path,
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Review round 4, finding 2.

    Accepting run 2 (which removes node 4) pauses after it has read run 3's
    status, and run 3 (which adds node 4 back) is accepted meanwhile. Without
    a per-source lock run 3 commits first and run 2 then deletes node 4 on
    stale reads. With the lock, run 3's decision waits on it (seen in
    pg_locks) until run 2 commits, then puts node 4 back.
    """
    import threading
    import time

    from okg.substrate.runtime import admission_workflows

    dep, emission = _deployment(tmp_path, monkeypatch, clean_runtime)
    _accept(_hold(clean_runtime, dep, emission, _FULL, _chain(_FULL)))
    removing = _hold(clean_runtime, dep, emission, _SHORT, _chain(_SHORT))
    readding = _hold(clean_runtime, dep, emission, _FULL, _chain(_FULL))

    original = admission_workflows._retractions_still_standing
    readd_may_start = threading.Event()
    readd_done = threading.Event()
    seen: dict[str, Any] = {"readd_waited_on_lock": False}
    errors: list[BaseException] = []

    def readd_waits_on_an_advisory_lock() -> bool:
        with psycopg.connect(clean_runtime, autocommit=True) as conn:
            return bool(conn.execute(
                "SELECT count(*) FROM pg_locks"
                " WHERE locktype = 'advisory' AND NOT granted"
                "   AND database = (SELECT oid FROM pg_database"
                "                    WHERE datname = current_database())",
            ).fetchone()[0])

    def paused(graph_conn: Any, **kwargs: Any) -> list[Any]:
        kept = original(graph_conn, **kwargs)
        if threading.current_thread().name == "accept-removing":
            readd_may_start.set()
            deadline = time.monotonic() + 120
            while not readd_done.is_set():
                if readd_waits_on_an_advisory_lock():
                    seen["readd_waited_on_lock"] = True
                    break
                if time.monotonic() > deadline:
                    raise AssertionError(
                        "run 3's decision neither finished nor waited",
                    )
                time.sleep(0.05)
        return kept

    monkeypatch.setattr(admission_workflows, "_retractions_still_standing", paused)

    def accept_removing() -> None:
        try:
            _accept(removing)
        except BaseException as exc:  # noqa: BLE001 - reported below
            errors.append(exc)
            readd_may_start.set()

    def accept_readding() -> None:
        try:
            assert readd_may_start.wait(120)
            _accept(readding)
        except BaseException as exc:  # noqa: BLE001 - reported below
            errors.append(exc)
        finally:
            readd_done.set()

    threads = [
        threading.Thread(target=accept_removing, name="accept-removing"),
        threading.Thread(target=accept_readding, name="accept-readding"),
    ]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(300)
    assert not any(thread.is_alive() for thread in threads)
    assert not errors, errors

    assert _envelope(clean_runtime, removing["admission_id"])["status"] == (
        "accepted"
    )
    assert _envelope(clean_runtime, readding["admission_id"])["status"] == (
        "accepted"
    )
    assert _graph(clean_runtime, "default") == _expected(_FULL, _chain(_FULL))
    assert seen["readd_waited_on_lock"] is True


@postgres
@pytest.mark.parametrize("writer_status", ["accepted", "blocked"])
def test_removal_is_held_back_by_an_accepted_write_of_another_intent(
    tmp_path: Path,
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
    writer_status: str,
) -> None:
    """Review round 4, note 3: a live row written by an accepted envelope of
    another intent under the same source name is on `default` as well, so it
    keeps the record and is not reported as an unidentified writer. The edge
    3->4 is written that way too, so a live edge counts like a live node.

    Round 5: the same holds once a blocked publish has moved that accepted
    envelope to ``blocked`` (it keeps its ``accepted`` event). No marker is
    involved here, so this is the case where only the live-writer check can
    keep the record."""
    import logging

    from okg.substrate.graph_store import ChronosGraphStore
    from okg.substrate.library.sources.base import EdgeFact, NodeFact

    dep, emission = _deployment(tmp_path, monkeypatch, clean_runtime)
    _accept(_hold(clean_runtime, dep, emission, _FULL, _chain(_FULL)))
    removing = _hold(clean_runtime, dep, emission, _SHORT, _chain(_SHORT))
    writer_run = str(uuid.uuid4())
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        conn.execute(
            """
            INSERT INTO okg.admission_envelopes (
              deployment_name, producer_id, producer_kind, intent,
              admission_mode, status, source, run_id, idempotency_key
            ) VALUES (
              'svc', 'operator', 'human', 'operator_write',
              'direct', %s, %s, %s::uuid, %s
            )
            RETURNING admission_id::text
            """,
            (writer_status, _SOURCE, writer_run, f"operator-write:{writer_run}"),
        )
        conn.execute(
            "INSERT INTO okg.admission_events (admission_id, event_type, actor)"
            " SELECT admission_id, unnest(%s::text[]), 'operator'"
            "   FROM okg.admission_envelopes WHERE run_id = %s::uuid",
            (
                ["accepted"] if writer_status == "accepted"
                else ["accepted", "blocked"],
                writer_run,
            ),
        )
    store = ChronosGraphStore.connect(clean_runtime, ensure_schema=False)
    try:
        store.apply_facts(
            [NodeFact(
                "svc:held-4",
                "document",
                {"text": "held document 4"},
                {"id": "held-4"},
                {"v": 1},
            ), EdgeFact(
                src="svc:held-3",
                dst="svc:held-4",
                edge_type="references",
                source_record_id={"id": "edge-3-4"},
                source_revision={"v": 1},
            )],
            branch=QUARANTINE_BRANCH,
            source=_SOURCE,
            run_id=writer_run,
        )
    finally:
        store.close()

    with caplog.at_level(
        logging.WARNING, logger="okg.substrate.runtime.admission_workflows",
    ):
        decided = _accept(removing)

    assert decided["emitted"]["retractions_applied"] == 0, decided["emitted"]
    assert _graph(clean_runtime, "default") == _expected(_FULL, _chain(_FULL))
    assert not [
        record.getMessage()
        for record in caplog.records
        if "envelope for their run_id" in record.getMessage()
    ]


# ---------------------------------------------------------------------------
# Review round 5: a blocked publish after an acceptance
# ---------------------------------------------------------------------------


def _block(dsn: str, *, recovery_ids: list[str] | None = None) -> list[str]:
    """Run the blocked-publish UPDATE the generation feedback runs.

    Without ``recovery_ids``: ``admission._mark_generation_blocked``, which
    moves the deployment's accepted, unbound envelopes to ``blocked``. With
    them: ``admission._mark_recovery_generation_blocked`` over exactly those
    ids, the one UPDATE that also takes ``pending`` envelopes. Each moved
    envelope gets its ``blocked`` event, as ``apply_generation_feedback``
    records it.
    """
    from okg.substrate import admission

    generation = {
        "deployment_name": "svc",
        "branch_name": "default",
        "generation_id": f"blocked-{uuid.uuid4()}",
        "blocked_reason": {"reason": "issue-2999 round 5"},
        "invariant_summary": {},
    }
    with psycopg.connect(dsn) as conn:
        with conn.transaction():
            if recovery_ids is None:
                changed = admission._mark_generation_blocked(
                    conn, generation=generation, workflow_ref=None,
                )
            else:
                changed = admission._mark_recovery_generation_blocked(
                    conn,
                    generation=generation,
                    predecessor_generation_id="none",
                    expected_admission_ids=recovery_ids,
                    workflow_ref=None,
                )
            for row in changed:
                admission.record_event(
                    conn,
                    admission_id=row["admission_id"],
                    event_type="blocked",
                    actor="publisher",
                )
    return sorted(row["admission_id"] for row in changed)


@postgres
def test_older_removal_keeps_a_record_whose_accepted_readd_was_then_blocked(
    tmp_path: Path,
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Review round 5.

    Run 2 removes node 4 and waits. Run 3 adds it back (and adds node 5, so
    its approval writes a row to default and it gets the record refs a
    blocked publish selects) and is accepted, and the publish of its
    generation is blocked, so run 3's envelope is now ``blocked`` while its
    rows stay on default. Run 4 changes node 4 and waits. Accepting run 2
    must keep node 4 and edge 3->4, and rejecting run 4 afterwards leaves
    them published.
    """
    grown = [*_FULL, 5]
    dep, emission = _deployment(tmp_path, monkeypatch, clean_runtime)
    _accept(_hold(clean_runtime, dep, emission, _FULL, _chain(_FULL)))
    removing = _hold(clean_runtime, dep, emission, _SHORT, _chain(_SHORT))
    readding = _hold(clean_runtime, dep, emission, grown, _chain(grown))
    _accept(readding)
    assert readding["admission_id"] in _block(clean_runtime)
    assert _envelope(clean_runtime, readding["admission_id"])["status"] == (
        "blocked"
    )
    assert _envelope(clean_runtime, removing["admission_id"])["status"] == (
        "pending"
    )
    changing = _hold(
        clean_runtime, dep, emission, grown, _chain(grown),
        texts={4: "held document 4, changed"},
    )
    assert _quarantine_writer(clean_runtime, "svc:held-4") == changing["run_id"]

    decided = _accept(removing)

    assert decided["emitted"]["retractions_applied"] == 0, decided["emitted"]
    assert _graph(clean_runtime, "default") == _expected(grown, _chain(grown))
    _reject(changing)
    assert _graph(clean_runtime, "default") == _expected(grown, _chain(grown))


@postgres
def test_a_held_run_blocked_without_an_acceptance_does_not_keep_the_record(
    tmp_path: Path,
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Review round 5: ``blocked`` counts only after an acceptance.

    Run 3 re-adds node 4 and is still pending when the recovery UPDATE (the
    one that also moves ``pending`` envelopes) blocks it. Its rows are only on
    the quarantine branch, so accepting run 2 still removes node 4.
    """
    dep, emission = _deployment(tmp_path, monkeypatch, clean_runtime)
    _accept(_hold(clean_runtime, dep, emission, _FULL, _chain(_FULL)))
    removing = _hold(clean_runtime, dep, emission, _SHORT, _chain(_SHORT))
    readding = _hold(clean_runtime, dep, emission, _FULL, _chain(_FULL))
    assert _block(
        clean_runtime, recovery_ids=[readding["admission_id"]],
    ) == [readding["admission_id"]]
    assert _envelope(clean_runtime, readding["admission_id"])["status"] == (
        "blocked"
    )

    decided = _accept(removing)

    assert decided["emitted"]["retractions_applied"] == 2, decided["emitted"]
    assert _graph(clean_runtime, "default") == _expected(_SHORT, _chain(_SHORT))
