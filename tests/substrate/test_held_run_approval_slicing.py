"""Issue #2994: approving a held source run slices it into bounded chunks.

A review-mode (held) source run waits on the quarantine branch until an
operator accepts it; the accept copies every held upsert and retraction onto
``default``. That copy used to be ONE FactChunk, and ``FactChunk`` refuses a
chunk over ``MAX_FACTS_PER_CHUNK``, so a held run above the limit could never
be approved. The approval now applies consecutive slices of at most the limit,
all inside the single transaction ``_run_decide`` already opens: a failure on
any slice rolls back every earlier slice, leaving the envelope pending and
retryable, and nothing reaches ``default`` for a later publish to show.

Every test drives the real operator path: a real ``run_ingest`` of a
review-mode source, then ``_run_decide(decision="accept")``, the call behind
``okg admission decide``. Most tests lower the limit (and the runner's flush
boundary with it) so a few dozen facts cross several slices; one test runs at
the real limit of 10,000.

Retractions. The runner deletes a retracted record from the quarantine branch,
but the approval rebuilds the run's retractions from the ``okg.node_facts``
op='R' log (``_load_run_retraction_facts``), and the Chronos runner no longer
writes that log. So on dev a real held re-sync hands the approval no
retractions at all: its deletions never reach ``default``. That gap is separate
from #2994 and not fixed here. The tests write the op='R' log rows the loader
reads, for exactly the records the runner retracted, as
``test_approve_propagates_retractions_to_default`` does, so the approval's
chunks carry retractions the way the approval code expects them.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Any

import psycopg
import pytest
import yaml

from okg.substrate.admission import QUARANTINE_BRANCH
from tests.pytest.catalog_fixture_builders import seed_catalog
from tests.substrate.test_ingest_service import (
    _claim_layered_fixture_catalog,
    _fixture_source_privacy_policy,
    _source,
    _write_layered_fixture_deployment,
)

pytestmark = pytest.mark.postgres

_SOURCE = "svc.held"

_HELD_ADAPTER = '''
import json
from pathlib import Path

from okg.substrate.library.sources.base import NodeFact, SourceRun


class HeldRunSource:
    name = "held_run"
    profile = "discovery_crawl"
    last_run_stats: dict = {}

    def __init__(self, ids_path, **kwargs):
        self.ids_path = Path(ids_path)

    def run(self, run_id, *, mode="scope_complete", cursor=None,
            sync_scope=None, **kwargs):
        ids = json.loads(self.ids_path.read_text())

        def facts():
            for index in ids:
                yield NodeFact(
                    node_id="svc:held-%d" % index,
                    subtype="document",
                    attrs={"text": "held document %d" % index},
                    source_record_id={"id": "held-%d" % index},
                    source_revision={"v": 1},
                )

        # No record_set: the stream is the completed-scope footprint, so an
        # id missing from the next run is retracted.
        return SourceRun(
            facts=facts(),
            completed_scope=True,
            next_cursor={"ids": len(ids)},
            run_mode=mode,
        )
'''


def _held_deployment(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    dsn: str,
    *,
    limit: int | None,
) -> tuple[Path, Path]:
    """A deployment whose one source is held for review.

    ``limit`` lowers MAX_FACTS_PER_CHUNK and the runner's flush boundary
    together, as the #2988 tests do; ``None`` keeps the real 10,000.
    """
    from okg.substrate.runtime import admission_workflows

    module_dir = tmp_path / "adapters"
    module_dir.mkdir()
    (module_dir / "held_run_adapter.py").write_text(
        _HELD_ADAPTER, encoding="utf-8",
    )
    monkeypatch.syspath_prepend(str(module_dir))
    sys.modules.pop("held_run_adapter", None)
    with psycopg.connect(dsn, autocommit=True) as conn:
        seed_catalog(conn, subtypes=["document"])
    dep = _write_layered_fixture_deployment(tmp_path)
    # Outside the deployment repository: the tests rewrite it between runs.
    ids_path = tmp_path / "held_ids.json"
    registry_path = dep / "source_registry.yaml"
    data = yaml.safe_load(registry_path.read_text())
    entry = {
        **_source(_SOURCE),
        "module": "held_run_adapter",
        "class": "HeldRunSource",
        "params": {"ids_path": str(ids_path)},
        "source_policy": _fixture_source_privacy_policy(),
    }
    entry["admission_policy"]["admission_mode"] = "review"
    data["sources"] = {"held": entry}
    registry_path.write_text(yaml.safe_dump(data))
    _claim_layered_fixture_catalog(dsn, dep)
    monkeypatch.setattr(admission_workflows, "_resolve_dsn", lambda _name: dsn)
    if limit is not None:
        import okg.substrate.fact_batch as fact_batch_module
        import okg.substrate.ingest.runner as runner_module

        monkeypatch.setattr(runner_module, "COPY_CHUNK_ROWS", limit)
        monkeypatch.setattr(fact_batch_module, "MAX_FACTS_PER_CHUNK", limit)
    return dep, ids_path


def _hold_run(dsn: str, dep: Path, ids_path: Path, ids: list[int]) -> dict[str, Any]:
    """Ingest ``ids`` in review mode; return the new pending envelope."""
    from okg.substrate.ingest import IngestOptions, run_ingest

    ids_path.write_text(json.dumps(ids))
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
    envelope = _latest_envelope(dsn)
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
        cur.execute(
            "SELECT event_type FROM okg.admission_events"
            " WHERE admission_id = %s::uuid ORDER BY event_id",
            (admission_id,),
        )
        events = [row[0] for row in cur.fetchall()]
    return {
        "admission_id": admission_id,
        "status": status,
        "run_id": run_id,
        "verdict": verdict or {},
        "events": events,
    }


def _log_retractions(dsn: str, envelope: dict[str, Any], ids: list[int]) -> None:
    """Write the op='R' fact-log rows the approval reads (see module note)."""
    with psycopg.connect(dsn, autocommit=True) as conn, conn.cursor() as cur:
        for index in ids:
            cur.execute(
                """
                INSERT INTO okg.node_facts
                  (node_id, subtype, attrs, source, source_record_id,
                   source_revision, observed_at, dedupe_key, run_id, op)
                VALUES
                  (%s, 'document', '{}'::jsonb, %s, %s::jsonb,
                   '{"v": 1}'::jsonb, now(), %s, %s::uuid, 'R')
                """,
                (
                    f"svc:held-{index}",
                    _SOURCE,
                    json.dumps({"id": f"held-{index}"}),
                    f"retract:{envelope['run_id']}:{index}",
                    envelope["run_id"],
                ),
            )


def _decide(envelope: dict[str, Any]) -> dict[str, Any]:
    from okg.substrate.runtime import admission_workflows

    return admission_workflows._run_decide(
        deployment="svc",
        admission_id=envelope["admission_id"],
        decision="accept",
        actor="operator",
        reason="issue-2994",
        verdict=None,
    )


def _node_ids(dsn: str, branch: str) -> list[str]:
    from okg.substrate.graph_store import ChronosGraphStore

    store = ChronosGraphStore.connect(dsn, ensure_schema=False)
    try:
        rows = store.checkout(branch).query(
            "SELECT node_id FROM okg.graph_nodes WHERE source = :source"
            " ORDER BY node_id",
            {"source": _SOURCE},
        )
    finally:
        store.close()
    return [str(row["node_id"]) for row in rows]


def _ids(indexes: range | list[int]) -> list[str]:
    return sorted(f"svc:held-{index}" for index in indexes)


def _promotion_receipts(dsn: str, admission_id: str) -> list[tuple[int, str]]:
    """(chunk_ordinal, state) of the approval's receipts on ``default``."""
    with psycopg.connect(dsn, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT chunk_ordinal, state FROM okg.fact_batch_receipts"
            " WHERE admission_id = %s::uuid AND target_branch = 'default'"
            " ORDER BY chunk_ordinal",
            (admission_id,),
        )
        return [(int(row[0]), str(row[1])) for row in cur.fetchall()]


def _pending_authority_rows(dsn: str, admission_id: str) -> int:
    with psycopg.connect(dsn, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT count(*) FROM okg.source_record_authority_pending_admissions"
            " WHERE admission_id = %s::uuid",
            (admission_id,),
        )
        return int(cur.fetchone()[0])


def _spy_chunks(
    monkeypatch: pytest.MonkeyPatch,
    *,
    fail_at: int | None = None,
) -> list[tuple[int, int, int]]:
    """Record (ordinal, upserts, retractions) of every chunk applied.

    ``fail_at`` raises before applying the chunk with that ordinal, after
    every earlier chunk was applied.
    """
    from okg.substrate.fact_application import FactBatchApplication

    seen: list[tuple[int, int, int]] = []
    real = FactBatchApplication.apply_chunk

    def spy(self, chunk):
        retractions = sum(1 for fact in chunk.facts if fact.op == "R")
        seen.append((chunk.ordinal, len(chunk.facts) - retractions, retractions))
        if chunk.ordinal == fail_at:
            raise RuntimeError(f"injected failure on slice {fail_at + 1}")
        return real(self, chunk)

    monkeypatch.setattr(FactBatchApplication, "apply_chunk", spy)
    return seen


# Lowered-limit scenario. Run 1 holds ids 0-9 and is approved (one chunk).
# Run 2 streams ids 5-49: ids 0-4 are retracted and ids 10-49 are new, so the
# approval carries 40 upserts and 5 retractions: slices of 20, 20 and 5. The
# retractions stay below the limit so the runner's own tail flush (issue #2988,
# a separate fix) is not what this test exercises.
_LIMIT = 20
_RUN1 = list(range(10))
_RUN2 = list(range(5, 50))


def _approve_run1_then_hold_run2(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    dsn: str,
) -> tuple[Path, Path, dict[str, Any]]:
    dep, ids_path = _held_deployment(tmp_path, monkeypatch, dsn, limit=_LIMIT)
    first = _hold_run(dsn, dep, ids_path, _RUN1)
    assert _decide(first)["status"] == "accepted"
    assert _node_ids(dsn, "default") == _ids(_RUN1)
    held = _hold_run(dsn, dep, ids_path, _RUN2)
    assert _node_ids(dsn, QUARANTINE_BRANCH) == _ids(_RUN2)
    _log_retractions(dsn, held, sorted(set(_RUN1) - set(_RUN2)))
    # Run 2 is held: `default` still shows run 1 only.
    assert _node_ids(dsn, "default") == _ids(_RUN1)
    return dep, ids_path, held


def test_held_run_above_the_chunk_limit_is_approved_in_slices(
    tmp_path: Path,
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """#2994: a held run of upserts and retractions above the limit applies."""
    _, _, held = _approve_run1_then_hold_run2(
        tmp_path, monkeypatch, clean_runtime,
    )
    chunks = _spy_chunks(monkeypatch)

    decided = _decide(held)

    assert decided["status"] == "accepted", decided
    assert not decided.get("already_decided"), decided
    emitted = decided["emitted"]
    upserts = emitted["node_facts_inserted"] + emitted["edge_facts_inserted"]
    retractions = emitted["retractions_applied"]
    total = upserts + retractions
    assert emitted["node_facts_inserted"] == 40, emitted
    assert retractions == 5, emitted
    assert total == 45, emitted
    # Slices of exactly the limit, then the remainder; nothing dropped or
    # counted twice, and the order (upserts before retractions) is kept.
    expected_sizes = [_LIMIT] * (total // _LIMIT) + (
        [total % _LIMIT] if total % _LIMIT else []
    )
    assert [up + rt for _, up, rt in chunks] == expected_sizes, chunks
    assert [ordinal for ordinal, _, _ in chunks] == list(range(len(chunks)))
    assert sum(up for _, up, _ in chunks) == upserts
    assert sum(rt for _, _, rt in chunks) == retractions
    assert chunks[-1][2] == retractions, "retractions ride in the last slice"
    # One receipt per slice, all applied; the result lists them in order.
    assert _promotion_receipts(clean_runtime, held["admission_id"]) == [
        (ordinal, "applied") for ordinal in range(len(chunks))
    ]
    assert emitted["fact_batch_receipt"] is None
    assert emitted["fact_batch_chunk_count"] == len(chunks)
    assert [
        receipt["chunk_ordinal"] for receipt in emitted["fact_batch_receipts"]
    ] == list(range(len(chunks)))
    # The envelope records the whole run, the sum of the slices.
    after = _latest_envelope(clean_runtime)
    assert after["admission_id"] == held["admission_id"]
    assert after["status"] == "accepted"
    assert after["events"].count("accepted") == 1, after["events"]
    assert after["verdict"]["node_facts_inserted"] == (
        emitted["node_facts_inserted"]
    )
    assert _pending_authority_rows(clean_runtime, held["admission_id"]) == 0
    # `default` now holds exactly run 2's footprint.
    assert _node_ids(clean_runtime, "default") == _ids(_RUN2)


def test_failure_on_slice_two_rolls_back_the_whole_approval(
    tmp_path: Path,
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """#2994: a failed slice leaves the run pending and nothing on `default`.

    All slices share the one decide transaction, so the failure on slice 2
    undoes slice 1 as well: no receipt, no row, no envelope transition, the
    indexed-authority handoff still waiting. A later publish (by this or any
    other source) checkpoints `default` and so still shows run 1 only.
    Accepting again applies the whole run.
    """
    from okg.substrate.graph_store import ChronosGraphStore

    _, _, held = _approve_run1_then_hold_run2(
        tmp_path, monkeypatch, clean_runtime,
    )
    held_rows = _node_ids(clean_runtime, QUARANTINE_BRANCH)
    authority_rows = _pending_authority_rows(clean_runtime, held["admission_id"])
    chunks = _spy_chunks(monkeypatch)
    real_apply = ChronosGraphStore.apply_facts
    default_calls: list[int] = []

    def failing_apply(self, facts, *args, **kwargs):
        if kwargs.get("branch") == "default":
            default_calls.append(len(facts))
            if len(default_calls) == 2:
                raise RuntimeError("injected failure on approval slice 2")
        return real_apply(self, facts, *args, **kwargs)

    monkeypatch.setattr(ChronosGraphStore, "apply_facts", failing_apply)
    with pytest.raises(RuntimeError, match="injected failure on approval slice 2"):
        _decide(held)
    assert default_calls == [_LIMIT, _LIMIT], default_calls
    assert [ordinal for ordinal, _, _ in chunks] == [0, 1]

    state = _latest_envelope(clean_runtime)
    assert state["admission_id"] == held["admission_id"]
    assert state["status"] == "pending", state
    assert "accepted" not in state["events"], state["events"]
    assert _promotion_receipts(clean_runtime, held["admission_id"]) == []
    assert _pending_authority_rows(clean_runtime, held["admission_id"]) == (
        authority_rows
    )
    assert _node_ids(clean_runtime, "default") == _ids(_RUN1)
    assert _node_ids(clean_runtime, QUARANTINE_BRANCH) == held_rows
    store = ChronosGraphStore.connect(clean_runtime, ensure_schema=False)
    try:
        generation = store.checkpoint_generation(
            deployment_name="svc", branch="default",
        )
        published = store.checkout_generation(generation).query(
            "SELECT node_id FROM okg.graph_nodes WHERE source = :source"
            " ORDER BY node_id",
            {"source": _SOURCE},
        )
    finally:
        store.close()
    assert [str(row["node_id"]) for row in published] == _ids(_RUN1)

    monkeypatch.setattr(ChronosGraphStore, "apply_facts", real_apply)
    retried = _decide(held)
    assert retried["status"] == "accepted", retried
    assert not retried.get("already_decided"), retried
    assert _node_ids(clean_runtime, "default") == _ids(_RUN2)
    receipts = _promotion_receipts(clean_runtime, held["admission_id"])
    assert receipts == [(ordinal, "applied") for ordinal in range(len(receipts))]
    assert len(receipts) == retried["emitted"]["fact_batch_chunk_count"]


def test_small_held_run_still_approves_as_one_chunk(
    tmp_path: Path,
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """#2994: an approval within the limit is unchanged: one chunk, one receipt."""
    dep, ids_path = _held_deployment(
        tmp_path, monkeypatch, clean_runtime, limit=None,
    )
    held = _hold_run(clean_runtime, dep, ids_path, _RUN1)
    chunks = _spy_chunks(monkeypatch)

    decided = _decide(held)

    assert decided["status"] == "accepted", decided
    emitted = decided["emitted"]
    assert set(emitted) == {
        "source", "run_id", "node_facts_inserted", "edge_facts_inserted",
        "retractions_applied", "applied_from_branch", "fact_batch_receipt",
    }
    assert chunks == [(0, len(_RUN1), 0)]
    assert emitted["fact_batch_receipt"]["chunk_ordinal"] == 0
    assert emitted["fact_batch_receipt"]["state"] == "applied"
    assert _promotion_receipts(clean_runtime, held["admission_id"]) == [
        (0, "applied"),
    ]
    assert _node_ids(clean_runtime, "default") == _ids(_RUN1)


def test_held_run_above_the_real_10000_limit_is_approved(
    tmp_path: Path,
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """#2994 at the real limit: 10,050 held upserts plus 50 retractions.

    Before the fix this accept failed every time with
    ``chunk exceeds MAX_FACTS_PER_CHUNK=10000``.
    """
    from okg.substrate.fact_batch import MAX_FACTS_PER_CHUNK

    assert MAX_FACTS_PER_CHUNK == 10_000
    dep, ids_path = _held_deployment(
        tmp_path, monkeypatch, clean_runtime, limit=None,
    )
    first = _hold_run(clean_runtime, dep, ids_path, list(range(100)))
    assert _decide(first)["status"] == "accepted"
    run2 = list(range(50, 10_150))
    held = _hold_run(clean_runtime, dep, ids_path, run2)
    assert _node_ids(clean_runtime, QUARANTINE_BRANCH) == _ids(run2)
    _log_retractions(clean_runtime, held, list(range(50)))
    chunks = _spy_chunks(monkeypatch)

    decided = _decide(held)

    assert decided["status"] == "accepted", decided
    emitted = decided["emitted"]
    total = (
        emitted["node_facts_inserted"]
        + emitted["edge_facts_inserted"]
        + emitted["retractions_applied"]
    )
    assert emitted["retractions_applied"] == 50, emitted
    assert emitted["node_facts_inserted"] == 10_050, emitted
    assert total == 10_100, emitted
    assert [up + rt for _, up, rt in chunks] == [
        MAX_FACTS_PER_CHUNK, total - MAX_FACTS_PER_CHUNK,
    ], chunks
    assert _promotion_receipts(clean_runtime, held["admission_id"]) == [
        (0, "applied"), (1, "applied"),
    ]
    assert _node_ids(clean_runtime, "default") == _ids(run2)


def test_review_payload_above_the_chunk_limit_is_accepted_in_slices(
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """#2994: an accepted review payload above the limit also applies in slices.

    Only an auto-approver's max_nodes/max_edges bounded this payload; an
    operator can accept one of any size.
    """
    import okg.substrate.fact_batch as fact_batch_module
    from okg.substrate.graph_store import ChronosGraphStore
    from okg.substrate.runtime import admission_workflows
    from tests.substrate.runtime.test_admission_workflows import (
        _seed_catalog_with_narrowing,
    )

    monkeypatch.setattr(
        admission_workflows, "_resolve_dsn", lambda _name: clean_runtime,
    )
    monkeypatch.setattr(fact_batch_module, "MAX_FACTS_PER_CHUNK", _LIMIT)
    _seed_catalog_with_narrowing(
        clean_runtime, parent="alpha", edge_type="links_to", child="beta",
    )
    count = 2 * _LIMIT + 5
    submitted = admission_workflows._run_submit(
        deployment="review-test",
        payload={
            "nodes": [
                {
                    "node_id": f"review:bulk-{index}",
                    "subtype": "alpha",
                    "attrs": {"text": f"bulk {index}"},
                }
                for index in range(count)
            ],
        },
        producer_id="agent:bulk",
        producer_kind="agent",
        intent="review_proposal",
        idempotency_key="review-bulk-2994",
        authority={
            "producer_policy": {
                "producer_id": "agent:bulk",
                "producer_kind": "agent",
                "trust_label": "review-trusted",
                "admission_mode": "review",
                "allowed_record_kinds": ["node"],
                "output_signature": {
                    "nodes": [{"subtype": "alpha", "clause_id": "allow_alpha"}],
                },
                "policy_clauses": [
                    {
                        "id": "allow_alpha",
                        "effect": "allow",
                        "record_kind": "node",
                        "subtype": "alpha",
                    },
                ],
            },
        },
        check_summary=None,
    )
    chunks = _spy_chunks(monkeypatch)

    decided = admission_workflows._run_decide(
        deployment="review-test",
        admission_id=submitted["admission_id"],
        decision="accept",
        actor="reviewer",
        reason="issue-2994",
        verdict=None,
    )

    assert decided["status"] == "accepted", decided
    emitted = decided["emitted"]
    assert emitted["node_count"] == count
    assert chunks == [(0, _LIMIT, 0), (1, _LIMIT, 0), (2, 5, 0)]
    assert emitted["fact_batch_receipt"] is None
    assert emitted["fact_batch_chunk_count"] == 3
    assert [
        (receipt["chunk_ordinal"], receipt["state"])
        for receipt in emitted["fact_batch_receipts"]
    ] == [(0, "applied"), (1, "applied"), (2, "applied")]
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        rows = ChronosGraphStore.from_connection(conn).checkout().query(
            "SELECT count(*) AS n FROM okg.graph_nodes"
            " WHERE node_id LIKE 'review:bulk-%'"
        )
    assert rows == [{"n": count}]


# Fast-track submit. `_run_submit`'s fast-track branch applies the payload at
# submit time through the same `_emit_review_payload_facts`, inside the submit
# transaction. On dev it refused a payload above the limit; it now slices.

_FAST_POLICY = {
    "producer_id": "agent:fastbulk",
    "producer_kind": "agent",
    "trust_label": "fast-track-trusted",
    "admission_mode": "fast_track",
    "allowed_record_kinds": ["node"],
    "output_signature": {
        "nodes": [{"subtype": "alpha", "clause_id": "allow_alpha"}],
    },
    "policy_clauses": [
        {
            "id": "allow_alpha",
            "effect": "allow",
            "record_kind": "node",
            "subtype": "alpha",
        },
    ],
}
_FAST_COUNT = 2 * _LIMIT + 5


def _fast_submit(monkeypatch: pytest.MonkeyPatch, dsn: str) -> dict[str, Any]:
    from okg.substrate.runtime import admission_workflows

    return admission_workflows._run_submit(
        deployment="review-test",
        payload={
            "nodes": [
                {
                    "node_id": f"fast:bulk-{index}",
                    "subtype": "alpha",
                    "attrs": {"text": f"fast bulk {index}"},
                }
                for index in range(_FAST_COUNT)
            ],
        },
        producer_id="agent:fastbulk",
        producer_kind="agent",
        intent="record_observation",
        idempotency_key="fast-bulk-2994",
        authority={"producer_policy": dict(_FAST_POLICY)},
        check_summary={"record": {"kind": "observation"}},
    )


def _fast_state(dsn: str) -> dict[str, Any]:
    from okg.substrate.graph_store import ChronosGraphStore

    from tests.pytest.admission_envelope_reads import fetch_admission_envelope

    with psycopg.connect(dsn, autocommit=True) as conn:
        with conn.cursor() as cur:
            # Raises when more than one envelope matches, so "at most one
            # envelope" is part of what every caller asserts.
            envelope = fetch_admission_envelope(
                cur,
                columns=("admission_id::text", "status"),
                intent="record_observation",
                deployment_name="review-test",
                producer_id="agent:fastbulk",
            )
            envelopes = (
                [(str(envelope[0]), str(envelope[1]))]
                if envelope is not None
                else []
            )
            cur.execute(
                "SELECT r.chunk_ordinal, r.state FROM okg.fact_batch_receipts r"
                " JOIN okg.admission_envelopes e USING (admission_id)"
                " WHERE e.deployment_name = 'review-test'"
                " AND e.producer_id = 'agent:fastbulk'"
                " AND e.intent = 'record_observation'"
                " ORDER BY r.chunk_ordinal"
            )
            receipts = [(int(row[0]), str(row[1])) for row in cur.fetchall()]
            cur.execute(
                "SELECT count(*) FROM okg.node_facts WHERE source = 'agent:fastbulk'"
            )
            fact_log = int(cur.fetchone()[0])
        nodes = ChronosGraphStore.from_connection(conn).checkout("default").query(
            "SELECT count(*) AS n FROM okg.graph_nodes"
            " WHERE node_id LIKE 'fast:bulk-%'"
        )
    return {
        "envelopes": envelopes,
        "receipts": receipts,
        "fact_log": fact_log,
        "nodes": int(nodes[0]["n"]),
    }


def test_fast_track_submit_above_the_chunk_limit_is_applied_in_slices(
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """#2994 scope: a fast-track submit above the limit is accepted in slices."""
    import okg.substrate.fact_batch as fact_batch_module
    from okg.substrate.runtime import admission_workflows

    monkeypatch.setattr(
        admission_workflows, "_resolve_dsn", lambda _name: clean_runtime,
    )
    monkeypatch.setattr(fact_batch_module, "MAX_FACTS_PER_CHUNK", _LIMIT)
    chunks = _spy_chunks(monkeypatch)

    submitted = _fast_submit(monkeypatch, clean_runtime)

    assert submitted["status"] == "accepted", submitted
    assert submitted["admission_mode"] == "fast_track"
    assert chunks == [(0, _LIMIT, 0), (1, _LIMIT, 0), (2, 5, 0)]
    emitted = submitted["emitted"]
    assert emitted["node_count"] == _FAST_COUNT
    assert emitted["fact_batch_chunk_count"] == 3
    state = _fast_state(clean_runtime)
    assert state["envelopes"] == [(submitted["admission_id"], "accepted")]
    assert state["receipts"] == [(0, "applied"), (1, "applied"), (2, "applied")]
    assert state["nodes"] == _FAST_COUNT


def test_fast_track_submit_failure_on_slice_two_leaves_nothing(
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """#2994 scope: a fast-track slice failure rolls back the whole submit.

    The fast-track apply runs inside the submit transaction, so a failure on
    slice 2 leaves no envelope, no receipt, no fact-log row and no graph row;
    the same submit then succeeds in full.
    """
    import okg.substrate.fact_batch as fact_batch_module
    from okg.substrate.fact_application import FactBatchApplication
    from okg.substrate.runtime import admission_workflows

    monkeypatch.setattr(
        admission_workflows, "_resolve_dsn", lambda _name: clean_runtime,
    )
    monkeypatch.setattr(fact_batch_module, "MAX_FACTS_PER_CHUNK", _LIMIT)
    real_apply_chunk = FactBatchApplication.apply_chunk
    chunks = _spy_chunks(monkeypatch, fail_at=1)

    with pytest.raises(RuntimeError, match="injected failure on slice 2"):
        _fast_submit(monkeypatch, clean_runtime)

    assert [ordinal for ordinal, _, _ in chunks] == [0, 1]
    assert _fast_state(clean_runtime) == {
        "envelopes": [],
        "receipts": [],
        "fact_log": 0,
        "nodes": 0,
    }

    monkeypatch.setattr(FactBatchApplication, "apply_chunk", real_apply_chunk)
    retried = _fast_submit(monkeypatch, clean_runtime)
    assert retried["status"] == "accepted", retried
    state = _fast_state(clean_runtime)
    assert state["envelopes"] == [(retried["admission_id"], "accepted")]
    assert state["receipts"] == [(0, "applied"), (1, "applied"), (2, "applied")]
    assert state["fact_log"] == _FAST_COUNT
    assert state["nodes"] == _FAST_COUNT
