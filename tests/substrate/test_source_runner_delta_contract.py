"""End-to-end delta-contract emission through a live SourceRunner.

`deltaflow-source-contracts` / `migrate-readiness-sources`: prove that a
readiness-critical source run attaches a validated SourceDeltaBatch to
its RunResult for the three cases the task names — small-change (insert),
no-change (re-scan with nothing new), and delete/reconcile (record set
shrinks). This drives the real runner against a clean Postgres so the
streaming walk, watermark advance, and contract construction all run on
the production path, not a synthetic fixture.
"""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

import psycopg
import pytest

from okg.substrate.ingest import SourceRunner
from okg.substrate.library.sources.base import (
    EdgeFact,
    EdgeKey,
    NodeFact,
    RecordEmission,
    RecordNode,
    RecordSet,
    SourceRun,
)
from tests.substrate._db_helpers import seed_catalog_version


pytestmark = pytest.mark.postgres


class _ScriptedAdapter:
    name = "test.delta-contract"
    profile = "discovery_crawl"

    def __init__(
        self,
        runs: list[tuple[list[NodeFact | EdgeFact], RecordSet | None]],
    ) -> None:
        self._runs = list(runs)
        self._seq = 0

    def run(
        self,
        run_id: str,
        *,
        mode: str = "cursor",
        cursor: dict[str, Any] | None = None,
        since_progress: dict[str, str] | None = None,
    ) -> SourceRun:
        del run_id, cursor, since_progress
        self._seq += 1
        facts, record_set = self._runs.pop(0)
        return SourceRun(
            facts=facts,
            completed_scope=True,
            next_cursor={"seq": self._seq},
            run_mode=mode,
            record_set=record_set,
        )


def _node(node_id: str, subtype: str = "concept") -> NodeFact:
    return NodeFact(
        node_id=node_id,
        subtype=subtype,
        attrs={"text": node_id},
        source_record_id={"id": node_id},
        source_revision={"rev": "1"},
    )


def _edge(src: str, edge_type: str, dst: str) -> EdgeFact:
    return EdgeFact(
        src=src,
        dst=dst,
        edge_type=edge_type,
        source_record_id={"src": src, "edge_type": edge_type, "dst": dst},
        source_revision={"rev": "1"},
    )


def _record_set() -> RecordSet:
    return RecordSet(records={
        "rec:a": RecordEmission(
            primary=RecordNode(
                "doc:a", "concept", source_record_id={"id": "doc:a"},
            ),
            descendants=[
                RecordNode(
                    "chunk:a:1", "concept",
                    source_record_id={"id": "chunk:a:1"},
                ),
            ],
            edges=[
                EdgeKey(
                    src="doc:a", edge_type="contains", dst="chunk:a:1",
                    source_record_id={
                        "src": "doc:a",
                        "edge_type": "contains",
                        "dst": "chunk:a:1",
                    },
                ),
            ],
        ),
    })


def test_readiness_source_emits_delta_contract(clean_runtime: str) -> None:
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        seed_catalog_version(conn)

    record_set = _record_set()
    insert_facts = [
        _node("doc:a"),
        _node("chunk:a:1"),
        _edge("doc:a", "contains", "chunk:a:1"),
    ]
    adapter = _ScriptedAdapter([
        (insert_facts, record_set),   # A: small-change (insert)
        ([], record_set),             # B: no-change (same record set)
        ([], RecordSet()),            # C: delete/reconcile (record set empties)
    ])
    from okg.substrate.ingest.full_retraction_guard import (
        FullRetractionOverride,
    )

    runner = SourceRunner(
        clean_runtime,
        source_name="git-files",
        write_fact_log=True,
        # Issue #2818 backstop: case C empties the whole source, which the
        # runner refuses without `okg ingest --allow-full-retraction`.
        full_retraction_override=FullRetractionOverride(
            source="git-files",
            requested_by="delta-contract-test",
            requested_at=datetime.now(timezone.utc),
        ),
    )

    # A — small change: the changed records and their scopes are named.
    a = runner.run(
        adapter, mode="scope_complete", emit_delta_contract=True,
    )
    batch = a.source_delta_batch
    assert batch is not None
    assert batch["contract"] == "valid"
    assert '{"id":"doc:a"}' in batch["inserted_record_ids"]
    assert '{"id":"chunk:a:1"}' in batch["inserted_record_ids"]
    assert batch["retracted_record_ids"] == []
    assert "concept" in batch["affected_scopes"]
    assert "edge:contains" in batch["affected_scopes"]
    # The contract is also surfaced through RunResult.as_dict().
    assert a.as_dict()["source_delta_batch"]["contract"] == "valid"

    # B — no change: a re-scan with nothing new is bounded to an empty
    # delta, not a spurious dirty set.
    b = runner.run(
        adapter, mode="scope_complete", emit_delta_contract=True,
    )
    no_change = b.source_delta_batch
    assert no_change is not None
    assert no_change["contract"] == "valid"
    assert no_change["changed_record_ids"] == []
    assert no_change["inserted_record_ids"] == []
    assert no_change["retracted_record_ids"] == []
    assert no_change["affected_scopes"] == []

    # C — delete/reconcile: the disappeared records are reported as
    # retractions with their affected scopes.
    c = runner.run(
        adapter, mode="scope_complete", emit_delta_contract=True,
    )
    deletes = c.source_delta_batch
    assert deletes is not None
    assert deletes["contract"] == "valid"
    assert set(deletes["retracted_record_ids"]) == {
        '{"id":"doc:a"}',
        '{"id":"chunk:a:1"}',
        '{"dst":"chunk:a:1","edge_type":"contains","src":"doc:a"}',
    }
    assert set(deletes["changed_record_ids"]) == set(
        deletes["retracted_record_ids"]
    )
    assert deletes["inserted_record_ids"] == []
    assert set(deletes["affected_scopes"]) == {"concept", "edge:contains"}


def test_non_readiness_run_omits_contract(clean_runtime: str) -> None:
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        seed_catalog_version(conn)

    adapter = _ScriptedAdapter([([_node("doc:a")], None)])
    runner = SourceRunner(
        clean_runtime,
        source_name="git-files",
        write_fact_log=True,
    )

    # emit_delta_contract defaults to False: no contract is attached and
    # the field stays out of the serialized result.
    result = runner.run(adapter, mode="scope_complete")
    assert result.source_delta_batch is None
    assert "source_delta_batch" not in result.as_dict()


def test_unidentified_fact_makes_contract_invalid(clean_runtime: str) -> None:
    """A graph-writing fact with no stable source_record_id is dropped from the
    change set (it has no invalidation key), so the delta contract must report
    `invalid`, not `valid` — else a change frontier built from the batch would
    silently under-cover those records and a dependent output keyed on them
    would be wrongly reused (stale). (deltaflow review follow-up B)
    """
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        seed_catalog_version(conn)

    # Writes graph state but carries NO source_record_id -> unidentified.
    unnamed = NodeFact(
        node_id="concept:unnamed",
        subtype="concept",
        attrs={"text": "no record id"},
        source_record_id={},
        source_revision={"rev": "1"},
    )
    adapter = _ScriptedAdapter([([unnamed, _node("concept:named")], None)])
    runner = SourceRunner(
        clean_runtime, source_name="git-files", write_fact_log=True,
    )
    result = runner.run(
        adapter, mode="scope_complete", emit_delta_contract=True,
    )
    batch = result.source_delta_batch
    assert batch is not None
    assert batch["contract"] == "invalid"
    assert batch["unidentified_facts"] == 1
    assert "source_record_id" in batch.get("reason", "")
