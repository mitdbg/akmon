"""Runtime delta-contract emission for readiness-critical sources.

`deltaflow-source-contracts` / `migrate-readiness-sources`: the
SourceRunner observes the streamed fact sequence once and builds a
validated SourceDeltaBatch for readiness-critical sources. These tests
exercise the same `SourceDeltaAccumulator` the runner drives, so they
prove the contract evidence the runtime emits for the three cases the
task names — no-change, small-change, and delete/reconcile — without a
live Postgres deployment.
"""
from __future__ import annotations

import pytest

from okg.substrate.library.sources.base import (
    EdgeFact,
    NodeFact,
    ProgressMarker,
)
from okg.substrate.source_sync_contract import (
    SourceDeltaAccumulator,
    SourceSyncContractError,
    classify_delta_item,
)


def _node(
    record_id: dict[str, str] | None = None,
    *,
    node_id: str = "doc:a",
    subtype: str = "document",
    op: str = "I",
) -> NodeFact:
    return NodeFact(
        node_id=node_id,
        subtype=subtype,
        attrs={"text": "A"},
        source_record_id=record_id or {"path": "docs/a.md"},
        source_revision={"sha": "abc"},
        op=op,
    )


def _edge(
    record_id: dict[str, str] | None = None,
    *,
    edge_type: str = "contains",
    op: str = "I",
) -> EdgeFact:
    return EdgeFact(
        src="doc:a",
        edge_type=edge_type,
        dst="chunk:a:1",
        source_record_id=record_id if record_id is not None else {},
        source_revision={"sha": "abc"},
        op=op,
    )


def _cursor(value: str) -> dict[str, str]:
    return {"cursor": value}


# ---------------------------------------------------------------------------
# classify_delta_item
# ---------------------------------------------------------------------------


def test_classify_node_insert_carries_subtype_scope() -> None:
    cls = classify_delta_item(_node({"path": "docs/a.md"}))
    assert cls.op == "I"
    assert cls.record_ids == ('{"path":"docs/a.md"}',)
    assert cls.scopes == ("document",)


def test_classify_node_retraction_is_op_r() -> None:
    cls = classify_delta_item(_node({"path": "docs/a.md"}, op="R"))
    assert cls.op == "R"
    assert cls.scopes == ("document",)


def test_classify_edge_with_record_id_scopes_by_edge_type() -> None:
    cls = classify_delta_item(
        _edge({"path": "docs/a.md", "edge": "contains:chunk:a:1"}),
    )
    assert cls.record_ids == (
        '{"edge":"contains:chunk:a:1","path":"docs/a.md"}',
    )
    assert cls.scopes == ("edge:contains",)


def test_classify_edge_without_record_id_contributes_nothing() -> None:
    cls = classify_delta_item(_edge(None))
    assert cls.record_ids == ()
    assert cls.scopes == ()


def test_classify_marker_is_progress_not_change() -> None:
    cls = classify_delta_item(ProgressMarker("docs/a.md", "sha:abc"))
    assert cls.op == "progress"
    assert cls.record_ids == ("docs/a.md",)
    # A marker carries no graph scope: re-emitting it must not dirty
    # anything.
    assert cls.scopes == ()


def test_classify_rejects_unknown_item() -> None:
    with pytest.raises(SourceSyncContractError, match="classification"):
        classify_delta_item(object())  # type: ignore[arg-type]


# ---------------------------------------------------------------------------
# SourceDeltaAccumulator — the three migration cases
# ---------------------------------------------------------------------------


def test_no_change_run_is_valid_with_empty_delta() -> None:
    # The "all unchanged" case: the source re-emits per-record progress
    # markers and no facts. The contract must treat this as zero changed
    # records (not dirty every marker id) and still validate.
    acc = SourceDeltaAccumulator()
    acc.observe_all(
        ProgressMarker(f"docs/{i}.md", f"sha:{i}") for i in range(5)
    )

    batch = acc.build(
        "git-files",
        previous_frontier=_cursor("1"),
        next_frontier=_cursor("1"),
        run_mode="scope_complete",
    )

    assert batch.changed_record_ids == ()
    assert batch.inserted_record_ids == ()
    assert batch.retracted_record_ids == ()
    assert batch.affected_scopes == ()
    assert acc.progress_marker_count == 5
    assert acc.changed_record_count == 0


def test_small_change_names_changed_record_and_scope() -> None:
    acc = SourceDeltaAccumulator()
    acc.observe_all([
        _node({"path": "docs/a.md"}, subtype="document"),
        ProgressMarker("docs/a.md", "sha:new"),
    ])

    batch = acc.build(
        "git-files",
        previous_frontier=_cursor("1"),
        next_frontier=_cursor("2"),
        run_mode="scope_complete",
    )

    assert batch.inserted_record_ids == ('{"path":"docs/a.md"}',)
    assert batch.changed_record_ids == ('{"path":"docs/a.md"}',)
    assert batch.retracted_record_ids == ()
    assert batch.affected_scopes == ("document",)
    # The unchanged-record marker does not widen the change set.
    assert acc.changed_record_count == 1


def test_delete_reconcile_reports_retractions_and_scope() -> None:
    acc = SourceDeltaAccumulator()
    acc.observe_all([
        _node({"path": "docs/gone.md"}, subtype="document", op="R"),
    ])

    batch = acc.build(
        "git-files",
        previous_frontier=_cursor("2"),
        next_frontier=_cursor("3"),
        run_mode="reconcile",
    )

    assert batch.retracted_record_ids == ('{"path":"docs/gone.md"}',)
    assert batch.changed_record_ids == ('{"path":"docs/gone.md"}',)
    assert batch.inserted_record_ids == ()
    assert batch.affected_scopes == ("document",)


def test_edge_without_record_id_counts_as_unidentified() -> None:
    acc = SourceDeltaAccumulator()
    acc.observe(_edge(None))

    batch = acc.build(
        "code-structure",
        next_frontier=_cursor("9"),
        run_mode="scope_complete",
    )

    # The edge wrote graph state but cannot be named in the contract;
    # it is counted, not silently dropped, and creates no false change.
    assert acc.unidentified_fact_count == 1
    assert batch.changed_record_ids == ()
    assert batch.affected_scopes == ()


def test_mixed_run_partitions_inserts_retractions_and_scopes() -> None:
    acc = SourceDeltaAccumulator()
    acc.observe_all([
        _node({"path": "src/a.py"}, node_id="sym:a", subtype="symbol"),
        _node({"path": "src/b.py"}, node_id="sym:b", subtype="symbol",
              op="R"),
        _edge({"path": "src/a.py", "edge": "calls"}, edge_type="calls"),
        ProgressMarker("src/a.py", "sha:1"),
    ])

    batch = acc.build(
        "code-structure",
        previous_frontier=_cursor("3"),
        next_frontier=_cursor("4"),
        run_mode="scope_complete",
    )

    assert batch.inserted_record_ids == (
        '{"path":"src/a.py"}',
        '{"edge":"calls","path":"src/a.py"}',
    )
    assert batch.retracted_record_ids == ('{"path":"src/b.py"}',)
    assert set(batch.affected_scopes) == {"symbol", "edge:calls"}
    assert acc.progress_marker_count == 1


def test_summary_carries_contract_counts_alongside_batch() -> None:
    acc = SourceDeltaAccumulator()
    acc.observe_all([
        _node({"path": "docs/a.md"}),
        _edge(None),
        ProgressMarker("docs/a.md", "sha:1"),
    ])

    summary = acc.summary(
        "git-files",
        next_frontier=_cursor("2"),
        run_mode="scope_complete",
    )

    assert summary["source_id"] == "git-files"
    assert summary["observed_progress_markers"] == 1
    assert summary["unidentified_facts"] == 1
    assert summary["changed_record_ids"] == ['{"path":"docs/a.md"}']
    assert summary["affected_scopes"] == ["document"]
