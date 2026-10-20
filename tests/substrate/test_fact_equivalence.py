"""One "is this fact unchanged?" rule (#2570 item 1; #2334, #2335, #2336).

`okg.substrate.fact_equivalence` decides for the source runner's live-branch
dedupe, its legacy fact-log dedupe and the graph writer's no-op check. These
tests pin the rule itself, then each caller to it.
"""
from __future__ import annotations

from typing import Any

import pytest

from okg.substrate import fact_equivalence as fe
from okg.substrate.graph_store_internal.writes import ChronosGraphWriteMixin
from okg.substrate.ingest.runner import (
    DEDUPE_REFERENCE_FACT_LOG,
    DEDUPE_REFERENCE_LIVE_BRANCH,
    DEDUPE_REFERENCE_NONE,
    SourceRunner,
)
from okg.substrate.library.sources.base import EdgeFact, NodeFact
from okg.substrate.nomos.source_lineage import (
    LINEAGE_ATTR,
    stamp_attrs_with_source_lineage,
)


def _stamped(attrs: dict, *, run_id: str, record_id: str = "rec-1",
             policy: str = "sha256:policy") -> dict:
    return stamp_attrs_with_source_lineage(
        attrs, source_id="test-source", source_record_id=record_id,
        source_revision={"run_id": run_id, "content_hash": "sha256:same"},
        policy_fingerprint=policy,
    )


# --- the rule ----------------------------------------------------------------


def test_the_lineage_attr_is_the_one_writers_stamp():
    assert fe.LINEAGE_ATTR == LINEAGE_ATTR


def test_a_revision_only_lineage_change_is_unchanged():
    comparison = fe.compare_attrs(
        _stamped({"title": "A"}, run_id="run-2"), _stamped({"title": "A"}, run_id="run-1"),
    )
    assert comparison == fe.AttrsComparison(True, True, ())


@pytest.mark.parametrize(
    ("new", "stored", "differing"),
    [
        (_stamped({"title": "B"}, run_id="run-2"), _stamped({"title": "A"}, run_id="run-1"),
         ("nomos_source_lineage.source_revision", "title")),
        (_stamped({"title": "A"}, run_id="run-1", record_id="rec-2"),
         _stamped({"title": "A"}, run_id="run-1"), (LINEAGE_ATTR,)),
        (_stamped({"title": "A"}, run_id="run-1", policy="sha256:other"),
         _stamped({"title": "A"}, run_id="run-1"), (LINEAGE_ATTR,)),
        ({"flag": 1}, {"flag": True}, ("flag",)),
        ({"n": 1.0}, {"n": 1}, ("n",)),
        ({"x": None}, {}, ("x",)),
        ({"body_text": "new"}, {"body_text": "old"}, ("body_text",)),
    ],
)
def test_a_real_change_is_reported_by_key(new, stored, differing):
    comparison = fe.compare_attrs(new, stored)
    assert not comparison.unchanged
    assert comparison.differing == differing


def test_mixed_type_dict_keys_compare_as_json_stores_them():
    assert fe.canonical({1: "x", "b": 2}) == fe.canonical({"1": "x", "b": 2})
    assert fe.compare_attrs({"m": {1: "x", "b": 2}}, {"m": {"1": "x", "b": 2}}).unchanged


def test_the_writer_dirty_scope_uses_json_equality():
    """A value that changes only its JSON type (True -> 1) is written, and
    the dirty scope names the key that changed."""
    prior = _node_row(attrs={"flag": True})
    upsert = _node_row(attrs={"flag": 1})
    assert ChronosGraphWriteMixin._node_upsert_semantic_noop(upsert, prior) is False
    out = ChronosGraphWriteMixin._node_attr_changes_by_subtype(
        None, node_upserts=[upsert], node_deletes=[], prior_by_id={"doc:1": prior},
    )
    assert out["doc"]["doc:1"] == ("flag",)


def test_only_the_named_volatile_fields_are_ignored():
    new, stored = {"a": 1, "parsed_at": "t2"}, {"a": 1, "parsed_at": "t1"}
    assert fe.compare_attrs(new, stored, ignore={"parsed_at"}).unchanged
    assert not fe.compare_attrs(new, stored).unchanged


def test_volatile_fields_are_per_subtype():
    volatile = {"Document": frozenset({"parsed_at"})}
    assert fe.volatile_fields_for(volatile, "Document") == {"parsed_at"}
    assert fe.volatile_fields_for(volatile, "ticket") == frozenset()
    assert fe.volatile_fields_for(None, "Document") == frozenset()


def test_edge_confidence_is_compared_at_real_precision_and_null_stays_distinct():
    stored_real = fe.as_real(0.7)
    assert stored_real != 0.7
    assert fe.edge_columns_differing(
        new_provenance="explicit", new_confidence=0.7,
        stored_provenance="explicit", stored_confidence=stored_real,
    ) == ()
    assert fe.edge_columns_differing(
        new_provenance="explicit", new_confidence=None,
        stored_provenance="explicit", stored_confidence=0.0,
    ) == ("edge.confidence",)
    assert fe.edge_columns_differing(
        new_provenance=None, new_confidence=1.0,
        stored_provenance="", stored_confidence=1.0,
    ) == ("edge.provenance",)


# --- the graph writer (#2335) -------------------------------------------------


def _node_row(**overrides: Any) -> dict[str, Any]:
    row = {
        "node_id": "doc:1",
        "subtype": "doc",
        "attrs": _stamped({"title": "A"}, run_id="run-1"),
        "source": "_publisher_derived",
        "source_record_id": {"id": "doc:1"},
        "source_revision": {"run_id": "run-1"},
    }
    row.update(overrides)
    return row


def _edge_row(**overrides: Any) -> dict[str, Any]:
    row = {
        "edge_id": 7,
        "src": "doc:1",
        "dst": "doc:2",
        "edge_type": "references",
        "attrs": _stamped({"w": 1}, run_id="run-1"),
        "source": "_publisher_derived",
        "source_record_id": {"id": "e:1"},
        "source_revision": {"run_id": "run-1"},
        "provenance": "explicit",
        "confidence": 0.7,
    }
    row.update(overrides)
    return row


def test_the_writer_skips_a_node_whose_only_change_is_its_revision():
    noop = ChronosGraphWriteMixin._node_upsert_semantic_noop
    moved = _node_row(
        attrs=_stamped({"title": "A"}, run_id="run-2"),
        source_revision={"run_id": "run-2"},
    )
    assert noop(moved, _node_row()) is True


@pytest.mark.parametrize(
    "overrides",
    [
        {"attrs": _stamped({"title": "B"}, run_id="run-2")},
        {"attrs": _stamped({"title": "A"}, run_id="run-1", record_id="rec-9")},
        {"subtype": "ticket"},
        {"source": "other-source"},
        {"source_record_id": {"id": "doc:9"}},
    ],
)
def test_the_writer_still_writes_a_node_with_a_real_change(overrides):
    noop = ChronosGraphWriteMixin._node_upsert_semantic_noop
    assert noop(_node_row(**overrides), _node_row()) is False


def test_the_writer_skips_an_edge_whose_only_change_is_its_revision():
    noop = ChronosGraphWriteMixin._edge_upsert_semantic_noop
    moved = _edge_row(
        attrs=_stamped({"w": 1}, run_id="run-2"), source_revision={"run_id": "run-2"},
    )
    # The stored REAL confidence reads back at single precision.
    stored = _edge_row(confidence=fe.as_real(0.7))
    assert noop(moved, stored) is True
    assert noop(_edge_row(confidence=0.5), stored) is False
    assert noop(_edge_row(provenance="derived_semantic"), stored) is False
    assert noop(_edge_row(attrs=_stamped({"w": 2}, run_id="run-2")), stored) is False


# --- the runner: which store it compares against (#2336) ---------------------


@pytest.mark.parametrize(
    ("chronos", "fact_log", "reference"),
    [
        (True, True, DEDUPE_REFERENCE_LIVE_BRANCH),
        (True, False, DEDUPE_REFERENCE_LIVE_BRANCH),
        (False, True, DEDUPE_REFERENCE_FACT_LOG),
        (False, False, DEDUPE_REFERENCE_NONE),
    ],
)
def test_an_ordinary_run_compares_against_the_live_branch_when_chronos_is_on(
    chronos, fact_log, reference,
):
    runner = SourceRunner(
        "postgres://unused", source_name="test-source",
        write_chronos_graph=chronos, write_fact_log=fact_log,
    )
    assert runner._ordinary_dedupe_reference() == reference


# --- the runner: the fact-log comparison (#2336) -----------------------------


class _FactLogCursor:
    def __init__(self, nodes: list[tuple], edges: list[tuple]) -> None:
        self.nodes, self.edges, self._rows = nodes, edges, []

    def __enter__(self) -> "_FactLogCursor":
        return self

    def __exit__(self, *_exc: object) -> None:
        return None

    def execute(self, sql: str, _params: object) -> None:
        self._rows = self.nodes if "okg.node_facts" in sql else self.edges

    def fetchall(self) -> list[tuple]:
        return list(self._rows)


class _FactLogConn:
    def __init__(self, cursor: _FactLogCursor) -> None:
        self._cursor = cursor

    def cursor(self) -> _FactLogCursor:
        return self._cursor


def _fact_log_runner(nodes: list[tuple], edges: list[tuple], *, volatile=None) -> SourceRunner:
    runner = SourceRunner(
        "postgres://unused", source_name="test-source",
        write_chronos_graph=False, write_fact_log=True,
    )
    runner._dedupe_reference = DEDUPE_REFERENCE_FACT_LOG
    runner._volatile_keys_cache = dict(volatile or {})
    conn = _FactLogConn(_FactLogCursor(nodes, edges))
    runner._acquire_conn = lambda: conn  # type: ignore[method-assign]
    return runner


def _node(attrs: dict, subtype: str = "doc") -> NodeFact:
    return NodeFact(node_id="doc:1", subtype=subtype, attrs=attrs,
                    source_record_id={"id": "doc:1"}, source_revision={"v": 1})


def _edge(attrs: dict, *, provenance: str = "explicit", confidence: float = 0.7) -> EdgeFact:
    return EdgeFact(src="doc:1", dst="doc:2", edge_type="references", attrs=attrs,
                    provenance=provenance, confidence=confidence)


def test_fact_log_drops_a_revision_only_node_change():
    stored = _stamped({"title": "A", "body_text": "same"}, run_id="run-1")
    runner = _fact_log_runner([("doc:1", "I", "doc", stored)], [])
    node = _node(_stamped({"title": "A", "body_text": "same"}, run_id="run-2"))
    kept, dropped, _ = runner._dedupe_against_persisted([node])
    assert kept == [] and dropped == 1
    assert runner._persisted_dedupe_detail.revision_only_nodes == 1


@pytest.mark.parametrize("key", ["body_text", "text"])
def test_fact_log_writes_a_text_only_change(key):
    stored = _stamped({"title": "A", key: "old"}, run_id="run-1")
    runner = _fact_log_runner([("doc:1", "I", "doc", stored)], [])
    node = _node(_stamped({"title": "A", key: "new"}, run_id="run-1"))
    kept, dropped, _ = runner._dedupe_against_persisted([node])
    assert kept == [node] and dropped == 0
    assert runner._persisted_dedupe_detail.kept_by_key == {key: 1}


def test_fact_log_ignores_only_the_subtypes_own_volatile_fields():
    stored = {"title": "A", "parsed_at": "t1"}
    new = {"title": "A", "parsed_at": "t2"}
    volatile = {"Document": frozenset({"parsed_at"})}
    runner = _fact_log_runner([("doc:1", "I", "Document", stored)], [], volatile=volatile)
    kept, dropped, _ = runner._dedupe_against_persisted([_node(new, "Document")])
    assert kept == [] and dropped == 1
    runner = _fact_log_runner([("doc:1", "I", "ticket", stored)], [], volatile=volatile)
    node = _node(new, "ticket")
    kept, dropped, _ = runner._dedupe_against_persisted([node])
    assert kept == [node] and dropped == 0


def test_fact_log_compares_edge_provenance_and_confidence():
    stored = ("doc:1", "references", "doc:2", "I", {"w": 1}, "explicit", fe.as_real(0.7))
    runner = _fact_log_runner([], [stored])
    kept, _, dropped = runner._dedupe_against_persisted([_edge({"w": 1})])
    assert kept == [] and dropped == 1
    for edge in (_edge({"w": 1}, provenance="derived_semantic"), _edge({"w": 1}, confidence=0.5)):
        runner = _fact_log_runner([], [stored])
        kept, _, dropped = runner._dedupe_against_persisted([edge])
        assert kept == [edge] and dropped == 0


def test_fact_log_writes_over_a_retraction():
    runner = _fact_log_runner([("doc:1", "R", "doc", {"title": "A"})], [])
    node = _node({"title": "A"})
    kept, dropped, _ = runner._dedupe_against_persisted([node])
    assert kept == [node] and dropped == 0
