from __future__ import annotations

from typing import Any

import pytest

from okg.substrate.errors import SubstrateError
from okg.substrate.graph_store import _edge_id
from okg.substrate.graph_store_internal.writes import ChronosGraphWriteMixin
from okg.substrate.ingest.runner import SourceRunner
from okg.substrate.library.sources.base import EdgeFact, NodeFact
from okg.substrate.nomos.source_lineage import LINEAGE_ATTR, stamp_attrs_with_source_lineage


class _WriterTextRule(ChronosGraphWriteMixin):
    """The writer's real searchable-text rule with a fixed field catalog.

    ``doc`` declares ``title`` (the stamped ``text`` is derived from it),
    ``goal`` declares only ``text`` (the raw value is stored), and any other
    subtype declares nothing (``text`` is stored as given).
    """

    _FIELDS = {"doc": ["title"], "goal": ["text"]}

    def _searchable_text_fields(self, subtype: str, *, key: str = "searchable_text_fields") -> list[str]:
        if key != "searchable_text_fields":
            return []
        return list(self._FIELDS.get(subtype, []))


class _FakeSession:
    def __init__(self, *, source: str = "test-source") -> None:
        self.calls: list[tuple[str, dict[str, object]]] = []
        self.source = source

    def query(self, sql: str, params: dict[str, object]) -> list[dict[str, Any]]:
        self.calls.append((sql, dict(params)))
        if params.get("source") != self.source:
            return []
        if "okg.graph_nodes" in sql:
            return [{
                "node_id": "node:subtype",
                "subtype": "old_type",
                "attrs": {"rank": 1},
            }]
        if "okg.graph_edges" in sql:
            return [{
                "src": "node:a",
                "edge_type": "relates_to",
                "dst": "node:b",
                "attrs": {"rank": 1},
                "provenance": "explicit",
                "confidence": 1.0,
            }]
        return []


class _FakeGraphStore(_WriterTextRule):
    def __init__(self, *, source: str = "test-source") -> None:
        self.session = _FakeSession(source=source)

    def checkout(self, _branch: str) -> _FakeSession:
        return self.session


def test_chronos_edge_dedupe_queries_only_candidate_edges() -> None:
    graph_store = _FakeGraphStore()
    runner = SourceRunner(
        "postgres://unused",
        source_name="test-source",
        graph_store=graph_store,
    )
    runner._volatile_keys_cache = {}
    edge = EdgeFact(
        src="node:a",
        dst="node:b",
        edge_type="relates_to",
        attrs={"rank": 1},
    )

    kept, node_drops, edge_drops = runner._dedupe_against_chronos_graph(
        [edge],
        candidates_nodes=[],
        candidates_edges=[edge],
    )

    assert kept == []
    assert node_drops == 0
    assert edge_drops == 1
    sql, params = graph_store.session.calls[0]
    assert "FROM okg.graph_edges" in sql
    assert "provenance, confidence" in sql
    assert "edge_id = ANY(:edge_ids)" in sql
    assert params == {
        "edge_ids": [_edge_id("node:a", "relates_to", "node:b")],
        "source": "test-source",
    }


def test_chronos_node_dedupe_preserves_subtype_migration() -> None:
    graph_store = _FakeGraphStore()
    runner = SourceRunner(
        "postgres://unused",
        source_name="test-source",
        graph_store=graph_store,
    )
    runner._volatile_keys_cache = {}
    node = NodeFact(
        node_id="node:subtype",
        subtype="new_type",
        attrs={"rank": 1},
        source_record_id={"id": "node:subtype"},
        source_revision={"v": "new"},
    )

    kept, node_drops, edge_drops = runner._dedupe_against_chronos_graph(
        [node],
        candidates_nodes=[node],
        candidates_edges=[],
    )

    assert kept == [node]
    assert node_drops == 0
    assert edge_drops == 0
    sql, params = graph_store.session.calls[0]
    assert "SELECT node_id, subtype, attrs" in sql
    assert "FROM okg.graph_nodes" in sql
    assert params == {"node_ids": ["node:subtype"], "source": "test-source"}


def test_chronos_node_dedupe_preserves_source_reassertion() -> None:
    graph_store = _FakeGraphStore(source="_catalog:restamp")
    runner = SourceRunner(
        "postgres://unused",
        source_name="real-source",
        graph_store=graph_store,
    )
    runner._volatile_keys_cache = {}
    node = NodeFact(
        node_id="node:subtype",
        subtype="old_type",
        attrs={"rank": 1},
        source_record_id={"id": "node:subtype"},
        source_revision={"v": "new"},
    )

    kept, node_drops, edge_drops = runner._dedupe_against_chronos_graph(
        [node],
        candidates_nodes=[node],
        candidates_edges=[],
    )

    assert kept == [node]
    assert node_drops == 0
    assert edge_drops == 0


# ---------------------------------------------------------------------------
# Revision-only churn (PACT archi-repeat-dedupe-cause): the lineage envelope
# embeds each fact's source_revision, so a content_hash revision that also
# carries a per-run id used to defeat the persisted dedupe on every run.
# ---------------------------------------------------------------------------



def _stamped(attrs: dict, *, run_id: str, content_hash: str = "sha256:same",
             record_id: str = "rec-1", policy: str = "sha256:policy") -> dict:
    return stamp_attrs_with_source_lineage(
        attrs, source_id="test-source", source_record_id=record_id,
        source_revision={"run_id": run_id, "content_hash": content_hash, "n_records": 3},
        policy_fingerprint=policy,
    )


class _StoredSession:
    """Fake chronos session returning one stored node and one stored edge."""

    def __init__(self, node_attrs: dict, edge_attrs: dict, *, subtype: str = "doc",
                 provenance: str = "explicit", confidence: float = 1.0) -> None:
        self.node_attrs, self.edge_attrs, self.subtype = node_attrs, edge_attrs, subtype
        self.provenance, self.confidence = provenance, confidence

    def query(self, sql: str, params: dict[str, object]) -> list[dict[str, Any]]:
        if params.get("source") != "test-source":
            return []
        if "okg.graph_nodes" in sql:
            return [{"node_id": "doc:1", "subtype": self.subtype, "attrs": self.node_attrs}]
        if "okg.graph_edges" in sql:
            return [{"src": "doc:1", "edge_type": "contains", "dst": "doc:2", "attrs": self.edge_attrs,
                     "provenance": self.provenance, "confidence": self.confidence}]
        return []


class _StoredGraphStore(_WriterTextRule):
    def __init__(self, session: _StoredSession) -> None:
        self.session = session

    def checkout(self, _branch: str) -> _StoredSession:
        return self.session


def _run_dedupe(stored_node: dict, stored_edge: dict, new_node: dict, new_edge: dict,
                *, kind: str | None, subtype: str = "doc", new_subtype: str = "doc",
                stored_provenance: str = "explicit", stored_confidence: float = 1.0,
                new_provenance: str = "explicit", new_confidence: float = 1.0):
    # A stored node row went through the writer, which stamped its ``text``.
    stored_node = _WriterTextRule()._stamp_searchable_text(subtype, dict(stored_node), cache={})
    session = _StoredSession(stored_node, stored_edge, subtype=subtype,
                             provenance=stored_provenance, confidence=stored_confidence)
    runner = SourceRunner("postgres://unused", source_name="test-source",
                          graph_store=_StoredGraphStore(session),
                          source_revision_kind=kind)
    runner._volatile_keys_cache = {}
    revision = {"run_id": "run-2", "content_hash": "sha256:same"}
    node = NodeFact(node_id="doc:1", subtype=new_subtype, attrs=new_node,
                    source_record_id={"id": "doc:1"}, source_revision=revision)
    edge = EdgeFact(src="doc:1", dst="doc:2", edge_type="contains", attrs=new_edge,
                    source_record_id={"id": "doc:1->doc:2"}, source_revision=revision,
                    provenance=new_provenance, confidence=new_confidence)
    kept, node_drops, edge_drops = runner._dedupe_against_chronos_graph(
        [node, edge], candidates_nodes=[node], candidates_edges=[edge])
    return kept, node_drops, edge_drops, runner._persisted_dedupe_detail


@pytest.mark.parametrize("kind", ["content_hash", None, "updated_at", "release_tag", "version_token"])
def test_revision_only_churn_is_dropped_and_counted_for_every_revision_kind(kind) -> None:
    """Issue #2076: the rule no longer depends on the declared revision kind.
    A fact whose content matches the stored row is not rewritten because its
    revision (here a run-scoped one) moved."""
    stored_n, stored_e = _stamped({"title": "A"}, run_id="run-1"), _stamped({"w": 1}, run_id="run-1")
    new_n, new_e = _stamped({"title": "A"}, run_id="run-2"), _stamped({"w": 1}, run_id="run-2")
    assert stored_n != new_n and stored_n[LINEAGE_ATTR] != new_n[LINEAGE_ATTR]
    kept, node_drops, edge_drops, detail = _run_dedupe(stored_n, stored_e, new_n, new_e, kind=kind)
    assert kept == [] and (node_drops, edge_drops) == (1, 1)
    assert (detail.revision_only_nodes, detail.revision_only_edges) == (1, 1)
    assert detail.kept_by_key == {}


def test_content_hash_revision_change_alone_is_dropped_and_attr_change_is_kept() -> None:
    stored_n, stored_e = _stamped({"title": "A"}, run_id="run-1"), _stamped({"w": 1}, run_id="run-1")
    # Only the revision's content hash moved; the stored content is identical,
    # so the row is not rewritten (a revision is not content).
    new_n = _stamped({"title": "A"}, run_id="run-2", content_hash="sha256:other")
    # Visible attribute moved: reported by its key, revision differences aside.
    new_e = _stamped({"w": 2}, run_id="run-2")
    kept, node_drops, edge_drops, detail = _run_dedupe(stored_n, stored_e, new_n, new_e, kind="content_hash")
    assert [type(f).__name__ for f in kept] == ["EdgeFact"] and (node_drops, edge_drops) == (1, 0)
    assert detail.revision_only_nodes == 1 and detail.revision_only_edges == 0
    assert detail.kept_by_key == {LINEAGE_ATTR + ".source_revision": 1, "w": 1}
    assert not any("run-" in key or "sha256" in key for key in detail.kept_by_key)


def test_policy_fingerprint_or_record_id_change_is_not_revision_only_churn() -> None:
    stored_n, stored_e = _stamped({"title": "A"}, run_id="run-1"), _stamped({"w": 1}, run_id="run-1")
    new_n = _stamped({"title": "A"}, run_id="run-2", policy="sha256:new-policy")
    new_e = _stamped({"w": 1}, run_id="run-2", record_id="rec-2")
    kept, node_drops, edge_drops, detail = _run_dedupe(stored_n, stored_e, new_n, new_e, kind="content_hash")
    assert len(kept) == 2 and (node_drops, edge_drops) == (0, 0)
    assert detail.kept_by_key == {LINEAGE_ATTR: 2}


def test_missing_lineage_or_subtype_change_keeps_current_behaviour() -> None:
    stored_n, stored_e = {"title": "A"}, {"w": 1}
    new_n, new_e = _stamped({"title": "A"}, run_id="run-1"), {"w": 1}
    kept, node_drops, edge_drops, detail = _run_dedupe(stored_n, stored_e, new_n, new_e, kind="content_hash")
    assert len(kept) == 1 and (node_drops, edge_drops) == (0, 1)
    assert detail.kept_by_key == {LINEAGE_ATTR: 1}
    kept, node_drops, edge_drops, detail = _run_dedupe(
        _stamped({"title": "A"}, run_id="run-1"), stored_e, _stamped({"title": "A"}, run_id="run-1"), {"w": 1},
        kind="content_hash", subtype="old_type")
    assert len(kept) == 1 and node_drops == 0 and detail.kept_by_key == {"subtype": 1}


def test_unchanged_facts_are_plain_drops_not_revision_churn() -> None:
    stored_n, stored_e = _stamped({"title": "A"}, run_id="run-1"), _stamped({"w": 1}, run_id="run-1")
    kept, node_drops, edge_drops, detail = _run_dedupe(stored_n, stored_e, dict(stored_n), dict(stored_e), kind="content_hash")
    assert kept == [] and (node_drops, edge_drops) == (1, 1)
    assert (detail.revision_only_nodes, detail.revision_only_edges) == (0, 0)


def test_dedupe_report_fields_surface_only_nonzero_detail() -> None:
    from types import SimpleNamespace
    from okg.substrate.ingest.runner import dedupe_report_fields as _dedupe_report_fields
    assert _dedupe_report_fields(SimpleNamespace()) == {}
    assert _dedupe_report_fields(SimpleNamespace(dedupe_revision_only_node=0, dedupe_revision_only_edge=0,
                                                 dedupe_kept_by_key={})) == {}
    assert _dedupe_report_fields(SimpleNamespace(dedupe_revision_only_node=3, dedupe_revision_only_edge=0,
                                                 dedupe_kept_by_key={"title": 2})) == {
        "dedupe_revision_only_node": 3, "dedupe_revision_only_edge": 0, "dedupe_kept_by_key": {"title": 2}}


def test_dedupe_report_fields_carry_the_dedupe_reference() -> None:
    from types import SimpleNamespace
    from okg.substrate.ingest.runner import dedupe_report_fields as _dedupe_report_fields
    assert _dedupe_report_fields(SimpleNamespace(dedupe_reference="live_branch")) == {
        "dedupe_reference": "live_branch"}
    assert _dedupe_report_fields(SimpleNamespace(dedupe_reference=None)) == {}


def test_every_revision_field_is_disregarded_but_record_ids_still_count() -> None:
    """Any change confined to the revision (a timestamp, a row hash, a run
    id, a null content hash) leaves the content unchanged, so nothing is
    rewritten. Record ids and the policy fingerprint are still compared (see
    test_policy_fingerprint_or_record_id_change_is_not_revision_only_churn)."""
    def with_revision(attrs: dict, revision: dict) -> dict:
        return stamp_attrs_with_source_lineage(
            attrs, source_id="test-source", source_record_id="rec-1",
            source_revision=revision, policy_fingerprint="sha256:policy")
    base = {"run_id": "run-1", "content_hash": "sha256:same", "updated_at": "2026-01-01"}
    stored_n = with_revision({"title": "A"}, base)
    stored_e = with_revision({"w": 1}, base)
    new_n = with_revision({"title": "A"}, {**base, "run_id": "run-2", "updated_at": "2026-02-02"})
    new_e = with_revision({"w": 1}, {**base, "run_id": "run-2", "row_hash": "sha256:rows"})
    kept, node_drops, edge_drops, detail = _run_dedupe(stored_n, stored_e, new_n, new_e, kind="content_hash")
    assert kept == [] and (node_drops, edge_drops) == (1, 1)
    assert detail.revision_only_nodes == 1 and detail.revision_only_edges == 1
    assert detail.kept_by_key == {}
    new_n = with_revision({"title": "A"}, {"run_id": "run-2", "content_hash": None})
    stored_n = with_revision({"title": "A"}, {"run_id": "run-1", "content_hash": None})
    kept, node_drops, _, detail = _run_dedupe(stored_n, stored_e, new_n, dict(stored_e), kind="updated_at")
    assert node_drops == 1 and detail.kept_by_key == {}


def test_revisions_are_stripped_at_every_level_of_the_envelope() -> None:
    """Issue #2308: a repo-wide revision (git files' {tree_sha, head_sha})
    differs in ``source_revisions``, inside every ``source_records`` entry,
    and possibly at the top level of a legacy envelope. None of it is
    content, so the fact is not rewritten; a record id that is really new
    still is."""
    def envelope(tree: str, *, records: list[dict], top_level: bool) -> dict:
        lineage = {
            "schema": "okg.nomos.source_lineage.v1", "status": "ok", "source_ids": ["test-source"],
            "sources": [{
                "source_id": "test-source",
                "source_record_ids": sorted({r["source_record_id"] for r in records}),
                "source_records": records,
                "source_revisions": [{"tree_sha": tree, "head_sha": tree}],
                "policy_fingerprints": ["sha256:policy"],
            }],
        }
        if top_level:
            lineage["source_revision"] = {"tree_sha": tree}
        return {"title": "A", LINEAGE_ATTR: lineage}

    def rec(record_id: str, tree: str) -> dict:
        return {"source_record_id": record_id, "source_revision": {"tree_sha": tree, "head_sha": tree}}

    stored = envelope("t1", records=[rec("f.py", "t1")], top_level=True)
    # Same record id seen under two revisions collapses to one record.
    new = envelope("t2", records=[rec("f.py", "t0"), rec("f.py", "t2")], top_level=True)
    kept, node_drops, edge_drops, detail = _run_dedupe(stored, {"w": 1}, new, {"w": 1}, kind="updated_at")
    assert kept == [] and (node_drops, edge_drops) == (1, 1)
    assert detail.revision_only_nodes == 1
    new = envelope("t2", records=[rec("f.py", "t2"), rec("g.py", "t2")], top_level=False)
    kept, node_drops, _, detail = _run_dedupe(stored, {"w": 1}, new, {"w": 1}, kind="updated_at")
    assert node_drops == 0 and detail.kept_by_key == {LINEAGE_ATTR: 1}


@pytest.mark.parametrize("stored_confidence, new_confidence, new_provenance, expect_key", [
    (1.0, 0.5, "explicit", "edge.confidence"),
    (1.0, 1.0, "derived_deterministic", "edge.provenance"),
])
def test_changed_edge_confidence_or_provenance_is_written(
        stored_confidence, new_confidence, new_provenance, expect_key) -> None:
    """Both live in edge columns, not attrs; the writer treats a change to
    either as a real change, so the dedupe must not drop it."""
    stored_n, stored_e = _stamped({"title": "A"}, run_id="run-1"), _stamped({"w": 1}, run_id="run-1")
    kept, node_drops, edge_drops, detail = _run_dedupe(
        stored_n, stored_e, dict(stored_n), dict(stored_e), kind="updated_at",
        stored_confidence=stored_confidence, new_confidence=new_confidence, new_provenance=new_provenance)
    assert [type(f).__name__ for f in kept] == ["EdgeFact"] and (node_drops, edge_drops) == (1, 0)
    assert detail.kept_by_key == {expect_key: 1}


def test_edge_confidence_is_compared_at_the_stored_real_precision() -> None:
    """``graph_edges.confidence`` is REAL: 0.7 may read back as 0.69999999.
    That is the same stored value, so the edge is not rewritten."""
    stored_n, stored_e = _stamped({"title": "A"}, run_id="run-1"), _stamped({"w": 1}, run_id="run-1")
    kept, _, edge_drops, detail = _run_dedupe(
        stored_n, stored_e, dict(stored_n), dict(stored_e), kind="updated_at",
        stored_confidence=0.699999988079071, new_confidence=0.7)
    assert kept == [] and edge_drops == 1 and detail.kept_by_key == {}


def test_body_text_edit_is_written() -> None:
    """``body_text`` used to be stripped before the comparison, so an edit
    confined to it was never written on a normal run."""
    stored_n = _stamped({"title": "A", "body_text": "old body"}, run_id="run-1")
    stored_e = _stamped({"w": 1}, run_id="run-1")
    new_n = _stamped({"title": "A", "body_text": "new body"}, run_id="run-2")
    kept, node_drops, edge_drops, detail = _run_dedupe(stored_n, stored_e, new_n, dict(stored_e), kind="updated_at")
    assert [type(f).__name__ for f in kept] == ["NodeFact"] and (node_drops, edge_drops) == (0, 1)
    assert detail.kept_by_key == {"body_text": 1, LINEAGE_ATTR + ".source_revision": 1}


@pytest.mark.parametrize("subtype", ["goal", "plain"])
def test_raw_text_edit_is_written_when_the_writer_stores_it(subtype) -> None:
    """A subtype whose searchable text is its raw ``text`` (``goal``) or that
    declares no searchable fields (``plain``) stores ``text`` as given, so an
    edit to it is a content change."""
    stored_n = _stamped({"title": "A", "text": "old words"}, run_id="run-1")
    stored_e = _stamped({"w": 1}, run_id="run-1")
    new_n = _stamped({"title": "A", "text": "new words"}, run_id="run-1")
    kept, node_drops, _, detail = _run_dedupe(
        stored_n, stored_e, new_n, dict(stored_e), kind="updated_at", subtype=subtype, new_subtype=subtype)
    assert [type(f).__name__ for f in kept] == ["NodeFact"] and node_drops == 0
    assert detail.kept_by_key == {"text": 1}


def test_stamped_text_matches_an_unchanged_candidate() -> None:
    """``doc`` derives ``text`` from ``title``. The stored row carries the
    stamped value and the candidate does not; after the writer's own rule is
    applied to the candidate the two are equal, so nothing is written. A
    title edit moves the stamped text and is written."""
    stored_n, stored_e = _stamped({"title": "A"}, run_id="run-1"), _stamped({"w": 1}, run_id="run-1")
    kept, node_drops, _, detail = _run_dedupe(stored_n, stored_e, _stamped({"title": "A"}, run_id="run-2"),
                                              dict(stored_e), kind="updated_at")
    assert kept == [] and node_drops == 1
    kept, node_drops, _, detail = _run_dedupe(stored_n, stored_e, _stamped({"title": "B"}, run_id="run-1"),
                                              dict(stored_e), kind="updated_at")
    assert node_drops == 0 and detail.kept_by_key == {"text": 1, "title": 1}


def test_volatile_keys_are_not_compared() -> None:
    """Issue #1754: ``parsed_at`` is a declared volatile key; a wall-clock
    change to it alone is not a content change."""
    stored_n = _stamped({"title": "A", "parsed_at": "2026-01-01T00:00:00Z"}, run_id="run-1")
    stored_e = _stamped({"w": 1}, run_id="run-1")
    new_n = _stamped({"title": "A", "parsed_at": "2026-09-22T00:00:00Z"}, run_id="run-1")
    session = _StoredSession(_WriterTextRule()._stamp_searchable_text("doc", dict(stored_n), cache={}), stored_e)
    runner = SourceRunner("postgres://unused", source_name="test-source",
                          graph_store=_StoredGraphStore(session))
    runner._volatile_keys_cache = {"doc": frozenset({"parsed_at"})}
    node = NodeFact(node_id="doc:1", subtype="doc", attrs=new_n,
                    source_record_id={"id": "doc:1"}, source_revision={"v": 1})
    kept, node_drops, _ = runner._dedupe_against_chronos_graph([node], candidates_nodes=[node], candidates_edges=[])
    assert kept == [] and node_drops == 1


def test_another_subtypes_volatile_key_is_still_compared() -> None:
    """Issue #2334: ``parsed_at`` is volatile on ``doc`` only. A change
    confined to ``parsed_at`` on a ``ticket`` is a real change and is written."""
    stored_n = _stamped({"title": "A", "parsed_at": "2026-01-01T00:00:00Z"}, run_id="run-1")
    stored_e = _stamped({"w": 1}, run_id="run-1")
    new_n = _stamped({"title": "A", "parsed_at": "2026-09-22T00:00:00Z"}, run_id="run-1")
    session = _StoredSession(
        _WriterTextRule()._stamp_searchable_text("ticket", dict(stored_n), cache={}),
        stored_e,
        subtype="ticket",
    )
    runner = SourceRunner("postgres://unused", source_name="test-source",
                          graph_store=_StoredGraphStore(session))
    runner._volatile_keys_cache = {"doc": frozenset({"parsed_at"})}
    node = NodeFact(node_id="doc:1", subtype="ticket", attrs=new_n,
                    source_record_id={"id": "doc:1"}, source_revision={"v": 1})
    kept, node_drops, _ = runner._dedupe_against_chronos_graph([node], candidates_nodes=[node], candidates_edges=[])
    assert kept == [node] and node_drops == 0
    assert runner._persisted_dedupe_detail.kept_by_key == {"parsed_at": 1}


def test_a_store_without_the_writer_text_rule_is_refused() -> None:
    """The comparison must apply the writer's own text rule; a store that
    cannot is an error, not a silent skip."""
    class _NoRuleStore:
        def __init__(self) -> None:
            self.session = _StoredSession({"title": "A"}, {"w": 1})

        def checkout(self, _branch: str) -> _StoredSession:
            return self.session

    runner = SourceRunner("postgres://unused", source_name="test-source", graph_store=_NoRuleStore())
    runner._volatile_keys_cache = {}
    node = NodeFact(node_id="doc:1", subtype="doc", attrs={"title": "A"},
                    source_record_id={"id": "doc:1"}, source_revision={"v": 1})
    with pytest.raises(SubstrateError, match="searchable text"):
        runner._dedupe_against_chronos_graph([node], candidates_nodes=[node], candidates_edges=[])


def test_json_equality_distinguishes_bool_from_int_and_int_from_float() -> None:
    """Review finding: Python treats True == 1 and 1 == 1.0, JSONB does not."""
    stored_n = _stamped({"flag": True, "n": 1}, run_id="run-1")
    stored_e = _stamped({"w": 1}, run_id="run-1")
    new_n = _stamped({"flag": 1, "n": 1}, run_id="run-2")
    new_e = _stamped({"w": 1.0}, run_id="run-2")
    kept, node_drops, edge_drops, detail = _run_dedupe(stored_n, stored_e, new_n, new_e, kind="content_hash")
    assert len(kept) == 2 and (node_drops, edge_drops) == (0, 0)
    assert detail.kept_by_key == {"flag": 1, "w": 1, LINEAGE_ATTR + ".source_revision": 2}
