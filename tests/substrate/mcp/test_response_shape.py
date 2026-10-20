"""Unit rules of the compact MCP wire shape (issue #2674).

The end-to-end proof on a real Postgres, with measured bytes, lives in
tests/substrate/mcp/test_search_context_budget.py. These cases need no
database: they pin the edges of each rule on hand-built payloads.
"""
from __future__ import annotations

import copy
import inspect
import json

from okg.substrate.mcp.provenance import FENCE_CLOSE, FENCE_OPEN
from okg.substrate.mcp.response_shape import (
    LINEAGE_SELECT_TOKEN,
    RESPONSE_SHAPE,
    compact_for_wire,
    lineage_select_plan,
    move_lineage_to_rows,
)
from okg.substrate.nomos.source_lineage import LINEAGE_ATTR


def _fenced(value: str, source: str = "src:x") -> str:
    return f"{FENCE_OPEN.format(source=source)}\n{value}\n{FENCE_CLOSE}"


def _lineage(status: str) -> dict:
    return {"schema": "okg.nomos.source_lineage.v1", "status": status,
            "source_ids": ["src:x"]}


def _row(node_id: str, **extra: object) -> dict:
    row = {
        "node_id": node_id,
        "canonical_node_id": node_id,
        "subtype": "chunk",
        "method": "lexical",
        "match_kind": "lexical",
        "source_family": None,
        "evidence_kind": "unknown",
        "evidence_kind_provenance": {
            "basis": "unclassified", "subtype": "chunk", "source": "src:x",
        },
        "provenance": {"source": "src:x", "trust": "untrusted"},
        "source_revision": {"v": 1},
        "rollup_best_chunk_id": None,
    }
    row.update(extra)
    return row


def _page(rows: list[dict], **extra: object) -> dict:
    page = {
        "operator_name": "search",
        "method": "lexical",
        "rank_by": "self",
        "results": rows,
        "pinned_generation_id": "g1",
        "generation_id": "g1",
        "session_default_generation_id": "g1",
        "logical_session_id": None,
        "next_offset": None,
        "max_per_parent": None,
        "pagination": {"truncated": True},
    }
    page.update(extra)
    return page


def test_compaction_never_mutates_its_input() -> None:
    page = _page([_row("a"), _row("b")])
    before = copy.deepcopy(page)
    out = compact_for_wire(page, {})
    assert page == before
    assert out is not page
    assert out["row_defaults"] == {"match_kind": "lexical", "source_family": None}
    assert all("match_kind" not in row for row in out["results"])
    assert all("match_kind" in row for row in page["results"])


def test_only_the_listed_nulls_and_equal_generation_ids_go() -> None:
    out = compact_for_wire(_page([_row("a")]), {})
    assert "max_per_parent" not in out
    assert "pagination" not in out
    assert "generation_id" not in out
    assert "session_default_generation_id" not in out
    # Promised explicit nulls stay.
    assert out["logical_session_id"] is None
    assert out["next_offset"] is None
    assert out["response_shape"] == RESPONSE_SHAPE


def test_a_differing_generation_id_is_kept() -> None:
    out = compact_for_wire(
        _page([_row("a")], session_default_generation_id="g0"), {},
    )
    assert out["session_default_generation_id"] == "g0"


def test_one_row_or_differing_values_hoist_nothing() -> None:
    assert "row_defaults" not in compact_for_wire(_page([_row("a")]), {})
    mixed = _page([_row("a"), _row("b", match_kind="exact_id", source_family="docs")])
    out = compact_for_wire(mixed, {})
    assert "row_defaults" not in out
    assert [row["match_kind"] for row in out["results"]] == ["lexical", "exact_id"]


def test_alias_rows_keep_match_kind_on_every_row() -> None:
    out = compact_for_wire(_page([_row("a"), _row("b")], method="alias"), {})
    assert out["row_defaults"] == {"source_family": None}
    assert all(row["match_kind"] == "lexical" for row in out["results"])


def test_identity_floor_and_trust_stay_on_every_row() -> None:
    out = compact_for_wire(_page([_row("a"), _row("b")]), {})
    for row in out["results"]:
        assert row["evidence_kind"] == "unknown"
        assert row["evidence_kind_provenance"] == {"basis": "unclassified"}
        assert row["provenance"] == {"source": "src:x", "trust": "untrusted"}
        assert row["source_revision"] == {"v": 1}
        assert "canonical_node_id" not in row
        assert "method" not in row
        assert "rollup_best_chunk_id" not in row


def test_a_differing_canonical_id_or_method_is_kept() -> None:
    row = _row("a", canonical_node_id="b", method="semantic")
    out = compact_for_wire(_page([row]), {})["results"][0]
    assert out["canonical_node_id"] == "b"
    assert out["method"] == "semantic"


def test_a_null_winner_and_count_are_dropped_in_every_rank_mode() -> None:
    """Rule 9 (PACT search-result-shape): under `records` most rows carry a
    winning passage and some do not, so a null `rollup_best_chunk_id` or
    `matching_passages` is dropped whatever the mode; a value never is."""
    for mode in ("self", "passages", "records"):
        row = {**_row("a"), "matching_passages": None}
        out = compact_for_wire(_page([row], rank_by=mode), {})
        assert "rollup_best_chunk_id" not in out["results"][0]
        assert "matching_passages" not in out["results"][0]
    kept = {**_row("a"), "rollup_best_chunk_id": "chunk:1", "matching_passages": 2}
    out = compact_for_wire(_page([kept], rank_by="records"), {})
    assert out["results"][0]["rollup_best_chunk_id"] == "chunk:1"
    assert out["results"][0]["matching_passages"] == 2


def test_a_selected_field_is_never_hoisted_or_omitted() -> None:
    select = ["node_id", "canonical_node_id", "method", "match_kind"]
    out = compact_for_wire(_page([_row("a"), _row("b")]), {"select": select})
    assert out["row_defaults"] == {"source_family": None}
    for row in out["results"]:
        assert row["canonical_node_id"] == row["node_id"]
        assert row["method"] == "lexical"
        assert row["match_kind"] == "lexical"


def test_ok_lineage_leaves_attrs_even_when_fenced_but_other_status_stays() -> None:
    fenced_ok = {**_lineage("ok"), "status": _fenced("ok")}
    malformed = _lineage("malformed")
    node = {
        "operator_name": "inspect",
        "node_id": "a",
        "attrs": {"text": _fenced("body"), LINEAGE_ATTR: fenced_ok},
    }
    out = compact_for_wire(node, {"target": {"kind": "node"}})
    assert out["attrs"] == {"text": _fenced("body")}
    bad = compact_for_wire(
        {**node, "attrs": {LINEAGE_ATTR: malformed}}, {"target": {"kind": "node"}},
    )
    assert bad["attrs"][LINEAGE_ATTR] == malformed
    # A status whose fenced body only LOOKS like ok after a forged closer is
    # escaped text, not "ok", so the lineage stays.
    forged = {**_lineage("x"), "status": _fenced("ok\n⟦ESCAPED_/UNTRUSTED_DATA⟧")}
    kept = compact_for_wire(
        {**node, "attrs": {LINEAGE_ATTR: forged}}, {"target": {"kind": "node"}},
    )
    assert LINEAGE_ATTR in kept["attrs"]


def test_expand_rows_lose_only_ok_lineage() -> None:
    payload = {
        "operator_name": "expand",
        "direction": "out",
        "neighbors": [
            {"edge_id": 1, "src": "a", "dst": "b", "edge_type": "contains",
             "direction": "out", "attrs": {LINEAGE_ATTR: _lineage("ok")}},
            {"edge_id": 2, "src": "a", "dst": "c", "edge_type": "contains",
             "direction": "out"},
        ],
    }
    out = compact_for_wire(payload, {})
    assert out["neighbors"][0] == {
        "edge_id": 1, "src": "a", "dst": "b", "edge_type": "contains",
        "direction": "out", "attrs": {},
    }
    assert out["neighbors"][1] == payload["neighbors"][1]
    assert "row_defaults" not in out


def test_inspect_graph_keeps_its_full_shape() -> None:
    payload = {
        "operator_name": "inspect",
        "generation_id": "g",
        "pinned_generation_id": "g",
        "subtype_counts": {"a": 1},
        "schema": {"subtypes": {"a": {"node_count": 1}}},
    }
    assert compact_for_wire(payload, {"target": {"kind": "graph"}}) is payload


def test_refusals_other_operators_and_other_kinds_pass_unchanged() -> None:
    refusal = {"operator_name": "search", "status": "refused",
               "error": {"code": "x"}, "generation_id": "g", "pinned_generation_id": "g"}
    assert compact_for_wire(refusal, {}) is refusal
    rows = {"operator_name": "filter", "items": [], "generation_id": "g",
            "pinned_generation_id": "g"}
    assert compact_for_wire(rows, {}) is rows
    session = {"operator_name": "inspect", "generation_id": "g",
               "pinned_generation_id": "g"}
    assert compact_for_wire(session, {"target": {"kind": "session"}}) is session


def test_compaction_is_idempotent() -> None:
    once = compact_for_wire(_page([_row("a"), _row("b")]), {})
    assert compact_for_wire(once, {}) == once


# ── trusted pages state one `source_revision` (mcp-search-compact-results) ──
#
# The fixture copies the shape of a real `search` row served from the cms-kb
# graph on 2026-09-28 (one trusted source, TWiki chunks). Every row repeated
# the same 235-byte `source_revision`.

_CMS_REVISION = {
    "run_id": "253337af-6e30-5c65-9198-c4d9348d9a1e",
    "eos_root": "/ceph/submit/data/user/p/pmlugato/cms-kb/twiki",
    "n_records": 15762,
    "content_hash": "2ac065c20c4fe2d3f89442dd05221ec063013b79f14050547e5c6081fa79e864",
}
_CMS_SOURCE = "lane-a-cmskb.twiki-eos"


def _cms_row(idx: int, *, trust: str = "trusted") -> dict:
    return {
        "node_id": f"chunk:{idx:016x}",
        "canonical_node_id": f"chunk:{idx:016x}",
        "subtype": "document_chunk",
        "excerpt": ("...set up CMSSW, run cmsenv and scram b, then " * 7)[:277] + "...",
        "score": -25.681170225143433 + idx,
        "method": "lexical",
        "match_kind": "lexical",
        "source_record_id": {"path": f"Topic{idx}.txt", "chunk_index": idx % 4},
        "source_revision": dict(_CMS_REVISION),
        "rollup_best_chunk_id": None,
        "evidence_kind": "unknown",
        "evidence_kind_provenance": {
            "basis": "unclassified", "subtype": "document_chunk",
            "source": _CMS_SOURCE,
        },
        "provenance": {"source": _CMS_SOURCE, "trust": trust},
        "source_family": None,
    }


def _wire_bytes(payload: dict) -> int:
    return len(json.dumps(payload, default=str).encode("utf-8"))


def test_trusted_page_states_source_revision_once_and_rebuilds_exactly() -> None:
    page = _page([_cms_row(i) for i in range(10)])
    out = compact_for_wire(page, {})
    assert out["row_defaults"]["source_revision"] == _CMS_REVISION
    assert all("source_revision" not in row for row in out["results"])
    # Citation identity stays on every row.
    for row, full in zip(out["results"], page["results"]):
        assert row["node_id"] == full["node_id"]
        assert row["source_record_id"] == full["source_record_id"]
        assert row["provenance"] == full["provenance"]
    assert out["pinned_generation_id"] == "g1"
    # Reversible by rule 1: a missing row field takes the `row_defaults` value.
    rebuilt = [{**out["row_defaults"], **row} for row in out["results"]]
    assert [row["source_revision"] for row in rebuilt] == [_CMS_REVISION] * 10
    # Before this change the ten-row page compacted to 8,716 bytes (10,860
    # uncompacted); stating the revision once brings it to 6,583.
    assert _wire_bytes(out) <= 7_000


def test_one_untrusted_row_keeps_source_revision_on_every_row() -> None:
    rows = [_cms_row(i) for i in range(9)] + [_cms_row(9, trust="untrusted")]
    out = compact_for_wire(_page(rows), {})
    assert "source_revision" not in out["row_defaults"]
    assert all(row["source_revision"] == _CMS_REVISION for row in out["results"])


def test_a_missing_trust_label_or_differing_revision_hoists_nothing() -> None:
    unlabeled = [_cms_row(i) for i in range(3)]
    unlabeled[1] = {**unlabeled[1], "provenance": {"source": _CMS_SOURCE}}
    out = compact_for_wire(_page(unlabeled), {})
    assert all("source_revision" in row for row in out["results"])
    differing = [_cms_row(i) for i in range(3)]
    differing[2] = {**differing[2], "source_revision": {**_CMS_REVISION, "n_records": 1}}
    out = compact_for_wire(_page(differing), {})
    assert "source_revision" not in out["row_defaults"]
    assert all("source_revision" in row for row in out["results"])


def test_a_selected_source_revision_stays_on_every_trusted_row() -> None:
    page = _page([_cms_row(i) for i in range(3)])
    out = compact_for_wire(page, {"select": ["node_id", "source_revision"]})
    assert "source_revision" not in out.get("row_defaults", {})
    assert all(row["source_revision"] == _CMS_REVISION for row in out["results"])


def test_mcp_search_resolves_an_omitted_limit_to_eight_cards(monkeypatch) -> None:
    """A call with no `limit` returns eight cards (ten before PACT
    read-reply-size-3666), not fifty.

    PACT read-model-s4-list: the schema no longer publishes `default: 10`,
    because the SDK fills a published default into every call and an
    explicit 10 could then not be told from an omitted limit, which means
    100 for `view="list"`. The omitted value is resolved per view instead.
    """
    import anyio

    from okg.substrate.mcp import server as server_mod
    from okg.substrate.mcp.surface_budget import PROBE_DSN

    monkeypatch.setenv("OKG_MCP_SURFACE_PROBE", "1")
    monkeypatch.setattr(
        server_mod, "_server_dsn_resolution",
        lambda _dsn, _name: {"dsn": PROBE_DSN, "authority": {"source": "surface_probe_stub"}},
    )
    mcp = server_mod.build_server(dsn=PROBE_DSN, deployment_name="default")
    try:
        tools = anyio.run(mcp.list_tools)
        # The callable the SDK invokes, not only the published schema: a call
        # that omits `limit` binds this default.
        registered = mcp._tool_manager.get_tool("search").fn
    finally:
        mcp._okg_pool.close()
    search_tool = next(tool for tool in tools if tool.name == "search")
    from okg.substrate.mcp.search_views import default_search_limit

    assert server_mod.MCP_SEARCH_DEFAULT_LIMIT == 8
    assert "default" not in search_tool.inputSchema["properties"]["limit"]
    assert inspect.signature(registered).parameters["limit"].default is None
    assert default_search_limit(None) == default_search_limit("cards") == 8
    assert default_search_limit("list") == 100


def test_lineage_select_plan() -> None:
    assert lineage_select_plan(None) == (None, False, False, True)
    assert lineage_select_plan(["node_id"]) == (["node_id"], False, False, False)
    lexical_default = ("node_id", "excerpt")
    assert lineage_select_plan(
        [LINEAGE_SELECT_TOKEN], default_without_attrs=lexical_default,
    ) == (["node_id", "excerpt", "attrs"], True, True, True)
    assert lineage_select_plan([LINEAGE_SELECT_TOKEN]) == (None, True, False, True)
    assert lineage_select_plan(["node_id", LINEAGE_SELECT_TOKEN]) == (
        ["node_id", "attrs"], True, True, False,
    )
    assert lineage_select_plan(["attrs", LINEAGE_SELECT_TOKEN]) == (
        ["attrs"], True, False, False,
    )


def test_move_lineage_to_rows() -> None:
    lineage = _lineage("ok")
    payload = {"results": [
        {"node_id": "a", "attrs": {"text": "t", LINEAGE_ATTR: lineage}},
        {"node_id": "b", "attrs": {"text": "u"}},
    ]}
    before = copy.deepcopy(payload)
    moved = move_lineage_to_rows(payload, drop_attrs=True)
    assert payload == before
    assert moved["results"] == [
        {"node_id": "a", LINEAGE_SELECT_TOKEN: lineage},
        {"node_id": "b", LINEAGE_SELECT_TOKEN: None},
    ]
    kept = move_lineage_to_rows(payload, drop_attrs=False)
    assert kept["results"][0]["attrs"] == {"text": "t"}
    node = move_lineage_to_rows(
        {"node_id": "a", "attrs": {LINEAGE_ATTR: lineage}},
        drop_attrs=False, single_row=True,
    )
    assert node == {"node_id": "a", "attrs": {}, LINEAGE_SELECT_TOKEN: lineage}


# ── parent summaries once per page (issue #2675) ──────────────────────────


def _parent(node_id: str, state: str = "resolved", title: str = "T") -> dict:
    return {
        "state": state,
        "node_id": node_id,
        "citation": {"label": _fenced(f"JIRA {node_id}")},
        "citation_source": "producer_attrs:citation_label",
        "title": _fenced(title),
        "title_attr": "title",
        "status": _fenced("Open"),
        "passage_count": 3,
    }


_SUMMARY_KEYS = {
    "citation", "citation_source", "title", "title_attr", "status", "passage_count",
}


def test_a_shared_parent_summary_is_sent_once_and_fences_move_unchanged() -> None:
    page = _page([
        _row("a", parent=_parent("p1")),
        _row("b", parent=_parent("p1")),
        _row("c", parent=_parent("p2")),
    ])
    before = copy.deepcopy(page)
    out = compact_for_wire(page, {})
    assert page == before
    assert set(out["parent_summaries"]) == {"p1", "p2"}
    assert out["parent_summaries"]["p1"] == {
        key: value for key, value in _parent("p1").items() if key in _SUMMARY_KEYS
    }
    for row, parent_id in zip(out["results"], ("p1", "p1", "p2")):
        assert row["parent"] == {"state": "resolved", "node_id": parent_id}


def test_candidates_of_multiple_parents_share_the_page_entries() -> None:
    multiple = {
        "state": "multiple_parents",
        "node_id": None,
        "citation": None,
        "citation_source": None,
        "parents": [_parent("p1", "candidate"), _parent("p2", "candidate")],
    }
    out = compact_for_wire(
        _page([_row("a", parent=multiple), _row("b", parent=_parent("p1"))]), {},
    )
    assert set(out["parent_summaries"]) == {"p1", "p2"}
    outer = out["results"][0]["parent"]
    # The outer block resolved nothing and names no parent: it keeps its nulls.
    assert outer["state"] == "multiple_parents"
    assert outer["citation"] is None
    assert outer["parents"] == [
        {"state": "candidate", "node_id": "p1"},
        {"state": "candidate", "node_id": "p2"},
    ]


def test_a_differing_summary_for_the_same_parent_stays_on_its_card() -> None:
    out = compact_for_wire(
        _page([
            _row("a", parent=_parent("p1", title="first")),
            _row("b", parent=_parent("p1", title="second")),
        ]),
        {},
    )
    assert out["parent_summaries"]["p1"]["title"] == _fenced("first")
    assert out["results"][0]["parent"] == {"state": "resolved", "node_id": "p1"}
    assert out["results"][1]["parent"]["title"] == _fenced("second")
    assert _SUMMARY_KEYS <= set(out["results"][1]["parent"])


def test_a_selected_parent_and_unresolved_states_keep_their_blocks() -> None:
    rows = [_row("a", parent=_parent("p1")), _row("b", parent=_parent("p1"))]
    selected = compact_for_wire(_page(rows), {"select": ["node_id", "parent"]})
    assert "parent_summaries" not in selected
    assert all(_SUMMARY_KEYS <= set(row["parent"]) for row in selected["results"])

    unlinked = {
        "state": "declared_but_unlinked",
        "node_id": None,
        "citation": None,
        "citation_source": None,
        "reason": "r",
    }
    out = compact_for_wire(
        _page([_row("a", parent=unlinked), _row("b", parent=None)]), {},
    )
    assert "parent_summaries" not in out
    assert out["results"][0]["parent"] == unlinked
    assert out["results"][1]["parent"] is None


def test_a_partial_summary_block_stays_whole_and_is_never_merged() -> None:
    partial = {
        key: value for key, value in _parent("p1").items() if key != "passage_count"
    }
    out = compact_for_wire(
        _page([_row("a", parent=partial), _row("b", parent=_parent("p1"))]), {},
    )
    # The partial block neither feeds the page entry nor loses its fields.
    assert out["results"][0]["parent"] == partial
    assert out["parent_summaries"]["p1"]["passage_count"] == 3
    assert out["results"][1]["parent"] == {"state": "resolved", "node_id": "p1"}
    only_partial = compact_for_wire(_page([_row("a", parent=partial), _row("b")]), {})
    assert "parent_summaries" not in only_partial
