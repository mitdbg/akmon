"""Lexical `records` pages collapse exact duplicates (issue #3465, `rank3465 A`).

PACT lexical-duplicate-collapse-3465. A row whose type and served text equal a
higher-ranked row's leaves the list; the kept row lists it in `copies`. The
count, offset paging, the filtered fold's exact prefix and byte-fitted pages
all see the collapsed list. Hidden rows are neither listed nor counted, a
row whose matched text is redacted is not admitted at all (#3377), and a row
served with different text never collapses.

PostgreSQL tests on a published fixture graph, plus database-free unit tests
of the grouping rule. Hiding and redaction run under Nomos `enforce`.
"""
from __future__ import annotations

import json

import psycopg
import pytest

from okg.substrate.library.sources.base import NodeFact
from okg.substrate.mcp import open_session, search
from okg.substrate.mcp.search_profiles import ResolvedScope
from tests.substrate.mcp.paging_mismatch import describe
from tests.substrate.mcp.test_search_records import (
    MAIN,
    MASKED,
    OPEN,
    READER,
    SECRET,
    _activate,
    _allow,
    _edge,
    _node,
    _publish,
)

pytestmark = pytest.mark.postgres

QUERY = "zirconium cladding"
#: One chunk published at three paths (three releases of one repository).
COPIED = "zirconium cladding inspection copied across releases."
#: The own text of two tickets that also match through different comments.
WELD = "zirconium cladding weld ticket text copied."


def _bare(node_id: str, subtype: str, **attrs) -> NodeFact:
    """A node with no `text` attribute: its served text is empty."""
    return NodeFact(
        node_id=node_id, subtype=subtype, attrs=dict(attrs),
        source_record_id={"id": node_id}, source_revision={"v": 1},
    )


def _facts() -> list:
    return [
        _node("note:a", "note", COPIED, path="v1/notes/a.md"),
        _node("note:b", "note", COPIED, path="v2/notes/a.md"),
        _node("note:c", "note", COPIED, path="v3/notes/a.md"),
        _node("note:other", "note", "zirconium cladding inspection, a different note."),
        # The same text under another type: never a copy of the notes.
        _node("ticket:same", "ticket", COPIED, status="open"),
        _node("ticket:same2", "ticket", COPIED, status="open"),
        # Two identical tickets reached only through their (different)
        # comments: they match through a passage, so they never collapse.
        _node("ticket:q1", "ticket", "Quarterly audit ticket.", status="open"),
        _node("ticket:q2", "ticket", "Quarterly audit ticket.", status="open"),
        _node("comment:q1", "comment", f"{QUERY} first sample."),
        _node("comment:q2", "comment", f"{QUERY} second sample retained."),
        _edge("ticket:q1", "comment:q1"),
        _edge("ticket:q2", "comment:q2"),
        # Two tickets whose own text matches and is equal, each also matched
        # by a different comment: ranked through a passage, both stay.
        _node("ticket:w1", "ticket", WELD, status="open"),
        _node("ticket:w2", "ticket", WELD, status="open"),
        _node("comment:w1", "comment", f"{QUERY} weld seam first reading."),
        _node("comment:w2", "comment", f"{QUERY} weld seam second reading, retained."),
        _edge("ticket:w1", "comment:w1"),
        _edge("ticket:w2", "comment:w2"),
        # Two tickets with no text at all: empty served text never collapses.
        _bare("ticket:nt1", "ticket", status="open"),
        _bare("ticket:nt2", "ticket", status="open"),
        _node("comment:nt1", "comment", f"{QUERY} untexted one."),
        _node("comment:nt2", "comment", f"{QUERY} untexted two."),
        _edge("ticket:nt1", "comment:nt1"),
        _edge("ticket:nt2", "comment:nt2"),
    ]


def _deployment(tmp_path, dsn: str) -> str:
    return _publish(tmp_path, dsn, groups={}, main=_facts())


def _search(dsn: str, deployment: str, query: str = QUERY, **kwargs) -> dict:
    with psycopg.connect(dsn, autocommit=True) as conn:
        session = open_session(
            conn, deployment_name=deployment,
            **({"nomos_subject": kwargs.pop("subject")} if "subject" in kwargs else {}),
        )
        return search(conn, session, query, method=kwargs.pop("method", "lexical"), **kwargs)


def _ids(page: dict) -> list[str]:
    return [row["node_id"] for row in page["results"]]


def _copies(page: dict) -> dict[str, list[str]]:
    return {row["node_id"]: row["copies"] for row in page["results"] if "copies" in row}


def _assert_both_stay(ids: list[str], copies: dict[str, list[str]], *pair: str) -> None:
    """Records that matched through a passage neither collapse nor keep copies."""
    assert set(pair) <= set(ids), ids
    assert not set(pair) & set(copies), copies


# ---------- the unfiltered fold ----------

def test_same_text_records_on_different_paths_are_one_row_with_copies(clean_runtime, tmp_path):
    deployment = _deployment(tmp_path, clean_runtime)
    page = _search(clean_runtime, deployment, completeness="exact", limit=50)
    ids, copies = _ids(page), _copies(page)
    assert "note:a" in ids and "note:b" not in ids and "note:c" not in ids
    assert copies["note:a"] == ["note:b", "note:c"]
    # Another type with the same text is its own row; its own copy folds.
    assert "ticket:same" in ids and copies["ticket:same"] == ["ticket:same2"]
    assert "ticket:same2" not in ids
    # Records ranked through a passage stay, whatever their own text.
    _assert_both_stay(ids, copies, "ticket:q1", "ticket:q2")
    _assert_both_stay(ids, copies, "ticket:w1", "ticket:w2")
    # Empty text never collapses; a different text is its own row.
    assert "ticket:nt1" in ids and "ticket:nt2" in ids
    assert "note:other" in ids
    assert set(copies) == {"note:a", "ticket:same"}
    # The count is of rows left, and the page says how many left.
    assert page["duplicates_collapsed"] == 3
    assert page["total_count"] == len(ids) == len(set(ids))
    every = set(ids) | {c for listed in copies.values() for c in listed}
    assert len(every) == len(ids) + 3


def test_the_bounded_default_lists_copies_but_states_no_count(clean_runtime, tmp_path):
    deployment = _deployment(tmp_path, clean_runtime)
    page = _search(clean_runtime, deployment, limit=50)
    assert page["total_count"] is None
    assert page["duplicates_collapsed"] is None
    assert _copies(page)["note:a"] == ["note:b", "note:c"]


def test_offset_paging_neither_repeats_nor_skips_a_row(clean_runtime, tmp_path):
    deployment = _deployment(tmp_path, clean_runtime)
    whole = _search(clean_runtime, deployment, completeness="exact", limit=50)
    seen: list[str] = []
    offset = 0
    while offset is not None:
        page = _search(clean_runtime, deployment, completeness="exact", limit=1,
                       offset=offset)
        assert page["total_count"] == whole["total_count"]
        assert page["duplicates_collapsed"] == whole["duplicates_collapsed"]
        seen += _ids(page)
        offset = page["next_offset"]
        assert len(seen) <= len(_ids(whole))
    assert seen == _ids(whole)
    assert "note:b" not in seen and "note:c" not in seen


def test_self_keeps_every_copy_as_its_own_row(clean_runtime, tmp_path):
    deployment = _deployment(tmp_path, clean_runtime)
    records = _search(clean_runtime, deployment, completeness="exact", limit=50)
    assert "note:b" not in _ids(records)
    page = _search(clean_runtime, deployment, completeness="exact", limit=50,
                   rank_by="self")
    ids = _ids(page)
    assert {"note:a", "note:b", "note:c", "ticket:same", "ticket:same2"} <= set(ids)
    assert not _copies(page)
    assert "duplicates_collapsed" not in page


def test_passages_and_grep_carry_no_copies(clean_runtime, tmp_path):
    deployment = _deployment(tmp_path, clean_runtime)
    filtered = _search(clean_runtime, deployment, completeness="exact", limit=50,
                       filter={"subtype": "ticket"})
    assert _copies(filtered)
    passages = _search(clean_runtime, deployment, completeness="exact", limit=50,
                       filter={"subtype": "ticket"}, rank_by="passages")
    assert not _copies(passages)
    assert "duplicates_collapsed" not in passages
    grep = _search(clean_runtime, deployment, "zirconium", method="grep", limit=50)
    assert not _copies(grep)
    assert "duplicates_collapsed" not in grep


# ---------- the filtered fold ----------

def test_the_filtered_fold_prefix_counts_kept_rows(clean_runtime, tmp_path):
    deployment = _deployment(tmp_path, clean_runtime)
    page = _search(clean_runtime, deployment, completeness="exact", limit=50,
                   filter={"subtype": "ticket"})
    ids, copies = _ids(page), _copies(page)
    assert {row["subtype"] for row in page["results"]} == {"ticket"}
    assert copies["ticket:same"] == ["ticket:same2"] and "ticket:same2" not in ids
    _assert_both_stay(ids, copies, "ticket:q1", "ticket:q2")
    _assert_both_stay(ids, copies, "ticket:w1", "ticket:w2")
    assert "ticket:nt1" in ids and "ticket:nt2" in ids
    assert page["duplicates_collapsed"] == 1
    assert page["total_count"] == len(ids)
    # Every kept row is exactly ordered; the prefix does not count the copies.
    assert page["ordered_exactly_through"] == len(ids)
    # Paging the prefix one row at a time serves the same rows once each.
    seen: list[str] = []
    for offset in range(len(ids)):
        seen += _ids(_search(clean_runtime, deployment, completeness="exact", limit=1,
                             offset=offset, filter={"subtype": "ticket"}))
    assert seen == ids


# ---------- byte-fitted pages ----------

def test_byte_fitted_pages_neither_repeat_nor_skip(clean_runtime, okg_mcp_can_log_in, tmp_path):
    from tests.substrate.mcp.test_search_list import Served

    deployment = _deployment(tmp_path, clean_runtime)
    expected = _ids(_search(clean_runtime, deployment, completeness="exact", limit=50))
    served = Served(clean_runtime, deployment)
    try:
        whole = served.json(query=QUERY, completeness="exact", limit=50, max_bytes=262_144)
        assert _ids(whole) == expected
        assert whole["duplicates_collapsed"] == 3
        assert _copies(whole)["note:a"] == ["note:b", "note:c"]
        seen: list[str] = []
        sizes: list[int] = []
        offset = 0
        while offset is not None:
            page = served.json(query=QUERY, completeness="exact", limit=50,
                               offset=offset, max_bytes=6144)
            assert "results" in page, json.dumps(page)[:600]
            sizes.append(len(page["results"]))
            seen += _ids(page)
            offset = page["next_offset"]
            assert len(sizes) < 50
        assert seen == expected
        assert len(sizes) > 1, "the budget must cut the page for this test to mean anything"
        text = served.text(query=QUERY, completeness="exact", limit=50, max_bytes=262_144)
        assert "identical copies: note:b, note:c" in text
        assert "! 3 exact duplicate rows collapsed" in text
    finally:
        served.close()


# ---------- Nomos enforce: hidden and redacted copies ----------

def _nomos_deployment(tmp_path, dsn: str, rules: list[dict]) -> str:
    groups = {
        OPEN: [
            _node("note:open1", "note", COPIED),
            _node("note:open2", "note", COPIED),
            _node("ticket:q1", "ticket", "Quarterly audit ticket.", status="open"),
            _node("ticket:q2", "ticket", "Quarterly audit ticket.", status="open"),
            _node("comment:q1", "comment", f"{QUERY} first sample."),
            _node("comment:q2", "comment", f"{QUERY} second sample retained."),
        ],
        # Ranks first among the copies (node id order on equal scores): were
        # hidden rows grouped, it would be the kept row.
        SECRET: [_node("note:a-secret", "note", COPIED)],
        MASKED: [_node("note:a-masked", "note", "zirconium cladding masked copy text.")],
    }
    main = [_edge("ticket:q1", "comment:q1"), _edge("ticket:q2", "comment:q2")]
    deployment = _publish(tmp_path, dsn, groups=groups, main=main)
    _activate(dsn, deployment, rules)
    return deployment


def test_under_enforce_a_hidden_copy_is_neither_listed_nor_counted(
    clean_runtime, tmp_path, monkeypatch,
):
    deployment = _nomos_deployment(
        tmp_path, clean_runtime, [_allow(OPEN), _allow(MAIN), _allow(MASKED)],
    )
    monkeypatch.setenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", "1")
    page = _search(clean_runtime, deployment, completeness="exact", limit=50,
                   subject=READER)
    ids, copies = _ids(page), _copies(page)
    assert "note:a-secret" not in json.dumps(page)
    assert copies["note:open1"] == ["note:open2"] and "note:open2" not in ids
    # Records reached through their comments never collapse.
    assert "ticket:q1" in ids and "ticket:q2" in ids
    assert "ticket:q1" not in copies and "ticket:q2" not in copies
    # Exact and uncapped, so the count stands under enforce: the hidden copy
    # is in neither number.
    assert page["total_count_basis"] == "exact"
    assert page["duplicates_collapsed"] == 1
    assert page["total_count"] == len(ids)


def test_under_enforce_a_copy_whose_text_is_redacted_is_dropped_not_collapsed(
    clean_runtime, tmp_path, monkeypatch,
):
    """Issue #3377: a row matched only through text the reader may not read
    as stored (`free_text` under a redacting rule) is not admitted, so it is
    neither listed nor kept as anyone's copy; the readable copies still
    collapse. (Before #3377 the masked copy was served with redacted text.)"""
    rules = [_allow(OPEN), _allow(MAIN), {
        "rule_id": "redact_masked", "effect": "redact", "actions": ["graph.read.mcp"],
        "subjects": [READER], "match": {"source_id": MASKED}, "priority": 100,
        "obligations": [{"type": "redaction"}, {"type": "audit"}],
    }]
    # The masked note holds the copied text too, so its stored text_hash
    # equals the others'; only its served text differs.
    deployment = _publish(tmp_path, clean_runtime, groups={
        OPEN: [_node("note:open1", "note", COPIED), _node("note:open2", "note", COPIED)],
        MASKED: [_node("note:a-masked", "note", COPIED)],
    }, main=[])
    _activate(clean_runtime, deployment, rules)
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        conn.execute(
            "UPDATE okg.subtypes SET metadata = coalesce(metadata, '{}'::jsonb)"
            " || '{\"pii_classes\": {\"text\": \"free_text\"}}'::jsonb"
            " WHERE name = 'note'"
        )
    monkeypatch.setenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", "1")
    page = _search(clean_runtime, deployment, completeness="exact", limit=50,
                   subject=READER, select=["node_id", "subtype", "attrs"])
    ids, copies = _ids(page), _copies(page)
    assert "note:a-masked" not in ids
    assert "note:a-masked" not in json.dumps(copies)
    assert copies["note:open1"] == ["note:open2"] and "note:open2" not in ids


# ---------- the grouping rule, database-free ----------

def _row(node_id: str, text, *, subtype: str = "note", text_hash="h1") -> dict:
    return {"node_id": node_id, "subtype": subtype, "text_hash": text_hash,
            "attrs": {} if text is None else {"text": text}}


def test_unhashed_empty_cross_type_and_differing_text_never_collapse():
    from okg.substrate.mcp.search import _collapse_exact_duplicates

    rows = [
        _row("a", "same"), _row("b", "same"),
        _row("n1", "same", text_hash=None), _row("n2", "same", text_hash=None),
        _row("e1", ""), _row("e2", ""), _row("w1", "  "), _row("w2", "  "),
        _row("x1", None), _row("x2", None),
        _row("t1", "same", subtype="ticket"),
        # Same stored hash, different served text (a mask or an overlay).
        _row("m", "[redacted]"),
    ]
    kept, copies = _collapse_exact_duplicates(rows, self_read=None)
    assert [row["node_id"] for row in kept] == [
        "a", "n1", "n2", "e1", "e2", "w1", "w2", "x1", "x2", "t1", "m",
    ]
    assert copies == {"a": ["b"]}


def test_the_kept_row_is_the_first_in_rank_order_and_order_is_kept():
    from okg.substrate.mcp.search import _collapse_exact_duplicates

    rows = [_row("z", "one"), _row("y", "two", text_hash="h2"), _row("a", "one"),
            _row("b", "two", text_hash="h2"), _row("c", "one")]
    kept, copies = _collapse_exact_duplicates(rows, self_read=None)
    assert [row["node_id"] for row in kept] == ["z", "y"]
    assert copies == {"z": ["a", "c"], "y": ["b"]}


def test_a_row_that_matched_through_a_passage_never_takes_part():
    from okg.substrate.mcp.record_fold import MATCHING_KEY, WINNER_KEY
    from okg.substrate.mcp.search import _collapse_exact_duplicates

    winner = {"node_id": "p", "attrs": {"text": "passage"}}
    rows = [
        {**_row("r1", "same"), WINNER_KEY: winner, MATCHING_KEY: 1},
        {**_row("r2", "same"), WINNER_KEY: winner, MATCHING_KEY: 1},
        {**_row("r3", "same"), WINNER_KEY: None, MATCHING_KEY: 2},
        {**_row("k1", "same"), WINNER_KEY: None, MATCHING_KEY: 0},
        {**_row("k2", "same"), WINNER_KEY: None, MATCHING_KEY: 0},
        # Not read row by row yet: never compared.
        _row("u1", "same"),
    ]
    kept, copies = _collapse_exact_duplicates(
        rows, self_read={"r1", "r2", "r3", "k1", "k2"},
    )
    assert [row["node_id"] for row in kept] == ["r1", "r2", "r3", "k1", "u1"]
    assert copies == {"k1": ["k2"]}


# ---------- review follow-ups ----------

def test_a_record_ranked_through_a_passage_keeps_its_row_in_both_folds(
    clean_runtime, tmp_path,
):
    deployment = _deployment(tmp_path, clean_runtime)
    for kwargs in ({}, {"filter": {"subtype": "ticket"}}):
        page = _search(clean_runtime, deployment, completeness="exact", limit=50, **kwargs)
        ids, copies = _ids(page), _copies(page)
        rows = {row["node_id"]: row for row in page["results"]}
        # Each folded its own comment (the comment may or may not beat the
        # record's own text, so the winner may be named or not).
        for record in ("ticket:w1", "ticket:w2"):
            assert rows[record]["matching_passages"] == 1, (kwargs, rows[record])
        _assert_both_stay(ids, copies, "ticket:w1", "ticket:w2")
        # The tickets that matched on their own text alone still collapse.
        assert copies["ticket:same"] == ["ticket:same2"], kwargs


def test_twelve_copies_list_the_first_ten_and_count_them_all(clean_runtime, tmp_path):
    text = "zirconium cladding repeated release note."
    few = "hafnium cladding repeated release note."
    facts = [_node(f"note:r{i:02d}", "note", text, path=f"v{i}/note.md") for i in range(13)]
    facts += [_node(f"note:h{i:02d}", "note", few, path=f"v{i}/h.md") for i in range(11)]
    deployment = _publish(tmp_path, clean_runtime, groups={}, main=facts)
    page = _search(clean_runtime, deployment, "zirconium", completeness="exact", limit=50)
    (row,) = page["results"]
    assert row["node_id"] == "note:r00"
    assert row["copies"] == [f"note:r{i:02d}" for i in range(1, 11)]
    assert row["copies_total"] == 12
    assert page["duplicates_collapsed"] == 12 and page["total_count"] == 1
    # Under any `select`, both stay.
    narrow = _search(clean_runtime, deployment, "zirconium", completeness="exact",
                     limit=50, select=["node_id"])
    assert narrow["results"][0]["copies_total"] == 12
    # Ten copies are all listed and carry no total.
    (ten,) = _search(clean_runtime, deployment, "hafnium", completeness="exact",
                     limit=50)["results"]
    assert ten["copies"] == [f"note:h{i:02d}" for i in range(1, 11)]
    assert "copies_total" not in ten


def test_under_audit_own_text_copies_collapse_and_passage_matches_stay(
    clean_runtime, tmp_path, monkeypatch,
):
    deployment = _deployment(tmp_path, clean_runtime)
    _activate(clean_runtime, deployment, [_allow(MAIN)])
    monkeypatch.setenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", "audit")
    page = _search(clean_runtime, deployment, completeness="exact", limit=50,
                   subject=READER)
    ids, copies = _ids(page), _copies(page)
    assert copies["note:a"] == ["note:b", "note:c"]
    assert copies["ticket:same"] == ["ticket:same2"]
    _assert_both_stay(ids, copies, "ticket:q1", "ticket:q2")
    _assert_both_stay(ids, copies, "ticket:w1", "ticket:w2")
    assert page["duplicates_collapsed"] == 3
    assert page["total_count"] == len(ids)


def test_the_collapse_runs_before_max_per_parent(clean_runtime, tmp_path):
    """A copy must not take its parent's only slot.

    `search()` never applies a per-parent bound under `records` (an argument
    bound is refused, a profile bound dropped), so this drives the lexical
    path directly with one, on a passage-typed records page: c1 (parent P1)
    and d1 (parent P2) share their text, and d2 (P2) ranks below d1. Run
    after the bound, d1 would take P2's slot, suppress d2 and then collapse
    into c1, so P2 would have no row; run first, d2 is served."""
    from okg.substrate.mcp.search import RANK_BY_RECORDS, _search_lexical

    facts = [
        _node("ticket:p1", "ticket", "First parent.", status="open"),
        _node("ticket:p2", "ticket", "Second parent.", status="open"),
        _node("comment:c1", "comment", f"{QUERY} {QUERY} shared reading."),
        _node("comment:d1", "comment", f"{QUERY} {QUERY} shared reading."),
        _node("comment:d2", "comment", f"{QUERY} a lone reading of another kind entirely."),
        _edge("ticket:p1", "comment:c1"),
        _edge("ticket:p2", "comment:d1"),
        _edge("ticket:p2", "comment:d2"),
    ]
    deployment = _publish(tmp_path, clean_runtime, groups={}, main=facts)
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        session = open_session(conn, deployment_name=deployment)
        page = _search_lexical(
            conn, session, QUERY, filter={"subtype": "comment"}, limit=10, offset=0,
            scope=ResolvedScope(max_per_parent=1, max_per_parent_origin="argument"),
            rank_by=RANK_BY_RECORDS, completeness="exact",
        )
    ids = _ids(page)
    assert page["diversity_applied"] is True
    assert ids == ["comment:c1", "comment:d2"]
    assert _copies(page) == {"comment:c1": ["comment:d1"]}
    assert page["duplicates_collapsed"] == 1
    # "Before diversity" is after the collapse: the copy is in neither count.
    assert page["total_count_before_diversity"] == 2
    assert page["suppressed_sibling_count"] == 0
    assert page["total_count"] == 2


def test_a_lower_bound_filtered_fold_remaps_its_exact_prefix(clean_runtime, tmp_path):
    from okg.substrate.mcp.record_fold import (
        NEXT_OFFSET_REASON_BEYOND_EXACT_ORDER,
        RecordsOffsetBeyondExactOrder,
    )
    from tests.substrate.mcp.test_search_records import _bulk_facts

    groups, main = _bulk_facts(hidden_comments=False)
    # Two own-text copies that rank inside the exact prefix.
    groups[OPEN] = groups[OPEN] + [
        _node("ticket:own:copy:a", "ticket", f"{QUERY} {QUERY} own copy.", status="open"),
        _node("ticket:own:copy:b", "ticket", f"{QUERY} {QUERY} own copy.", status="open"),
    ]
    deployment = _publish(tmp_path, clean_runtime, groups=groups, main=main)
    filt = {"subtype": "ticket"}
    first = _search(clean_runtime, deployment, completeness="exact", limit=10, filter=filt)
    assert first["total_count_basis"] == "lower_bound"
    prefix = first["ordered_exactly_through"]
    assert 0 < prefix < first["total_count"]
    whole = _search(clean_runtime, deployment, completeness="exact", limit=prefix, filter=filt)
    ids = _ids(whole)
    assert len(ids) == prefix
    assert "ticket:own:copy:a" in ids and "ticket:own:copy:b" not in ids
    assert _copies(whole)["ticket:own:copy:a"] == ["ticket:own:copy:b"]
    # The remap itself: with the collapse switched off, the same exact prefix
    # holds one more row (copy b), and every other row in the same order.
    import importlib

    search_module = importlib.import_module("okg.substrate.mcp.search")
    with pytest.MonkeyPatch.context() as patch:
        patch.setattr(search_module, "_collapse_exact_duplicates",
                      lambda rows, *, self_read: (rows, {}))
        raw = _search(clean_runtime, deployment, completeness="exact", limit=10, filter=filt)
        raw_whole = _search(clean_runtime, deployment, completeness="exact",
                            limit=raw["ordered_exactly_through"], filter=filt)
    assert raw["ordered_exactly_through"] == prefix + 1
    assert [i for i in _ids(raw_whole) if i != "ticket:own:copy:b"] == ids
    # The last exact row is served at offset prefix - 1, and no row past it.
    last = _search(clean_runtime, deployment, completeness="exact", limit=1,
                   offset=prefix - 1, filter=filt)
    assert _ids(last) == ids[-1:]
    assert last["next_offset"] is None
    assert last["next_offset_reason"] == NEXT_OFFSET_REASON_BEYOND_EXACT_ORDER
    with pytest.raises(RecordsOffsetBeyondExactOrder):
        _search(clean_runtime, deployment, completeness="exact", limit=1, offset=prefix,
                filter=filt)
    # Paging by ten reaches the same rows once each.
    pages = [
        _search(clean_runtime, deployment, completeness="exact", limit=10, offset=offset,
                filter=filt)
        for offset in range(0, prefix, 10)
    ]
    seen = [node_id for page in pages for node_id in _ids(page)]
    assert seen == ids, describe(pages, whole, note=f"offset pages of ten vs limit={prefix}")


def test_the_phrase_block_checks_the_collapsed_rows(clean_runtime, tmp_path):
    deployment = _deployment(tmp_path, clean_runtime)
    page = _search(clean_runtime, deployment, '"copied across releases"',
                   completeness="exact", limit=50)
    ids = _ids(page)
    assert _copies(page)["note:a"] == ["note:b", "note:c"]
    phrase = page["phrase"]
    assert phrase["checked"] == len(ids) == page["total_count"]
    assert phrase["matched"] == sum(1 for row in page["results"] if row["phrase_match"])
    # note:a and ticket:same hold the phrase; their copies are not counted.
    assert {"note:a", "ticket:same"} <= {
        row["node_id"] for row in page["results"] if row["phrase_match"]
    }
    assert phrase["matched"] == 2
