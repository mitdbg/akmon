"""`rank_by="records"`: PACT search-result-shape (issues #3069, #3070, #3071).

PostgreSQL tests on a published fixture graph. Each folding rule of DESIGN.md
D1, the filtered two-read fold of D3-i with decision 5 = 5b, the count basis
of D2 and the excerpt window of D4, each with its failure case.

Hiding and redaction run under Nomos `enforce`: `audit` never hides a row, so
a test of hiding under `audit` could not fail. `audit` is used only for the
decision-row assertions.
"""
from __future__ import annotations

import math
import uuid

import psycopg
import pytest

from okg.substrate.library.sources.base import EdgeFact, NodeFact
from okg.substrate.mcp import open_session, search
from okg.substrate.mcp.passage_rollup import resolve_edge_segment
from okg.substrate.mcp.record_fold import (
    NEXT_OFFSET_REASON_BEYOND_EXACT_ORDER,
    RecordsOffsetBeyondExactOrder,
    SearchFoldReadCheckMismatch,
    cutoff,
    exact_prefix_length,
)
from okg.substrate.mcp.result_diversity import DiversityNotApplicable
from okg.substrate.mcp.search_profiles import ResolvedScope
from tests.pytest.catalog_fixture_builders import SubtypeSpec
from tests.substrate.mcp.search_fixture import make_search_deployment, publish_nodes

pytestmark = pytest.mark.postgres

TERM = "zirconium"
QUERY = "zirconium cladding"
OPEN = "connector:open_tickets"
SECRET = "connector:secret_tickets"
MASKED = "connector:masked_comments"
MAIN = "system:analyst_search_fixture"
READER = "subject:records-reader"

TICKET = SubtypeSpec(
    "ticket",
    metadata={
        "searchable_text_fields": ["text"],
        "passage_rollup": {"edge_type": "contains", "child_subtype": "comment"},
    },
)
PAGE = SubtypeSpec(
    "page",
    metadata={
        "searchable_text_fields": ["text"],
        "passage_rollup": {"edge_type": "contains", "child_subtype": "comment"},
    },
)
COMMENT = SubtypeSpec("comment", metadata={"searchable_text_fields": ["text"]})
NOTE = SubtypeSpec("note", metadata={"searchable_text_fields": ["text"]})
SUBTYPES = [TICKET, PAGE, COMMENT, NOTE]
NARROWINGS = [("ticket", "contains", "comment"), ("page", "contains", "comment")]

FILLER = "routine maintenance log entry without findings. " * 250  # ~12,000 chars


def _node(node_id: str, subtype: str, text: str, **attrs) -> NodeFact:
    return NodeFact(
        node_id=node_id, subtype=subtype, attrs={"text": text, **attrs},
        source_record_id={"id": node_id}, source_revision={"v": 1},
    )


def _edge(src: str, dst: str) -> EdgeFact:
    return EdgeFact(
        src=src, dst=dst, edge_type="contains",
        source_record_id={"id": f"{src}|contains|{dst}"}, source_revision={"v": 1},
    )


def _publish(tmp_path, dsn: str, *, groups: dict[str, list], main: list,
             aliases=None) -> str:
    deployment = make_search_deployment(tmp_path, dsn=dsn)
    publish_nodes(
        dsn, deployment, subtypes=SUBTYPES, narrowings=NARROWINGS,
        nodes=[f for f in main if isinstance(f, NodeFact)],
        edges=[f for f in main if isinstance(f, EdgeFact)],
        node_groups=list(groups.items()),
        aliases=aliases,
    )
    return deployment


# ---------- the basic corpus ----------

def _basic_facts() -> list:
    facts: list = [
        # A long ticket whose answer sits deep in its own text and in one short
        # comment; near misses are short and carry one answer word.
        _node("ticket:long", "ticket", FILLER + f" {QUERY} failed at the weld. " + FILLER,
              status="open"),
        *[
            _node(f"comment:long:{i}", "comment", f"Comment {i} on the weld review.",
                  chunk_ordinal=i)
            for i in range(3)
        ],
        _node("comment:long:3", "comment", f"The {QUERY} failed at the weld.",
              chunk_ordinal=3, heading_path=["Comments", "Comment 3"]),
        *[_edge("ticket:long", f"comment:long:{i}") for i in range(4)],
        *[
            _node(f"ticket:near:{i}", "ticket", f"{TERM} {TERM} inventory note {i}.",
                  status="open")
            for i in range(6)
        ],
        # A ticket whose own text does not match: reached only through its
        # comment, so it is fetched from outside the ranked candidates.
        _node("ticket:quiet", "ticket", "Quarterly audit ticket.", status="open"),
        _node("comment:quiet:0", "comment", f"{TERM} cladding sample retained."),
        _edge("ticket:quiet", "comment:quiet:0"),
        # A closed ticket, reached through its comment; `attr_filters` test it.
        _node("ticket:closed", "ticket", "Closed archive ticket.", status="closed"),
        _node("comment:closed:0", "comment", f"{TERM} cladding retest scheduled."),
        _edge("ticket:closed", "comment:closed:0"),
        # A comment with no parent, and one with two ticket parents.
        _node("comment:orphan", "comment", f"An orphan {TERM} cladding remark."),
        _node("ticket:twin:a", "ticket", "Twin ticket A.", status="open"),
        _node("ticket:twin:b", "ticket", "Twin ticket B.", status="open"),
        _node("comment:twin", "comment", f"A shared {TERM} cladding remark."),
        _edge("ticket:twin:a", "comment:twin"),
        _edge("ticket:twin:b", "comment:twin"),
        # A comment whose parent is a page, not a ticket (a shared child type).
        _node("page:wiki", "page", "Wiki page.", status="open"),
        _node("comment:wiki:0", "comment", f"Wiki {TERM} cladding summary."),
        _edge("page:wiki", "comment:wiki:0"),
        _node("note:0", "note", f"A {TERM} note."),
    ]
    return facts


def _basic(tmp_path, dsn: str, **kwargs) -> str:
    return _publish(tmp_path, dsn, groups={}, main=_basic_facts(), **kwargs)


def _ids(page: dict) -> list[str]:
    return [row["node_id"] for row in page["results"]]


def _by_id(page: dict) -> dict:
    return {row["node_id"]: row for row in page["results"]}


def _search(dsn: str, deployment: str, query: str = QUERY, **kwargs) -> dict:
    with psycopg.connect(dsn, autocommit=True) as conn:
        session = open_session(
            conn, deployment_name=deployment,
            **({"nomos_subject": kwargs.pop("subject")} if "subject" in kwargs else {}),
        )
        return search(conn, session, query, method="lexical", **kwargs)


# ---------- D1: unfiltered fold ----------

def test_records_is_the_lexical_default_and_counts_records_once(clean_runtime, tmp_path):
    deployment = _basic(tmp_path, clean_runtime)
    page = _search(clean_runtime, deployment, completeness="exact", limit=50)
    assert page["rank_by"] == "records"
    assert page["result_unit"] == "record"
    ids = _ids(page)
    assert len(ids) == len(set(ids))
    # The long ticket and its winning comment are ONE row: the comment folded.
    assert "ticket:long" in ids and "comment:long:3" not in ids
    row = _by_id(page)["ticket:long"]
    assert row["rollup_best_chunk_id"] == "comment:long:3"
    assert row["matching_passages"] == 1
    assert row["position"]["index"] == 3
    assert page["total_count"] == len(ids)


def test_self_still_returns_record_and_passage_as_separate_rows(clean_runtime, tmp_path):
    deployment = _basic(tmp_path, clean_runtime)
    page = _search(clean_runtime, deployment, limit=50, rank_by="self")
    ids = _ids(page)
    assert "ticket:long" in ids and "comment:long:3" in ids
    assert all(row.get("rollup_best_chunk_id") is None for row in page["results"])
    assert all(row.get("matching_passages") is None for row in page["results"])
    assert "result_unit" not in page


def test_an_unlinked_or_two_parent_passage_stays_its_own_row(clean_runtime, tmp_path):
    deployment = _basic(tmp_path, clean_runtime)
    ids = _ids(_search(clean_runtime, deployment, limit=50))
    assert "comment:orphan" in ids
    assert "comment:twin" in ids
    assert "ticket:twin:a" not in ids and "ticket:twin:b" not in ids


def test_a_parent_outside_the_cap_is_fetched_and_ranked_by_its_passage(clean_runtime, tmp_path):
    deployment = _basic(tmp_path, clean_runtime)
    page = _search(clean_runtime, deployment, limit=50)
    rows = _by_id(page)
    assert "ticket:quiet" in rows and "comment:quiet:0" not in rows
    assert rows["ticket:quiet"]["rollup_best_chunk_id"] == "comment:quiet:0"


def test_a_parent_outside_the_scope_leaves_its_passage_a_row(clean_runtime, tmp_path):
    deployment = _basic(tmp_path, clean_runtime)
    scope = ResolvedScope(profile="comments-only", subtypes=("comment", "note"))
    ids = _ids(_search(clean_runtime, deployment, limit=50, scope=scope))
    assert "comment:long:3" in ids and "comment:quiet:0" in ids
    assert not [i for i in ids if i.startswith("ticket:")]


def test_attr_filters_test_the_parent_of_a_folded_passage(clean_runtime, tmp_path):
    deployment = _basic(tmp_path, clean_runtime)
    page = _search(
        clean_runtime, deployment, limit=50,
        filter={"attr_filters": {"status": "open"}},
    )
    ids = _ids(page)
    # The quiet ticket is open: its passage folds into it.
    assert "ticket:quiet" in ids
    # The closed ticket fails the filter, so its passage stays its own row,
    # tested on its own attrs; it has no `status`, so it is dropped too.
    assert "ticket:closed" not in ids and "comment:closed:0" not in ids


def test_an_alias_promoted_passage_stays_a_passage_row_at_the_head(clean_runtime, tmp_path):
    facts = _basic_facts() + [
        _node("comment:alias", "comment", "zrweld inspection record."),
        _edge("ticket:long", "comment:alias"),
    ]
    deployment = _publish(
        tmp_path, clean_runtime, groups={}, main=facts,
        aliases=[("zrweld", "comment:alias", None)],
    )
    page = _search(clean_runtime, deployment, "zrweld", limit=50)
    ids = _ids(page)
    # Promoted, it stays a passage row at the head and is not also folded
    # into its parent, which has no other match and so does not appear.
    assert ids[0] == "comment:alias"
    assert ids.count("comment:alias") == 1
    assert "ticket:long" not in ids


def test_an_expansion_only_passage_does_not_lift_its_record_above_an_exact_one(
    clean_runtime, tmp_path,
):
    from okg.substrate.mcp.concept_expansion import ConceptGroup

    facts = [
        _node("ticket:exact", "ticket", "hafnium " + FILLER, status="open"),
        _node("ticket:expanded", "ticket", "Unrelated ticket.", status="open"),
        _node("comment:expanded", "comment", "zircaloy zircaloy zircaloy."),
        _edge("ticket:expanded", "comment:expanded"),
    ]
    deployment = _publish(tmp_path, clean_runtime, groups={}, main=facts)
    scope = ResolvedScope(
        concept_groups=(ConceptGroup(canonical="hafnium", variants=("zircaloy",)),),
    )
    page = _search(clean_runtime, deployment, "hafnium", limit=10, scope=scope)
    assert page["concept_expansion"]["applied"] is True
    ids = _ids(page)
    assert ids.index("ticket:exact") < ids.index("ticket:expanded")


# ---------- D1: max_per_parent ----------

def test_a_profile_bound_is_disclosed_not_applied_under_records(clean_runtime, tmp_path):
    deployment = _basic(tmp_path, clean_runtime)
    scope = ResolvedScope(max_per_parent=1, max_per_parent_origin="profile")
    page = _search(clean_runtime, deployment, limit=50, scope=scope)
    assert page["rank_by"] == "records"
    assert page["diversity_applicable"] is False
    assert page["diversity_reason"] == "records_returns_one_row_per_parent"
    assert page["max_per_parent_origin"] == "profile"


def test_an_explicit_bound_selects_self_and_refuses_records(clean_runtime, tmp_path):
    deployment = _basic(tmp_path, clean_runtime)
    scope = ResolvedScope(max_per_parent=1, max_per_parent_origin="argument")
    page = _search(clean_runtime, deployment, limit=50, scope=scope)
    assert page["rank_by"] == "self"
    assert page["max_per_parent_origin"] == "argument"
    with pytest.raises(DiversityNotApplicable):
        _search(clean_runtime, deployment, limit=50, scope=scope, rank_by="records")


def test_records_is_refused_on_semantic():
    from okg.substrate.mcp.passage_rollup import RankByNotApplicable

    with pytest.raises(RankByNotApplicable, match="only to method='lexical'"):
        search(None, None, "q", method="semantic", rank_by="records",  # type: ignore[arg-type]
               embedder_spec="hash")


# ---------- D2: count basis ----------

def test_an_uncapped_page_reports_an_exact_count_and_no_cap(clean_runtime, tmp_path):
    deployment = _basic(tmp_path, clean_runtime)
    for kwargs in ({}, {"rank_by": "self"}, {"filter": {"subtype": "ticket"}},
                   {"filter": {"subtype": "ticket"}, "rank_by": "passages"}):
        page = _search(clean_runtime, deployment, completeness="exact", limit=50, **kwargs)
        assert page["total_count_basis"] == "exact", kwargs
        assert "ranked_cap" not in page, kwargs


# ---------- D3-i: the filtered fold ----------

def test_a_long_record_with_a_deep_answer_outranks_short_near_misses(clean_runtime, tmp_path):
    deployment = _basic(tmp_path, clean_runtime)
    filt = {"subtype": "ticket"}
    self_ids = _ids(_search(clean_runtime, deployment, limit=50, filter=filt, rank_by="self"))
    records = _search(clean_runtime, deployment, limit=50, filter=filt, completeness="exact")
    ids = _ids(records)
    near = [f"ticket:near:{i}" for i in range(6)]
    # Today (`self`) BM25 length normalisation sinks the long ticket below a
    # short near miss; ranked by its best passage it beats every near miss.
    assert self_ids.index("ticket:long") > min(self_ids.index(n) for n in near)
    assert ids.index("ticket:long") < min(ids.index(n) for n in near)
    assert records["ordered_exactly_through"] == len(ids)
    # The bounded default serves the same records but claims no exact prefix:
    # its candidates may have been cut (PACT lexical-index-scan-cap-3162, M1).
    bounded = _search(clean_runtime, deployment, limit=50, filter=filt)
    assert _ids(bounded) == ids
    assert bounded["ordered_exactly_through"] is None


def test_every_row_on_a_filtered_page_has_the_filtered_type(clean_runtime, tmp_path):
    deployment = _basic(tmp_path, clean_runtime)
    page = _search(clean_runtime, deployment, limit=50, filter={"subtype": "ticket"})
    assert page["results"]
    assert {row["subtype"] for row in page["results"]} == {"ticket"}
    ids = _ids(page)
    # Two parents: dropped, and neither twin is lifted by it.
    assert "ticket:twin:a" not in ids and "ticket:twin:b" not in ids
    # A page's comment never reaches a ticket page.
    assert "page:wiki" not in ids and "comment:wiki:0" not in ids
    # Failing attr_filters / source: dropped, never shown as a passage.
    for kwargs in ({"attr_filters": {"status": "open"}}, {"source": "connector:nobody"}):
        page = _search(clean_runtime, deployment, limit=50,
                       filter={"subtype": "ticket", **kwargs})
        assert {row["subtype"] for row in page["results"]} <= {"ticket"}
        assert "ticket:closed" not in _ids(page)


def test_a_filtered_fold_with_a_path_exclusion_drops_the_passage(clean_runtime, tmp_path):
    facts = _basic_facts() + [
        _node("ticket:path", "ticket", "Path ticket.", status="open", path="/private/x"),
        _node("comment:path:0", "comment", f"{TERM} cladding under a private path."),
        _edge("ticket:path", "comment:path:0"),
    ]
    deployment = _publish(tmp_path, clean_runtime, groups={}, main=facts)
    scope = ResolvedScope(excluded_path_globs=("/private/**",))
    page = _search(clean_runtime, deployment, limit=50, scope=scope,
                   filter={"subtype": "ticket"})
    assert "ticket:path" not in _ids(page)
    assert {row["subtype"] for row in page["results"]} == {"ticket"}


def test_the_passage_read_runs_with_a_custom_plan(clean_runtime, tmp_path):
    """Asserted through `pg_prepared_statements`, not timing: after 12 runs on
    one connection no BM25-ordered read (the passage read or the self read)
    was server-prepared, while other statements were (so preparing does
    happen on this connection)."""
    deployment = _basic(tmp_path, clean_runtime)
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        session = open_session(conn, deployment_name=deployment)
        for _ in range(12):
            search(conn, session, QUERY, method="lexical", limit=10,
                   filter={"subtype": "ticket"})
        statements = [
            row[0] for row in conn.execute(
                "SELECT statement FROM pg_prepared_statements"
            ).fetchall()
        ]
    assert statements
    assert not [sql for sql in statements if "<@>" in sql]


# ---------- D3-i: exactness and paging, unit level ----------

def _rows(*scores: float, expanded: int = 0) -> list[dict]:
    return [
        {"node_id": f"r{i}", "score": s, "expansion_rank": expanded}
        for i, s in enumerate(scores)
    ]


@pytest.mark.parametrize(
    ("cut_self", "cut_passages", "expected"),
    [
        # A long-record self read near -2 and a passage read near -9: the
        # passage cutoff is the stricter one ...
        (-1.5, -8.5, 2),
        # ... and the reverse.
        (-8.5, -1.5, 2),
        # One read unsaturated sets no cutoff.
        (None, -8.5, 2),
        (None, None, 5),
    ],
)
def test_the_stricter_cutoff_bounds_the_exact_prefix(cut_self, cut_passages, expected):
    rows = _rows(-10.0, -9.0, -8.5, -2.0, -1.0)
    assert exact_prefix_length(rows, (cut_self, cut_passages)) == expected


def test_a_record_at_the_cutoff_is_outside_the_prefix_and_expansion_ends_it():
    rows = _rows(-10.0, -9.0)
    assert exact_prefix_length(rows, (-9.0,)) == 1
    mixed = _rows(-10.0) + _rows(-12.0, expanded=1)
    assert exact_prefix_length(mixed, (-1.0,)) == 1


def test_cutoffs_come_from_visible_rows_and_an_all_hidden_read_empties_the_prefix():
    # Hidden last rows make the visible cutoff STRICTER (lower), so the prefix
    # can only shrink.
    assert cutoff(True, [-9.0, -5.0]) == -5.0
    assert cutoff(False, [-9.0]) is None
    assert cutoff(True, []) == -math.inf
    assert exact_prefix_length(_rows(-10.0, -9.0), (cutoff(True, []),)) == 0


# ---------- D3-i: saturated reads on a real graph ----------

def _bulk_facts(*, hidden_comments: bool) -> tuple[dict[str, list], list]:
    tickets = [
        _node(f"ticket:bulk:{i:03d}", "ticket", f"Bulk ticket {i}.", status="open")
        for i in range(110)
    ]
    comments = [
        _node(f"comment:bulk:{i:03d}:{j}", "comment",
              f"{TERM} cladding reading {i} {j} " + "detail " * (i % 7))
        for i in range(110) for j in range(2)
    ]
    edges = [
        _edge(f"ticket:bulk:{i:03d}", f"comment:bulk:{i:03d}:{j}")
        for i in range(110) for j in range(2)
    ]
    own = [
        _node(f"ticket:own:{i}", "ticket", f"{TERM} cladding ticket {i}.", status="open")
        for i in range(5)
    ]
    groups = {OPEN: tickets + own}
    if hidden_comments:
        groups[SECRET] = comments
        return groups, edges
    groups[OPEN] = tickets + own + comments
    return groups, edges


def test_a_saturated_passage_read_pages_only_through_the_exact_prefix(clean_runtime, tmp_path):
    groups, main = _bulk_facts(hidden_comments=False)
    deployment = _publish(tmp_path, clean_runtime, groups=groups, main=main)
    filt = {"subtype": "ticket"}
    first = _search(clean_runtime, deployment, completeness="exact", limit=10, filter=filt)
    assert first["total_count_basis"] == "lower_bound"
    assert first["ranked_cap"] == {"rows": 200, "passages": 200}
    prefix = first["ordered_exactly_through"]
    assert 0 < prefix < first["total_count"]
    assert first["next_offset"] == 10 and "next_offset_reason" not in first
    # The page that reaches the prefix stops there and says why.
    last_start = (prefix - 1) // 10 * 10
    last = _search(clean_runtime, deployment, completeness="exact", limit=10, offset=last_start, filter=filt)
    assert len(last["results"]) == prefix - last_start
    assert last["next_offset"] is None
    assert last["next_offset_reason"] == NEXT_OFFSET_REASON_BEYOND_EXACT_ORDER
    assert last["truncated"] is True
    # An offset at or past the prefix is refused under a lower-bound count.
    for offset in (prefix, prefix + 5):
        with pytest.raises(RecordsOffsetBeyondExactOrder):
            _search(clean_runtime, deployment, completeness="exact", limit=10, offset=offset, filter=filt)
    # Unfiltered, the self scan saturates on rows.
    unfiltered = _search(clean_runtime, deployment, completeness="exact", limit=10)
    assert unfiltered["total_count_basis"] == "lower_bound"
    assert unfiltered["ranked_cap"] == {"rows": 200}


def test_an_offset_past_an_exact_list_returns_an_empty_page(clean_runtime, tmp_path):
    deployment = _basic(tmp_path, clean_runtime)
    page = _search(clean_runtime, deployment, completeness="exact", limit=10, offset=500,
                   filter={"subtype": "ticket"})
    assert page["results"] == [] and page["total_count_basis"] == "exact"


# ---------- Nomos: enforce for hiding and redaction ----------

def _activate(dsn: str, deployment: str, rules: list[dict]) -> None:
    from okg.substrate.nomos import activate_policy_bundle, compile_policy_graph

    bundle = compile_policy_graph({
        "policy_bundle_id": f"nomos:test:records:{uuid.uuid4().hex[:8]}",
        "policy_version": "v1",
        "default_effect": "deny",
        "rules": rules,
    })
    with psycopg.connect(dsn) as conn:
        activate_policy_bundle(
            conn, deployment_name=deployment, bundle=bundle,
            scope=f"deployment:{deployment}", activated_by="test:operator",
        )
        conn.commit()


def _allow(source: str, priority: int = 100, **match) -> dict:
    return {
        "rule_id": f"allow_{source}_{priority}".replace(":", "_"),
        "effect": "allow", "actions": ["graph.read.mcp"], "subjects": [READER],
        "match": {"source_id": source, **match}, "priority": priority,
    }


def _nomos_facts() -> tuple[dict[str, list], list]:
    open_nodes = [
        _node("ticket:open", "ticket", "Open ticket.", status="open"),
        _node("comment:open:0", "comment", f"{QUERY} measured in the open."),
        _node("ticket:open2", "ticket", "Second open ticket.", status="open"),
        _node("ticket:open3", "ticket", "Third open ticket.", status="open"),
        _node("comment:viaedge", "comment", f"{QUERY} linked by a secret edge."),
        _node("comment:tosecret", "comment", f"{QUERY} under a secret ticket."),
        _node("ticket:masked", "ticket", "Masked ticket.", status="open"),
    ]
    secret = [
        _node("ticket:secret", "ticket", "Secret ticket.", status="open"),
        _node("comment:secret:0", "comment", f"{QUERY} {QUERY} secret finding."),
        _node("comment:secret:1", "comment", f"{QUERY} second secret finding."),
        _edge("ticket:open3", "comment:viaedge"),
    ]
    masked = [
        _node("comment:masked", "comment", f"{QUERY} {QUERY} masked words."),
    ]
    main = [
        _edge("ticket:open", "comment:open:0"),
        _edge("ticket:open", "comment:secret:1"),
        _edge("ticket:open2", "comment:secret:0"),
        _edge("ticket:secret", "comment:tosecret"),
        _edge("ticket:masked", "comment:masked"),
    ]
    return {OPEN: open_nodes, SECRET: secret, MASKED: masked}, main


def _nomos_deployment(tmp_path, dsn: str, rules: list[dict]) -> str:
    groups, main = _nomos_facts()
    deployment = _publish(tmp_path, dsn, groups=groups, main=main)
    _activate(dsn, deployment, rules)
    return deployment


_VISIBLE_RULES = [_allow(OPEN), _allow(MAIN), _allow(MASKED)]


def test_under_enforce_hidden_rows_never_lift_or_surface_a_record(
    clean_runtime, tmp_path, monkeypatch,
):
    deployment = _nomos_deployment(tmp_path, clean_runtime, _VISIBLE_RULES)
    monkeypatch.setenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", "1")
    page = _search(clean_runtime, deployment, limit=50, subject=READER,
                   filter={"subtype": "ticket"})
    rows = _by_id(page)
    assert {row["subtype"] for row in page["results"]} == {"ticket"}
    # A hidden passage does not lift its (visible) record.
    assert "ticket:open2" not in rows
    # A hidden edge links nothing.
    assert "ticket:open3" not in rows
    # A hidden parent never surfaces, and its passage is not shown instead.
    assert "ticket:secret" not in rows and "comment:tosecret" not in rows
    # Only the readable passage counts.
    assert rows["ticket:open"]["matching_passages"] == 1
    assert rows["ticket:open"]["rollup_best_chunk_id"] == "comment:open:0"
    assert "secret finding" not in str(page)


def test_under_enforce_a_masked_passage_does_not_lift_its_record(
    clean_runtime, tmp_path, monkeypatch,
):
    """#3377: a passage matches only on text the reader reads as stored. The
    masked comment's stored text holds the query, its served text does not,
    so it lifts nothing: its record is absent, as for a hidden passage."""
    rules = [_allow(OPEN), _allow(MAIN), {
        "rule_id": "redact_masked", "effect": "redact", "actions": ["graph.read.mcp"],
        "subjects": [READER], "match": {"source_id": MASKED}, "priority": 100,
        # A redact effect must declare its obligation, or the runtime fails the
        # decision closed and the row disappears instead of arriving masked.
        "obligations": [{"type": "redaction"}, {"type": "audit"}],
    }]
    deployment = _nomos_deployment(tmp_path, clean_runtime, rules)
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        conn.execute(
            "UPDATE okg.subtypes SET metadata = coalesce(metadata, '{}'::jsonb)"
            " || '{\"pii_classes\": {\"text\": \"free_text\"}}'::jsonb"
            " WHERE name = 'comment'"
        )
    monkeypatch.setenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", "1")
    for kwargs in ({"filter": {"subtype": "ticket"}}, {}, {"rank_by": "self"},
                   {"rank_by": "passages", "filter": {"subtype": "ticket"}}):
        page = _search(clean_runtime, deployment, limit=50, subject=READER, **kwargs)
        rows = _by_id(page)
        assert "ticket:masked" not in rows and "comment:masked" not in rows, kwargs
        assert "masked words" not in str(page), kwargs
        # The readable passage still lifts its record.
        assert "ticket:open" in rows or "comment:open:0" in rows, kwargs


def test_a_policy_that_counts_what_it_will_not_read_fails_the_call(
    clean_runtime, tmp_path, monkeypatch,
):
    """Failure case: the class check (a count) admits the secret passage, the
    per-row read hides it; the call fails with the registered error rather
    than return a page ranked by a passage the reader cannot read."""
    rules = [
        *_VISIBLE_RULES,
        _allow(SECRET, priority=400, nomos_read_purpose="count"),
    ]
    deployment = _nomos_deployment(tmp_path, clean_runtime, rules)
    monkeypatch.setenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", "1")
    with pytest.raises(SearchFoldReadCheckMismatch) as raised:
        _search(clean_runtime, deployment, limit=50, subject=READER,
                filter={"subtype": "ticket"})
    assert raised.value.code == "search_fold_read_check_mismatch"
    assert "because" not in str(raised.value)


@pytest.fixture(params=[True, False], ids=["sql-view", "no-sql-view"])
def sql_view(request, monkeypatch) -> bool:
    """Whether the search reads apply the reader's SQL access view (#3560).

    True is the default. False stands in for a session the view cannot place
    (an identity claim, an access context): the view's predicate is then TRUE,
    which is what `_reader_predicate_sql` returning None gives the reads, so
    the raw reads are taken before the per-row filter, as before #3560."""
    if not request.param:
        import importlib

        monkeypatch.setattr(
            importlib.import_module("okg.substrate.mcp.search"),
            "_reader_predicate_sql", lambda conn, session: None,
        )
    return request.param


def test_an_all_hidden_saturated_passage_read_exact_order(
    clean_runtime, tmp_path, monkeypatch, sql_view,
):
    """220 passages the reader may not read, 5 readable records that match.

    With the SQL view the hidden passages never reach the 200-row passage
    read, so the exact order is the one a graph without them gives: all 5
    records, an exact count, the order proven through all 5 (#3560: a word
    only hidden rows hold must page like a word nobody stored). Without the
    SQL view the raw read is still filled by hidden rows, so exact's proof
    fails, retrieval is unknown under enforce, the count and exact prefix say
    no more than that and no record is served past the exact prefix (PACT
    lexical-index-scan-cap-3162, review 3)."""
    groups, main = _bulk_facts(hidden_comments=True)
    deployment = _publish(tmp_path, clean_runtime, groups=groups, main=main)
    _activate(clean_runtime, deployment, [_allow(OPEN), _allow(MAIN)])
    monkeypatch.setenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", "1")
    page = _search(clean_runtime, deployment, completeness="exact", limit=10, subject=READER,
                   filter={"subtype": "ticket"})
    if sql_view:
        assert page["coverage"]["retrieval"] == {
            "status": "exhaustive", "basis": "exact_scalar",
        }
        assert sorted(_ids(page)) == [f"ticket:own:{i}" for i in range(5)]
        assert page["ordered_exactly_through"] == 5
        assert page["total_count"] == 5 and page["total_count_basis"] == "exact"
        assert page["truncated"] is False and page["next_offset"] is None
        assert "next_offset_reason" not in page
        return
    assert page["coverage"]["retrieval"] == {"status": "unknown"}
    assert page["ordered_exactly_through"] is None
    assert page["total_count"] is None and page["total_count_basis"] == "unknown"
    assert page["results"] == []
    assert page["next_offset"] is None
    assert page["next_offset_reason"] == NEXT_OFFSET_REASON_BEYOND_EXACT_ORDER
    # offset=0 is never refused; a later offset is.
    with pytest.raises(RecordsOffsetBeyondExactOrder):
        _search(clean_runtime, deployment, completeness="exact", limit=10, offset=10, subject=READER,
                filter={"subtype": "ticket"})


def test_an_all_hidden_saturated_passage_read_floor_fields(
    clean_runtime, tmp_path, monkeypatch, sql_view,
):
    """What the filtered records page says under enforce when 220 passages
    the reader may not read match, in both modes.

    With the SQL view the passage read counts only readable passages, so the
    page carries no floor: `rank_capped` is false and there is no `ranked_cap`,
    as on a graph without the hidden passages (#3560). Before #3560 the
    filtered records fold took its lower bound from the raw passage read,
    before access rules hid its rows, so `rank_capped` was true and
    `ranked_cap` named the 200-row read although every passage was hidden:
    dev f0da24a6a3 returns the same two fields (with `total_count` 5, basis
    `lower_bound`, "0 of 5+"), and PACT lexical-index-scan-cap-3162 kept them
    as a documented signal. Without the SQL view those answers stand."""
    groups, main = _bulk_facts(hidden_comments=True)
    deployment = _publish(tmp_path, clean_runtime, groups=groups, main=main)
    _activate(clean_runtime, deployment, [_allow(OPEN), _allow(MAIN)])
    monkeypatch.setenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", "1")
    for mode, retrieval in (
        ("bounded", {"status": "bounded", "basis": "bm25_index",
                     "recovery": {"completeness": "exact"}}),
        ("exact", {"status": "unknown"}),
    ):
        page = _search(clean_runtime, deployment, completeness=mode, limit=10,
                       subject=READER, filter={"subtype": "ticket"})
        assert "index_scan_cap_reads" not in page and "lexical_exact_reads" not in page
        if sql_view:
            assert sorted(_ids(page)) == [f"ticket:own:{i}" for i in range(5)], mode
            assert page["rank_capped"] is False, mode
            assert "ranked_cap" not in page, mode
            assert page["truncated"] is False and page["next_offset"] is None, mode
            assert "next_offset_reason" not in page, mode
            if mode == "bounded":
                assert page["total_count"] is None, mode
                assert page["total_count_basis"] == "unknown", mode
                assert page["ordered_exactly_through"] is None, mode
                assert page["coverage"]["retrieval"] == retrieval, mode
            else:
                assert page["total_count"] == 5, mode
                assert page["total_count_basis"] == "exact", mode
                assert page["ordered_exactly_through"] == 5, mode
                assert page["coverage"]["retrieval"] == {
                    "status": "exhaustive", "basis": "exact_scalar",
                }, mode
            continue
        assert page["results"] == [], mode
        # Dev's fields, from the pre-access read: kept, as on dev.
        assert page["rank_capped"] is True, mode
        assert page["ranked_cap"] == {"rows": 200, "passages": 200}, mode
        assert page["truncated"] is True and page["next_offset"] is None, mode
        assert page["next_offset_reason"] == NEXT_OFFSET_REASON_BEYOND_EXACT_ORDER, mode
        # Less than dev: no count, no floor, no exact prefix, no raw read facts.
        assert page["total_count"] is None, mode
        assert page["total_count_basis"] == "unknown", mode
        assert page["ordered_exactly_through"] is None, mode
        assert page["coverage"]["retrieval"] == retrieval, mode


# ---------- Nomos: decision rows under audit ----------

def _trail(dsn: str, deployment: str) -> list[tuple[str | None, str | None, str]]:
    with psycopg.connect(dsn, autocommit=True) as conn:
        return conn.execute(
            "SELECT target->>'node_id', target->>'edge_id', target->>'kind'"
            " FROM okg.nomos_decisions WHERE deployment_name = %s",
            (deployment,),
        ).fetchall()


def test_the_fold_counts_its_candidates_and_reads_what_it_returns(
    clean_runtime, tmp_path, monkeypatch,
):
    deployment = _basic(tmp_path, clean_runtime)
    _activate(clean_runtime, deployment, [_allow(MAIN)])
    monkeypatch.setenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", "audit")
    page = _search(clean_runtime, deployment, limit=1, subject=READER,
                   filter={"subtype": "ticket"})
    (top,) = page["results"]
    winner = top["rollup_best_chunk_id"]
    assert winner is not None
    trail = _trail(clean_runtime, deployment)
    read_nodes = {n for n, _e, k in trail if n and k.endswith("_read")}
    count_nodes = {n for n, _e, k in trail if n and k.endswith("_count")}
    read_edges = {e for _n, e, k in trail if e and k.endswith("_read")}
    # One class (one producer): one count decision for the passages, under its
    # representative only.
    comment_counts = {n for n in count_nodes if n.startswith("comment:")}
    assert len(comment_counts) == 1
    # The winning passage and its relation edge are read.
    assert winner in read_nodes
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        edge_id = conn.execute(
            f"SELECT edge_id FROM {resolve_edge_segment(conn)}"
            " WHERE src = %s AND dst = %s",
            (top["node_id"], winner),
        ).fetchone()[0]
    assert str(edge_id) in read_edges
    # Passages the page does not return are not read ...
    unreturned = {"comment:long:3", "comment:quiet:0", "comment:closed:0"} - {winner}
    assert not unreturned & read_nodes
    # ... and the self candidates are still read row by row (no per-class
    # self read).
    assert {f"ticket:near:{i}" for i in range(6)} <= read_nodes


# ---------- decision 142a: the unfiltered fold checks scan-only rows per class ----------

def test_under_enforce_the_unfiltered_fold_never_lifts_or_leaks_a_hidden_row(
    clean_runtime, tmp_path, monkeypatch,
):
    deployment = _nomos_deployment(tmp_path, clean_runtime, _VISIBLE_RULES)
    monkeypatch.setenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", "1")
    page = _search(clean_runtime, deployment, limit=50, subject=READER)
    rows = _by_id(page)
    # A hidden edge links nothing: its passage stays its own row and the
    # parent it names is not lifted.
    assert "comment:viaedge" in rows and "ticket:open3" not in rows
    assert rows["comment:viaedge"]["parent"]["node_id"] is None
    # A hidden parent never surfaces, not even through its passage's card.
    assert "ticket:secret" not in rows
    assert rows["comment:tosecret"]["parent"]["node_id"] is None
    # A hidden passage lifts nothing and is not counted.
    assert "ticket:open2" not in rows
    assert rows["ticket:open"]["matching_passages"] == 1
    assert "Secret ticket" not in str(page) and "secret finding" not in str(page)


def test_the_unfiltered_fold_counts_scan_only_rows_and_reads_what_it_serves(
    clean_runtime, tmp_path, monkeypatch,
):
    deployment = _basic(tmp_path, clean_runtime)
    _activate(clean_runtime, deployment, [_allow(MAIN)])
    monkeypatch.setenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", "audit")
    page = _search(clean_runtime, deployment, limit=1, subject=READER)
    (top,) = page["results"]
    served = top["node_id"]
    trail = _trail(clean_runtime, deployment)
    read_nodes = {n for n, _e, k in trail if n and k.endswith("_read")}
    count_nodes = {n for n, _e, k in trail if n and k.endswith("_count")}
    read_edges = {e for _n, e, k in trail if e and k.endswith("_read")}
    count_edges = {e for _n, e, k in trail if e and k.endswith("_count")}
    # Scan-only rows: one count decision per class (one producer here) for the
    # relation edges and for the parents fetched outside the ranked read.
    assert len(count_edges) == 1
    fetched = {
        "ticket:quiet", "ticket:closed", "ticket:twin:a", "ticket:twin:b",
        "page:wiki",
    }
    assert len(count_nodes & fetched) == 1
    # A fetched parent the page does not serve is never read.
    assert not (fetched - {served}) & read_nodes
    # The self read's candidates are still read row by row (comments match
    # on their own text).
    assert {"comment:long:3", "comment:quiet:0", "comment:orphan"} <= read_nodes
    # The served row is read, and so is the edge of its winning passage.
    assert served in read_nodes
    winner = top.get("rollup_best_chunk_id")
    if winner is not None:
        with psycopg.connect(clean_runtime, autocommit=True) as conn:
            edge_id = conn.execute(
                f"SELECT edge_id FROM {resolve_edge_segment(conn)}"
                " WHERE src = %s AND dst = %s",
                (served, winner),
            ).fetchone()[0]
        assert str(edge_id) in read_edges


def test_the_unfiltered_fold_reads_a_served_fetched_parent(
    clean_runtime, tmp_path, monkeypatch,
):
    """`ticket:quiet` matches only through its comment, so it is fetched
    outside the ranked read; served, it gets its own `read` decision."""
    deployment = _basic(tmp_path, clean_runtime)
    _activate(clean_runtime, deployment, [_allow(MAIN)])
    monkeypatch.setenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", "audit")
    page = _search(clean_runtime, deployment, "cladding sample retained",
                   limit=50, subject=READER)
    assert "ticket:quiet" in _ids(page)
    trail = _trail(clean_runtime, deployment)
    assert ("ticket:quiet" in {n for n, _e, k in trail if n and k.endswith("_read")})


# ---------- code review of f664046d91 ----------

def _assignee_deployment(tmp_path, dsn: str) -> str:
    """A ticket whose `assignee` the reader sees masked, reachable only
    through its comment (so both folds fetch it outside the ranked read)."""
    groups = {
        OPEN: [
            _node("comment:assigned", "comment", f"{QUERY} assigned review."),
            _node("ticket:visible", "ticket", f"{QUERY} visible ticket.",
                  assignee="alice@x"),
        ],
        MASKED: [
            _node("ticket:assigned", "ticket", "Assigned ticket.", assignee="alice@x"),
        ],
    }
    main = [_edge("ticket:assigned", "comment:assigned")]
    deployment = _publish(tmp_path, dsn, groups=groups, main=main)
    _activate(dsn, deployment, [_allow(OPEN), _allow(MAIN), {
        "rule_id": "redact_masked", "effect": "redact", "actions": ["graph.read.mcp"],
        "subjects": [READER], "match": {"source_id": MASKED}, "priority": 100,
        "obligations": [{"type": "redaction"}, {"type": "audit"}],
    }])
    with psycopg.connect(dsn, autocommit=True) as conn:
        conn.execute(
            "UPDATE okg.subtypes SET metadata = coalesce(metadata, '{}'::jsonb)"
            " || '{\"pii_classes\": {\"assignee\": \"free_text\"}}'::jsonb"
            " WHERE name = 'ticket'"
        )
    return deployment


@pytest.mark.parametrize("subtype", ["ticket", None])
def test_a_masked_attribute_never_admits_a_fetched_parent(
    clean_runtime, tmp_path, monkeypatch, subtype,
):
    """Review B1: admission sees what the reader sees. The fetched ticket's
    `assignee` is masked for this reader, so `attr_filters` on it must not
    match, in the filtered fold and in the unfiltered fold alike; otherwise
    the record's presence reveals the hidden value one guess at a time."""
    deployment = _assignee_deployment(tmp_path, clean_runtime)
    monkeypatch.setenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", "1")
    filt = {"attr_filters": {"assignee": "alice@x"}}
    if subtype:
        filt["subtype"] = subtype
    page = _search(clean_runtime, deployment, limit=50, subject=READER, filter=filt)
    ids = _ids(page)
    # Control: the same value on a ticket the reader may read does match.
    assert "ticket:visible" in ids
    assert "ticket:assigned" not in ids and "comment:assigned" not in ids
    # Without the filter the masked ticket is reachable, so the filter is
    # what removed it.
    plain = {"subtype": subtype} if subtype else {}
    unfiltered = _search(clean_runtime, deployment, limit=50, subject=READER,
                         filter=plain or None)
    assert "ticket:assigned" in _ids(unfiltered)


def test_passage_ranked_excerpts_centre_on_the_match(clean_runtime, tmp_path):
    """Review B2: under `rank_by="passages"` the page's matcher must see the
    winning passages' text, or every long passage excerpt starts at char 0."""
    facts = [
        _node("ticket:deep", "ticket", "Deep ticket.", status="open"),
        _node("comment:deep", "comment", FILLER + f" {QUERY} at the end."),
        _edge("ticket:deep", "comment:deep"),
    ]
    deployment = _publish(tmp_path, clean_runtime, groups={}, main=facts)
    page = _search(clean_runtime, deployment, limit=5, max_chars=120,
                   filter={"subtype": "ticket"}, rank_by="passages")
    (row,) = page["results"]
    assert row["rollup_best_chunk_id"] == "comment:deep"
    assert "zirconium" in row["excerpt"]


def test_a_record_type_with_no_passage_relation_has_a_null_count(clean_runtime, tmp_path):
    """Review: `matching_passages` is null, not 0, on a type that takes no
    passages; 0 on a record type that declares a relation but matched none."""
    deployment = _basic(tmp_path, clean_runtime)
    rows = _by_id(_search(clean_runtime, deployment, limit=50))
    assert rows["note:0"]["matching_passages"] is None
    assert rows["ticket:near:0"]["matching_passages"] == 0


def test_an_alias_promoted_record_stays_at_the_head_of_a_filtered_fold(
    clean_runtime, tmp_path,
):
    """Review: rule 8 in the filtered fold. A strongly matching passage of
    another ticket must not fold that ticket above the promoted canonical."""
    facts = [
        _node("ticket:canon", "ticket", "Canonical ticket.", status="open"),
        _node("ticket:other", "ticket", "Other ticket.", status="open"),
        _node("comment:other", "comment", "zrcanon zrcanon zrcanon."),
        _edge("ticket:other", "comment:other"),
    ]
    deployment = _publish(
        tmp_path, clean_runtime, groups={}, main=facts,
        aliases=[("zrcanon", "ticket:canon", None)],
    )
    ids = _ids(_search(clean_runtime, deployment, "zrcanon", limit=10,
                       filter={"subtype": "ticket"}))
    assert ids[0] == "ticket:canon"
    assert "ticket:other" in ids


def test_an_alias_promoted_passage_is_not_counted_in_its_parent(clean_runtime, tmp_path):
    facts = _basic_facts() + [
        _node("comment:alias", "comment", "zrweld inspection record."),
        _node("comment:alias2", "comment", "A second zrweld note."),
        _edge("ticket:long", "comment:alias"),
        _edge("ticket:long", "comment:alias2"),
    ]
    deployment = _publish(
        tmp_path, clean_runtime, groups={}, main=facts,
        aliases=[("zrweld", "comment:alias", None)],
    )
    page = _search(clean_runtime, deployment, "zrweld", limit=50)
    rows = _by_id(page)
    assert _ids(page)[0] == "comment:alias"
    assert rows["ticket:long"]["matching_passages"] == 1
    assert rows["ticket:long"]["rollup_best_chunk_id"] == "comment:alias2"


def _source_split_deployment(tmp_path, dsn: str) -> str:
    groups = {
        OPEN: [_node("comment:fam", "comment", f"{QUERY} family remark.")],
        SECRET: [_node("ticket:fam", "ticket", "Family ticket.", status="open")],
    }
    return _publish(tmp_path, dsn, groups=groups,
                    main=[_edge("ticket:fam", "comment:fam")])


def test_a_parent_failing_the_source_filter_leaves_its_passage_a_row(clean_runtime, tmp_path):
    """Rule 4 on the unfolded path: `source` selects the passage; its parent
    from another producer fails it, so the passage stays its own row."""
    deployment = _source_split_deployment(tmp_path, clean_runtime)
    ids = _ids(_search(clean_runtime, deployment, limit=10, filter={"source": OPEN}))
    assert ids == ["comment:fam"]


def test_a_parent_outside_the_source_family_leaves_its_passage_a_row(clean_runtime, tmp_path):
    deployment = _source_split_deployment(tmp_path, clean_runtime)
    scope = ResolvedScope(source_families=("open",), source_ids=(OPEN,))
    ids = _ids(_search(clean_runtime, deployment, limit=10, scope=scope))
    assert ids == ["comment:fam"]


def test_a_two_parent_passage_with_one_parent_hidden_folds_into_the_visible_one(
    clean_runtime, tmp_path, monkeypatch,
):
    """A parent the reader may not read is not reached (as in today's parent
    resolution), so the passage resolves to the one visible parent; the
    hidden one never surfaces."""
    groups = {
        OPEN: [
            _node("ticket:seen", "ticket", "Seen ticket.", status="open"),
            _node("comment:both", "comment", f"{QUERY} shared remark."),
        ],
        SECRET: [_node("ticket:unseen", "ticket", "Unseen ticket.", status="open")],
    }
    main = [_edge("ticket:seen", "comment:both"), _edge("ticket:unseen", "comment:both")]
    deployment = _publish(tmp_path, clean_runtime, groups=groups, main=main)
    _activate(clean_runtime, deployment, [_allow(OPEN), _allow(MAIN)])
    monkeypatch.setenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", "1")
    for filt in ({"subtype": "ticket"}, None):
        page = _search(clean_runtime, deployment, limit=10, subject=READER, filter=filt)
        assert _ids(page) == ["ticket:seen"], filt
        assert "Unseen" not in str(page)


def test_the_self_cutoffs_are_per_read():
    """Review: when only the effective-attribute read saturates, its own
    worst visible score bounds the prefix, not the literal read's rows."""
    from okg.substrate.mcp.record_fold import self_cutoffs

    rows = [
        {"node_id": "lit:1", "score": -9.0},
        {"node_id": "lit:2", "score": -1.0},
        {"node_id": "eff:1", "score": -5.0},
    ]
    cuts = self_cutoffs(
        rows, literal_saturated=False, effective_saturated=True,
        effective_ids={"eff:1"},
    )
    assert cuts == [-5.0]
    assert self_cutoffs(rows, literal_saturated=True, effective_saturated=False,
                        effective_ids={"eff:1"}) == [-1.0]
    assert self_cutoffs(rows, literal_saturated=False, effective_saturated=False,
                        effective_ids=set()) == []


def test_the_pre_join_executor_pins_self(monkeypatch):
    """Lead call: the pre-join tool executor keeps today's row-level pages."""
    import importlib

    from okg.substrate.mcp.framework.executor import _execute_step
    from okg.substrate.mcp.framework.schema import PreJoinBundleStep

    captured = {}
    search_module = importlib.import_module("okg.substrate.mcp.search")
    monkeypatch.setattr(search_module, "search",
                        lambda conn, session, **kw: captured.update(kw) or {"results": []})
    _execute_step(
        step=PreJoinBundleStep(call="search", args={"query": "q"}, emit_as="hits"),
        tool_name="t", index=0, conn=None, session=None, params={}, bundles={},
        ctx=None, resolvers=None,
    )
    assert captured["rank_by"] == "self"


# ---------- D4: the excerpt window on PostgreSQL's english config ----------

def test_the_excerpt_centres_on_the_stemmed_match_not_the_first_stopword(
    clean_runtime, tmp_path,
):
    facts = [
        _node("note:long", "note", "the " + FILLER + " the valves were replaced. " + FILLER),
    ]
    deployment = _publish(tmp_path, clean_runtime, groups={}, main=facts)
    page = _search(clean_runtime, deployment, "the valve", limit=5, max_chars=200)
    assert page["excerpt_window_basis"] == "lexized"
    assert "valves" in page["results"][0]["excerpt"]


# ---------- the bench binder ----------

def test_the_bench_binder_pins_self():
    from okg.bench.agent_memory.binder import search_nodes

    class _Client:
        def __init__(self):
            self.calls = []

        def call(self, name, args):
            self.calls.append((name, args))
            return {"results": []}

    client = _Client()
    search_nodes(client, "ticket", "q")
    assert client.calls[0][1]["rank_by"] == "self"
