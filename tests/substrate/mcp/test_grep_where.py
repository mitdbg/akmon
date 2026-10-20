"""`search(method="grep", where=...)` on the shared scanner (#3154, #3226).

PACT mcp-grep-page with PACT shared-read-scanners-3199's compiler and
finishing contract. Every case runs on a published PostgreSQL graph:

* parity: grep with a `where` lists exactly the records grep without it
  lists whose read row the reader's own check (`search_where.matches` on
  `filter_rows_by_read_class` rows) admits, in the same order, under Nomos
  off and `enforce`; `id`/`type`/`source` clauses compile to SQL (`exact`),
  attribute, title and date clauses stay residual (`superset`), NOT and OR
  included;
* the count is exact only on finished proof: a residual count finishes
  each stored unit (a SQL count would count rows the residual rejects) and
  is a lower bound past `COUNT_FINISH_ROWS`;
* retrieval is exhaustive only on a terminal read whose every candidate was
  decided: a bounded, complete or continued page never claims it;
* continuation tokens round-trip with a `where`, its diagnostics object
  excluded from the binding.

Synthetic data only.
"""
from __future__ import annotations

import re

import psycopg
import pytest
from psycopg.rows import dict_row

from okg.substrate.attributes import apply_effective_attrs_to_rows
from okg.substrate.bm25_node_search import branch_point
from okg.substrate.mcp import grep_search, open_session, search, search_where
from okg.substrate.mcp.envelope import with_read_coverage
from okg.substrate.nomos.read_filter import filter_rows_by_read_class
from tests.substrate.mcp.test_search_records import (
    MAIN,
    READER,
    SECRET,
    TERM,
    _activate,
    _allow,
    _assignee_deployment,
    _basic,
    _basic_facts,
    _node,
    _publish,
)
from tests.substrate.mcp.test_search_where_db import _date_corpus

pytestmark = pytest.mark.postgres


def _grep(dsn: str, deployment: str, query: str = TERM, **kwargs) -> dict:
    with psycopg.connect(dsn, autocommit=True) as conn:
        session = open_session(
            conn, deployment_name=deployment,
            **({"nomos_subject": kwargs.pop("subject")} if "subject" in kwargs else {}),
        )
        return search(conn, session, query, method="grep", **kwargs)


def _ids(page: dict) -> list[str]:
    return [row["node_id"] for row in page["results"]]


def _regime(dsn: str, deployment: str, monkeypatch, regime: str) -> dict:
    """Nomos `off` (no policy), `audit` or `enforce` (an allow-all-of-MAIN
    bundle and a named reader); the call kwargs for that reader."""
    if regime == "off":
        monkeypatch.setenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", "off")
        return {}
    _activate(dsn, deployment, [_allow(MAIN)])
    monkeypatch.setenv(
        "OKG_NOMOS_RUNTIME_ENFORCEMENT", "1" if regime == "enforce" else "audit",
    )
    return {"subject": READER}


def _reader_admits(dsn: str, deployment: str, ids: list[str], where: dict,
                   **kwargs) -> set[str]:
    """The reader's own verdict, independent of grep's code: each row read
    through the Nomos read filter and the effective-attribute overlay, then
    `search_where.matches` (three-valued, only TRUE admits)."""
    with psycopg.connect(dsn, autocommit=True) as conn:
        session = open_session(
            conn, deployment_name=deployment,
            **({"nomos_subject": kwargs["subject"]} if "subject" in kwargs else {}),
        )
        segment = grep_search.resolve_trgm_segment(conn)
        bp = branch_point(conn, session)
        with conn.cursor(row_factory=dict_row) as cur:
            cur.execute(
                f"SELECT node_id, subtype, attrs, text_hash, source, source_record_id, "
                f"source_revision FROM {segment} WHERE node_id = ANY(%(ids)s) "
                f"AND deleted = FALSE AND live_lo <= %(bp)s AND %(bp)s < live_hi",
                {"ids": ids, "bp": bp},
            )
            rows = cur.fetchall()
        read = filter_rows_by_read_class(conn, session, rows, row_kind="node")
        read = apply_effective_attrs_to_rows(
            conn, read, target_kind="node", target_id_field="node_id",
            generation_id=session.generation_id,
        )
        context = search_where.load_where_context(conn, session, where)
        return {
            str(row["node_id"]) for row in read
            if search_where.matches(row, where, context)
        }


# id/type/source only: the shared compiler decides them in SQL.
EXACT = [
    {"op": "in", "field": "type", "values": ["ticket"]},
    {"op": "not", "clause": {"op": "eq", "field": "type", "value": "ticket"}},
    {"op": "or", "clauses": [
        {"op": "eq", "field": "type", "value": "note"},
        {"op": "eq", "field": "type", "value": "page"},
    ]},
    {"op": "in", "field": "id", "values": ["ticket:closed", "note:0", "comment:orphan"]},
    {"op": "not", "clause": {"op": "eq", "field": "source", "value": MAIN}},
]
# Attribute clauses: a residual the reader checks on read values.
SUPERSET = [
    {"op": "in", "field": "attrs.status", "values": ["closed"]},
    {"op": "not", "clause": {"op": "in", "field": "attrs.status", "values": ["open"]}},
    {"op": "or", "clauses": [
        {"op": "eq", "field": "type", "value": "note"},
        {"op": "in", "field": "attrs.status", "values": ["closed"]},
    ]},
    {"op": "and", "clauses": [
        {"op": "in", "field": "type", "values": ["ticket", "page"]},
        {"op": "not", "clause": {"op": "exists", "field": "attrs.status"}},
    ]},
]


@pytest.mark.parametrize("regime", ["off", "audit", "enforce"])
@pytest.mark.parametrize(
    "where,qualification",
    [(w, "exact") for w in EXACT] + [(w, "superset") for w in SUPERSET],
)
def test_grep_where_lists_exactly_what_the_reader_admits(
    clean_runtime, tmp_path, monkeypatch, regime, where, qualification,
):
    deployment = _basic(tmp_path, clean_runtime)
    kwargs = _regime(clean_runtime, deployment, monkeypatch, regime)
    base = _grep(clean_runtime, deployment, limit=50, **kwargs)
    base_ids = _ids(base)
    # The corpus reaches records on their own text, through a passage, and
    # passages listed on their own (no parent, two parents).
    assert {"ticket:long", "ticket:closed", "comment:orphan", "comment:twin",
            "note:0", "page:wiki"} <= set(base_ids)
    admitted = _reader_admits(clean_runtime, deployment, base_ids, where, **kwargs)
    expected = [i for i in base_ids if i in admitted]

    page = _grep(clean_runtime, deployment, limit=50, where=where, **kwargs)
    assert _ids(page) == expected
    assert page["where_qualification"] == qualification
    assert page["total_count"] == len(expected)
    assert page["total_count_basis"] == "exact"
    # A passage whose readable parent the clause rejects is dropped, never
    # listed on its own (D6): the closed ticket's comment never surfaces.
    assert "comment:closed:0" not in _ids(page)


@pytest.mark.parametrize("regime", ["off", "audit", "enforce"])
def test_a_residual_count_finishes_units_and_is_a_lower_bound_past_its_bound(
    clean_runtime, tmp_path, monkeypatch, regime,
):
    deployment = _basic(tmp_path, clean_runtime)
    kwargs = _regime(clean_runtime, deployment, monkeypatch, regime)
    strict = regime == "enforce"
    where = {"op": "in", "field": "attrs.status", "values": ["closed"]}
    seen: list[dict] = []
    real = grep_search.count_grep_matches

    def spy(*args, **kwargs):
        seen.append(kwargs)
        return real(*args, **kwargs)

    monkeypatch.setattr(grep_search, "count_grep_matches", spy)
    page = _grep(clean_runtime, deployment, limit=50, where=where, **kwargs)
    assert _ids(page) == ["ticket:closed"]
    assert page["total_count"] == 1 and page["total_count_basis"] == "exact"
    # R1 (off, audit) finishes stored units; R2 (enforce) finishes its walk.
    assert seen[-1]["strict"] is strict
    assert (seen[-1]["finish_sql"] is not None) is not strict
    # Failure case: the count without the residual counts every record the
    # terms reach, which the residual rejects.
    stored = _grep(clean_runtime, deployment, limit=50, **kwargs)
    assert stored["total_count"] > 1
    # An exact compile keeps the SQL count in R1: under audit too, since
    # this policy cannot redact and every row has source lineage, so audit
    # hides nothing (#3384).
    exact = _grep(clean_runtime, deployment, limit=50,
                  where={"op": "in", "field": "type", "values": ["ticket"]}, **kwargs)
    assert seen[-1]["finish_sql"] is None
    assert exact["total_count"] == len(exact["results"])
    assert exact["total_count_basis"] == "exact"

    bound_name = "COUNT_SCOPE_ROWS" if strict else "COUNT_FINISH_ROWS"
    monkeypatch.setattr(grep_search, bound_name, 3)
    bounded = _grep(clean_runtime, deployment, limit=50,
                    where={"op": "not", "clause": {
                        "op": "in", "field": "attrs.status", "values": ["open"]}}, **kwargs)
    assert bounded["total_count_basis"] == "lower_bound"
    assert bounded["scan_cap"] == ({"scope_rows": 3} if strict else {"rows": 3})
    assert "scan_cap" in bounded["count_basis_reasons"]
    # Never below the records the page itself found.
    assert bounded["total_count"] >= len(bounded["results"])


def test_catalog_title_and_date_clauses_judge_the_record(clean_runtime, tmp_path):
    deployment, generation = _date_corpus(tmp_path, clean_runtime)
    for where in (
        {"op": "eq", "field": "title", "value": "Policy"},
        {"op": "eq", "field": "date.modified", "value": "2026-10-03T08:00:00Z"},
    ):
        page = _grep(clean_runtime, deployment, "zirconium", limit=10, where=where)
        assert _ids(page) == ["policy:effective"]
        assert page["generation_id"] == generation
        assert page["where_qualification"] == "superset"
        assert page["where_evaluation_scope"] == "grep_scanned_candidates"
        assert any(m["field"] == where["field"] for m in page["where_fields"])
    # Failure case: without the clause both records match through a passage.
    assert sorted(_ids(_grep(clean_runtime, deployment, "zirconium", limit=10))) == [
        "policy:effective", "policy:missing",
    ]


@pytest.mark.parametrize("subtype", ["ticket", None])
def test_a_not_on_a_masked_value_never_admits_the_secret_record(
    clean_runtime, tmp_path, monkeypatch, subtype,
):
    """`ticket:assigned` holds `assignee` masked for this reader; both
    guesses leave it (and its comment) out, while the readable control
    answers as ordinary logic, as lexical's record view does."""
    deployment = _assignee_deployment(tmp_path, clean_runtime)
    monkeypatch.setenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", "1")

    def ids(where: dict) -> list[str]:
        clauses = [where]
        if subtype:
            clauses.append({"op": "eq", "field": "type", "value": subtype})
        page = _grep(clean_runtime, deployment, "zirconium", limit=50, subject=READER,
                     where={"op": "and", "clauses": clauses})
        assert page["total_count_basis"] == "lower_bound"  # the policy redacts
        return _ids(page)

    plain = _grep(clean_runtime, deployment, "zirconium", limit=50, subject=READER)
    assert "ticket:assigned" in _ids(plain)
    wrong = {"op": "not", "clause": {"op": "eq", "field": "attrs.assignee", "value": "bob@x"}}
    right = {"op": "not", "clause": {"op": "eq", "field": "attrs.assignee", "value": "alice@x"}}
    assert "ticket:visible" in ids(wrong)
    assert "ticket:visible" not in ids(right)
    for guess in (wrong, right):
        found = ids(guess)
        assert "ticket:assigned" not in found and "comment:assigned" not in found
    exists = {"op": "exists", "field": "attrs.assignee"}
    assert "ticket:assigned" not in ids(exists)
    assert "ticket:assigned" not in ids({"op": "not", "clause": exists})


def _coverage(page: dict) -> dict:
    return with_read_coverage({**page, "operator_name": "search"})["coverage"]


@pytest.mark.parametrize("regime,basis", [
    ("off", "grep_whole_sql"), ("audit", "grep_whole_sql"),
    ("enforce", "grep_terminal_keyset"),
])
def test_exhaustive_retrieval_only_on_finished_terminal_proof(
    clean_runtime, tmp_path, monkeypatch, regime, basis,
):
    deployment = _basic(tmp_path, clean_runtime)
    kwargs = _regime(clean_runtime, deployment, monkeypatch, regime)
    where = {"op": "not", "clause": {"op": "in", "field": "attrs.status", "values": ["open"]}}
    whole = _grep(clean_runtime, deployment, limit=50, where=where, **kwargs)
    assert whole["retrieval_exhaustive"] is True
    assert whole["retrieval_basis"] == basis
    coverage = _coverage(whole)
    assert coverage["retrieval"]["status"] == "exhaustive"
    assert coverage["retrieval"]["basis"] == basis

    # A complete page stopped before reading every match: no claim.
    first = _grep(clean_runtime, deployment, limit=1, where=where, **kwargs)
    assert "retrieval_exhaustive" not in first
    assert _coverage(first)["retrieval"]["status"] != "exhaustive"

    # A bounded scan: no claim, whatever the count says.
    monkeypatch.setattr(grep_search, "PAGE_SCAN_BOUND", 1)
    bounded = _grep(clean_runtime, deployment, limit=50, where=where, **kwargs)
    assert bounded["page_basis"] == "scan_bound_reached"
    assert "retrieval_exhaustive" not in bounded
    assert _coverage(bounded)["retrieval"]["status"] != "exhaustive"

    # A continuation of it skips rows earlier pages decided: no claim.
    token = bounded["continuation_token"]
    nxt = _grep(clean_runtime, deployment, limit=50, where=where,
                continuation_token=token, **kwargs)
    assert "retrieval_exhaustive" not in nxt


@pytest.mark.parametrize("regime", ["off", "audit", "enforce"])
@pytest.mark.parametrize("where", [
    {"op": "in", "field": "type", "values": ["ticket", "note"]},
    {"op": "not", "clause": {"op": "in", "field": "attrs.status", "values": ["closed"]}},
])
def test_a_where_chain_lists_the_unbounded_order_with_no_repeat_or_gap(
    clean_runtime, tmp_path, monkeypatch, regime, where,
):
    facts = [
        _node(f"ticket:{i:02d}", "ticket", f"curlew {'curlew ' * (i % 3)}ticket {i}",
              status="closed" if i % 4 == 0 else "open")
        for i in range(16)
    ] + [_node(f"note:{i:02d}", "note", f"curlew note {i}") for i in range(6)]
    deployment = _publish(tmp_path, clean_runtime, groups={}, main=facts)
    kwargs = _regime(clean_runtime, deployment, monkeypatch, regime)
    unbounded = _ids(_grep(clean_runtime, deployment, "curlew", limit=50, where=where, **kwargs))
    assert unbounded
    monkeypatch.setattr(grep_search, "PAGE_SCAN_BOUND", 3)
    page = _grep(clean_runtime, deployment, "curlew", limit=4, where=where, **kwargs)
    pages = [page]
    while len(pages) < 40:
        if page.get("continuation_token"):
            follow = {"continuation_token": page["continuation_token"]}
        elif page.get("next_offset") is not None:
            follow = {"offset": page["next_offset"]}
        else:
            break
        page = _grep(clean_runtime, deployment, "curlew", limit=4, where=where,
                     **follow, **kwargs)
        pages.append(page)
    chained = [i for p in pages for i in _ids(p)]
    assert any(p.get("continuation_token") for p in pages)
    assert chained == unbounded


def test_a_title_where_token_round_trips_although_the_scan_fills_its_diagnostics(
    clean_runtime, tmp_path, monkeypatch,
):
    """The `where` diagnostics object (`where_context`) collects missing
    titles while the scan runs. It is not part of the token's binding, so
    the token the first page issues is accepted by the next call."""
    deployment, _generation = _date_corpus(tmp_path, clean_runtime)
    where = {"op": "eq", "field": "title", "value": "Policy"}
    monkeypatch.setattr(grep_search, "PAGE_SCAN_BOUND", 1)
    first = _grep(clean_runtime, deployment, "zirconium", limit=10, where=where)
    assert first["page_basis"] == "scan_bound_reached"
    token = first["continuation_token"]
    nxt = _grep(clean_runtime, deployment, "zirconium", limit=10, where=where,
                continuation_token=token)
    assert _ids(first) + _ids(nxt) == ["policy:effective"]
    # Failure case: a token is bound to its `where`; another one refuses.
    from okg.substrate.mcp.continuation import ContinuationTokenInvalid

    with pytest.raises(ContinuationTokenInvalid):
        _grep(clean_runtime, deployment, "zirconium", limit=10,
              where={"op": "eq", "field": "title", "value": "Other"},
              continuation_token=token)


@pytest.mark.parametrize("value", ["ticket", "comment", "note"])
def test_a_type_equality_behaves_as_the_same_type_in_a_list(clean_runtime, tmp_path, value):
    """Review L1: `type = X` and `type in [X]` give the same page. For a
    record type the clause judges the record. For a passage type
    (`comment`) both now list the passages, as `subtype_filter` does
    (#3666): judged against the record a passage folds into, a passage type
    dropped every passage with one parent, and on a graph where every
    passage has one the call answered "0 of 0" (the false zero). This test
    pinned the old comment page ({orphan, twin}); it now pins the fix."""
    deployment = _basic(tmp_path, clean_runtime)
    eq = _grep(clean_runtime, deployment, limit=50,
               where={"op": "eq", "field": "type", "value": value})
    listed = _grep(clean_runtime, deployment, limit=50,
                   where={"op": "in", "field": "type", "values": [value]})
    assert _ids(eq) == _ids(listed)
    assert eq["total_count"] == listed["total_count"]
    assert eq.get("where_qualification") == listed.get("where_qualification")
    if value == "comment":
        # Issue #3666: a passage type can mean only its passages. Judged
        # through each record, it listed only comments with no single parent
        # (orphan, twin) and answered "0 of 0" where every comment had one.
        # Both spellings now list the passages, as the subtype filter does.
        by_subtype = _grep(clean_runtime, deployment, limit=50, filter={"subtype": "comment"})
        assert _ids(eq) == _ids(by_subtype)
        assert eq["total_count"] == by_subtype["total_count"]
        assert eq["result_unit"] == by_subtype["result_unit"] == "passage"
        # The comments under a parent are listed, not only the parentless.
        assert {"comment:long:3", "comment:quiet:0", "comment:orphan", "comment:twin"} <= set(_ids(eq))
    else:
        # Failure case: a record type still judges records; no passage row.
        assert eq["result_unit"] == "record"
        assert eq["where_qualification"] == "exact"
        assert not any(i.startswith("comment:") and i not in (
            "comment:orphan", "comment:twin") for i in _ids(eq))


@pytest.mark.parametrize("kwargs", [
    {"query": "zirconium \ud800"},
    {"query": "zirconium", "path_prefix": "app/\ud800"},
], ids=["query", "path_prefix"])
def test_a_value_the_scanner_cannot_fingerprint_is_refused_by_name(
    clean_runtime, tmp_path, kwargs,
):
    """#3226 refuses a scan identity with no JSON form (`invalid_scan_value`);
    grep refuses it first, by name, before any database read."""
    from okg.substrate.mcp.grep_query import GrepQueryInvalid

    deployment = _basic(tmp_path, clean_runtime)
    decoded = dict(kwargs)  # the escapes are lone surrogates, not printable
    query = decoded.pop("query")
    with pytest.raises(GrepQueryInvalid) as refused:
        _grep(clean_runtime, deployment, query, limit=5, **decoded)
    assert refused.value.data["reason"] == "invalid_scan_value"
    assert refused.value.data["part"] == "query"


# ---------- profile decisions: `type = X` and `type in [...]` alike (review M1) ----------

PROFILES = {
    "default_profile": "notes",
    "profiles": {"notes": {"subtypes": ["note"]}, "tickets": {"subtypes": ["ticket"]}},
}


@pytest.fixture
def search_tool(clean_runtime, okg_mcp_can_log_in, tmp_path, monkeypatch):
    """`_basic` with a deployment `search:` block (default profile `notes`),
    served by a real server so the default-profile decision is the
    server's own; the server's `search` tool."""
    import json
    from pathlib import Path

    from okg.substrate.mcp import server as srv

    deployment = _basic(tmp_path, clean_runtime)
    manifest = Path(deployment) / "deployment.yaml"
    body = json.loads(manifest.read_text(encoding="utf-8"))
    manifest.write_text(json.dumps({**body, "search": PROFILES}), encoding="utf-8")
    monkeypatch.setenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", "off")
    mcp = srv.build_server(dsn=clean_runtime, deployment_name=deployment)
    try:
        yield mcp._tool_manager._tools["search"].fn
    finally:
        mcp._okg_pool.close()


TICKET_EQ = {"op": "eq", "field": "type", "value": "ticket"}
TICKET_IN = {"op": "in", "field": "type", "values": ["ticket"]}


def test_a_grep_type_clause_names_its_subtype_for_the_default_profile(search_tool):
    """Review M1: `ticket` is outside the default profile `notes`. Naming it
    in `where` (either spelling) states the caller's scope, as
    `subtype_filter` does (D9), so the default profile does not apply and
    the tickets match; the two spellings give the same page."""
    eq = search_tool(query=TERM, method="grep", where=TICKET_EQ, limit=50)
    listed = search_tool(query=TERM, method="grep", where=TICKET_IN, limit=50)
    # Failure case: with no `where` the default profile scopes to notes.
    plain = search_tool(query=TERM, method="grep", limit=50)
    assert _ids(plain) == ["note:0"] and plain["search_profile"] == "notes"
    assert "ticket:closed" in _ids(eq) and "note:0" not in _ids(eq)
    assert eq.get("search_profile") is None and eq.get("scoped_subtypes") is None
    for key in ("results", "total_count", "total_count_basis", "search_profile",
                "scoped_subtypes", "where_qualification"):
        assert eq.get(key) == listed.get(key), key


@pytest.mark.parametrize("where", [
    TICKET_EQ,
    TICKET_IN,
    {"op": "in", "field": "type", "values": ["note", "ticket"]},
    {"op": "and", "clauses": [TICKET_EQ, {"op": "exists", "field": "attrs.status"}]},
], ids=["eq", "in", "in-partly-outside", "and"])
def test_a_grep_type_clause_outside_an_explicit_profile_is_refused(search_tool, where):
    """Review M1: under an explicit profile a subtype named outside it is
    refused `subtype_outside_profile`, never answered with an empty page."""
    from okg.substrate.mcp.search_profiles import SubtypeOutsideProfile

    with pytest.raises(SubtypeOutsideProfile) as refused:
        search_tool(query=TERM, method="grep", where=where, profile="notes", limit=50)
    assert refused.value.code == "subtype_outside_profile"
    assert refused.value.data["subtype"] == "ticket"
    assert refused.value.data["profile"] == "notes"


def test_a_grep_type_clause_inside_an_explicit_profile_is_served(search_tool):
    eq = search_tool(query=TERM, method="grep", where=TICKET_EQ, profile="tickets",
                     limit=50)
    listed = search_tool(query=TERM, method="grep", where=TICKET_IN, profile="tickets",
                         limit=50)
    assert eq["search_profile"] == "tickets" and "ticket:near:0" in _ids(eq)
    assert eq["results"] == listed["results"]
    assert eq["total_count"] == listed["total_count"]


def test_a_type_clause_under_or_names_no_subtype(search_tool):
    """Only top-level conjuncts name subtypes: under `or` the clause is a
    record-judged condition and the default profile still applies."""
    page = search_tool(query=TERM, method="grep", limit=50, where={"op": "or", "clauses": [
        TICKET_EQ, {"op": "eq", "field": "type", "value": "note"}]})
    assert page["search_profile"] == "notes" and _ids(page) == ["note:0"]


def test_lexical_profile_decisions_are_unchanged(search_tool):
    """Lexical keeps its own rule: the equality lifts into its subtype
    filter and names the subtype; `type in [...]` stays a residual and the
    default profile applies (unchanged by review M1, disclosed in the PACT)."""
    from okg.substrate.mcp.search_profiles import SubtypeOutsideProfile

    eq = search_tool(query=TERM, where=TICKET_EQ, limit=50)
    listed = search_tool(query=TERM, where=TICKET_IN, limit=50)
    with pytest.raises(SubtypeOutsideProfile):
        search_tool(query=TERM, where=TICKET_EQ, profile="notes", limit=50)
    outside_in = search_tool(query=TERM, where=TICKET_IN, profile="notes", limit=50)
    assert eq.get("search_profile") is None and "ticket:closed" in _ids(eq)
    assert listed["search_profile"] == "notes" and _ids(listed) == []
    assert outside_in["search_profile"] == "notes" and _ids(outside_in) == []


def test_profile_subtypes_reads_only_top_level_type_conjuncts():
    names = search_where.profile_subtypes
    assert names(None) == ()
    assert names(TICKET_EQ) == names(TICKET_IN) == ("ticket",)
    assert names({"op": "and", "clauses": [
        TICKET_EQ, {"op": "in", "field": "type", "values": ["note", "ticket"]},
        {"op": "eq", "field": "source", "value": MAIN},
    ]}) == ("ticket", "note")
    assert names({"op": "not", "clause": TICKET_EQ}) == ()
    assert names({"op": "or", "clauses": [TICKET_EQ, TICKET_IN]}) == ()


# ---------- audit with a bundle that hides rows (review L1, L2) ----------

def _decisions(dsn: str, source: str, effect: str) -> int:
    """Decision rows for rows of `source` with `effect` (a decision class is
    recorded once, on its representative row)."""
    with psycopg.connect(dsn, autocommit=True) as conn:
        return conn.execute(
            "SELECT count(*) FROM okg.nomos_decisions"
            " WHERE resource->'source_ids' ? %s AND effect = %s",
            (source, effect),
        ).fetchone()[0]


SECRET_FACTS = [
    _node("ticket:sec", "ticket", f"{TERM} secret ticket.", status="open"),
    _node("note:sec", "note", f"A {TERM} secret note."),
]


@pytest.mark.parametrize("where,qualification", [
    (None, None),
    ({"op": "in", "field": "type", "values": ["ticket"]}, "exact"),
    ({"op": "in", "field": "attrs.status", "values": ["open"]}, "superset"),
], ids=["no-where", "exact", "superset"])
def test_audit_logs_a_refused_row_and_still_shows_it(
    clean_runtime, tmp_path, monkeypatch, where, qualification,
):
    """Review L1: the bundle allows only MAIN, so SECRET's rows are refused.
    Under `audit` (R1) a refusal is logged (a `deny` decision row) and the
    row is shown: listed, counted exactly, and judged by the `where` as the
    reader reads it. Under `enforce` the same bundle hides it."""
    deployment = _publish(tmp_path, clean_runtime, groups={SECRET: SECRET_FACTS},
                          main=_basic_facts())
    _activate(clean_runtime, deployment, [_allow(MAIN)])
    extra = {} if where is None else {"where": where}
    monkeypatch.setenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", "audit")
    denied_before = _decisions(clean_runtime, SECRET, "deny")
    base = _grep(clean_runtime, deployment, limit=50, subject=READER)
    assert _decisions(clean_runtime, SECRET, "deny") > denied_before
    assert {"ticket:sec", "note:sec"} <= set(_ids(base))
    admitted = (
        set(_ids(base)) if where is None
        else _reader_admits(clean_runtime, deployment, _ids(base), where, subject=READER)
    )
    expected = [i for i in _ids(base) if i in admitted]
    page = _grep(clean_runtime, deployment, limit=50, subject=READER, **extra)
    assert _ids(page) == expected and "ticket:sec" in expected
    assert page["total_count"] == len(expected)
    assert page["total_count_basis"] == "exact"
    assert page.get("where_qualification") == qualification
    # Failure case: enforce hides the refused rows from the page and count.
    monkeypatch.setenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", "1")
    hidden = _grep(clean_runtime, deployment, limit=50, subject=READER, **extra)
    assert "ticket:sec" not in _ids(hidden) and "note:sec" not in _ids(hidden)
    assert hidden["total_count"] == len(expected) - len(
        {"ticket:sec", "note:sec"} & set(expected))


def test_audit_never_counts_a_row_it_hides(
    clean_runtime, tmp_path, monkeypatch,
):
    """Review L2, #3384: `note:masked`'s text is masked for this reader, so
    even `audit` hides it (B1/B2). Under `audit` every count finishes each
    unit on the values the reader reads (no stored-row SQL count), so it
    never counts it, with or without a `where`, as under `enforce`. The
    policy can redact, so every count is a lower bound (198a)."""
    from tests.substrate.mcp.test_search_grep import READER as GREP_READER
    from tests.substrate.mcp.test_search_grep import _redacting_deployment

    deployment = _redacting_deployment(tmp_path, clean_runtime, {"text": "quasi_identifier"})
    residual = {"op": "not", "clause": {"op": "exists", "field": "attrs.zz"}}
    exact = {"op": "in", "field": "type", "values": ["note", "doc"]}
    counts = {}
    for mode in ("audit", "1"):
        monkeypatch.setenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", mode)
        for name, kwargs in (("plain", {}), ("exact", {"where": exact}),
                             ("residual", {"where": residual})):
            page = _grep(clean_runtime, deployment, "launch", subject=GREP_READER,
                         limit=50, **kwargs)
            assert "note:masked" not in _ids(page)
            assert page["total_count_basis"] == "lower_bound"
            assert page["count_basis_reasons"] == ["redaction"]
            counts[mode, name] = (page["total_count"], len(page["results"]))
    # The hidden row is never counted (#3384; before, audit's SQL count
    # gave 3 here).
    assert counts["audit", "plain"] == counts["1", "plain"] == (2, 2)
    assert counts["audit", "exact"] == counts["1", "exact"] == (2, 2)
    # The residual count counts only what the reader can judge.
    assert counts["audit", "residual"] == counts["1", "residual"] == (1, 1)


@pytest.mark.parametrize("where", [None, {"op": "in", "field": "type", "values": ["ticket"]}],
                         ids=["no-where", "exact"])
def test_audit_counts_readable_units_as_far_as_the_sql_count_did(
    clean_runtime, tmp_path, monkeypatch, where,
):
    """#3384 review: under `audit` the count finishes units instead of
    counting stored rows, but over the SQL count's own bound
    (`COUNT_SCAN_BOUND`), not the residual count's smaller one: a readable
    count is no less exact than it was. Failure case: lowered to fewer
    stored units than match, it is a lower bound with that bound named."""
    deployment = _basic(tmp_path, clean_runtime)
    kwargs = _regime(clean_runtime, deployment, monkeypatch, "audit")
    extra = {} if where is None else {"where": where}
    monkeypatch.setattr(grep_search, "COUNT_FINISH_ROWS", 1)
    page = _grep(clean_runtime, deployment, limit=50, **kwargs, **extra)
    assert len(page["results"]) > 1
    assert page["total_count"] == len(page["results"])
    assert page["total_count_basis"] == "exact"
    monkeypatch.setattr(grep_search, "COUNT_SCAN_BOUND", 1)
    bounded = _grep(clean_runtime, deployment, limit=50, **kwargs, **extra)
    assert bounded["total_count_basis"] == "lower_bound"
    assert bounded["scan_cap"] == {"rows": 1}


def _count_spy(monkeypatch) -> list[dict]:
    seen: list[dict] = []
    real = grep_search.count_grep_matches

    def spy(*args, **kwargs):
        seen.append(kwargs)
        return real(*args, **kwargs)

    monkeypatch.setattr(grep_search, "count_grep_matches", spy)
    return seen


def test_audit_keeps_the_sql_count_when_it_can_hide_nothing(
    clean_runtime, tmp_path, monkeypatch,
):
    """#3384 review: under `audit` with no residual `where`, the single SQL
    count stands when the policy cannot redact and every row of the
    generation has source lineage; a term nobody stored counts 0 the same
    way."""
    deployment = _basic(tmp_path, clean_runtime)
    kwargs = _regime(clean_runtime, deployment, monkeypatch, "audit")
    seen = _count_spy(monkeypatch)
    page = _grep(clean_runtime, deployment, limit=50, **kwargs)
    assert seen[-1]["finish_sql"] is None and seen[-1]["strict"] is False
    assert page["total_count"] == len(page["results"]) > 1
    assert page["total_count_basis"] == "exact"
    absent = _grep(clean_runtime, deployment, "yttriumabsent", limit=50, **kwargs)
    assert seen[-1]["finish_sql"] is None and absent["total_count"] == 0


@pytest.mark.parametrize("matched", [True, False], ids=["matching-row", "other-row"])
def test_audit_counts_per_row_when_a_row_lacks_lineage(
    clean_runtime, tmp_path, monkeypatch, matched,
):
    """Failure case: one row of the generation without source lineage sends
    the audit count per row, whether or not the term matches that row (the
    test is generation-wide, never over the match set, so it cannot tell
    which rows hold the term); the lineage-less row is never counted."""
    from okg.substrate.mcp.semantic_readiness import resolve_node_segment
    from okg.substrate.nomos.source_lineage import LINEAGE_ATTR

    deployment = _basic(tmp_path, clean_runtime)
    monkeypatch.setenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", "off")
    stored = _ids(_grep(clean_runtime, deployment, limit=50))
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        segment = resolve_node_segment(conn)
        if matched:
            victim = stored[0]
        else:
            victim = conn.execute(
                f"SELECT node_id FROM {segment} WHERE deleted = FALSE "
                "AND NOT (node_id = ANY(%s)) ORDER BY node_id LIMIT 1", (stored,),
            ).fetchone()[0]
        assert conn.execute(
            f"UPDATE {segment} SET source = NULL, attrs = attrs - '{LINEAGE_ATTR}' "
            "WHERE node_id = %s", (victim,),
        ).rowcount
    kwargs = _regime(clean_runtime, deployment, monkeypatch, "audit")
    seen = _count_spy(monkeypatch)
    page = _grep(clean_runtime, deployment, limit=50, **kwargs)
    assert seen[-1]["finish_sql"] is not None
    assert victim not in _ids(page)
    assert page["total_count"] == len(page["results"])
    assert page["total_count_basis"] == "exact"


def test_audit_hides_nothing_reads_its_cache_in_one_lookup(monkeypatch):
    """Tool calls run in worker threads (#3493): another call's `clear()`
    between a membership test and the read would raise `KeyError`. Failure
    case: a cache cleared right after `in` answers True."""
    from types import SimpleNamespace

    class ClearedAfterContains(dict):
        def __contains__(self, key):
            found = super().__contains__(key)
            self.clear()
            return found

    conn = SimpleNamespace(info=SimpleNamespace(host="h", port=1, dbname="d"))
    session = SimpleNamespace(generation_id="g", nomos_identity_claim=None,
                              nomos_access_context=None)
    monkeypatch.setattr(grep_search, "resolve_edge_segment", lambda _conn: "okg.e")
    key = ("h", 1, "d", "g", "okg.n", "okg.e", "7")
    monkeypatch.setattr(grep_search, "_AUDIT_HIDES_NOTHING", ClearedAfterContains({key: True}))

    def no_scan(*_args, **_kwargs):
        raise AssertionError("a cached answer must not scan the generation")

    monkeypatch.setattr(grep_search, "_bounded", no_scan)
    assert grep_search._audit_hides_nothing(
        conn, session, redaction=False, segment="okg.n", branch_point_value=7,
    ) is True


def test_a_passage_type_where_lists_the_passages_and_says_so(
    clean_runtime, tmp_path, monkeypatch,
):
    """Issue #3666: `where type = <a passage type>` on grep judged each
    passage through its record, so a passage with a record never matched
    and the page said "0 of 0 · complete" for text the passage holds. It
    lists the passages, the rest of the `where` judges each passage itself,
    and the reply notes the reading. Failure case: a record type is not
    read this way and notes nothing."""
    from okg.substrate.mcp import response_bounds

    deployment = _basic(tmp_path, clean_runtime)
    noted: list = []
    monkeypatch.setattr(response_bounds, "note_adjusted", noted.extend)
    where = {"op": "and", "clauses": [
        {"op": "eq", "field": "type", "value": "comment"},
        {"op": "exists", "field": "attrs.heading_path"},
    ]}
    page = _grep(clean_runtime, deployment, "cladding", limit=50, where=where)
    assert _ids(page) == ["comment:long:3"]
    assert page["result_unit"] == "passage"
    assert page["total_count"] == 1
    assert noted == [{
        "rule": "grep_passage_type", "sent": "where type = `comment`",
        "served": "its passages, each judged itself (subtype `comment`)",
    }]

    noted.clear()
    record = _grep(clean_runtime, deployment, "cladding", limit=50,
                   where={"op": "eq", "field": "type", "value": "ticket"})
    assert record["result_unit"] == "record" and noted == []
    assert "ticket:long" in _ids(record)


# ---------- #3666: the passage-type false zero ----------

PASSAGE_EQ = {"op": "eq", "field": "type", "value": "comment"}


def test_a_passage_type_in_where_lists_the_passages_not_zero(clean_runtime, tmp_path):
    """Must-fail on the base: `weld` is held only by `ticket:long` and its
    four comments, each with one parent. The type clause judged the parent
    (a ticket, never a `comment`), dropped every row and answered "0 of 0,
    complete". Now it lists the four comments, as `subtype_filter` does."""
    deployment = _basic(tmp_path, clean_runtime)
    page = _grep(clean_runtime, deployment, "weld", limit=50, where=PASSAGE_EQ)
    by_subtype = _grep(clean_runtime, deployment, "weld", limit=50,
                       filter={"subtype": "comment"})
    assert sorted(_ids(page)) == [f"comment:long:{i}" for i in range(4)]
    assert page["total_count"] == 4 and page["total_count_basis"] == "exact"
    assert page["result_unit"] == "passage"
    assert _ids(page) == _ids(by_subtype)
    # Failure case: a record type in the same call still lists the record.
    record = _grep(clean_runtime, deployment, "weld", limit=50,
                   where={"op": "eq", "field": "type", "value": "ticket"})
    assert _ids(record) == ["ticket:long"]


def test_the_rest_of_the_where_judges_the_passages_themselves(clean_runtime, tmp_path):
    deployment = _basic(tmp_path, clean_runtime)
    where = {"op": "and", "clauses": [
        PASSAGE_EQ, {"op": "eq", "field": "attrs.chunk_ordinal", "value": 3},
    ]}
    page = _grep(clean_runtime, deployment, "weld", limit=50, where=where)
    assert _ids(page) == ["comment:long:3"]
    assert page["total_count"] == 1
    # Failure case: an ordinal no comment has lists nothing, and says so.
    none = _grep(clean_runtime, deployment, "weld", limit=50, where={"op": "and", "clauses": [
        PASSAGE_EQ, {"op": "eq", "field": "attrs.chunk_ordinal", "value": 9},
    ]})
    assert _ids(none) == [] and none["total_count"] == 0


def test_a_passage_type_lift_is_pure_and_leaves_other_clauses():
    passage = {"comment"}.__contains__
    lifted = search_where.lift_passage_type(
        {"where": {"op": "and", "clauses": [
            {"op": "in", "field": "type", "values": ["comment"]},
            {"op": "exists", "field": "attrs.status"},
        ]}}, passage,
    )
    assert lifted == {"subtype": "comment",
                      "where": {"op": "exists", "field": "attrs.status"}}
    # Failure cases: a record type, two types, a type under `not`, or a
    # subtype already set are left to judge the record.
    for filter in (
        {"where": {"op": "eq", "field": "type", "value": "ticket"}},
        {"where": {"op": "in", "field": "type", "values": ["comment", "ticket"]}},
        {"where": {"op": "not", "clause": PASSAGE_EQ}},
        {"subtype": "ticket", "where": PASSAGE_EQ},
    ):
        assert search_where.lift_passage_type(filter, passage) is None


# ---------- #3666 R5: folded spellings, one line per match, what was searched ----------

COMPOSED = (
    "summary: Placement review\n"
    "description: We set keep-value for the site.\n"
    "It stays until the review ends.\n"
    "recent_comments:\n"
    "Alex Doe 2026-04-21T17:54:32.224+0200: Done: keep_value set to False\n"
    "Sam Roe 2026-04-22T09:00:00.000+0000: Thanks, closing."
)


#: The untrusted-data fence markers the read boundary wraps source text in.
_FENCE = re.compile(r"⟦/?UNTRUSTED_DATA[^⟧]*⟧\n?")


def _unfenced(value):
    return None if value is None else _FENCE.sub("", value).strip()


def _spelling_corpus(tmp_path, dsn: str) -> str:
    from tests.substrate.mcp.test_search_records import _edge

    return _publish(tmp_path, dsn, groups={}, main=[
        _node("ticket:place", "ticket", "Placement ticket.", status="open"),
        _node("comment:place:0", "comment", COMPOSED),
        _edge("ticket:place", "comment:place:0"),
        _node("note:cfg", "note", "first line\nmode = KEEP.VALUE\nlast line",
              path="conf/site.cfg"),
        _node("note:other", "note", "nothing to see here"),
    ])


def test_grep_folds_separators_and_reports_lines_and_spellings(clean_runtime, tmp_path):
    deployment = _spelling_corpus(tmp_path, clean_runtime)
    page = _grep(clean_runtime, deployment, "keep_value", limit=10)
    rows = {row["node_id"]: row for row in page["results"]}
    assert set(rows) == {"ticket:place", "note:cfg"}
    searched = page["grep_searched"]
    assert set(searched["spellings_matched"]) == {"keep-value", "keep_value", "KEEP.VALUE"}
    assert searched["spellings_tried"] == [
        "keep_value: any case, and any run of _ - . or space where it has one "
        "between letters or digits",
    ]
    assert searched["fields"] == ["text"] and searched["comments"] is True
    lines = rows["ticket:place"]["match_lines"]
    # The fixture's source is untrusted: the line and the author are fenced
    # as the windows' excerpts are (failure case for an unfenced author).
    assert all(_FENCE.search(e["text"]) for e in lines)
    assert [bool(_FENCE.search(e["author"])) for e in lines if e["author"]] == [True]
    assert [(e["field"], _unfenced(e["text"]), _unfenced(e["author"]), e["date"], e["line"])
            for e in lines] == [
        # A description paragraph shows whole (#3666, lane RD).
        ("description", "We set keep-value for the site.\nIt stays until the review ends.",
         None, None, None),
        ("comment", "Done: keep_value set to False", "Alex Doe", "2026-04-21", None),
    ]
    assert [e["chars_left"] for e in lines] == [0, 0]
    # A file's text: the line number, the line as written.
    assert [(e["field"], _unfenced(e["text"]), e["line"])
            for e in rows["note:cfg"]["match_lines"]] == [
        ("text", "mode = KEEP.VALUE", 2),
    ]
    # JSON compatibility: the windows are still served.
    assert rows["note:cfg"]["match_windows"]


def test_a_zero_hit_grep_says_what_it_searched(clean_runtime, tmp_path):
    deployment = _spelling_corpus(tmp_path, clean_runtime)
    # Failure case: no separator to fold, so the joined spelling finds nothing.
    page = _grep(clean_runtime, deployment, "keepvalue", limit=10)
    assert page["results"] == [] and page["total_count"] == 0
    searched = page["grep_searched"]
    assert searched["spellings_matched"] == []
    assert searched["spellings_tried"] == ["keepvalue: any case"]
    assert searched["comments"] is True and searched["subtypes"] is None
    # Scoped to a type with no passages, the reply does not claim comments.
    scoped = _grep(clean_runtime, deployment, "keepvalue", limit=10,
                   filter={"subtype": "note"})
    assert scoped["grep_searched"]["comments"] is False
    assert scoped["grep_searched"]["subtypes"] == ["note"]


@pytest.mark.parametrize("select", [["node_id", "attrs"], ["node_id", "attrs.mode"]])
def test_a_select_keeps_the_match_lines(clean_runtime, tmp_path, select):
    """Must-fail on f9634f77389: a grep with a `select` that did not name
    `match_lines` (it is not a select field) dropped every row's match
    lines, so the rows printed a head line and nothing that matched. A
    select naming attributes keeps them (PACT read-reply-size-3666)."""
    deployment = _spelling_corpus(tmp_path, clean_runtime)
    page = _grep(clean_runtime, deployment, "keep_value", limit=10, select=select)
    rows = {row["node_id"]: row for row in page["results"]}
    assert [_unfenced(e["text"]) for e in rows["note:cfg"]["match_lines"]] == [
        "mode = KEEP.VALUE",
    ]
    assert rows["ticket:place"]["match_lines"]
    # Failure case: the select still decides the other fields.
    assert ("attrs" in rows["note:cfg"]) is ("attrs" in select)


def test_a_select_of_id_and_head_fields_serves_head_lines_only(clean_runtime, tmp_path):
    """PACT read-reply-size-3666: `select=["node_id"]` (or any id and head
    fields alone) asks for head lines, so a grep row carries no match
    lines; the rows and their order are the same as without a select."""
    deployment = _spelling_corpus(tmp_path, clean_runtime)
    narrow = _grep(clean_runtime, deployment, "keep_value", limit=10, select=["node_id"])
    full = _grep(clean_runtime, deployment, "keep_value", limit=10)
    assert [row["node_id"] for row in narrow["results"]] == [
        row["node_id"] for row in full["results"]
    ]
    assert all("match_lines" not in row for row in narrow["results"])
    assert all(row["match_lines"] for row in full["results"])


def _long_comment_corpus(tmp_path, dsn: str) -> tuple[str, str, str]:
    from tests.substrate.mcp.test_search_records import _edge

    def sentences(start: int, count: int) -> str:
        return " ".join(
            f"Sentence {i} says the panel keeps setting {i} after restart."
            for i in range(start, start + count)
        )

    medium = "The keep_value flag was moved. " + sentences(0, 12)
    long = sentences(0, 25) + " The keep_value flag was moved here. " + sentences(25, 25)
    text = (
        "summary: Panel review\n"
        "description: A short body.\n"
        "recent_comments:\n"
        f"Alex Doe 2026-04-21T17:54:32.224+0200: {medium}\n"
        f"Sam Roe 2026-04-22T09:00:00.000+0000: {long}"
    )
    deployment = _publish(tmp_path, dsn, groups={}, main=[
        _node("ticket:panel", "ticket", "Panel ticket.", status="open"),
        _node("comment:panel:0", "comment", text),
        _edge("ticket:panel", "comment:panel:0"),
        _node("note:wide", "note", "x " * 300 + "keep_value " + "y " * 300,
              path="conf/wide.cfg"),
    ])
    return deployment, medium, long


def test_a_matched_comment_is_served_whole_or_cut_with_its_count(clean_runtime, tmp_path):
    """Must-fail on a58e7786ff (#3666, lane RD): every match line was cut to
    200 characters, a comment's sentences after the cut were never served,
    and the line did not say it was cut."""
    deployment, medium, long = _long_comment_corpus(tmp_path, clean_runtime)
    page = _grep(clean_runtime, deployment, "keep_value", limit=10)
    rows = {row["node_id"]: row for row in page["results"]}
    lines = rows["ticket:panel"]["match_lines"]
    assert [(e["field"], _unfenced(e["author"])) for e in lines] == [
        ("comment", "Alex Doe"), ("comment", "Sam Roe"),
    ]
    first, second = lines
    assert _unfenced(first["text"]) == medium and first["chars_left"] == 0
    shown = _unfenced(second["text"])
    assert "The keep_value flag was moved here." in shown
    assert shown.startswith("...") and shown.endswith("...")
    assert len(shown) <= grep_search.PROSE_LINE_CUT_CHARS + 6
    assert second["chars_left"] == len(long) - len(shown[3:-3])
    # Failure case: a file's line keeps the 200-character window, uncounted.
    (wide,) = rows["note:wide"]["match_lines"]
    assert len(_unfenced(wide["text"])) <= grep_search.MATCH_LINE_CHARS + 6
    assert "chars_left" not in wide
    # A caller's max_chars (D2) sizes a comment line: wide enough, it is whole.
    sized = _grep(clean_runtime, deployment, "keep_value", limit=10, passage_chars=4_000)
    sized_rows = {row["node_id"]: row for row in sized["results"]}
    (_, whole) = sized_rows["ticket:panel"]["match_lines"]
    assert _unfenced(whole["text"]) == long and whole["chars_left"] == 0


# ---------- Format v10 item G2: a selective `where` and the page bound ----------

#: Not a plain alternation of literals, so no BM25 rank words: the page is
#: read in node-id order, as the replayed regex calls were.
G2_QUERY = "re:/curle[w]|plove[r]/"
G2_REPO = {"op": "eq", "field": "attrs.repo", "value": "r1"}
G2_NOTE = {"op": "eq", "field": "type", "value": "note"}


def _bound_corpus(tmp_path, dsn: str) -> str:
    """Matching passages under tickets of another repo come first in
    node-id order (`comment:g*`), then the comment of a ticket of the asked
    repo (`comment:zz` under `ticket:r1x`), notes of another repo
    (`note:a*`), and the two notes the filter asks for (`note:b*`)."""
    from tests.substrate.mcp.test_search_records import _edge

    facts: list = []
    for i in range(6):
        facts += [
            _node(f"ticket:g{i}", "ticket", "a quiet ticket", repo="r2"),
            _node(f"comment:g{i}", "comment", f"curlew seen in comment {i}"),
            _edge(f"ticket:g{i}", f"comment:g{i}"),
        ]
    facts += [
        _node("ticket:r1x", "ticket", "a quiet ticket", repo="r1"),
        _node("comment:zz", "comment", "curlew seen under the asked repo"),
        _edge("ticket:r1x", "comment:zz"),
    ]
    facts += [_node(f"note:a{i}", "note", f"plover elsewhere {i}", repo="r2") for i in range(4)]
    facts += [_node(f"note:b{i}", "note", f"curlew here {i}", repo="r1") for i in range(2)]
    return _publish(tmp_path, dsn, groups={}, main=facts)


@pytest.mark.parametrize("regime", ["off", "audit"])
@pytest.mark.parametrize("where,expected", [
    ({"op": "and", "clauses": [G2_NOTE, G2_REPO]}, ["note:b0", "note:b1"]),
    (G2_REPO, ["ticket:r1x", "note:b0", "note:b1"]),
], ids=["type-and-repo", "repo-only"])
def test_a_selective_where_does_not_spend_the_bound_on_other_records(
    clean_runtime, tmp_path, monkeypatch, regime, where, expected,
):
    """Must-fail on 123fb64253 (Format v10 item G2, 18 of 18 replayed
    calls): every passage of every parent passed the stored predicate, so
    the comments under other repos' tickets spent the candidate bound (3
    here, 2,000 live) and the page came back empty, "bound reached, no
    records on this page". The `where` type now narrows the scope to its
    types and their declared passages, and the attribute equality admits a
    passage only through its own or a declared parent's stored value."""
    deployment = _bound_corpus(tmp_path, clean_runtime)
    kwargs = _regime(clean_runtime, deployment, monkeypatch, regime)
    unbounded = _grep(clean_runtime, deployment, G2_QUERY, limit=100, where=where, **kwargs)
    assert _ids(unbounded) == expected
    monkeypatch.setattr(grep_search, "PAGE_SCAN_BOUND", 3)
    page = _grep(clean_runtime, deployment, G2_QUERY, limit=100, where=where, **kwargs)
    assert _ids(page) == expected
    assert page["page_basis"] == "top_of_all_visible_matches"
    assert "page_scan_bound_hit" not in page


def test_a_bound_reached_before_any_record_says_how_far_it_read(
    clean_runtime, tmp_path, monkeypatch,
):
    """Must-fail on 123fb64253: an empty page cut at the candidate bound
    said "! no match" though only the first candidates were read. A `where`
    with no top-level type or equality (here a `not`) narrows nothing, so
    the comments still come first and drop with their parents; the
    unbounded call shows matches did exist past the bound."""
    from okg.substrate.mcp.result_text import render

    deployment = _bound_corpus(tmp_path, clean_runtime)
    monkeypatch.setenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", "off")
    where = {"op": "not", "clause": {"op": "eq", "field": "attrs.repo", "value": "r2"}}
    unbounded = _ids(_grep(clean_runtime, deployment, G2_QUERY, limit=100, where=where))
    assert unbounded == ["ticket:r1x", "note:b0", "note:b1"]
    monkeypatch.setattr(grep_search, "PAGE_SCAN_BOUND", 3)
    page = _grep(clean_runtime, deployment, G2_QUERY, limit=100, where=where)
    assert _ids(page) == []
    assert page["page_basis"] == "scan_bound_reached"
    assert page["grep_searched"]["bound_read"] == {"bound": "candidate_rows", "rows": 3}
    lines = render(page, tool="search").splitlines()
    assert not any(line.startswith("! no match;") for line in lines), lines
    assert any(line.startswith(
        "! no match in the first 3 candidate records (of 4 or more); the rest are not read yet"
    ) for line in lines), lines


@pytest.fixture
def bound_search_tool(clean_runtime, okg_mcp_can_log_in, tmp_path, monkeypatch):
    """`_bound_corpus` served by a real server; the server's `search` tool."""
    from okg.substrate.mcp import server as srv

    deployment = _bound_corpus(tmp_path, clean_runtime)
    monkeypatch.setenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", "off")
    mcp = srv.build_server(dsn=clean_runtime, deployment_name=deployment)
    try:
        yield mcp._tool_manager._tools["search"].fn
    finally:
        mcp._okg_pool.close()


def test_a_grep_page_token_sent_back_without_method_continues_the_scan(
    bound_search_tool, monkeypatch,
):
    """Must-fail on 123fb64253: the token a bound-reached grep page printed
    (`next: continuation_token=...`) came back without `method`, and search
    refused it (`continuation_token_not_applicable`, "page with offset").
    It is now read as the grep call it continues, and the chain lists every
    record the unbounded call lists, in order, with no repeat or gap."""
    where = G2_NOTE
    unbounded = bound_search_tool(query=G2_QUERY, method="grep", where=where, limit=100)
    expected = [f"note:a{i}" for i in range(4)] + ["note:b0", "note:b1"]
    assert _ids(unbounded) == expected
    monkeypatch.setattr(grep_search, "PAGE_SCAN_BOUND", 2)
    page = bound_search_tool(query=G2_QUERY, method="grep", where=where, limit=100)
    assert page["page_basis"] == "scan_bound_reached"
    first_token = page["continuation_token"]
    chained = _ids(page)
    for _ in range(10):
        token = page.get("continuation_token")
        if not token:
            break
        page = bound_search_tool(query=G2_QUERY, where=where, limit=100,
                                 continuation_token=token)
        assert "error" not in page, page
        chained += _ids(page)
    assert chained == expected
    # An in-process call carries no sent-argument record, so `method` here
    # cannot be told from its default and the token decides: the same next
    # page. (An explicit other method sent through the MCP dispatch is
    # refused, naming the grep call: test_grep_mcp.py
    # ::test_a_grep_page_token_on_an_explicit_other_method_names_the_grep_call.)
    again = bound_search_tool(query=G2_QUERY, method="lexical", where=where,
                              limit=100, continuation_token=first_token)
    assert "error" not in again, again
    assert _ids(again) == expected[2:4]
