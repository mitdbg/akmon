"""`where` and record `scope` on search, on a published PostgreSQL graph.

PACT read-model-s3-where-scope (issue #3194; design
`pact/changes/read-model/design.md` 3.3, 3.4, 6). Each requirement with its
failure case:

* the old arguments and `where` return identical pages (D9);
* in a record view a clause applies to the record and a passage matches
  through it; under `rank_by="self"` each row is judged by itself (D6);
* a clause on a masked value is unknown, `not` keeps it unknown, and an
  unknown row is excluded: the secret ticket and its readable control
  differ only in what the reader may read (D5);
* `scope` restricts a lexical search to named records and their passages,
  and a hidden record answers exactly as a missing one (3.4);
* the query-less lookup takes a `where` with `prefix`.

Hiding and masking run under Nomos `enforce` (`audit` never hides a row).
"""
from __future__ import annotations

import itertools

import psycopg
import pytest
from psycopg import sql as pgsql
from psycopg.rows import dict_row

from okg.substrate.mcp import open_session, search, search_where as sw
from okg.substrate.mcp.continuation import ContinuationTokenInvalid, continuation_binding
from okg.substrate.mcp.read_scanner import (
    EnumerationEvidence,
    FinishedCandidate,
    ScanBatch,
    consume_scan_continuation,
    finish_scan_batch,
    issue_scan_continuation,
    make_scan_context,
)
from okg.substrate.mcp.search_where import SearchScopeUnsupported, WhereUnsupported
from okg.substrate.nomos.read_masking import MASK_TOKEN, REDACTION_MARKER
from tests.substrate.mcp.read_compiler_legacy import legacy_compile_where, legacy_sql
from tests.substrate.mcp.test_search_records import (
    MAIN,
    MASKED,
    OPEN,
    QUERY,
    READER,
    TERM,
    _activate,
    _allow,
    _assignee_deployment,
    _basic,
    _by_id,
    _ids,
    _nomos_deployment,
    _search,
)


def _date_corpus(tmp_path, dsn):
    from tests.pytest.catalog_fixture_builders import SubtypeSpec
    from tests.substrate.mcp.search_fixture import make_search_deployment, publish_nodes, node
    from okg.substrate.library.sources.base import EdgeFact

    deployment = make_search_deployment(tmp_path, dsn=dsn)
    metadata = {"searchable_text_fields": ["text"], "title_from": "heading",
                "path_from": "member_path", "created_at_from": "created",
                "last_modified_at_from": "edited", "published_at_from": "published",
                "resolved_at_from": "resolved", "effective_at_from": "effective",
                "passage_rollup": {"edge_type": "contains", "child_subtype": "date_chunk"}}
    _, generation = publish_nodes(dsn, deployment,
        subtypes=[SubtypeSpec("policy", metadata=metadata),
                  SubtypeSpec("date_chunk", metadata={"searchable_text_fields": ["text"], "effective_at_from": "chunk_date"})],
        narrowings=[("policy", "contains", "date_chunk")],
        nodes=[node("policy:effective", "policy", "Quiet parent", heading="Policy", member_path="cms/policy",
                    path="old/path", created="2025-01-01T00:00:00Z", edited="2026-10-03T08:00:00Z",
                    published="2026-10-03T08:00:00Z", resolved="2026-10-03T08:00:00Z",
                    effective="2026-10-03T10:00:00+02:00", domain_until="2026-10-03T08:00:00Z"),
               node("policy:missing", "policy", "Quiet missing parent", created="2026-10-03T08:00:00Z"),
               node("chunk:good", "date_chunk", "zirconium evidence", chunk_date="2025-01-01T00:00:00Z"),
               node("chunk:wrong", "date_chunk", "zirconium wrong record", chunk_date="2026-10-03T08:00:00Z")],
        edges=[EdgeFact(src=parent, dst=child, edge_type="contains",
                        source_record_id={"id": child}, source_revision={"v": 1})
               for parent, child in [("policy:effective", "chunk:good"), ("policy:missing", "chunk:wrong")]])
    return deployment, generation


@pytest.mark.parametrize("field,value", [("date.modified", "2026-10-03T08:00:00Z"),
                                        ("date.published", "2026-10-03T08:00:00Z"),
                                        ("date.resolved", "2026-10-03T08:00:00Z"),
                                        ("date.effective", "2026-10-03T08:00:00Z"),
                                        ("date.attrs.domain_until", "2026-10-03T08:00:00Z"),
                                        ("title", "Policy"), ("path", "cms/policy")])
def test_catalog_fields_test_record_metadata_not_passage(clean_runtime, tmp_path, field, value):
    deployment, generation = _date_corpus(tmp_path, clean_runtime)
    page = _search(clean_runtime, deployment, "zirconium", limit=10,
                   where={"op": "eq", "field": field, "value": value})
    assert _ids(page) == ["policy:effective"]
    assert page["generation_id"] == generation
    assert page["where_evaluation_scope"] == "bounded_ranked_candidates"
    assert any(mapping["subtype"] == "policy" and mapping["field"] == field for mapping in page["where_fields"])


def test_named_date_missing_never_falls_back_even_under_not(clean_runtime, tmp_path):
    deployment, _ = _date_corpus(tmp_path, clean_runtime)
    page = _search(clean_runtime, deployment, "zirconium", limit=10,
                   where={"op": "not", "clause": {"op": "eq", "field": "date.effective", "value": "2025-01-01T00:00:00Z"}})
    assert _ids(page) == ["policy:effective"]
    created = _search(clean_runtime, deployment, "zirconium", limit=10,
                      where={"op": "eq", "field": "date.created", "value": "2026-10-03T08:00:00Z"})
    assert _ids(created) == ["policy:missing"]


def test_date_catalog_read_is_pinned_and_unsupported_scope_refuses(clean_runtime, tmp_path):
    from okg.substrate.mcp import open_session, search
    from tests.pytest.catalog_fixture_builders import SubtypeSpec, seed_catalog

    deployment, generation = _date_corpus(tmp_path, clean_runtime)
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        session = open_session(conn, deployment_name=deployment)
        original = session.catalog_version_id
        # A different version must not redefine this session's date mapping.
        with conn.cursor() as cur:
            cur.execute("SELECT name, metadata FROM okg.subtypes WHERE catalog_version_id=%s", (original,))
            snapshots = cur.fetchall()
        newer = seed_catalog(conn, subtypes=[SubtypeSpec("policy", metadata={"effective_at_from": "created"})])
        assert newer != original
        predicate = {"op": "eq", "field": "date.effective", "value": "2026-10-03T08:00:00Z"}
        page = search(conn, session, "zirconium", method="lexical", where=predicate)
        assert page["generation_id"] == generation and _ids(page) == ["policy:effective"]
        assert original == session.catalog_version_id
        assert dict(snapshots)["policy"]["effective_at_from"] == "effective"
        with pytest.raises(WhereUnsupported) as error:
            search(conn, session, "zirconium", method="lexical", filter={"subtype": "date_chunk"},
                   where={"op": "exists", "field": "date.resolved"})
        assert error.value.data["reason"] == "field_not_declared"


def test_named_date_published_schema_and_bounded_wire(clean_runtime, okg_mcp_can_log_in, tmp_path):
    import json
    import anyio
    import jsonschema
    from mcp.shared.memory import create_connected_server_and_client_session
    from okg.substrate.mcp import server as srv

    deployment, _ = _date_corpus(tmp_path, clean_runtime)
    mcp = srv.build_server(dsn=clean_runtime, deployment_name=deployment)
    try:
        tool = mcp._tool_manager._tools["search"]
        args = {"where": {"op": "eq", "field": "date.effective", "value": "2026-10-03T08:00:00Z"},
                "max_bytes": 4096, "limit": 10}
        jsonschema.validate(args, tool.parameters)
        page = tool.fn(**args)
        assert page["where_evaluation_scope"] == "bounded_lookup_sql_page"
        assert page["total_count"] is None and page["total_count_basis"] == "unknown"
        served: dict[str, object] = {}

        async def call() -> None:
            # The served path, where `max_bytes` is enforced: `tool.fn` alone
            # returns the unfitted page, whose size depends on the deployment
            # path inside `pinned_generation_id` and on `coverage` (#3213).
            async with create_connected_server_and_client_session(mcp) as client:
                served["text"] = await client.call_tool("search", args)
                served["json"] = await client.call_tool(
                    "search", args, meta={"okg/result_format": "json"},
                )

        anyio.run(call)
        for result_format, wire in served.items():
            assert not wire.isError, wire.content
            assert len(wire.model_dump_json(by_alias=True, exclude_none=True).encode()) <= 4096
            assert "date.effective" in wire.content[0].text
            if result_format == "json":
                assert json.loads(wire.content[0].text)["where_fields"] == page["where_fields"]
    finally:
        mcp._okg_pool.close()
        if mcp._okg_write_pool:
            mcp._okg_write_pool.close()

pytestmark = pytest.mark.postgres

_VISIBLE_RULES = [_allow(OPEN), _allow(MAIN), _allow(MASKED)]


def _comparable(page: dict) -> dict:
    """A page without the fields that name the call rather than the answer."""
    return {k: v for k, v in page.items() if k not in ("scope_applied",)}


# ---------- D9: old arguments and `where` give identical pages ----------

@pytest.mark.parametrize("old,where", [
    ({"subtype": "ticket"}, {"op": "eq", "field": "type", "value": "ticket"}),
    ({"attr_filters": {"status": "open"}},
     {"op": "eq", "field": "attrs.status", "value": "open"}),
    ({"source": MAIN}, {"op": "eq", "field": "source", "value": MAIN}),
    ({"subtype": "ticket", "attr_filters": {"status": "closed"}},
     {"op": "and", "clauses": [
         {"op": "eq", "field": "subtype", "value": "ticket"},
         {"op": "eq", "field": "attrs.status", "value": "closed"},
     ]}),
])
@pytest.mark.parametrize("rank_by", ["records", "self"])
def test_where_and_the_old_arguments_return_identical_pages(
    clean_runtime, tmp_path, old, where, rank_by,
):
    deployment = _basic(tmp_path, clean_runtime)
    by_old = _search(clean_runtime, deployment, limit=50, filter=old, rank_by=rank_by)
    by_where = _search(clean_runtime, deployment, limit=50, where=where, rank_by=rank_by)
    assert by_where == by_old
    assert by_old["where_applied"] is not None


def test_no_predicate_says_none_was_applied(clean_runtime, tmp_path):
    deployment = _basic(tmp_path, clean_runtime)
    page = _search(clean_runtime, deployment, limit=5)
    # No predicate supplied: the key is absent, so the page carries no
    # bytes it did not carry before `where` existed.
    assert "where_applied" not in page


# ---------- D6: records by default, passages through their record ----------

def test_in_a_record_view_a_passage_matches_through_its_record(clean_runtime, tmp_path):
    """`in` is not lifted, so this is the residual path. `ticket:closed`
    matches only through its comment; the clause is on the record, so the
    record comes back, the comment is folded and `note:0` is out."""
    deployment = _basic(tmp_path, clean_runtime)
    where = {"op": "and", "clauses": [
        {"op": "in", "field": "type", "values": ["ticket"]},
        {"op": "in", "field": "attrs.status", "values": ["closed"]},
    ]}
    page = _search(clean_runtime, deployment, limit=50, where=where)
    assert _ids(page) == ["ticket:closed"]
    assert _by_id(page)["ticket:closed"]["rollup_best_chunk_id"] == "comment:closed:0"
    # Failure case: the same clause judged on each row (`self`) drops the
    # ticket, whose own text does not match, and its comment, which is not
    # a ticket.
    assert _ids(_search(clean_runtime, deployment, limit=50, where=where,
                        rank_by="self")) == []


def test_under_self_a_type_clause_reaches_the_ranked_sql(clean_runtime, tmp_path):
    deployment = _basic(tmp_path, clean_runtime)
    where = {"op": "in", "field": "type", "values": ["comment"]}
    page = _search(clean_runtime, deployment, TERM, limit=50, where=where,
                   rank_by="self")
    assert page["results"] and {r["subtype"] for r in page["results"]} == {"comment"}
    assert page["where_applied"] == "type in [comment]"


def test_a_not_on_a_stable_field_excludes_exactly_that_type(clean_runtime, tmp_path):
    deployment = _basic(tmp_path, clean_runtime)
    where = {"op": "not", "clause": {"op": "eq", "field": "type", "value": "ticket"}}
    page = _search(clean_runtime, deployment, TERM, limit=50, where=where,
                   rank_by="self")
    subtypes = {r["subtype"] for r in page["results"]}
    assert "ticket" not in subtypes and {"comment", "note"} <= subtypes


# ---------- D5: masked values are unknown; secret versus control ----------

@pytest.mark.parametrize("subtype", ["ticket", None])
def test_not_on_a_masked_value_never_admits_the_secret_record(
    clean_runtime, tmp_path, monkeypatch, subtype,
):
    """`ticket:assigned` has `assignee` masked for this reader;
    `ticket:visible` holds the same value readably. Before this change a
    two-valued `not` admitted the masked row (its mask token is not
    "bob@x"), so guesses would reveal the hidden value. Both guesses now
    leave the secret record out; the control behaves as ordinary logic."""
    deployment = _assignee_deployment(tmp_path, clean_runtime)
    monkeypatch.setenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", "1")

    def ids(where: dict) -> list[str]:
        clauses = [where]
        if subtype:
            clauses.append({"op": "eq", "field": "type", "value": subtype})
        page = _search(clean_runtime, deployment, limit=50, subject=READER,
                       where={"op": "and", "clauses": clauses})
        return _ids(page)

    # Control: without a predicate the masked ticket is reachable.
    plain = _search(clean_runtime, deployment, limit=50, subject=READER,
                    filter={"subtype": subtype} if subtype else None)
    assert "ticket:assigned" in _ids(plain)

    wrong = {"op": "not", "clause": {
        "op": "eq", "field": "attrs.assignee", "value": "bob@x"}}
    right = {"op": "not", "clause": {
        "op": "eq", "field": "attrs.assignee", "value": "alice@x"}}
    assert "ticket:visible" in ids(wrong)
    assert "ticket:visible" not in ids(right)
    for guess in (wrong, right):
        found = ids(guess)
        assert "ticket:assigned" not in found and "comment:assigned" not in found
    # `exists` and `not exists` on the masked key: unknown both ways.
    exists = {"op": "exists", "field": "attrs.assignee"}
    assert "ticket:assigned" not in ids(exists)
    assert "ticket:assigned" not in ids({"op": "not", "clause": exists})


# ---------- 3.4: record scope; hidden answers as missing ----------

def test_scope_restricts_to_the_record_and_its_readable_passages(
    clean_runtime, tmp_path, monkeypatch,
):
    deployment = _nomos_deployment(tmp_path, clean_runtime, _VISIBLE_RULES)
    monkeypatch.setenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", "1")
    page = _search(clean_runtime, deployment, limit=50, subject=READER,
                   record_scope={"record": "ticket:open"})
    assert _ids(page) == ["ticket:open"]
    row = _by_id(page)["ticket:open"]
    # Its readable comment folds in; the hidden one never counts.
    assert row["rollup_best_chunk_id"] == "comment:open:0"
    assert row["matching_passages"] == 1
    assert "secret finding" not in str(page)
    assert page["scope_applied"] == {"records": ["ticket:open"]}
    # Under `self` the scoped rows are the record and its passages.
    selfish = _search(clean_runtime, deployment, limit=50, subject=READER,
                      record_scope={"record": "ticket:open"}, rank_by="self")
    assert _ids(selfish) == ["comment:open:0"]
    # Failure case: without the scope other records answer too.
    assert len(_ids(_search(clean_runtime, deployment, limit=50,
                            subject=READER))) > 1


def test_a_hidden_record_scopes_exactly_as_a_missing_one(
    clean_runtime, tmp_path, monkeypatch,
):
    deployment = _nomos_deployment(tmp_path, clean_runtime, _VISIBLE_RULES)
    monkeypatch.setenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", "1")
    hidden = _search(clean_runtime, deployment, limit=50, subject=READER,
                     record_scope={"record": "ticket:secret"})
    missing = _search(clean_runtime, deployment, limit=50, subject=READER,
                      record_scope={"record": "ticket:does-not-exist"})
    assert hidden["results"] == []
    assert _comparable(hidden) == _comparable(missing)
    # Control: the same reader's scope to a readable record answers.
    assert _ids(_search(clean_runtime, deployment, limit=50, subject=READER,
                        record_scope={"record": "ticket:open"}))


def test_scope_with_a_record_subtype_folds_the_scoped_read(clean_runtime, tmp_path):
    """A ticket subtype would take the filtered two-read fold; under a scope
    the scoped rows fold instead and the subtype is a clause on the record."""
    deployment = _basic(tmp_path, clean_runtime)
    page = _search(clean_runtime, deployment, limit=50,
                   filter={"subtype": "ticket"},
                   record_scope={"records": ["ticket:quiet", "note:0"]})
    assert _ids(page) == ["ticket:quiet"]
    assert _by_id(page)["ticket:quiet"]["rollup_best_chunk_id"] == "comment:quiet:0"


@pytest.mark.parametrize("kwargs", [
    {"method": "alias"},
    {"rank_by": "passages", "filter": {"subtype": "ticket"}},
])
def test_scope_is_refused_where_it_is_not_served(clean_runtime, tmp_path, kwargs):
    deployment = _basic(tmp_path, clean_runtime)
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        from okg.substrate.mcp import open_session, search

        session = open_session(conn, deployment_name=deployment)
        method = kwargs.pop("method", "lexical")
        with pytest.raises(SearchScopeUnsupported):
            search(conn, session, QUERY, method=method,
                   record_scope={"record": "ticket:long"}, **kwargs)


def test_a_where_field_search_cannot_read_is_refused(clean_runtime, tmp_path):
    deployment = _basic(tmp_path, clean_runtime)
    with pytest.raises(WhereUnsupported):
        _search(clean_runtime, deployment, where={
            "op": "eq", "field": "date", "value": "2026-01-01"})


def test_scope_and_where_compose(clean_runtime, tmp_path):
    deployment = _basic(tmp_path, clean_runtime)
    page = _search(clean_runtime, deployment, TERM, limit=50,
                   record_scope={"records": ["ticket:near:0", "ticket:near:1"]},
                   where={"op": "eq", "field": "id", "value": "ticket:near:1"})
    assert _ids(page) == ["ticket:near:1"]
    assert page["where_applied"] == "id = ticket:near:1"


#: Issue #3249: a scope wider than the 200-row rank cap. Non-matching
#: comments `comment:big:000`..`249` sort, at score 0, before the one match
#: `<@>` scores 0 (`comment:big:zzz`), so a cap taken before the match test
#: spends every slot after the scored matches on them.
_BIG_NON_MATCHES = 250


def _big_scope(tmp_path, dsn: str, *, scored: int) -> str:
    from tests.substrate.mcp.test_search_records import _edge, _node, _publish

    facts: list = [_node("ticket:big", "ticket", "Weld inspection ticket.", status="open")]
    for i in range(_BIG_NON_MATCHES):
        facts += [_node(f"comment:big:{i:03d}", "comment", f"Routine weld review entry {i}."),
                  _edge("ticket:big", f"comment:big:{i:03d}")]
    for i in range(scored):
        facts += [_node(f"comment:big:m{i:03d}", "comment", f"The {TERM} sample {i} passed."),
                  _edge("ticket:big", f"comment:big:m{i:03d}")]
    facts += [_node("comment:big:zzz", "comment", f"A zeroed {TERM} remark."),
              _edge("ticket:big", "comment:big:zzz")]
    return _publish(tmp_path, dsn, groups={}, main=facts)


def _scoped_self(dsn: str, deployment: str, **kwargs) -> dict:
    from tests.substrate.test_bm25_node_search import _score_one_match_zero

    with psycopg.connect(dsn, autocommit=True) as conn:
        session = open_session(conn, deployment_name=deployment)
        _score_one_match_zero(conn, "zeroed")
        return search(conn, session, TERM, method="lexical", limit=500, rank_by="self",
                      record_scope={"record": "ticket:big"}, **kwargs)


@pytest.mark.parametrize("completeness", ["bounded", "exact"])
def test_a_zero_scored_match_past_the_rank_cap_is_returned(
    clean_runtime, tmp_path, completeness,
):
    """#3249: the record-scoped read took `LIMIT 200` before the match test.
    With 3 scored matches, 250 non-matching rows and one match scored 0, the
    cap held the 3 scored rows and 197 non-matches; the zero-scored match sat
    past it, the bounded page dropped it, and with 3 rows it did not say it
    was capped. The match test now runs before the cap: all 4 matches return,
    the zero-scored one last, and the page is not capped."""
    deployment = _big_scope(tmp_path, clean_runtime, scored=3)
    page = _scoped_self(clean_runtime, deployment, completeness=completeness)
    ids = _ids(page)
    assert ids[-1] == "comment:big:zzz"
    assert sorted(ids) == sorted(
        ["comment:big:zzz", *(f"comment:big:m{i:03d}" for i in range(3))])
    assert page["rank_capped"] is False
    if completeness == "exact":
        assert page["total_count"] == 4 and page["total_count_basis"] == "exact"


def test_a_scope_with_more_matches_than_the_cap_says_it_is_capped(clean_runtime, tmp_path):
    """Saturation counts matches before the cap: 205 scored matches fill it,
    so the bounded page says it is capped and the zero-scored match, ranked
    after every scored one, is past it."""
    deployment = _big_scope(tmp_path, clean_runtime, scored=205)
    page = _scoped_self(clean_runtime, deployment)
    ids = _ids(page)
    assert len(ids) == 200 and page["rank_capped"] is True
    assert all(i.startswith("comment:big:m") for i in ids)


def test_a_policy_hidden_record_does_not_unlock_its_readable_passages(
    clean_runtime, tmp_path, monkeypatch,
):
    """`comment:tosecret` is readable, its parent `ticket:secret` is not:
    scoping to the hidden parent must not serve the passage."""
    deployment = _nomos_deployment(tmp_path, clean_runtime, _VISIBLE_RULES)
    monkeypatch.setenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", "1")
    page = _search(clean_runtime, deployment, limit=50, subject=READER,
                   record_scope={"record": "ticket:secret"}, rank_by="self")
    assert page["results"] == []
    _activate  # imported for the fixture module's rules; keep the name used


# ---------- review: hidden records leave no trace in counts or flags ----------

#: Payload fields a caller can read without seeing a row.
_SIGNAL_KEYS = (
    "total_count", "zero_hit_reason", "zero_hit_scanned", "subtype_indexed",
    "rank_capped", "total_count_basis", "truncated", "next_offset",
)


def _signals(page: dict) -> dict:
    return {key: page.get(key) for key in _SIGNAL_KEYS}


@pytest.mark.parametrize("rank_by", ["records", "self"])
@pytest.mark.parametrize("query", ["secret finding", "xylophone"])
def test_a_where_on_a_hidden_id_answers_as_a_missing_id(
    clean_runtime, tmp_path, monkeypatch, rank_by, query,
):
    """An `id` clause joins the ranked SQL, which runs before the read
    filter. The zero-hit diagnosis read that raw match, so a hidden record's
    existence and its words showed as the presence of `zero_hit_reason`."""
    deployment = _nomos_deployment(tmp_path, clean_runtime, _VISIBLE_RULES)
    monkeypatch.setenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", "1")

    def signals(record_id: str) -> dict:
        where = {"op": "and", "clauses": [
            {"op": "eq", "field": "type", "value": "comment"},
            {"op": "eq", "field": "id", "value": record_id},
        ]}
        page = _search(clean_runtime, deployment, query, limit=10,
                       subject=READER, where=where, rank_by=rank_by)
        assert page["results"] == []
        return _signals(page)

    assert signals("comment:secret:0") == signals("comment:does-not-exist")


def test_a_record_scope_says_nothing_about_its_hidden_passages(
    clean_runtime, tmp_path, monkeypatch,
):
    """`comment:secret:1` hangs under the readable `ticket:open` but is
    hidden. A term only it holds must answer as a term nobody holds."""
    deployment = _nomos_deployment(tmp_path, clean_runtime, _VISIBLE_RULES)
    monkeypatch.setenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", "1")

    def signals(query: str) -> dict:
        page = _search(clean_runtime, deployment, query, limit=10,
                       subject=READER, filter={"subtype": "ticket"},
                       record_scope={"record": "ticket:open"})
        assert page["results"] == []
        return _signals(page)

    assert signals("second secret finding") == signals("xylophone")


def test_the_lookup_page_flags_do_not_read_a_masked_value(
    clean_runtime, okg_mcp_can_log_in, tmp_path, monkeypatch,
):
    """The query-less lookup used to push `where` conjuncts into its paged
    SQL, which reads stored values before the read filter, so `truncated`
    and `next_offset` answered a prefix guess on a masked value."""
    import okg.substrate.nomos as nomos_mod
    from okg.substrate.mcp import server as srv

    deployment = _assignee_deployment(tmp_path, clean_runtime)
    monkeypatch.setenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", "1")
    monkeypatch.setattr(
        nomos_mod, "deployment_requires_nomos_runtime", lambda name: False,
    )
    real_open = srv.open_session

    def as_reader(*args, **kwargs):
        kwargs["nomos_subject"] = READER
        return real_open(*args, **kwargs)

    monkeypatch.setattr(srv, "open_session", as_reader)
    mcp = srv.build_server(dsn=clean_runtime, deployment_name=deployment)
    search_tool = mcp._tool_manager._tools["search"].fn

    def flags(prefix: str) -> dict:
        where = {"op": "or", "clauses": [
            {"op": "and", "clauses": [
                {"op": "eq", "field": "id", "value": "ticket:assigned"},
                {"op": "prefix", "field": "attrs.assignee", "value": prefix},
            ]},
            {"op": "eq", "field": "id", "value": "ticket:visible"},
        ]}
        page = search_tool(where=where, limit=1)
        assert page["method"] == "attrs"
        return {key: page.get(key) for key in ("truncated", "next_offset")}

    try:
        # "alice@x" is the hidden value; "b" is not. These clauses sit
        # inside an `or`, so none lifts into attr_filters or the SQL page.
        assert flags("al") == flags("b")
        assert flags("alice@x") == flags("b")
    finally:
        mcp._okg_pool.close()


def test_an_eq_null_on_a_dropped_key_is_unknown(
    clean_runtime, tmp_path, monkeypatch,
):
    """`assignee` is free text for this reader, so the read filter drops it.
    `eq null` was lifted into the two-valued attr_filters path and admitted
    the redacted ticket; it must answer as `in [null]` does (D5)."""
    deployment = _assignee_deployment(tmp_path, clean_runtime)
    monkeypatch.setenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", "1")
    for where in (
        {"op": "eq", "field": "attrs.assignee", "value": None},
        {"op": "in", "field": "attrs.assignee", "values": [None]},
    ):
        page = _search(clean_runtime, deployment, limit=50, subject=READER,
                       where=where)
        assert "ticket:assigned" not in _ids(page), where


# ---------- the query-less lookup on a graph larger than `limit` ----------

_LOOKUP_LIMIT = 5


def _lookup_corpus(tmp_path, dsn):
    """30 filler documents that sort BEFORE the one target by node id, so an
    unnarrowed SQL page of `limit` rows never holds the target. A second
    subtype declares path on another attribute; `memo` declares no title."""
    from tests.pytest.catalog_fixture_builders import SubtypeSpec
    from tests.substrate.mcp.search_fixture import make_search_deployment, publish_nodes, node

    deployment = make_search_deployment(tmp_path, dsn=dsn)
    doc_meta = {"searchable_text_fields": ["text"], "title_from": "heading",
                "path_from": "member_path", "created_at_from": "created"}
    memo_meta = {"searchable_text_fields": ["text"], "path_from": "memo_path"}
    filler = [node(f"doc:a{i:02d}", "doc", "filler", heading=f"Filler {i}",
                   member_path=f"cms/filler/{i}") for i in range(30)]
    nodes = [*filler,
             node("doc:naive", "doc", "naive dated", heading="Naive dated",
                  member_path="cms/naive", created="2026-10-02T09:00:00"),
             node("doc:zz-target", "doc", "target", heading="Target heading\nbody line",
                  member_path="  cms/target/file.md", created="2026-10-03T08:00:00Z"),
             node("memo:zz-memo", "memo", "memo", memo_path="cms/target/file.md")]
    _, generation = publish_nodes(
        dsn, deployment,
        subtypes=[SubtypeSpec("doc", metadata=doc_meta), SubtypeSpec("memo", metadata=memo_meta)],
        nodes=nodes,
    )
    return deployment, generation


def _lookup_tool(dsn, deployment):
    from okg.substrate.mcp import server as srv

    mcp = srv.build_server(dsn=dsn, deployment_name=deployment)
    return mcp, mcp._tool_manager._tools["search"].fn


@pytest.mark.parametrize("where,expected", [
    # Catalog path, unscoped: both declaring attributes are searched; the
    # stored doc path is padded, the read path is stripped.
    ({"op": "eq", "field": "path", "value": "cms/target/file.md"},
     ["doc:zz-target", "memo:zz-memo"]),
    ({"op": "prefix", "field": "path", "value": "cms/target/"},
     ["doc:zz-target", "memo:zz-memo"]),
    # Catalog title, scoped to a type that declares it: full first line.
    ({"op": "and", "clauses": [{"op": "eq", "field": "type", "value": "doc"},
                               {"op": "eq", "field": "title", "value": "Target heading"}]},
     ["doc:zz-target"]),
    # A named date: declared, valid values pass; `doc:naive` holds no time
    # zone and is read as UTC (issue #3666).
    ({"op": "gt", "field": "date.created", "value": "2026-10-01T00:00:00Z"},
     ["doc:naive", "doc:zz-target"]),
    # An exact id clause narrows the page as well.
    ({"op": "in", "field": "id", "values": ["doc:zz-target", "doc:missing"]},
     ["doc:zz-target"]),
])
def test_lookup_finds_a_match_past_the_first_page_of_the_relation(
    clean_runtime, okg_mcp_can_log_in, tmp_path, where, expected,
):
    deployment, generation = _lookup_corpus(tmp_path, clean_runtime)
    mcp, search_tool = _lookup_tool(clean_runtime, deployment)
    try:
        page = search_tool(where=where, limit=_LOOKUP_LIMIT)
        assert page["method"] == "attrs"
        # The served envelope names the pin with its deployment.
        assert str(page["pinned_generation_id"]).endswith(str(generation)), page["pinned_generation_id"]
        assert sorted(_ids(page)) == expected
        assert page["truncated"] is False and page["next_offset"] is None
    finally:
        mcp._okg_pool.close()


def test_lookup_date_reads_a_value_with_no_time_zone_as_utc(clean_runtime, okg_mcp_can_log_in, tmp_path):
    """Mixed sources (issue #3666): `doc:naive` holds a naive wall time, the
    operand is an aware instant. The naive row is compared as UTC and the
    reply says so; it used to be set aside as incompatible."""
    deployment, _ = _lookup_corpus(tmp_path, clean_runtime)
    mcp, search_tool = _lookup_tool(clean_runtime, deployment)
    try:
        page = search_tool(where={"op": "gt", "field": "date.created",
                                  "value": "2026-10-01T00:00:00Z"}, limit=_LOOKUP_LIMIT)
        assert sorted(_ids(page)) == ["doc:naive", "doc:zz-target"]
        assert page["dates_read_as_utc"] == {"stored": ["date.created"]}
        assert "where_incompatible" not in page
        # After the naive value's instant, it no longer matches.
        page = search_tool(where={"op": "gt", "field": "date.created",
                                  "value": "2026-10-02T09:00:00Z"}, limit=_LOOKUP_LIMIT)
        assert _ids(page) == ["doc:zz-target"]
        # A day beside a timestamp is served: the day is its first instant,
        # in UTC, and the reply notes the bound too.
        page = search_tool(where={"op": "between", "field": "date.created",
                                  "values": ["2026-10-01", "2026-10-03T00:00:00Z"]},
                           limit=_LOOKUP_LIMIT)
        assert _ids(page) == ["doc:naive"]
        assert page["dates_read_as_utc"] == {
            "bounds": ["date.created"], "stored": ["date.created"],
        }
        with pytest.raises(WhereUnsupported):
            search_tool(where={"op": "between", "field": "date.created",
                               "values": ["2026-10-04T00:00:00Z", "2026-10-01"]})
    finally:
        mcp._okg_pool.close()


def test_lookup_without_a_safe_pushdown_answers_bounded_not_empty(
    clean_runtime, okg_mcp_can_log_in, tmp_path,
):
    """Failure case. An unscoped title (`memo` falls back to the fixed title
    list) stays residual; the first page then says it is truncated and where
    to continue, and paging reaches the match. The same clause under a
    bundle that can mask is covered on a real enforce publish below."""
    deployment, _ = _lookup_corpus(tmp_path, clean_runtime)
    mcp, search_tool = _lookup_tool(clean_runtime, deployment)
    try:
        title = {"op": "eq", "field": "title", "value": "Target heading"}
        first = search_tool(where=title, limit=_LOOKUP_LIMIT)
        assert first["results"] == []
        assert first["truncated"] is True and first["next_offset"] == _LOOKUP_LIMIT
        assert first["where_evaluation_scope"] == "bounded_lookup_sql_page"
        assert first["total_count"] is None and first["total_count_basis"] == "unknown"
        found, offset = [], 0
        while offset is not None and not found:
            page = search_tool(where=title, limit=_LOOKUP_LIMIT, offset=offset)
            found, offset = _ids(page), page["next_offset"]
        assert found == ["doc:zz-target"]
    finally:
        mcp._okg_pool.close()


def test_lookup_paging_ends_honestly_at_the_offset_ceiling(
    clean_runtime, okg_mcp_can_log_in, tmp_path, monkeypatch,
):
    """Failure case for the offset ceiling, lowered through its module seam
    so a 33-node graph is larger than it. The old lookup clamped any offset
    past the ceiling to the ceiling and still offered `offset + limit`, so a
    caller following `next_offset` read the same page forever. Now paging
    ends with `pagination_capped`, no cursor and a notice naming the remedy,
    and the remedy (a type) finds the match on the first page."""
    from okg.substrate.mcp import server as srv

    monkeypatch.setattr(srv, "ATTRS_LOOKUP_MAX_OFFSET", 10)
    deployment, _ = _lookup_corpus(tmp_path, clean_runtime)
    mcp, search_tool = _lookup_tool(clean_runtime, deployment)
    title = {"op": "eq", "field": "title", "value": "Target heading"}
    try:
        seen, offset, last = [], 0, None
        while offset is not None:
            assert offset not in seen and len(seen) < 10, seen  # never loops
            seen.append(offset)
            last = search_tool(where=title, limit=_LOOKUP_LIMIT, offset=offset)
            assert _ids(last) == [], last  # the target sorts past the ceiling
            offset = last["next_offset"]
        assert seen == [0, 5, 10]
        assert last["pagination_capped"] is True
        assert last["pagination_max_offset"] == 10
        assert last["truncated"] is True
        assert "add a type" in last["pagination_notice"]
        assert "pagination" not in last and "pagination_hint" not in last
        assert last["coverage"]["delivery"]["complete"] is False
        # A requested offset past the ceiling reads no clamped page.
        past = search_tool(where=title, limit=_LOOKUP_LIMIT, offset=25)
        assert past["results"] == [] and past["next_offset"] is None
        assert past["pagination_capped"] is True and past["requested_offset"] == 25
        assert "pagination_notice" in past
        assert past["coverage"]["delivery"]["complete"] is not True
        # The remedy: a type narrows the page and the match is on page one.
        typed = search_tool(where={"op": "and", "clauses": [
            {"op": "eq", "field": "type", "value": "doc"}, title,
        ]}, limit=_LOOKUP_LIMIT)
        assert _ids(typed) == ["doc:zz-target"]
        assert typed["next_offset"] is None and "pagination_capped" not in typed
    finally:
        mcp._okg_pool.close()


def _two_attribute_date_corpus(tmp_path, dsn):
    """`date.created` stored as `created` on `doc` and as `booked` on
    `ledger`; 30 filler docs without a date sort before both matches."""
    from tests.pytest.catalog_fixture_builders import SubtypeSpec
    from tests.substrate.mcp.search_fixture import make_search_deployment, publish_nodes, node

    deployment = make_search_deployment(tmp_path, dsn=dsn)
    filler = [node(f"doc:a{i:02d}", "doc", "filler", heading=f"Filler {i}")
              for i in range(30)]
    publish_nodes(
        dsn, deployment,
        subtypes=[
            SubtypeSpec("doc", metadata={"searchable_text_fields": ["text"],
                                         "title_from": "heading", "created_at_from": "created"}),
            SubtypeSpec("ledger", metadata={"searchable_text_fields": ["text"],
                                            "created_at_from": "booked",
                                            "title_from": "label"}),
        ],
        nodes=[*filler,
               node("doc:zz-dated", "doc", "dated", heading="Dated",
                    created="2026-10-03T08:00:00Z"),
               # The other type's attribute name on a doc is not its date.
               node("doc:zz-wrong-attr", "doc", "wrong", heading="Wrong",
                    booked="2026-10-03T08:00:00Z"),
               node("ledger:zz-entry", "ledger", "entry", label="Entry",
                    booked="2026-10-03T09:00:00Z")],
    )
    return deployment


def test_lookup_date_stored_under_two_attributes_across_types(clean_runtime, okg_mcp_can_log_in, tmp_path):
    deployment = _two_attribute_date_corpus(tmp_path, clean_runtime)
    mcp, search_tool = _lookup_tool(clean_runtime, deployment)
    try:
        page = search_tool(where={"op": "gt", "field": "date.created",
                                  "value": "2026-10-01T00:00:00Z"}, limit=_LOOKUP_LIMIT)
        assert sorted(_ids(page)) == ["doc:zz-dated", "ledger:zz-entry"]
        assert page["truncated"] is False and page["next_offset"] is None
        mapped = {(m["subtype"], m["attr"]) for m in page["where_fields"]}
        assert mapped == {("doc", "created"), ("ledger", "booked")}
    finally:
        mcp._okg_pool.close()


def test_lookup_type_list_where_only_one_type_declares_a_title(clean_runtime, okg_mcp_can_log_in, tmp_path):
    """`type in [doc, memo]`: `memo` declares no title, so the title clause
    is not narrowed in SQL. The page says it is cut and paging reaches the
    match; it never answers a bare empty page."""
    deployment, _ = _lookup_corpus(tmp_path, clean_runtime)
    mcp, search_tool = _lookup_tool(clean_runtime, deployment)
    where = {"op": "and", "clauses": [
        {"op": "in", "field": "type", "values": ["doc", "memo"]},
        {"op": "eq", "field": "title", "value": "Target heading"},
    ]}
    try:
        first = search_tool(where=where, limit=_LOOKUP_LIMIT)
        assert first["results"] == []
        assert first["truncated"] is True and first["next_offset"] == _LOOKUP_LIMIT
        found, offset = [], 0
        while offset is not None and not found:
            page = search_tool(where=where, limit=_LOOKUP_LIMIT, offset=offset)
            found, offset = _ids(page), page["next_offset"]
        assert found == ["doc:zz-target"]
    finally:
        mcp._okg_pool.close()


def test_lookup_under_enforce_narrows_by_type_and_attribute_equality(
    clean_runtime, okg_mcp_can_log_in, tmp_path, monkeypatch,
):
    """A real Nomos `enforce` publish and lookup. The mode is read once per
    call, so the same value keeps catalog `path` out of the SQL page and
    runs the read filter. The taught spelling (type plus an exact
    `attrs.path`) narrows in SQL under enforce and finds the readable match
    past the first page; the hidden document with the same path stays
    hidden; the `where` and `attr_filters` spellings return the same page."""
    from okg.substrate.mcp import server as srv
    from okg.substrate.mcp import search_where
    from tests.pytest.catalog_fixture_builders import SubtypeSpec
    from tests.substrate.mcp.search_fixture import make_search_deployment, publish_nodes, node
    from tests.substrate.mcp.test_search_records import SECRET

    deployment = make_search_deployment(tmp_path, dsn=clean_runtime)
    target_path = "cms/target/file.md"
    filler = [node(f"doc:a{i:02d}", "doc", "filler", path=f"cms/filler/{i}")
              for i in range(30)]
    publish_nodes(
        clean_runtime, deployment,
        subtypes=[SubtypeSpec("doc", metadata={"searchable_text_fields": ["text"],
                                               "path_from": "path"})],
        nodes=[],
        node_groups=[
            (OPEN, [*filler, node("doc:zz-target", "doc", "target", path=target_path)]),
            (SECRET, [node("doc:zz-secret", "doc", "secret", path=target_path)]),
        ],
    )
    _activate(clean_runtime, deployment, [_allow(OPEN), _allow(MAIN), {
        "rule_id": "allow_mcp_tool_call", "effect": "allow",
        "actions": ["mcp.tool.call"], "subjects": [READER],
        "match": {"resource_kind": "mcp_tool"}, "priority": 100,
    }, {
        "rule_id": "allow_graph_boundary", "effect": "allow",
        "actions": ["graph.read.mcp"], "subjects": [READER],
        "match": {"resource_kind": "mcp_graph_read_boundary"}, "priority": 100,
    }])
    # Every gate runs: the tool-call gate and the read filter, as READER.
    monkeypatch.setenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", "enforce")
    monkeypatch.setenv("OKG_NOMOS_SUBJECT", READER)
    readings: list[bool] = []
    real_masking = search_where.masking_possible

    def counted(name):
        readings.append(real_masking(name))
        return readings[-1]

    monkeypatch.setattr(search_where, "masking_possible", counted)
    real_open = srv.open_session

    def as_reader(*args, **kwargs):
        kwargs["nomos_subject"] = READER
        return real_open(*args, **kwargs)

    monkeypatch.setattr(srv, "open_session", as_reader)
    mcp, search_tool = _lookup_tool(clean_runtime, deployment)
    doc = {"op": "eq", "field": "type", "value": "doc"}
    try:
        taught = search_tool(where={"op": "and", "clauses": [
            doc, {"op": "eq", "field": "attrs.path", "value": target_path},
        ]}, limit=_LOOKUP_LIMIT)
        assert readings == [True]  # one reading of the mode for the call
        assert _ids(taught) == ["doc:zz-target"]
        assert taught["truncated"] is False and taught["next_offset"] is None
        legacy = search_tool(method="attrs", subtype_filter="doc",
                             attr_filters={"path": target_path}, limit=_LOOKUP_LIMIT)
        assert _ids(legacy) == _ids(taught)
        # Catalog `path` cannot narrow while a bundle can mask: only the
        # type narrows in SQL. The page still counts only rows that pass the
        # read filter and the `where` (#3208), so the first page holds the
        # readable match past the filler and is the whole answer; the hidden
        # document with the same path neither shows nor makes it look cut.
        catalog = search_tool(where={"op": "and", "clauses": [
            doc, {"op": "eq", "field": "path", "value": target_path},
        ]}, limit=_LOOKUP_LIMIT)
        assert _ids(catalog) == ["doc:zz-target"]
        assert catalog["truncated"] is False and catalog["next_offset"] is None
    finally:
        mcp._okg_pool.close()


# ---------- the lookup's `where` path prefix through the path index (#3194) ----------

_LONG_PATH = "deep/" + "q" * 700
_PREFIX_PATHS = {
    "doc:zz-src": "lib/a.py",
    "doc:zz-padded": "  lib/padded.py",  # reads as "lib/padded.py"
    "doc:zz-under": "lib/a_b/x",
    "doc:zz-anyb": "lib/axb/x",
    "doc:zz-pct": "50%/off",
    "doc:zz-pcx": "50x/off",
    "doc:zz-long": _LONG_PATH,
}
_PREFIX_CASES = [
    ("lib/", ["doc:zz-anyb", "doc:zz-padded", "doc:zz-src", "doc:zz-under"]),
    ("lib/a_", ["doc:zz-under"]),
    ("50%", ["doc:zz-pct"]),
    (_LONG_PATH[:600], ["doc:zz-long"]),
    (_LONG_PATH[:600] + "X", []),
    ("nomatch/", []),
]
# `attrs.path` reads the stored value as is: a padded path matches only a
# prefix that carries its leading spaces.
_ATTRS_PREFIX_CASES = [
    ("lib/", ["doc:zz-anyb", "doc:zz-src", "doc:zz-under"]),
    ("  lib/", ["doc:zz-padded"]),
    ("lib/a_", ["doc:zz-under"]),
    ("50%", ["doc:zz-pct"]),
    (_LONG_PATH[:600], ["doc:zz-long"]),
    (_LONG_PATH[:600] + "X", []),
    ("nomatch/", []),
]
_PATH_META = {"searchable_text_fields": ["text"], "path_from": "path",
              "filterable_attrs": {"path": "text"}}


def _path_index_corpus(tmp_path, dsn, *, filler: int = 40, declare: bool = True):
    """`doc` reads its catalog path from `attrs.path`, the attribute the
    bounded path-prefix index covers, and (with `declare`) declares it a
    filterable text attribute, so the drift checks compare it and its
    readiness gates the narrowing. Fillers sort before every target."""
    from okg.substrate import declared_attr_indexes as dai
    from tests.pytest.catalog_fixture_builders import SubtypeSpec
    from tests.substrate.mcp.search_fixture import make_search_deployment, publish_nodes, node

    deployment = make_search_deployment(tmp_path, dsn=dsn)
    meta = dict(_PATH_META) if declare else {
        k: v for k, v in _PATH_META.items() if k != "filterable_attrs"}
    nodes = [node(f"doc:a{i:04d}", "doc", "filler", path=f"bulk/{i}") for i in range(filler)]
    nodes += [node(node_id, "doc", "target", path=path) for node_id, path in _PREFIX_PATHS.items()]
    _, generation = publish_nodes(
        dsn, deployment, subtypes=[SubtypeSpec("doc", metadata=meta)], nodes=nodes,
    )
    if declare:
        report = dai.reconcile(dsn, deployment=deployment, apply=True, enable=True)
        assert report["status"] == "converged", report
    return deployment, generation


def _republish(dsn, deployment, facts) -> str:
    from okg.substrate.graph_store import ChronosGraphStore
    from okg.substrate.publisher import Publisher
    from tests.pytest.catalog_fixture_builders import bind_test_catalog_writer

    with psycopg.connect(dsn, autocommit=True) as conn:
        ChronosGraphStore.from_connection(conn).apply_facts(
            list(facts), source=MAIN, strict_endpoints=True)
    result = Publisher(dsn, deployment,
                       catalog_identity=bind_test_catalog_writer(dsn, deployment)).cycle()
    assert result["status"] == "published", result
    return str(result["generation_id"])


def _all_pages(search_tool, where, **kwargs) -> list[str]:
    found: list[str] = []
    offset = 0
    while offset is not None:
        page = search_tool(where=where, limit=_LOOKUP_LIMIT, offset=offset, **kwargs)
        found += _ids(page)
        offset = page["next_offset"]
    return sorted(found)


def _capture_page_sql(monkeypatch) -> list[tuple[str, dict]]:
    seen: list[tuple[str, dict]] = []
    real = psycopg.Cursor.execute

    def capture(self, query, params=None, **kwargs):
        if isinstance(query, str) and "effective_page" in query and "page_limit" in query:
            seen.append((query, dict(params or {})))
        return real(self, query, params, **kwargs)

    monkeypatch.setattr(psycopg.Cursor, "execute", capture)
    return seen


def _path_reasons(page) -> list[str]:
    return [a["reason"] for n in page.get("notes") or [] if n.get("pushdown") == "not_covered"
            for a in n["attrs"] if a["attr"] == "path"]


def _narrowed(sql: str) -> bool:
    return "LIKE %(pp0_b)s" in sql


def _plan(dsn, sql, params, *, analyze: bool = False, seqscan: bool = True):
    with psycopg.connect(dsn, autocommit=True) as conn:
        if not seqscan:
            conn.execute("SET enable_seqscan = off")
        options = "(ANALYZE, BUFFERS, FORMAT JSON)" if analyze else "(FORMAT JSON)"
        return conn.execute(f"EXPLAIN {options} " + sql, params, prepare=False).fetchone()[0][0]


def _plan_nodes(node):
    yield node
    for child in node.get("Plans") or []:
        yield from _plan_nodes(child)


def _buffers(plan) -> int:
    top = plan["Plan"]
    return int(top.get("Shared Hit Blocks", 0)) + int(top.get("Shared Read Blocks", 0))


@pytest.mark.parametrize("field", ["path", "attrs.path"])
def test_lookup_where_path_prefix_plans_on_the_path_prefix_index(
    clean_runtime, okg_mcp_can_log_in, tmp_path, monkeypatch, field,
):
    """Nomos off, path declared and ready: the page statement carries the
    bounded path expression and its plan reads `node_path_prefix_visible`,
    also with the planner free to choose (3,000 analyzed fillers). Failure
    case: with the candidate sets switched off (dev's statement), a `path`
    prefix returns the same first page from a plan with no path index that
    reads at least five times the buffers, and an `attrs.path` prefix, which
    dev never puts in SQL, returns an empty, truncated first page."""
    from okg.substrate import declared_attr_indexes as dai

    monkeypatch.delenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", raising=False)
    deployment, _ = _path_index_corpus(tmp_path, clean_runtime, filler=3000)
    with psycopg.connect(clean_runtime, autocommit=True) as conn, \
            conn.cursor(row_factory=dict_row) as cur:
        table = dai._graph_tables(cur)["node"]
        cur.execute(pgsql.SQL("ANALYZE {}").format(pgsql.Identifier("public", table)))
    seen = _capture_page_sql(monkeypatch)
    mcp, search_tool = _lookup_tool(clean_runtime, deployment)
    where = {"op": "prefix", "field": field, "value": "lib/a_"}
    try:
        page = search_tool(where=where, limit=_LOOKUP_LIMIT)
        assert _ids(page) == ["doc:zz-under"]
        assert page["truncated"] is False and page["next_offset"] is None
        assert _path_reasons(page) == []
        narrowed_sql, narrowed_params = seen[-1]
        assert _narrowed(narrowed_sql) and narrowed_params["pp0_b"] == "lib/a\\_%"
        forced = _plan(clean_runtime, narrowed_sql, narrowed_params, seqscan=False)
        free = _plan(clean_runtime, narrowed_sql, narrowed_params)
        for plan in (forced, free):
            assert any(n.get("Index Name", "").endswith("node_path_prefix_visible")
                       for n in _plan_nodes(plan["Plan"])), plan

        seen.clear()
        monkeypatch.setenv("OKG_ATTR_FILTER_INDEX", "off")
        before = search_tool(where=where, limit=_LOOKUP_LIMIT)
        dev_sql, dev_params = seen[-1]
        assert not _narrowed(dev_sql)
        assert "target_id = ANY(ARRAY(" in narrowed_sql
        if field == "attrs.path":
            # Dev put no `attrs.path` prefix in SQL at all: its first page held
            # 5 fillers and the match lay about 600 pages on. Since #3666 the
            # prefix is in the page's SQL, so the unnarrowed statement finds it.
            assert _ids(before) == _ids(page) and before["truncated"] is False
            return
        assert _ids(before) == _ids(page)
        dev = _plan(clean_runtime, dev_sql, dev_params, analyze=True)
        new = _plan(clean_runtime, narrowed_sql, narrowed_params, analyze=True)
        assert not any(n.get("Index Name", "").endswith("node_path_prefix_visible")
                       for n in _plan_nodes(dev["Plan"]))
        # Measured on lab-host (3,007 records, view tables not yet analyzed):
        # dev 15,131 buffers; narrowed with `IN (set)` 9,145 (the set was
        # hash-joined against the whole view); with `= ANY(ARRAY(set))`
        # 67. After ANALYZE: dev 351, `IN` 321, `ANY(ARRAY)` 31.
        assert _buffers(new) * 5 < _buffers(dev), (_buffers(new), _buffers(dev))
    finally:
        mcp._okg_pool.close()


@pytest.mark.parametrize("field, cases", [
    ("path", _PREFIX_CASES),
    ("attrs.path", _ATTRS_PREFIX_CASES),
], ids=["path", "attrs.path"])
def test_lookup_where_path_prefix_returns_what_the_unnarrowed_lookup_returns(
    clean_runtime, okg_mcp_can_log_in, tmp_path, monkeypatch, field, cases,
):
    """Identical results: every prefix, paged to the end, returns the same
    records with and without the index narrowing, on catalog `path` and on
    the stored `attrs.path`. The cases cover a padded stored path, LIKE
    wildcards `_` and `%` in the operand, a prefix longer than the
    512-character index bound, and no match."""
    monkeypatch.delenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", raising=False)
    deployment, _ = _path_index_corpus(tmp_path, clean_runtime)
    seen = _capture_page_sql(monkeypatch)
    mcp, search_tool = _lookup_tool(clean_runtime, deployment)
    try:
        for prefix, expected in cases:
            where = {"op": "prefix", "field": field, "value": prefix}
            monkeypatch.delenv("OKG_ATTR_FILTER_INDEX", raising=False)
            seen.clear()
            narrowed = _all_pages(search_tool, where)
            assert seen and all(_narrowed(sql) for sql, _ in seen), prefix
            monkeypatch.setenv("OKG_ATTR_FILTER_INDEX", "off")
            unnarrowed = _all_pages(search_tool, where)
            assert narrowed == unnarrowed == expected, prefix
    finally:
        mcp._okg_pool.close()


@pytest.mark.parametrize("field, expected", [
    ("path", ["doc:a0001", "doc:a0002", "doc:zz-anyb", "doc:zz-padded", "doc:zz-under"]),
    # The padded stored path does not start with `lib/` as stored.
    ("attrs.path", ["doc:a0001", "doc:a0002", "doc:zz-anyb", "doc:zz-under"]),
], ids=["path", "attrs.path"])
def test_lookup_where_path_prefix_follows_a_republished_path_and_an_overlay(
    clean_runtime, okg_mcp_can_log_in, tmp_path, monkeypatch, field, expected,
):
    """A node republished with a new path (row versions closed and opened:
    live_lo/live_hi) and an accepted overlay that sets a matching `path` on
    a node whose stored path does not match. Both modes return the same
    rows, the narrowed one through the index. Failure case: the graph-side
    candidate set alone misses the overlaid node; the overlay union is what
    returns it."""
    from okg.substrate import declared_attr_indexes as dai
    from okg.substrate.attributes import (
        materialize_effective_attributes_for_generation,
        record_attribute_observation,
    )
    from okg.substrate.mcp import grep_search
    from tests.substrate.mcp.search_fixture import node

    monkeypatch.delenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", raising=False)
    deployment, _ = _path_index_corpus(tmp_path, clean_runtime)
    moved_in = node("doc:a0001", "doc", "filler", path="lib/moved-in")
    moved_in.source_revision["v"] = 2
    moved_out = node("doc:zz-src", "doc", "target", path="old/a.py")
    moved_out.source_revision["v"] = 2
    head = _republish(clean_runtime, deployment, [moved_in, moved_out])
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        record_attribute_observation(
            conn, target_kind="node", target_id="doc:a0002", generation_id=head,
            attr_key="path", value="lib/overlaid", producer="test:where-path-prefix",
        )
        materialize_effective_attributes_for_generation(conn, generation_id=head)
    seen = _capture_page_sql(monkeypatch)
    mcp, search_tool = _lookup_tool(clean_runtime, deployment)
    where = {"op": "prefix", "field": field, "value": "lib/"}
    try:
        narrowed = _all_pages(search_tool, where)
        assert seen and all(_narrowed(sql) for sql, _ in seen)
        monkeypatch.setenv("OKG_ATTR_FILTER_INDEX", "off")
        assert narrowed == _all_pages(search_tool, where) == expected
    finally:
        mcp._okg_pool.close()
    with psycopg.connect(clean_runtime, autocommit=True) as conn, \
            conn.cursor(row_factory=dict_row) as cur:
        from okg.substrate.mcp.attr_filter_source import _head_branch_point

        table = dai._graph_tables(cur)["node"]
        params = {"bp": _head_branch_point(conn, head)}
        sql = grep_search.path_prefix_candidates_sql(
            "lib/", params, segment=f'public."{table}"', bp_ref="%(bp)s", key="pp0")
        graph_only = {row[0] for row in conn.execute(sql, params, prepare=False).fetchall()}
    assert "doc:a0001" in graph_only and "doc:zz-src" not in graph_only
    assert "doc:a0002" not in graph_only


def test_lookup_where_path_prefix_does_not_narrow_when_it_cannot_trust_the_index(
    clean_runtime, okg_mcp_can_log_in, tmp_path, monkeypatch,
):
    """Each guard, with its `not_covered` reason, and the same rows as the
    unnarrowed lookup: a read pinned to an older generation
    (`pinned_generation`), a drift flag on the declared `path` index
    (`view_graph_drift`), a prefix with more candidates than the bound
    (`broad_prefix`), and a deployment that does not declare `path`
    (`not_declared`)."""
    from okg.substrate.mcp import grep_search
    from tests.substrate.mcp.search_fixture import node

    monkeypatch.delenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", raising=False)
    deployment, first = _path_index_corpus(tmp_path, clean_runtime)
    moved = node("doc:zz-src", "doc", "target", path="old/a.py")
    moved.source_revision["v"] = 2
    _republish(clean_runtime, deployment, [moved])
    seen = _capture_page_sql(monkeypatch)
    mcp, search_tool = _lookup_tool(clean_runtime, deployment)
    where = {"op": "prefix", "field": "path", "value": "lib/"}

    def check(reason, expected, **kwargs):
        seen.clear()
        monkeypatch.delenv("OKG_ATTR_FILTER_INDEX", raising=False)
        page = search_tool(where=where, limit=_LOOKUP_LIMIT, **kwargs)
        assert reason in _path_reasons(page), page.get("notes")
        assert seen and not any(_narrowed(sql) for sql, _ in seen)
        guarded = _all_pages(search_tool, where, **kwargs)
        monkeypatch.setenv("OKG_ATTR_FILTER_INDEX", "off")
        assert guarded == _all_pages(search_tool, where, **kwargs) == expected

    try:
        check("pinned_generation", _PREFIX_CASES[0][1], generation_id=first)
        bound = grep_search.PATH_PREFIX_CANDIDATE_BOUND
        monkeypatch.setattr(grep_search, "PATH_PREFIX_CANDIDATE_BOUND", 2)
        check("broad_prefix", ["doc:zz-anyb", "doc:zz-padded", "doc:zz-under"])
        monkeypatch.setattr(grep_search, "PATH_PREFIX_CANDIDATE_BOUND", bound)
        with psycopg.connect(clean_runtime, autocommit=True) as conn:
            conn.execute("UPDATE okg.declared_attr_indexes SET drift = true "
                         "WHERE target_kind = 'node' AND attr_id = 'path'")
        check("view_graph_drift", ["doc:zz-anyb", "doc:zz-padded", "doc:zz-under"])
    finally:
        mcp._okg_pool.close()


def test_lookup_where_path_prefix_needs_path_declared(
    clean_runtime, okg_mcp_can_log_in, tmp_path, monkeypatch,
):
    """Without a `path` declaration the drift checks never compare it, so
    nothing narrows (`not_declared`) and the rows are the unnarrowed ones."""
    monkeypatch.delenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", raising=False)
    deployment, _ = _path_index_corpus(tmp_path, clean_runtime, declare=False)
    seen = _capture_page_sql(monkeypatch)
    mcp, search_tool = _lookup_tool(clean_runtime, deployment)
    where = {"op": "prefix", "field": "path", "value": "lib/"}
    try:
        page = search_tool(where=where, limit=_LOOKUP_LIMIT)
        assert _path_reasons(page) == ["not_declared"]
        assert seen and not any(_narrowed(sql) for sql, _ in seen)
        assert _all_pages(search_tool, where) == _PREFIX_CASES[0][1]
    finally:
        mcp._okg_pool.close()


def test_lookup_where_path_prefix_under_enforce_is_unchanged(
    clean_runtime, okg_mcp_can_log_in, tmp_path, monkeypatch,
):
    """Failure case for the leak class of #3208. Under Nomos `enforce` no
    path prefix reaches the page statement, even with `path` declared and
    ready; the readable match is found and the hidden document
    under the same path never appears."""
    from okg.substrate import declared_attr_indexes as dai
    from okg.substrate.mcp import server as srv
    from tests.pytest.catalog_fixture_builders import SubtypeSpec
    from tests.substrate.mcp.search_fixture import make_search_deployment, publish_nodes, node
    from tests.substrate.mcp.test_search_records import SECRET

    deployment = make_search_deployment(tmp_path, dsn=clean_runtime)
    filler = [node(f"doc:a{i:02d}", "doc", "filler", path=f"bulk/{i}") for i in range(30)]
    publish_nodes(
        clean_runtime, deployment,
        subtypes=[SubtypeSpec("doc", metadata=dict(_PATH_META))],
        nodes=[],
        node_groups=[
            (OPEN, [*filler, node("doc:zz-target", "doc", "target", path="lib/target.md")]),
            (SECRET, [node("doc:zz-secret", "doc", "secret", path="lib/secret.md")]),
        ],
    )
    report = dai.reconcile(clean_runtime, deployment=deployment, apply=True, enable=True)
    assert report["status"] == "converged", report
    _activate(clean_runtime, deployment, [_allow(OPEN), _allow(MAIN), {
        "rule_id": "allow_mcp_tool_call", "effect": "allow",
        "actions": ["mcp.tool.call"], "subjects": [READER],
        "match": {"resource_kind": "mcp_tool"}, "priority": 100,
    }, {
        "rule_id": "allow_graph_boundary", "effect": "allow",
        "actions": ["graph.read.mcp"], "subjects": [READER],
        "match": {"resource_kind": "mcp_graph_read_boundary"}, "priority": 100,
    }])
    monkeypatch.setenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", "enforce")
    monkeypatch.setenv("OKG_NOMOS_SUBJECT", READER)
    real_open = srv.open_session

    def as_reader(*args, **kwargs):
        kwargs["nomos_subject"] = READER
        return real_open(*args, **kwargs)

    monkeypatch.setattr(srv, "open_session", as_reader)
    seen = _capture_page_sql(monkeypatch)
    mcp, search_tool = _lookup_tool(clean_runtime, deployment)
    try:
        for field in ("path", "attrs.path"):
            where = {"op": "prefix", "field": field, "value": "lib/"}
            first = search_tool(where=where, limit=_LOOKUP_LIMIT)
            # #3331 pages over readable rows under `enforce`: the first page
            # already holds the readable match, never the hidden document.
            assert _ids(first) == ["doc:zz-target"]
            assert _path_reasons(first) == []
            assert _all_pages(search_tool, where) == ["doc:zz-target"]
        assert seen and all("pp0" not in sql and "node_path_prefix" not in sql for sql, _ in seen)
    finally:
        mcp._okg_pool.close()


# ══ shared compiler and scanner contracts (PACT shared-read-scanners-3199) ══
#
# The shared compiler and scanner contracts on real PostgreSQL.
#
# PACT shared-read-scanners-3199. Each claim with its failure case:
#
# * nullable string comparisons in the compiled SQL agree with the Python
#   reader under the database's real collation and under locale collations
#   (the pre-change helper did not, under a locale collation);
# * the compiled prefilter is a superset of reader acceptance for AND/OR/NOT,
#   equal when exact, and its negation equals reader FALSE when truth-exact;
#   masked values stay unknown under NOT;
# * lexical search on a published generation returns the same rows, ranks,
#   offsets and counts as the base helper, compiles once per call, and lowers
#   a clause only where each ranked row is its own membership target;
# * a scanner context built from a real continuation binding refuses a cursor
#   after a generation pin or reader authority change;
# * a real statement timeout during finishing raises, never empty output.

STRINGS = [None, "", "a", "A", "B", "b", "Z", "z", "_", "-", " a", "é", "e", "E", "ä", "ß",
           "aa", "a b", "ab", "Ticket", "ticket", "ticket:1", "😀", "中", "Å", "0", "9"]
OPERANDS = ["", "a", "B", "Z", "é", "_", "ticket", "😀", "Å", "0"]


def _leaf_predicates() -> list[dict]:
    out: list[dict] = [{"op": "exists", "field": "type"}, {"op": "eq", "field": "type", "value": None},
                       {"op": "in", "field": "type", "values": ["a", None, "é"]}]
    for value in OPERANDS:
        out += [{"op": "eq", "field": "type", "value": value},
                {"op": "lt", "field": "type", "value": value},
                {"op": "gt", "field": "type", "value": value}]
        if value:
            out += [{"op": "prefix", "field": "type", "value": value},
                    {"op": "suffix", "field": "type", "value": value}]
    for low, high in itertools.combinations(sorted(OPERANDS), 2):
        out.append({"op": "between", "field": "type", "values": [low, high]})
    return out


def _collations(conn) -> list[str | None]:
    with conn.cursor() as cur:
        cur.execute("""SELECT collname FROM pg_collation
                       WHERE collname IN ('en_US.utf8', 'en_US', 'en-x-icu', 'und-x-icu', 'de_DE.utf8')
                       ORDER BY collname""")
        return [None, *[row[0] for row in cur.fetchall()]]


def _string_table(conn, collation: str | None) -> None:
    with conn.cursor() as cur:
        cur.execute("DROP TABLE IF EXISTS pg_temp.rc_strings")
        column = pgsql.SQL("subtype text") if collation is None else pgsql.SQL(
            "subtype text COLLATE {}").format(pgsql.Identifier(collation))
        cur.execute(pgsql.SQL("CREATE TEMP TABLE rc_strings (id int PRIMARY KEY, {})").format(column))
        cur.executemany("INSERT INTO rc_strings VALUES (%s, %s)", list(enumerate(STRINGS)))


def _select(conn, where_sql: str, params: dict) -> set[int]:
    with conn.cursor() as cur:
        cur.execute(f"SELECT id FROM rc_strings WHERE {where_sql}", params)
        return {row[0] for row in cur.fetchall()}


def _python(predicate: dict, verdict) -> set[int]:
    return {i for i, value in enumerate(STRINGS) if sw.evaluate({"subtype": value}, predicate) is verdict}


def test_nullable_string_comparisons_agree_with_python_under_real_collations(clean_runtime) -> None:
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT datcollate FROM pg_database WHERE datname = current_database()")
            default_collation = cur.fetchone()[0]
        collations = _collations(conn)
        checked = 0
        for collation in collations:
            _string_table(conn, collation)
            for leaf in _leaf_predicates():
                for predicate in (leaf, {"op": "not", "clause": leaf}):
                    compiled = sw.compile_where(predicate, columns={"type": "subtype"})
                    assert compiled.qualification == "exact" and compiled.truth_exact
                    got = _select(conn, compiled.sql, compiled.params)
                    assert got == _python(predicate, True), (collation, default_collation, predicate)
                    checked += 1
        assert checked >= 2 * len(_leaf_predicates()) and default_collation


def test_the_pre_change_helper_disagreed_under_a_locale_collation(clean_runtime) -> None:
    """Failure case for the fix above: the base SQL ordered by locale."""
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        locales = [c for c in _collations(conn) if c is not None]
        if not locales:
            pytest.skip("this server has no locale collation to compare against")
        _string_table(conn, locales[0])
        disagreements = 0
        for leaf in _leaf_predicates():
            if leaf["op"] not in {"lt", "gt", "between"}:
                continue
            params: dict = {}
            old_sql = legacy_sql(leaf, params, columns={"type": "subtype"}, prefix="w")
            disagreements += _select(conn, old_sql, params) != _python(leaf, True)
        assert disagreements > 0


# ── reader truth: superset, exactness, unknown under NOT ──────────────

ROWS = [
    {"node_id": "t:1", "subtype": "ticket", "source": "s1", "attrs": {"status": "open"}},
    {"node_id": "t:2", "subtype": "ticket", "source": None, "attrs": {"status": "closed"}},
    {"node_id": "t:3", "subtype": "ticket", "source": "s2", "attrs": {"status": MASK_TOKEN}},
    {"node_id": "t:4", "subtype": "ticket", "source": "s1", "attrs": {REDACTION_MARKER: True}},
    {"node_id": "c:1", "subtype": "comment", "source": "s1", "attrs": {}},
    {"node_id": "c:2", "subtype": "comment", "source": "s2", "attrs": {"status": None}},
    {"node_id": "n:1", "subtype": "note", "source": "s2", "attrs": {"status": "open"}},
]
LEAVES = [
    {"op": "eq", "field": "type", "value": "ticket"},
    {"op": "prefix", "field": "id", "value": "c:"},
    {"op": "eq", "field": "source", "value": None},
    {"op": "eq", "field": "attrs.status", "value": "open"},
    {"op": "exists", "field": "attrs.status"},
    {"op": "eq", "field": "attrs.status", "value": None},
]


def _composites() -> list[dict]:
    out = list(LEAVES)
    for a, b in itertools.combinations(LEAVES, 2):
        out += [{"op": "and", "clauses": [a, b]}, {"op": "or", "clauses": [a, b]}]
    out += [{"op": "not", "clause": p} for p in list(out)]
    out += [{"op": "or", "clauses": [{"op": "and", "clauses": [LEAVES[0], LEAVES[3]]},
                                     {"op": "not", "clause": LEAVES[1]}]}]
    return out


def test_prefilter_is_a_superset_exact_when_claimed_and_truth_exact_under_not(clean_runtime) -> None:
    columns = {"id": "node_id", "type": "subtype", "source": "source"}
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.execute("CREATE TEMP TABLE rc_nodes (node_id text, subtype text, source text)")
            cur.executemany("INSERT INTO rc_nodes VALUES (%s, %s, %s)",
                            [(r["node_id"], r["subtype"], r["source"]) for r in ROWS])

        def ids(where_sql, params):
            with conn.cursor() as cur:
                cur.execute(f"SELECT node_id FROM rc_nodes WHERE {where_sql}", params)
                return {row[0] for row in cur.fetchall()}

        kinds = set()
        for predicate in _composites():
            compiled = sw.compile_where(predicate, columns=columns)
            admitted = ids(compiled.sql, compiled.params)
            true_rows = {r["node_id"] for r in ROWS if sw.evaluate(r, predicate) is True}
            false_rows = {r["node_id"] for r in ROWS if sw.evaluate(r, predicate) is False}
            assert true_rows <= admitted, predicate
            if compiled.qualification == "exact":
                assert admitted == true_rows, predicate
            if compiled.truth_exact:
                assert ids(f"NOT ({compiled.sql})", compiled.params) == false_rows, predicate
            kinds.add((compiled.qualification, admitted != {r["node_id"] for r in ROWS}))
        # Both exact and narrowing superset prefilters were exercised.
        assert ("exact", True) in kinds and ("superset", True) in kinds
        masked_not = {"op": "not", "clause": LEAVES[3]}
        assert sw.evaluate(ROWS[2], masked_not) is None and sw.evaluate(ROWS[3], masked_not) is None


# ── lexical search on a published generation ──────────────────────────

LEXICAL_WHERE = [
    None,
    {"op": "in", "field": "type", "values": ["comment"]},
    {"op": "not", "clause": {"op": "eq", "field": "type", "value": "ticket"}},
    {"op": "or", "clauses": [{"op": "eq", "field": "type", "value": "comment"},
                             {"op": "eq", "field": "attrs.status", "value": "open"}]},
    {"op": "or", "clauses": [
        {"op": "and", "clauses": [{"op": "eq", "field": "type", "value": "ticket"},
                                  {"op": "eq", "field": "attrs.status", "value": "open"}]},
        {"op": "and", "clauses": [{"op": "eq", "field": "type", "value": "comment"},
                                  {"op": "not", "clause": {"op": "exists", "field": "attrs.status"}}]}]},
    {"op": "lt", "field": "id", "value": "comment:t"},
    {"op": "between", "field": "type", "values": ["comment", "note"]},
    {"op": "and", "clauses": [{"op": "prefix", "field": "id", "value": "ticket:"},
                              {"op": "eq", "field": "attrs.status", "value": "open"}]},
    {"op": "eq", "field": "type", "value": "x' OR '1'='1"},
]


def _page(dsn, deployment, query, **kwargs):
    with psycopg.connect(dsn, autocommit=True) as conn:
        session = open_session(conn, deployment_name=deployment)
        return search(conn, session, query, method="lexical", **kwargs), session.generation_id


def test_lexical_pages_match_the_base_helper_on_a_published_generation(clean_runtime, tmp_path, monkeypatch) -> None:
    deployment = _basic(tmp_path, clean_runtime)
    cases = [(q, where, rank_by, filt, offset)
             for q in (QUERY, TERM) for where in LEXICAL_WHERE
             for rank_by in ("self", "records") for filt in ({}, {"subtype": "ticket"})
             for offset in (0, 2)]
    current, generations, nonempty = {}, set(), 0
    for index, (q, where, rank_by, filt, offset) in enumerate(cases):
        page, generation = _page(clean_runtime, deployment, q, where=where, rank_by=rank_by,
                                 filter=filt, limit=3, offset=offset)
        current[index] = page
        generations.add(generation)
        nonempty += bool(page["results"])
    monkeypatch.setattr(sw, "compile_where", legacy_compile_where)
    for index, (q, where, rank_by, filt, offset) in enumerate(cases):
        legacy, _ = _page(clean_runtime, deployment, q, where=where, rank_by=rank_by,
                          filter=filt, limit=3, offset=offset)
        assert current[index] == legacy, cases[index]
    assert len(generations) == 1 and nonempty > len(cases) // 4
    hostile = [current[i] for i, case in enumerate(cases) if case[1] == LEXICAL_WHERE[-1]]
    assert hostile and all(page["results"] == [] for page in hostile)


@pytest.mark.parametrize("rank_by,filt,calls", [
    ("self", {}, 1),
    ("records", {"subtype": "ticket"}, 1),  # filtered fold: each row is a record
    ("records", {}, 0),  # unfolded passages are judged through their record
])
def test_where_compiles_once_and_only_for_self_membership(clean_runtime, tmp_path, monkeypatch,
                                                           rank_by, filt, calls) -> None:
    deployment = _basic(tmp_path, clean_runtime)
    seen = []
    real = sw.compile_where

    def spy(*args, **kwargs):
        seen.append(args[0] if args else kwargs.get("predicate"))
        return real(*args, **kwargs)

    monkeypatch.setattr(sw, "compile_where", spy)
    where = {"op": "and", "clauses": [{"op": "in", "field": "type", "values": ["ticket"]},
                                      {"op": "in", "field": "attrs.status", "values": ["closed"]}]}
    page, _ = _page(clean_runtime, deployment, QUERY, where=where, rank_by=rank_by, filter=filt, limit=50)
    assert len(seen) == calls
    if rank_by == "records" and not filt:
        # The record is reached through its passage: lowering `type` onto the
        # passage rows would have dropped it.
        assert [r["node_id"] for r in page["results"]] == ["ticket:closed"]


# ── scanner contracts bound to real pins and reader authority ─────────


def test_real_bindings_refuse_cursors_after_pin_or_authority_change(clean_runtime, tmp_path) -> None:
    from tests.substrate.mcp.search_fixture import node, publish_nodes
    from tests.substrate.mcp.test_search_records import SUBTYPES, NARROWINGS

    deployment = _basic(tmp_path, clean_runtime)
    compiled = sw.compile_where({"op": "eq", "field": "type", "value": "ticket"},
                                columns={"id": "node_id", "type": "subtype", "source": "source"})

    def context(conn, session):
        binding = continuation_binding(conn, session, kind="search", sort="bm25",
                                       projection={"query": "q", "max_bytes": 4096})
        return make_scan_context(binding, query=QUERY, scope=None, compiled=compiled, source="lexical",
                                 result_unit="record", membership_target="parent_record",
                                 max_bytes=4096, ordering_identity="keyset:node_id")

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        first = open_session(conn, deployment_name=deployment)
        ctx = context(conn, first)
        token = issue_scan_continuation(ctx, {"after": "ticket:long"}, measure=len)
        assert consume_scan_continuation(context(conn, open_session(conn, deployment_name=deployment)),
                                         token) == {"after": "ticket:long"}
        reader = open_session(conn, deployment_name=deployment, nomos_subject=READER)
        with pytest.raises(ContinuationTokenInvalid) as authority:
            consume_scan_continuation(context(conn, reader), token)
        assert authority.value.data["reason"] == "binding_mismatch"
    publish_nodes(clean_runtime, deployment, subtypes=SUBTYPES, narrowings=NARROWINGS,
                  nodes=[node("note:late", "note", f"A late {TERM} note.")])
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        moved = open_session(conn, deployment_name=deployment)
        assert moved.generation_id != first.generation_id
        with pytest.raises(ContinuationTokenInvalid) as pin:
            consume_scan_continuation(context(conn, moved), token)
        assert pin.value.data["reason"] == "binding_mismatch"
        pinned = open_session(conn, deployment_name=deployment, requested_generation=first.generation_id)
        assert consume_scan_continuation(context(conn, pinned), token) == {"after": "ticket:long"}


def test_a_real_statement_timeout_while_finishing_is_not_empty_output(clean_runtime) -> None:
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        binding = {"kind": "search", "deployment": "d", "database": "db", "branch": "main",
                   "generation": "g", "catalog_version": 1, "authority": "a", "policy": None,
                   "key_id": "k", "projection": {}, "sort": "bm25"}
        ctx = make_scan_context(binding, query="q", scope=None,
                                compiled=sw.compile_where(None, columns={"id": "node_id"}),
                                source="lexical", result_unit="record",
                                membership_target="parent_record", max_bytes=4096)
        batch = ScanBatch(rows=({"node_id": "a"},), terminal=True, count_basis="exact", total_count=1,
                          enumeration=EnumerationEvidence("prefilter_candidates", ctx.scope_identity,
                                                          "whole_sql"))

        def finish(row):
            with conn.cursor() as cur:
                cur.execute("SET statement_timeout = '50ms'")
                try:
                    cur.execute("SELECT pg_sleep(1)")
                finally:
                    cur.execute("RESET statement_timeout")
            return FinishedCandidate(rows=(row,))

        with pytest.raises(psycopg.errors.QueryCanceled):
            finish_scan_batch(ctx, batch, finish, finished_domain="visible_matches")


# ── disclosed behavior changes: they only add accepted rows ───────────
#
# Two lexical changes in this PACT can only ADD rows the reader accepts:
#
# * range leaves on id/type/source compare with COLLATE "C" (code-point
#   order, as the Python reader does) instead of the database's locale;
# * a mixed OR (an invariant branch OR'ed with an attrs branch) now narrows
#   the ranked SQL, so the 200-row rank cap is spent on rows that can match.
#
# Each test shows new results contain every reader-accepted row the old SQL
# returned, and that the old SQL really returned fewer.


def _locale_database(conn, name: str) -> str:
    """Create ``name`` with a non-C default collation; return what was used.

    libc ``en_US.utf8`` first, then ICU ``en-US``: both are locale orders,
    and the test skips (naming both errors) only when neither exists.
    """
    attempts = [
        ("libc en_US.utf8", "LOCALE_PROVIDER libc LC_COLLATE 'en_US.utf8' LC_CTYPE 'en_US.utf8'"),
        ("icu en-US", "LOCALE_PROVIDER icu ICU_LOCALE 'en-US' LOCALE 'C'"),
    ]
    errors = []
    with conn.cursor() as cur:
        for label, clause in attempts:
            try:
                cur.execute(pgsql.SQL(
                    "CREATE DATABASE {} TEMPLATE template0 ENCODING 'UTF8' " + clause
                ).format(pgsql.Identifier(name)))
                return label
            except psycopg.Error as exc:  # this server lacks that locale
                errors.append(f"{label}: {exc}")
    pytest.skip("no non-C locale database can be created here: " + "; ".join(errors))
    raise AssertionError("unreachable")


def test_range_filters_never_drop_reader_rows_in_a_locale_collated_database(clean_runtime) -> None:
    """A database whose DEFAULT collation is a locale (no column COLLATE).

    The new SQL equals reader acceptance, so it is a superset of it. The
    pre-change SQL, on the same rows, dropped rows the reader accepts.
    """
    from psycopg.conninfo import conninfo_to_dict, make_conninfo

    with psycopg.connect(clean_runtime, autocommit=True) as admin:
        with admin.cursor() as cur:
            cur.execute("SELECT current_database()")
            name = f"{cur.fetchone()[0]}_loc"
        assert name.startswith("okg_test_") and len(name) <= 63
        with admin.cursor() as cur:
            cur.execute(pgsql.SQL("DROP DATABASE IF EXISTS {} WITH (FORCE)").format(
                pgsql.Identifier(name)))
        used = _locale_database(admin, name)
        try:
            dsn = make_conninfo(**{**conninfo_to_dict(clean_runtime), "dbname": name})
            with psycopg.connect(dsn, autocommit=True) as conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT datcollate, datlocprovider::text FROM pg_database "
                                "WHERE datname = current_database()")
                    collate, provider = cur.fetchone()
                assert provider == "i" or not (
                    collate in {"C", "POSIX"} or collate.startswith("C.")), (used, collate, provider)
                _string_table(conn, None)  # the column takes the database default
                dropped = checked = 0
                for leaf in _leaf_predicates():
                    if leaf["op"] not in {"lt", "gt", "between"}:
                        continue
                    accepted = _python(leaf, True)
                    compiled = sw.compile_where(leaf, columns={"type": "subtype"})
                    new = _select(conn, compiled.sql, compiled.params)
                    assert accepted <= new and new == accepted, (used, leaf)
                    params: dict = {}
                    old = _select(conn, legacy_sql(leaf, params, columns={"type": "subtype"},
                                                   prefix="w"), params)
                    dropped += bool(accepted - old)
                    checked += 1
                assert checked > 40
                # The old SQL lost reader-accepted rows: the risk the change removes.
                assert dropped > 0, used
        finally:
            with admin.cursor() as cur:
                cur.execute(pgsql.SQL("DROP DATABASE IF EXISTS {} WITH (FORCE)").format(
                    pgsql.Identifier(name)))


def _cap_corpus(tmp_path, dsn) -> str:
    from tests.substrate.mcp.test_search_records import _node, _publish

    facts = [_node(f"note:hot:{i:03d}", "note", f"{TERM} {TERM} {TERM}.") for i in range(210)]
    filler = "with several additional ordinary words about routine work"
    facts += [_node(f"ticket:cold:{i}", "ticket", f"A {TERM} ticket {filler} {i}.", status="open")
              for i in range(5)]
    facts += [_node("ticket:shut", "ticket", f"A {TERM} ticket {filler} shut.", status="closed")]
    facts += [_node(f"comment:cold:{i}", "comment", f"A {TERM} comment {filler} {i}.")
              for i in range(5)]
    deployment = _publish(tmp_path, dsn, groups={}, main=facts)
    # TEST-ONLY: ANALYZE the published node table before the first read. Both
    # cap-corpus tests compare ranked pages, and a first BM25 read racing
    # autovacuum's ANALYZE after a publish can score real matches 0 (#3232),
    # which reorders the 200-row cap and makes the old-versus-new comparison
    # flaky. Product reads keep matches through the shared match test (#3234);
    # this only pins the ranking inputs the assertions depend on.
    from okg.substrate.bm25_node_search import resolve_node_text_bm25_index

    with psycopg.connect(dsn, autocommit=True) as conn:
        segment, _index = resolve_node_text_bm25_index(conn)
        with conn.cursor() as cur:
            cur.execute(f"ANALYZE {segment}")
    return deployment


MIXED_OR = {"op": "or", "clauses": [
    {"op": "and", "clauses": [{"op": "eq", "field": "type", "value": "ticket"},
                              {"op": "eq", "field": "attrs.status", "value": "open"}]},
    {"op": "eq", "field": "type", "value": "comment"}]}
CAP_ACCEPTED = {f"ticket:cold:{i}" for i in range(5)} | {f"comment:cold:{i}" for i in range(5)}


def test_mixed_or_narrows_the_ranked_query_and_only_adds_accepted_rows(
        clean_runtime, tmp_path, monkeypatch) -> None:
    deployment = _cap_corpus(tmp_path, clean_runtime)
    compiled = sw.compile_where(MIXED_OR, columns={"id": "node_id", "type": "subtype",
                                                   "source": "source"})
    assert compiled.qualification == "superset" and "subtype" in compiled.sql
    new, _ = _page(clean_runtime, deployment, TERM, where=MIXED_OR, rank_by="self", limit=50)
    monkeypatch.setattr(sw, "compile_where", legacy_compile_where)
    old, _ = _page(clean_runtime, deployment, TERM, where=MIXED_OR, rank_by="self", limit=50)
    new_ids = {r["node_id"] for r in new["results"]}
    old_ids = {r["node_id"] for r in old["results"]}
    # 210 better-ranked notes fill the 200-row cap before any ticket/comment.
    assert old_ids & CAP_ACCEPTED <= new_ids
    assert new_ids <= CAP_ACCEPTED  # every new row is one the reader accepts
    assert new_ids == CAP_ACCEPTED and len(old_ids) < len(new_ids), (old_ids, new_ids)


# ── pseudonyms compare as read values ─────────────────────────────────


def test_pseudonymized_attrs_stay_python_residual_over_read_values(clean_runtime, tmp_path) -> None:
    """At-rest pseudonyms (the real sanitizer and vault) on a published graph.

    An attrs clause never lowers to SQL; the residual compares the value the
    reader reads (the pseudonym), so the raw identifier matches nothing.
    """
    from okg.substrate.alias.pseudonymize import PseudonymService
    from okg.substrate.ingest.pii_sanitize import sanitize_fact_pii
    from okg.substrate.mcp import inspect_node
    from tests.substrate.mcp.test_search_records import _node, _publish

    vault = PseudonymService(key=b"shared-scanner-pseudonym-test-key", epoch="e1")
    classes = {"email": "direct_identifier", "text": "non_pii"}
    raw = [_node("note:p:a", "note", f"A {TERM} note a.", email="alice@example.com"),
           _node("note:p:b", "note", f"A {TERM} note b.", email="alice@example.com"),
           _node("note:p:c", "note", f"A {TERM} note c.", email="bob@example.com")]
    facts = []
    for fact in raw:
        out = sanitize_fact_pii(fact, classifier=classes.get, store_raw=False,
                                pseudonym_service=vault)
        assert not out.held
        facts.append(out.fact)
    deployment = _publish(tmp_path, clean_runtime, groups={}, main=facts)
    ids = ("note:p:a", "note:p:b", "note:p:c")
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        session = open_session(conn, deployment_name=deployment)
        shown = {n: inspect_node(conn, session, n)["attrs"]["email"] for n in ids}
    # inspect fences source-authored text for display; the read value is inside.
    read = {}
    for n, value in shown.items():
        lines = value.split("\n")
        assert lines[0].startswith("⟦UNTRUSTED_DATA") and lines[-1] == "⟦/UNTRUSTED_DATA⟧"
        read[n] = "\n".join(lines[1:-1])
    alice = read["note:p:a"]
    assert alice == read["note:p:b"] != read["note:p:c"] and "_e1_" in alice
    assert "alice@example.com" not in read.values()
    cases = [
        ({"op": "eq", "field": "attrs.email", "value": alice}, {"note:p:a", "note:p:b"}),
        ({"op": "in", "field": "attrs.email", "values": [alice]}, {"note:p:a", "note:p:b"}),
        ({"op": "not", "clause": {"op": "eq", "field": "attrs.email", "value": alice}},
         {"note:p:c"}),
        ({"op": "eq", "field": "attrs.email", "value": "alice@example.com"}, set()),
        ({"op": "in", "field": "attrs.email", "values": ["alice@example.com"]}, set()),
        ({"op": "or", "clauses": [
            {"op": "eq", "field": "type", "value": "ticket"},
            {"op": "in", "field": "attrs.email", "values": [read["note:p:c"]]}]},
         {"note:p:c"}),
    ]
    columns = {"id": "node_id", "type": "subtype", "source": "source"}
    for where, expected in cases:
        compiled = sw.compile_where(where, columns=columns)
        # The attrs leaf is never SQL: no pseudonym or raw value is a parameter.
        assert compiled.qualification == "superset" and not compiled.truth_exact, where
        assert not {alice, "alice@example.com"} & set(map(str, compiled.params.values()))
        for rank_by in ("self", "records"):
            page, _ = _page(clean_runtime, deployment, TERM, where=where, rank_by=rank_by,
                            limit=50)
            assert {r["node_id"] for r in page["results"]} == expected, (where, rank_by)


# ── values with no canonical encoding refuse by name ──────────────────


@pytest.mark.parametrize("where", [
    {"op": "eq", "field": "attrs.x", "value": float("nan")},
    {"op": "eq", "field": "attrs.x", "value": float("inf")},
    {"op": "eq", "field": "attrs.x", "value": "bad\ud800"},
    {"op": "contains", "field": "attrs", "value": {"k\udfff": "v"}},
    {"op": "in", "field": "type", "values": ["ticket", "\ud83d"]},
])
def test_noncanonical_where_values_are_refused_on_a_real_search(clean_runtime, tmp_path,
                                                                where) -> None:
    deployment = _basic(tmp_path, clean_runtime)
    with pytest.raises(WhereUnsupported) as refused:
        _page(clean_runtime, deployment, QUERY, where=where, rank_by="self", limit=5)
    assert refused.value.data["reason"] == "invalid_where_value"


# ── measured database work, old versus new ranked SQL ─────────────────


class _RecordingCursor(psycopg.Cursor):
    log: list | None = None

    def execute(self, query, params=None, **kwargs):
        if _RecordingCursor.log is not None:
            _RecordingCursor.log.append((query, params))
        return super().execute(query, params, **kwargs)


def _work(dsn, deployment, where) -> dict:
    """Run one search, then EXPLAIN (ANALYZE, BUFFERS) every SELECT it ran."""
    totals = {"statements": 0, "shared_hit": 0, "shared_read": 0, "execution_ms": 0.0}
    ranked = None
    try:
        with psycopg.connect(dsn, autocommit=True, cursor_factory=_RecordingCursor) as conn:
            session = open_session(conn, deployment_name=deployment)
            _RecordingCursor.log = []
            page = search(conn, session, TERM, method="lexical", where=where, rank_by="self",
                          limit=50)
            statements, _RecordingCursor.log = list(_RecordingCursor.log), None
            with conn.cursor() as cur:
                for query, params in statements:
                    text = query if isinstance(query, str) else query.as_string(conn)
                    if not text.lstrip().upper().startswith(("SELECT", "WITH")):
                        continue
                    cur.execute("EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) " + text, params)
                    plan = cur.fetchone()[0][0]
                    top = plan["Plan"]
                    row = {"shared_hit": top.get("Shared Hit Blocks", 0),
                           "shared_read": top.get("Shared Read Blocks", 0),
                           "execution_ms": plan["Execution Time"],
                           "rows": top.get("Actual Rows")}
                    totals["statements"] += 1
                    for key in ("shared_hit", "shared_read", "execution_ms"):
                        totals[key] += row[key]
                    if "to_bm25query" in text:
                        ranked = row
    finally:
        _RecordingCursor.log = None
    return {"returned": len(page["results"]), "ranked_statement": ranked, **totals}


def test_measure_lexical_where_database_work_old_versus_new(clean_runtime, tmp_path,
                                                            monkeypatch) -> None:
    """EXPLAIN (ANALYZE, BUFFERS) totals for old and new ranked SQL.

    Writes the measurements to ``$OKG_READ_WORK_OUT`` when set. It asserts
    only what is deterministic: each case ran measured statements, the ranked
    query was among them, and the new SQL returned no fewer rows.
    """
    import json
    import os

    deployment = _cap_corpus(tmp_path, clean_runtime)
    cases = {
        "mixed_or": MIXED_OR,
        "range_lt_id": {"op": "lt", "field": "id", "value": "note:"},
        "not_type_note": {"op": "not", "clause": {"op": "eq", "field": "type", "value": "note"}},
        "type_ticket_and_attr": {"op": "and", "clauses": [
            {"op": "eq", "field": "type", "value": "ticket"},
            {"op": "in", "field": "attrs.status", "values": ["open"]}]},
    }
    report = {}
    real = sw.compile_where
    for name, where in cases.items():
        monkeypatch.setattr(sw, "compile_where", real)
        new = _work(clean_runtime, deployment, where)
        monkeypatch.setattr(sw, "compile_where", legacy_compile_where)
        old = _work(clean_runtime, deployment, where)
        report[name] = {"where": where, "old": old, "new": new}
        for side in (old, new):
            assert side["statements"] > 0 and side["ranked_statement"] is not None, (name, side)
        assert new["returned"] >= old["returned"], name
    out = os.environ.get("OKG_READ_WORK_OUT")
    if out:
        with open(out, "w", encoding="utf-8") as handle:
            json.dump(report, handle, indent=2, sort_keys=True)


# ---------- issue #3666: the rank cap, said and kept ----------

#: More matching notes than the 200-row rank cap, one of them flagged.
_CAPPED_NOTES = 230


def _capped_corpus(tmp_path, dsn: str) -> str:
    from tests.substrate.mcp.test_search_records import _node, _publish

    notes = [
        _node(f"note:cap:{i:03d}", "note", f"hafnium survey note {i}.",
              flag="x" if i == _CAPPED_NOTES - 1 else "n")
        for i in range(_CAPPED_NOTES)
    ]
    return _publish(tmp_path, dsn, groups={}, main=[
        *notes, _node("note:solo", "note", "A tantalum survey note.", flag="x"),
    ])


def _status_line(page: dict) -> str:
    from okg.substrate.mcp.result_text import render

    return render({**page, "operator_name": "search"}, tool="search").splitlines()[0]


def test_a_capped_lexical_where_says_it_checked_only_the_ranked_rows(clean_runtime, tmp_path):
    """Lexical checks a residual `where` on its ranked candidates only. When
    that read filled the cap, the page says so beside its count, in text
    and JSON, with the call that checks every record. Failure case: a page
    whose ranked read did not fill says nothing."""
    deployment = _capped_corpus(tmp_path, clean_runtime)
    flagged = {"op": "in", "field": "attrs.flag", "values": ["x"]}
    page = _search(clean_runtime, deployment, "hafnium", limit=10, where=flagged)
    assert page["rank_capped"] is True
    checked = page["where_checked_on"]
    assert checked["ranked_cap"] == page["ranked_cap"] and checked["ranked_cap"]["rows"] == 200
    assert checked["every_record"] == (
        "filter with the same where checks every record, without the text match"
    )
    line = _status_line(page)
    assert "where checked on the top 200 rows" in line
    assert ("ranked only; filter with the same where checks every record, "
            "without the text match") in line
    by_self = _search(clean_runtime, deployment, "hafnium", limit=10, where=flagged,
                      rank_by="self")
    assert by_self["rank_capped"] is True and "where_checked_on" in by_self

    small = _search(clean_runtime, deployment, "tantalum", limit=10, where=flagged)
    assert _ids(small) == ["note:solo"]
    assert "where_checked_on" not in small
    assert "where checked" not in _status_line(small)


@pytest.mark.parametrize("kwargs", [
    {"where": {"op": "eq", "field": "attrs.flag", "value": "x"}},
    {"filter": {"attr_filters": {"flag": "x"}}},
], ids=["attrs-equality", "legacy-attr-filters"])
def test_a_capped_attribute_equality_says_it_checked_only_the_ranked_rows(
    clean_runtime, tmp_path, kwargs,
):
    """Issue #3666 review: an `attrs.<key> = v` moves out of `where` into
    `attr_filters`, which are still checked after ranking, as the legacy
    argument is. A capped read says so for both spellings."""
    deployment = _capped_corpus(tmp_path, clean_runtime)
    page = _search(clean_runtime, deployment, "hafnium", limit=10, **kwargs)
    assert page["rank_capped"] is True
    assert page["where_checked_on"]["ranked_cap"]["rows"] == 200
    assert "where checked on the top 200 rows" in _status_line(page)


@pytest.mark.parametrize("where", [
    {"op": "in", "field": "type", "values": ["note", "ticket"]},
    {"op": "in", "field": "source", "values": [MAIN]},
], ids=["type-in", "source-in"])
def test_a_capped_where_the_ranked_sql_decides_is_not_said_partial(clean_runtime, tmp_path, where):
    """Issue #3666 review: `type`, `id` and `source` clauses, `in` lists
    included, join the ranked SQL when each row is judged by itself, so
    they are checked on every row and the page must not say "checked on
    the top 200 only", though the read still fills the cap."""
    deployment = _capped_corpus(tmp_path, clean_runtime)
    page = _search(clean_runtime, deployment, "hafnium", limit=10, where=where, rank_by="self")
    assert page["rank_capped"] is True
    assert "where_checked_on" not in page
    assert "where checked" not in _status_line(page)


def test_an_exact_lexical_page_cut_at_the_rank_cap_never_says_complete(clean_runtime, tmp_path):
    """Issue #3666: an exact lexical read that filled the rank cap is served
    (PACT lexical-index-scan-cap-3162) under the requested mode `exact`,
    with a listing that is not complete and names the cap. Text says it is
    exact only within the first 200 ranked rows, offers grep as a literal
    text match, and its last reachable page is never `complete`. Failure
    case: a read under the cap has no listing."""
    deployment = _capped_corpus(tmp_path, clean_runtime)
    first = _search(clean_runtime, deployment, "hafnium", limit=10, completeness="exact")
    assert first["completeness"] == "exact"
    assert first["listing"] == {
        "complete": False, "reason": "scan_bound", "ranked_cap": first["ranked_cap"],
        "literal_text_match": {"method": "grep", "completeness": "exact"},
    }
    line = _status_line(first)
    assert "exact within the first 200 rows ranked; listing stops there" in line
    assert "(a literal text match: method=grep completeness=exact)" in line
    assert 'next: offset=10 completeness="exact"' in line

    last = _search(clean_runtime, deployment, "hafnium", limit=10, offset=190,
                   completeness="exact")
    assert last["listing"]["complete"] is False
    line = _status_line(last)
    assert "bound reached (rank cap 200 rows) · next: none" in line
    assert " · complete" not in line

    whole = _search(clean_runtime, deployment, "tantalum", limit=10, completeness="exact")
    assert whole["completeness"] == "exact" and "listing" not in whole
    assert whole["total_count"] == 1 and whole["total_count_basis"] == "exact"
