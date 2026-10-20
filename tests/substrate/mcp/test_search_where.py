"""`where` and record `scope` on search: the parts that need no database.

PACT read-model-s3-where-scope (issue #3194; design
`pact/changes/read-model/design.md` 3.3, 3.4, 6). The PostgreSQL half is
`test_search_where_db.py`.
"""
from __future__ import annotations

import pytest

from okg.substrate.graph_store_internal.schema import GREP_PATH_INDEX_CHARS
from okg.substrate.mcp import grep_search as gs
from okg.substrate.mcp import search_where as sw
from okg.substrate.mcp.operator_models import Predicate, SearchScope
from okg.substrate.nomos.read_masking import MASK_TOKEN, REDACTION_MARKER

EQ_TICKET = {"op": "eq", "field": "type", "value": "ticket"}


# ── normalization: fields search can read, refused by name otherwise ──


def test_accepted_spellings_normalize_to_one_form() -> None:
    where = sw.normalize_where({"op": "and", "clauses": [
        {"op": "eq", "field": "subtype", "value": "ticket"},
        {"op": "eq", "field": "node_id", "value": "t:1"},
        {"op": "prefix", "field": "path", "value": "slack/"},
        {"op": "exists", "field": "attrs.owner"},
    ]})
    assert [c["field"] for c in where["clauses"]] == [
        "type", "id", "path", "attrs.owner",
    ]


@pytest.mark.parametrize("field", ["date", "source_family"])
def test_a_field_this_server_cannot_read_yet_is_refused_by_name(field) -> None:
    with pytest.raises(sw.WhereUnsupported) as err:
        sw.normalize_where({"op": "eq", "field": field, "value": "x"})
    assert err.value.code == "where_unsupported"
    assert err.value.data == {"field": field, "reason": "field_not_available"}


@pytest.mark.parametrize("where", [
    {"op": "eq", "field": "text_hash", "value": "x"},
    {"op": "prefix", "field": "path", "value": ""},
    {"op": "between", "field": "attrs.n", "values": [1]},
    {"op": "contains", "field": "attrs.k", "value": {"a": 1}},
    {"op": "eq", "field": "attrs", "value": {}},
    {"op": "not", "clause": {}},
])
def test_malformed_clauses_are_refused_before_any_read(where) -> None:
    with pytest.raises(sw.WhereUnsupported):
        sw.normalize_where(where)


def test_the_shared_predicate_model_accepts_prefix() -> None:
    model = Predicate.model_validate({"op": "prefix", "field": "attrs.path", "value": "a/"})
    assert model.op == "prefix"


@pytest.mark.parametrize("scope,ids", [
    ({"record": "t:1"}, ["t:1"]),
    ({"records": ["t:2", "t:1", "t:2"]}, ["t:1", "t:2"]),
    (None, None),
])
def test_record_scope_normalizes(scope, ids) -> None:
    assert sw.normalize_record_scope(scope) == ids


@pytest.mark.parametrize("scope", [
    {"record": "t:1", "records": ["t:2"]},
    {"records": []},
    {"children_of": "t:1", "record": "t:2"},
    {"record": ""},
])
def test_a_malformed_scope_is_refused(scope) -> None:
    with pytest.raises(sw.SearchScopeUnsupported):
        sw.normalize_record_scope(scope)


def test_the_scope_model_rejects_more_than_one_hundred_records() -> None:
    with pytest.raises(Exception):
        SearchScope.model_validate({"records": [f"t:{i}" for i in range(101)]})


# ── one predicate: old arguments and `where` lift to the same filter ──


def test_top_level_equalities_lift_into_the_old_keys() -> None:
    old = sw.plan_where(
        {"subtype": "ticket", "source": "s", "attr_filters": {"status": "open"}},
        None, lift_source=True,
    )
    new = sw.plan_where(None, sw.normalize_where({"op": "and", "clauses": [
        EQ_TICKET,
        {"op": "eq", "field": "source", "value": "s"},
        {"op": "eq", "field": "attrs.status", "value": "open"},
    ]}), lift_source=True)
    assert new.filter == old.filter
    assert new.residual is None and old.residual is None
    assert sw.describe(new.applied) == sw.describe(old.applied) == (
        "type = ticket and source = s and attrs.status = open"
    )


def test_source_lifts_only_where_the_ranked_read_serves_it() -> None:
    plan = sw.plan_where(
        None, {"op": "eq", "field": "source", "value": "s"}, lift_source=False,
    )
    assert "source" not in plan.filter
    assert plan.residual == {"op": "eq", "field": "source", "value": "s"}


def test_a_conflicting_equality_stays_residual_and_ands() -> None:
    plan = sw.plan_where(
        {"subtype": "ticket"},
        {"op": "eq", "field": "type", "value": "page"},
        lift_source=True,
    )
    assert plan.filter["subtype"] == "ticket"
    assert plan.residual == {"op": "eq", "field": "type", "value": "page"}
    assert sw.describe(plan.applied) == "type = ticket and type = page"


def test_or_and_not_are_never_lifted() -> None:
    where = sw.normalize_where({"op": "or", "clauses": [
        EQ_TICKET, {"op": "eq", "field": "type", "value": "page"},
    ]})
    plan = sw.plan_where(None, where, lift_source=True)
    assert plan.filter == {} and plan.residual == where


# ── three-valued evaluation on read rows (D5) ─────────────────────────


def _row(**attrs) -> dict:
    return {"node_id": "t:1", "subtype": "ticket", "source": "s", "attrs": attrs}


def test_a_clause_on_a_masked_value_is_unknown_and_not_keeps_it_unknown() -> None:
    masked = _row(assignee=MASK_TOKEN, **{REDACTION_MARKER: True})
    readable = _row(assignee="alice")
    eq = {"op": "eq", "field": "attrs.assignee", "value": "bob"}
    neg = {"op": "not", "clause": eq}
    assert sw.evaluate(masked, eq) is None
    assert sw.evaluate(masked, neg) is None
    assert not sw.matches(masked, neg)
    # Control: the same `not` on a readable value is ordinary logic.
    assert sw.evaluate(readable, neg) is True and sw.matches(readable, neg)


def test_a_dropped_key_on_a_redacted_row_is_unknown_not_absent() -> None:
    redacted = _row(**{REDACTION_MARKER: True})
    plain = _row()
    absent = {"op": "not", "clause": {"op": "exists", "field": "attrs.body"}}
    assert sw.evaluate(redacted, absent) is None
    assert sw.evaluate(plain, absent) is True


def test_kleene_or_and_and_around_an_unknown() -> None:
    masked = _row(assignee=MASK_TOKEN)
    unknown = {"op": "eq", "field": "attrs.assignee", "value": "x"}
    assert sw.evaluate(masked, {"op": "or", "clauses": [unknown, EQ_TICKET]}) is True
    assert sw.evaluate(masked, {"op": "and", "clauses": [
        unknown, {"op": "eq", "field": "type", "value": "page"},
    ]}) is False
    assert sw.evaluate(masked, {"op": "and", "clauses": [unknown, EQ_TICKET]}) is None


def test_contains_is_unknown_when_any_key_it_names_is_masked() -> None:
    masked = _row(assignee=MASK_TOKEN, status="open")
    clause = {"op": "contains", "field": "attrs",
              "value": {"assignee": "x", "status": "open"}}
    assert sw.evaluate(masked, clause) is None
    clause["value"]["status"] = "closed"
    assert sw.evaluate(masked, clause) is False


@pytest.mark.parametrize("clause,expected", [
    ({"op": "prefix", "field": "attrs.path", "value": "slack/"}, True),
    ({"op": "prefix", "field": "attrs.path", "value": "slack/x"}, False),
    ({"op": "suffix", "field": "attrs.path", "value": ".json"}, True),
    ({"op": "between", "field": "attrs.n", "values": [1, 3]}, True),
    ({"op": "gt", "field": "attrs.n", "value": "1"}, False),
    ({"op": "in", "field": "source", "values": ["s", "t"]}, True),
    ({"op": "exists", "field": "id"}, True),
])
def test_leaves_on_readable_values(clause, expected) -> None:
    assert sw.evaluate(_row(path="slack/a.json", n=2), clause) is expected


# ── policy-invariant SQL (D4) ─────────────────────────────────────────


def test_only_id_type_source_conjuncts_reach_sql() -> None:
    where = sw.normalize_where({"op": "and", "clauses": [
        {"op": "in", "field": "type", "values": ["ticket", "page"]},
        {"op": "prefix", "field": "path", "value": "slack/"},
        {"op": "not", "clause": {"op": "eq", "field": "source", "value": "s"}},
        {"op": "or", "clauses": [EQ_TICKET, {"op": "exists", "field": "attrs.k"}]},
    ]})
    conjuncts = sw.invariant_conjuncts(where)
    assert [c["op"] for c in conjuncts] == ["in", "not"]
    params: dict = {}
    sql = sw.invariant_sql(
        conjuncts, params, columns={"id": "node_id", "type": "subtype", "source": "source"},
    )
    assert sql == [
        "(COALESCE(subtype = ANY(%(w0)s), FALSE))",
        "(NOT COALESCE(source = %(w1)s, FALSE))",
    ]
    assert params == {"w0": ["ticket", "page"], "w1": "s"}


# ── the applied predicate, as text ────────────────────────────────────


def test_describe_quotes_only_values_that_are_not_one_plain_token() -> None:
    where = sw.normalize_where({"op": "and", "clauses": [
        {"op": "prefix", "field": "path", "value": "slack/eng"},
        {"op": "not", "clause": {"op": "eq", "field": "attrs.title", "value": "a b"}},
        {"op": "in", "field": "attrs.n", "values": [1, "and"]},
    ]})
    assert sw.describe(where) == (
        'path prefix slack/eng and not attrs.title = "a b" '
        'and attrs.n in [1, "and"]'
    )


DATE_META = {
    "title_from": "heading", "path_from": "member_path",
    "created_at_from": "created", "last_modified_at_from": "edited",
    "published_at_from": "published", "resolved_at_from": "resolved",
    "effective_at_from": "effective",
}


@pytest.mark.parametrize("meaning,attr", [("created", "created"), ("modified", "edited"),
                                        ("published", "published"), ("resolved", "resolved"),
                                        ("effective", "effective")])
def test_named_dates_compare_equal_instants_and_never_substitute(meaning, attr):
    context = sw.WhereContext({"policy": DATE_META}, {f"date.{meaning}"})
    predicate = sw.normalize_where({"op": "eq", "field": f"date.{meaning}",
                                    "value": "2026-10-03T08:00:00Z"})
    row = {"node_id": "p:1", "subtype": "policy", "attrs": {attr: "2026-10-03T10:00:00+02:00"}}
    assert sw.matches(row, predicate, context)
    row["attrs"] = {"other": "2026-10-03T08:00:00Z"}
    assert sw.evaluate(row, predicate, context) is None
    assert not sw.matches(row, {"op": "not", "clause": predicate}, context)


def test_typed_attrs_dates_do_not_reinterpret_literal_json_equality():
    row = {"node_id": "n:1", "attrs": {"payload": {"date": "2026-10-03"},
                                         "domain_date": "2026-10-03T10:00:00+02:00"}}
    literal = sw.normalize_where({"op": "eq", "field": "attrs.payload", "value": {"date": "2026-10-03"}})
    assert sw.matches(row, literal)
    typed = sw.normalize_where({"op": "in", "field": "date.attrs.domain_date",
                                "values": ["2026-10-03T08:00:00Z"]})
    assert sw.matches(row, typed)
    plan = sw.plan_where(None, typed, lift_source=False)
    assert not plan.lifted and "attr_filters" not in plan.filter
    assert sw.matches(row, sw.normalize_where({"op": "between", "field": "date.attrs.domain_date",
                                              "values": ["2026-10-03T07:00:00Z", "2026-10-03T09:00:00Z"]}))


@pytest.mark.parametrize("op", ["prefix", "suffix", "contains"])
def test_date_string_operations_refuse_before_database(op):
    with pytest.raises(sw.WhereUnsupported):
        sw.normalize_where({"op": op, "field": "date.effective", "value": "2026"})


@pytest.mark.parametrize("value", ["bad date", "2025-02-29", "2026-10-03T10:00:00+00:60", 123])
def test_bad_date_operand_refuses_but_bad_row_is_unknown(value):
    with pytest.raises(sw.WhereUnsupported) as error:
        sw.normalize_where({"op": "gt", "field": "date.attrs.at", "value": value})
    assert error.value.data["reason"] == "invalid_date_operand"
    good = sw.normalize_where({"op": "gt", "field": "date.attrs.at", "value": "2026-01-01"})
    context = sw.WhereContext({}, {"date.attrs.at"})
    row = {"node_id": "n:bad", "attrs": {"at": value}}
    assert sw.evaluate(row, good, context) is None
    assert context.disclosure(evaluation_scope="bounded_test")["where_unparseable"] == {"date.attrs.at": 1}


def test_a_date_with_no_time_zone_is_read_as_utc_and_the_reply_says_so():
    """Issue #3666: one rule on every read. A naive row value against an
    aware operand used to be unknown and counted as incompatible; it is now
    compared as UTC, and the disclosure notes it."""
    predicate = sw.normalize_where({"op": "eq", "field": "date.attrs.at", "value": "2026-10-03T00:00:00Z"})
    context = sw.WhereContext({}, {"date.attrs.at"})
    naive_row = {"node_id": "n:naive", "attrs": {"at": "2026-10-03"}}
    assert sw.evaluate(naive_row, predicate, context) is True
    assert sw.evaluate(naive_row, {"op": "not", "clause": predicate}, context) is False
    disclosure = context.disclosure(evaluation_scope="bounded_test")
    assert disclosure["dates_read_as_utc"] == {"stored": ["date.attrs.at"]}
    assert "where_incompatible" not in disclosure
    # An aware row against an aware operand reads nothing as UTC.
    aware = sw.WhereContext({}, {"date.attrs.at"})
    assert sw.evaluate({"attrs": {"at": "2026-10-03T02:00:00+02:00"}}, predicate, aware) is True
    assert "dates_read_as_utc" not in aware.disclosure(evaluation_scope="bounded_test")
    # Operands of both kinds are accepted, and a naive one is noted.
    mixed = sw.normalize_where({"op": "between", "field": "date.attrs.at",
                                "values": ["2026-10-03", "2026-10-03T01:00:00Z"]})
    context = sw.WhereContext({}, {"date.attrs.at"})
    assert sw.evaluate({"attrs": {"at": "2026-10-03T00:30:00"}}, mixed, context) is True
    assert sw.evaluate({"attrs": {"at": "2026-10-03T01:00:01Z"}}, mixed, context) is False
    assert context.disclosure(evaluation_scope="bounded_test")["dates_read_as_utc"] == {
        "bounds": ["date.attrs.at"], "stored": ["date.attrs.at"],
    }


def test_mixed_sources_are_all_compared_as_instants():
    """One source writes aware instants, another naive wall times, a third
    days in a zone: every one is compared, none is set aside."""
    predicate = sw.normalize_where({"op": "gt", "field": "date.attrs.at",
                                    "value": "2026-10-01T00:00:00Z"})
    context = sw.WhereContext({}, {"date.attrs.at"})
    rows = [
        {"node_id": "aware:new", "attrs": {"at": "2026-10-03T08:00:00+02:00"}},
        {"node_id": "aware:old", "attrs": {"at": "2026-09-01T08:00:00Z"}},
        {"node_id": "naive:new", "attrs": {"at": "2026-10-03T08:00:00"}},
        {"node_id": "naive:old", "attrs": {"at": "2026-09-30T23:59:59"}},
        {"node_id": "day:zoned", "attrs": {"at": "2026-10-03-07:00"}},
        {"node_id": "missing", "attrs": {}},
    ]
    kept = [row["node_id"] for row in rows if sw.matches(row, predicate, context)]
    assert kept == ["aware:new", "naive:new", "day:zoned"]
    disclosure = context.disclosure(evaluation_scope="bounded_test")
    assert disclosure["dates_read_as_utc"] == {"stored": ["date.attrs.at"]}
    assert disclosure["where_missing"] == {"date.attrs.at": 1}


def test_date_exists_distinguishes_valid_missing_and_masked():
    predicate = sw.normalize_where({"op": "exists", "field": "date.attrs.at"})
    assert sw.evaluate({"attrs": {"at": "2026-10-03"}}, predicate) is True
    assert sw.evaluate({"attrs": {"at": "bad"}}, predicate) is False
    assert sw.evaluate({"attrs": {}}, predicate) is False
    assert sw.evaluate({"attrs": {"at": MASK_TOKEN}}, predicate) is None
    assert sw.evaluate({"attrs": {REDACTION_MARKER: True}}, {"op": "not", "clause": predicate}) is None


def test_catalog_title_and_path_resolve_without_changing_raw_attrs_path():
    row = {"node_id": "p:1", "subtype": "policy", "attrs": {"heading": "Policy", "member_path": "cms/policy", "path": "old/path"}}
    context = sw.WhereContext({"policy": DATE_META}, {"title", "path"})
    assert sw.matches(row, sw.normalize_where({"op": "eq", "field": "path", "value": "cms/policy"}), context)
    assert sw.matches(row, sw.normalize_where({"op": "eq", "field": "title", "value": "Policy"}), context)
    assert sw.matches(row, sw.normalize_where({"op": "eq", "field": "attrs.path", "value": "old/path"}), context)


def test_published_predicate_shape_accepts_new_date_namespace():
    import jsonschema

    for field in ("date.effective", "date.attrs.valid_until", "title", "path"):
        call = {"op": "eq", "field": field, "value": "2026-10-03"}
        jsonschema.validate(call, Predicate.model_json_schema())
        assert Predicate.model_validate(call).field == field


def test_title_predicates_read_full_first_line_beyond_display_limit():
    title = "a" * 150 + " important suffix"
    context = sw.WhereContext({"policy": DATE_META}, {"title"})
    row = {"subtype": "policy", "attrs": {"heading": title + "\nbody-only text"}}
    assert sw.matches(row, sw.normalize_where({"op": "eq", "field": "title", "value": title}), context)
    assert sw.matches(row, sw.normalize_where({"op": "suffix", "field": "title", "value": "important suffix"}), context)
    assert not sw.matches(row, sw.normalize_where({"op": "suffix", "field": "title", "value": "body-only text"}), context)


# -- named-date precision (PACT read-model-s1-catalog-fields resolver) ------


def test_date_operands_keep_their_day_form():
    def value(raw):
        return sw.normalize_where({"op": "eq", "field": "date.attrs.at", "value": raw})["value"]

    assert value("2026-10-03") == "2026-10-03"
    assert value("20261003") == "2026-10-03"
    assert value("2026-10-03-07:00") == "2026-10-03-07:00"
    assert value("2026-10-03Z") == "2026-10-03+00:00"
    assert value("2026-10-03T08:00:00Z") == "2026-10-03T08:00:00+00:00"
    assert value("Sat, 3 Oct 2026 08:00:00 -0700") == "2026-10-03T08:00:00-07:00"


def test_a_day_operand_means_its_start_and_eq_means_on_that_day():
    """Issue #3666. `lt` and `gt` read a day as its first instant; `eq` with
    a day is "on that day" (it used to match only that midnight); as the
    upper bound of `between` a day is its last instant."""
    before = sw.normalize_where({"op": "lt", "field": "date.attrs.at", "value": "2026-10-03"})
    after = sw.normalize_where({"op": "gt", "field": "date.attrs.at", "value": "2026-10-03"})
    on_day = sw.normalize_where({"op": "eq", "field": "date.attrs.at", "value": "2026-10-03"})
    assert sw.evaluate({"attrs": {"at": "2026-10-02T23:59:00"}}, before) is True
    assert sw.evaluate({"attrs": {"at": "2026-10-03T00:30:00"}}, before) is False
    assert sw.evaluate({"attrs": {"at": "2026-10-03T00:00:00"}}, after) is False
    assert sw.evaluate({"attrs": {"at": "2026-10-03T00:00:01"}}, after) is True
    assert sw.evaluate({"attrs": {"at": "2026-10-03T00:00:00"}}, on_day) is True
    assert sw.evaluate({"attrs": {"at": "2026-10-03T15:00:00"}}, on_day) is True
    assert sw.evaluate({"attrs": {"at": "2026-10-03"}}, on_day) is True
    assert sw.evaluate({"attrs": {"at": "2026-10-04T00:00:00"}}, on_day) is False
    year = sw.normalize_where({"op": "between", "field": "date.attrs.at",
                               "values": ["2025-01-01", "2025-12-31"]})
    assert sw.evaluate({"attrs": {"at": "2025-01-01T00:00:00Z"}}, year) is True
    assert sw.evaluate({"attrs": {"at": "2025-12-31T23:59:59Z"}}, year) is True
    assert sw.evaluate({"attrs": {"at": "2025-12-31T23:30:00-05:00"}}, year) is False
    assert sw.evaluate({"attrs": {"at": "2024-12-31T23:59:59Z"}}, year) is False
    with pytest.raises(sw.WhereUnsupported) as error:
        sw.normalize_where({"op": "between", "field": "date.attrs.at",
                            "values": ["2025-12-31T00:00:01Z", "2025-12-30"]})
    assert error.value.data["reason"] == "date_bounds_reversed"
    # The last day as the upper bound still holds its own morning.
    assert sw.normalize_where({"op": "between", "field": "date.attrs.at",
                               "values": ["2025-12-31T00:00:01Z", "2025-12-31"]})


def test_a_zoned_day_compares_as_its_first_instant():
    # A day in a timezone (XML Schema date form) is the first instant of
    # that day in that zone. It used to be incomparable with an instant.
    instant = sw.normalize_where({"op": "gt", "field": "date.attrs.at",
                                  "value": "2026-10-03T06:00:00Z"})
    assert sw.evaluate({"node_id": "a", "attrs": {"at": "2026-10-03-07:00"}}, instant) is True
    assert sw.evaluate({"node_id": "a", "attrs": {"at": "2026-10-03+02:00"}}, instant) is False
    zoned_day = sw.normalize_where({"op": "eq", "field": "date.attrs.at",
                                    "value": "2026-10-03-07:00"})
    # On that day in that zone: 2026-10-03T07:00Z up to 2026-10-04T07:00Z.
    assert sw.evaluate({"attrs": {"at": "2026-10-03T07:00:00Z"}}, zoned_day) is True
    assert sw.evaluate({"attrs": {"at": "2026-10-03T06:59:59Z"}}, zoned_day) is False
    assert sw.evaluate({"attrs": {"at": "2026-10-03-07:00"}}, zoned_day) is True
    assert sw.evaluate({"attrs": {"at": "2026-10-04-07:00"}}, zoned_day) is False
    assert sw.evaluate({"attrs": {"at": "2026-10-03+02:00"}}, zoned_day) is False


def test_named_meaning_zoned_day_is_its_first_instant():
    context = sw.WhereContext({"policy": DATE_META}, {"date.effective"})
    predicate = sw.normalize_where({"op": "lt", "field": "date.effective",
                                    "value": "2026-10-04T00:00:00Z"})
    row = {"node_id": "p:1", "subtype": "policy", "attrs": {"effective": "2026-10-03Z"}}
    assert sw.evaluate(row, predicate, context) is True
    assert "dates_read_as_utc" not in context.disclosure(evaluation_scope="bounded_test")


@pytest.mark.parametrize(("values", "matching"), [
    # A day in -07:00 (07:00Z on the 3rd up to 07:00Z on the 4th), or 07:00Z.
    (["2026-10-03-07:00", "2026-10-03T07:00:00Z"], ["at-0700z", "noon-z", "late-0700"]),
    # That day, or the day of the 4th in +02:00 (22:00Z on the 3rd onward).
    (["2026-10-03-07:00", "2026-10-04+02:00"], ["at-0700z", "noon-z", "late-0700", "next-noon-z"]),
    # The UTC day of the 3rd, or 07:00Z on the 3rd.
    (["2026-10-03", "2026-10-03T07:00:00Z"], ["early-z", "at-0700z", "noon-z"]),
])
def test_operands_of_different_kinds_are_accepted(values, matching):
    """They used to refuse (`incompatible_date_precision`, or a naive value
    beside an aware one); each is now its own instant or day."""
    predicate = sw.normalize_where({"op": "in", "field": "date.attrs.at", "values": values})
    rows = [
        {"node_id": "early-z", "attrs": {"at": "2026-10-03T01:00:00Z"}},
        {"node_id": "at-0700z", "attrs": {"at": "2026-10-03T07:00:00Z"}},
        {"node_id": "noon-z", "attrs": {"at": "2026-10-03T12:00:00Z"}},
        {"node_id": "late-0700", "attrs": {"at": "2026-10-03T23:30:00-07:00"}},
        {"node_id": "next-noon-z", "attrs": {"at": "2026-10-04T12:00:00Z"}},
        {"node_id": "before", "attrs": {"at": "2026-10-02T23:59:59Z"}},
    ]
    assert [row["node_id"] for row in rows if sw.matches(row, predicate)] == matching


# ── both attribute-equality spellings run the same code (D9) ──────────


def test_an_attribute_equality_lifts_under_every_mode_like_attr_filters() -> None:
    where = sw.normalize_where({"op": "and", "clauses": [
        EQ_TICKET, {"op": "eq", "field": "attrs.status", "value": "open"},
    ]})
    plan = sw.plan_where(None, where, lift_source=True)
    legacy = sw.plan_where(
        {"subtype": "ticket", "attr_filters": {"status": "open"}}, None,
        lift_source=True,
    )
    # No mode argument exists: `where` and `attr_filters` reach one filter.
    assert plan.filter == legacy.filter == {
        "subtype": "ticket", "attr_filters": {"status": "open"},
    }
    assert plan.residual is None
    assert sw.describe(plan.applied) == sw.describe(legacy.applied)
    assert not hasattr(sw, "has_attr_equality")


def test_masking_possible_reads_the_nomos_runtime_mode(monkeypatch) -> None:
    monkeypatch.setenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", "off")
    assert sw.masking_possible("no-such-deployment") is False
    monkeypatch.setenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", "audit")
    assert sw.masking_possible("no-such-deployment") is True
    monkeypatch.setenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", "enforce")
    assert sw.masking_possible("no-such-deployment") is True


def test_parse_date_is_the_resolver_parser() -> None:
    from okg.substrate.mcp.handle_fields import _parse_date

    for raw in ("2026-10-03", "2026-10-03-07:00", "Sat, 3 Oct 2026 08:00:00 -0700", "bad", 7):
        assert sw.parse_date(raw) == _parse_date(raw)


# ── the query-less lookup's SQL page ──────────────────────────────────


_LOOKUP_META = {
    "source_file": {"path_from": "path", "title_from": "path"},
    "transcript": {"path_from": "transcript_path"},
    "note": {},
}


def _lookup(where: dict, *, masking: bool = False, explicit: bool = False,
            metadata: dict = _LOOKUP_META) -> list[dict]:
    predicate = sw.normalize_where(where)
    context = sw.WhereContext(metadata, {"title", "path", "date.created", "date.attrs.until"})
    return sw.lookup_sql_clauses(predicate, context, masking=masking, explicit_subtypes=explicit)


def test_lookup_pushes_exact_id_and_type_clauses() -> None:
    where = {"op": "and", "clauses": [
        {"op": "in", "field": "type", "values": ["source_file", "note"]},
        {"op": "prefix", "field": "id", "value": "file:repo:"},
        {"op": "lt", "field": "id", "value": "file:z"},
        {"op": "eq", "field": "source", "value": "repo-src"},
    ]}
    clauses = sw.lookup_sql_clauses(sw.normalize_where(where), None, masking=False,
                                    explicit_subtypes=False)
    # Range ops stay out (collation order); source has no column there.
    assert clauses == [
        {"op": "in", "field": "subtype", "values": ["source_file", "note"]},
        {"op": "prefix", "field": "node_id", "value": "file:repo:"},
    ]
    masked = sw.lookup_sql_clauses(sw.normalize_where(where), None, masking=True,
                                   explicit_subtypes=False)
    # Under a bundle an id clause would show a hidden id through `truncated`.
    assert masked == [{"op": "in", "field": "subtype", "values": ["source_file", "note"]}]


def test_lookup_path_superset_groups_declared_attributes() -> None:
    clauses = _lookup({"op": "eq", "field": "path", "value": "src/app.py"})
    contains = sw.RAW_TEXT_CONTAINS
    assert clauses == [{"op": "or", "clauses": [
        {"op": "and", "clauses": [
            {"op": "in", "field": "subtype", "values": ["source_file"]},
            {"op": contains, "field": "attrs.path", "value": "src/app.py"},
        ]},
        {"op": "and", "clauses": [
            {"op": "in", "field": "subtype", "values": ["transcript"]},
            {"op": contains, "field": "attrs.transcript_path", "value": "src/app.py"},
        ]},
    ]}]
    # One declaring attribute in scope: no subtype grouping needed.
    scoped = _lookup({"op": "and", "clauses": [
        {"op": "eq", "field": "type", "value": "source_file"},
        {"op": "prefix", "field": "path", "value": "src/"},
    ]})
    assert scoped[-1] == {"op": contains, "field": "attrs.path", "value": "src/"}


def test_lookup_supersets_admit_every_row_the_residual_admits() -> None:
    """Padded and multi-line stored values: a stripped or first-line match
    is never cut by the SQL superset."""
    rows = [
        {"subtype": "source_file", "attrs": {"path": "  src/app.py\n"}},
        {"subtype": "source_file", "attrs": {"path": "src/app.py"}},
        {"subtype": "transcript", "attrs": {"transcript_path": "src/app.py"}},
    ]
    context = sw.WhereContext(_LOOKUP_META, {"path"})
    for where in (
        {"op": "eq", "field": "path", "value": "src/app.py"},
        {"op": "suffix", "field": "path", "value": "app.py"},
        {"op": "in", "field": "path", "values": ["src/app.py", "x"]},
    ):
        predicate = sw.normalize_where(where)
        assert all(sw.matches(row, predicate, context) for row in rows), where
        (superset,) = sw.lookup_sql_clauses(predicate, context, masking=False,
                                            explicit_subtypes=False)
        for row in rows:
            assert _superset_admits(superset, row), (where, row)


def _superset_admits(clause: dict, row: dict) -> bool:
    """A Python reading of the lookup superset clauses, for the test above."""
    op = clause["op"]
    if op == "and":
        return all(_superset_admits(c, row) for c in clause["clauses"])
    if op == "or":
        return any(_superset_admits(c, row) for c in clause["clauses"])
    if op == "in":
        return row["subtype"] in clause["values"]
    raw = row["attrs"].get(clause["field"].split(".", 1)[1])
    if op == "exists":
        return raw is not None
    assert op == sw.RAW_TEXT_CONTAINS
    return isinstance(raw, str) and clause["value"] in raw


def test_lookup_title_needs_every_scoped_type_to_declare_it() -> None:
    title = {"op": "eq", "field": "title", "value": "src/app.py"}
    # Unscoped: `note` falls back to the fixed title list, so no superset.
    assert _lookup(title) == []
    scoped = _lookup({"op": "and", "clauses": [
        {"op": "eq", "field": "type", "value": "source_file"}, title,
    ]})
    assert scoped[-1] == {"op": sw.RAW_TEXT_CONTAINS, "field": "attrs.path",
                          "value": "src/app.py"}


def test_lookup_date_supersets_need_the_stored_key() -> None:
    meta = {"policy": {"created_at_from": "created"}, "note": {}}
    created = _lookup({"op": "gt", "field": "date.created", "value": "2026-01-01T00:00:00Z"},
                      metadata=meta)
    assert created == [{"op": "exists", "field": "attrs.created"}]
    until = _lookup({"op": "lt", "field": "date.attrs.until", "value": "2026-01-01"},
                    metadata=meta)
    assert until == [{"op": "exists", "field": "attrs.until"}]


def test_lookup_keeps_attribute_clauses_out_of_sql_when_masking_is_possible() -> None:
    meta = {"source_file": {"path_from": "path", "created_at_from": "c"}}
    for where in (
        {"op": "eq", "field": "path", "value": "src/app.py"},
        {"op": "gt", "field": "date.created", "value": "2026-01-01T00:00:00Z"},
    ):
        assert _lookup(where, masking=True, metadata=meta) == []


@pytest.mark.parametrize("where", [
    {"op": "eq", "field": "path", "value": None},
    {"op": "in", "field": "path", "values": ["a", None]},
    {"op": "not", "clause": {"op": "eq", "field": "path", "value": "a"}},
    {"op": "or", "clauses": [{"op": "eq", "field": "path", "value": "a"},
                             {"op": "eq", "field": "type", "value": "note"}]},
])
def test_lookup_never_narrows_a_clause_that_admits_missing_values(where) -> None:
    clauses = _lookup(where)
    assert not any("attrs." in repr(c) for c in clauses), clauses


def test_lookup_date_under_two_attributes_groups_by_type() -> None:
    """One named date stored under different attributes on different types:
    each attribute is required only on the types that declare it."""
    meta = {"policy": {"created_at_from": "created"},
            "ledger": {"created_at_from": "booked"}, "note": {}}
    clauses = _lookup({"op": "gt", "field": "date.created", "value": "2026-01-01T00:00:00Z"},
                      metadata=meta)
    assert clauses == [{"op": "or", "clauses": [
        {"op": "and", "clauses": [
            {"op": "in", "field": "subtype", "values": ["ledger"]},
            {"op": "exists", "field": "attrs.booked"},
        ]},
        {"op": "and", "clauses": [
            {"op": "in", "field": "subtype", "values": ["policy"]},
            {"op": "exists", "field": "attrs.created"},
        ]},
    ]}]
    for row in ({"subtype": "policy", "attrs": {"created": "x"}},
                {"subtype": "ledger", "attrs": {"booked": "x"}}):
        assert _superset_admits(clauses[0], row), row
    # A row carrying only the OTHER type's attribute is not admitted.
    assert not _superset_admits(clauses[0], {"subtype": "policy", "attrs": {"booked": "x"}})


def test_lookup_title_with_a_type_list_where_one_type_lacks_it() -> None:
    """`type in [a, b]` where only `a` declares a title: `b` falls back to
    the fixed title list, so the title stays residual and only the type list
    narrows."""
    where = {"op": "and", "clauses": [
        {"op": "in", "field": "type", "values": ["source_file", "note"]},
        {"op": "eq", "field": "title", "value": "src/app.py"},
    ]}
    assert _lookup(where) == [
        {"op": "in", "field": "subtype", "values": ["source_file", "note"]},
    ]
    declared = {"op": "and", "clauses": [
        {"op": "in", "field": "type", "values": ["source_file"]},
        {"op": "eq", "field": "title", "value": "src/app.py"},
    ]}
    assert _lookup(declared)[-1] == {"op": sw.RAW_TEXT_CONTAINS, "field": "attrs.path",
                                     "value": "src/app.py"}


# ── the lookup's `where` path prefix through the path-prefix index (#3194) ──

PATH_META = {"doc": {"path_from": "path"}, "memo": {"path_from": "memo_path"},
             "note": {}}


def _prefixes(where: dict, *, masking: bool, metadata=PATH_META) -> list[str]:
    context = sw.WhereContext(metadata, {"path"})
    return sw.lookup_path_prefixes(sw.normalize_where(where), context, masking=masking)


def test_a_path_prefix_reaches_the_index_with_nomos_off() -> None:
    doc = {"op": "eq", "field": "type", "value": "doc"}
    prefix = {"op": "prefix", "field": "path", "value": "src/a_b%"}
    # `memo` reads its path from another attribute, which no index covers;
    # `note` declares none and reads no path.
    assert _prefixes({"op": "and", "clauses": [doc, prefix]}, masking=False) == ["src/a_b%"]
    assert _prefixes({"op": "and", "clauses": [
        {"op": "in", "field": "type", "values": ["doc", "note"]}, prefix,
    ]}, masking=False) == ["src/a_b%"]
    assert _prefixes(prefix, masking=False) == []
    assert _prefixes(prefix, masking=False,
                     metadata={"doc": PATH_META["doc"], "note": {}}) == ["src/a_b%"]
    # The stored attribute itself, on any type.
    raw = {"op": "prefix", "field": "attrs.path", "value": "src/"}
    assert _prefixes(raw, masking=False) == ["src/"]
    # The lookup loads no catalog context for a predicate naming only attrs.
    assert sw.lookup_path_prefixes(sw.normalize_where(raw), None, masking=False) == ["src/"]
    assert sw.lookup_path_prefixes(sw.normalize_where(prefix), None, masking=False) == []
    assert sw.lookup_path_prefixes(sw.normalize_where(raw), None, masking=True) == []


@pytest.mark.parametrize("where", [
    {"op": "prefix", "field": "path", "value": "src/"},
    {"op": "prefix", "field": "attrs.path", "value": "src/"},
])
def test_under_audit_or_enforce_no_path_prefix_reaches_sql(where) -> None:
    """Failure case: a prefix tested before the read filter would show a
    hidden path in the page facts (#3208), so with Nomos on nothing is
    pushed and the lookup's SQL clauses are what they were."""
    metadata = {"doc": PATH_META["doc"]}
    assert _prefixes(where, masking=True, metadata=metadata) == []
    context = sw.WhereContext(metadata, {"path"})
    assert sw.lookup_sql_clauses(sw.normalize_where(where), context, masking=True,
                                 explicit_subtypes=False) == []


@pytest.mark.parametrize("where", [
    {"op": "or", "clauses": [{"op": "prefix", "field": "path", "value": "a"},
                             {"op": "eq", "field": "type", "value": "doc"}]},
    {"op": "not", "clause": {"op": "prefix", "field": "path", "value": "a"}},
    {"op": "suffix", "field": "path", "value": "a"},
])
def test_only_a_top_level_prefix_narrows(where) -> None:
    assert _prefixes(where, masking=False, metadata={"doc": PATH_META["doc"]}) == []


@pytest.mark.parametrize("overlay_keys, expected", [
    (["path"], True),
    (["owner", "path"], False),
    (["path", "status"], False),
    ([], False),
])
def test_only_a_path_only_set_is_looked_up_by_id(overlay_keys, expected) -> None:
    """The page looks candidates up one by one (`= ANY(ARRAY(...))`) only
    when the set is bounded: the path-prefix set plus overlay targets on
    `path`. Failure case: a predicate that also reads `attrs.owner` adds
    every target with an `owner` overlay, which no bound limits, so it keeps
    the `IN` form (which still narrows by the path set)."""
    assert sw.path_candidates_as_array(True, overlay_keys) is expected
    assert sw.path_candidates_as_array(False, ["path"]) is False


def test_grep_path_prefix_sql_is_unchanged() -> None:
    params: dict = {}
    clauses = gs._path_sql(gs.parse_grep_query("x"), "a_%\\b", params, alias="n")
    assert clauses[-1] == (
        f"left((n.attrs->>'path'), {GREP_PATH_INDEX_CHARS}) LIKE %(grep_path_b)s "
        f"AND (n.attrs->>'path') LIKE %(grep_path)s"
    )
    assert params == {"grep_path": "a\\_\\%\\\\b%", "grep_path_b": "a\\_\\%\\\\b%"}


def test_candidate_sql_repeats_the_index_and_escapes_like() -> None:
    params: dict = {}
    sql = gs.path_prefix_candidates_sql("a_b%c", params, segment="public.nodes",
                                        bp_ref="%(bp)s", key="pp0")
    arms = sql.split(" UNION ")
    assert len(arms) == 10  # the prefix and nine leading-space ranges
    for arm in arms:
        assert "g.deleted = FALSE AND g.attrs ? 'path'" in arm  # the index's predicate
        assert "g.live_lo <= %(bp)s AND %(bp)s < g.live_hi" in arm
    assert (f"left((g.attrs->>'path'), {GREP_PATH_INDEX_CHARS}) LIKE %(pp0_b)s "
            f"AND (g.attrs->>'path') LIKE %(pp0)s") in arms[0]
    assert params["pp0"] == params["pp0_b"] == "a\\_b\\%c%"
    bounded = f"left((g.attrs->>'path'), {GREP_PATH_INDEX_CHARS})"
    assert f"{bounded} ~<~ %(pp0_sp)s AND {bounded} <> ''" in arms[1]


def test_the_probe_counts_at_most_one_past_the_bound_and_stops_early() -> None:
    params: dict = {}
    sql = gs.path_prefix_probe_sql("src/", params, segment="t", bp_ref="%(bp)s",
                                   key="pq", bound=7)
    assert sql.startswith("SELECT count(*) AS n FROM (")
    assert sql.endswith("LIMIT %(pq_limit)s) probe")
    assert params["pq_limit"] == 8
    # UNION ALL streams into the LIMIT; UNION would read every arm first.
    assert sql.count(" UNION ALL ") == 9 and " UNION (" not in sql
    assert gs.PATH_PREFIX_CANDIDATE_BOUND == 10_000


@pytest.mark.parametrize("value", ["src\x00/", "src/\ud800", "\udfff"])
@pytest.mark.parametrize("op", ["prefix", "suffix"])
def test_a_prefix_with_nul_or_a_lone_surrogate_is_refused_by_name(op, value) -> None:
    with pytest.raises(sw.WhereUnsupported, match="NUL character or a lone surrogate"):
        sw.normalize_where({"op": op, "field": "path", "value": value})


def test_a_prefix_longer_than_the_index_bound_is_rechecked_in_full() -> None:
    prefix = "d/" * GREP_PATH_INDEX_CHARS + "_tail"
    params: dict = {}
    gs.path_prefix_candidates_sql(prefix, params, segment="t", bp_ref="%(bp)s", key="pp0")
    assert params["pp0_b"] == prefix[:GREP_PATH_INDEX_CHARS] + "%"
    assert params["pp0"] == prefix.replace("_", "\\_") + "%"


def test_strip_space_ranges_are_pythons_whitespace() -> None:
    covered = {c for low, high in gs.STRIP_SPACE_RANGES for c in range(low, high + 1)}
    assert covered == {c for c in range(0x110000) if chr(c).isspace()}
    assert all(not chr(c).strip() for c in covered)


def _candidate_admits(stored: str, prefix: str, params: dict) -> bool:
    """The candidate SQL's arms evaluated in Python: LIKE without wildcards
    in the operand is `startswith`; text_pattern_ops compares UTF-8 bytes."""
    bounded = stored[:GREP_PATH_INDEX_CHARS]
    if bounded.startswith(prefix[:GREP_PATH_INDEX_CHARS]) and stored.startswith(prefix):
        return True
    raw = bounded.encode("utf-8", "surrogatepass")
    if bounded and raw < params["pp0_sp"].encode():
        return True
    index = 0
    while f"pp0_s{index}_lo" in params:
        low, high = params[f"pp0_s{index}_lo"].encode(), params[f"pp0_s{index}_hi"].encode()
        if low <= raw < high:
            return True
        index += 1
    return False


@pytest.mark.parametrize("prefix", ["src/", "src/a", "a_b", "50%", "x\\y", "é", "s"])
def test_candidates_admit_every_path_the_reader_would_match(prefix) -> None:
    """The candidate set is a superset of the read rule (`handle_fields`
    strips the stored path), so the page keeps every row it held before."""
    stored = ["src/a.py", "  src/a.py", "\tsrc/b", " src/c", "　src/d",
              " src/e", "a_b/c", "axb/c", "50%/x", "50x/x", "x\\y/z", "é/1",
              "e/1", "src", "", "   ", "s" * 600, " " + "s" * 600, "​src/zero-width"]
    params: dict = {}
    gs.path_prefix_candidates_sql(prefix, params, segment="t", bp_ref="%(bp)s", key="pp0")
    for value in stored:
        read = value.strip() or None
        if read is not None and read.startswith(prefix):
            assert _candidate_admits(value, prefix, params), value
