"""`aggregate` counts per day, week, month or year of a date (issue #3658).

(a) Pure: the `<period>(<date field>)` grammar and the fold's calendar rules.
(b) Database: the served `aggregate` over a published generation, named dates
across record types, `date.attrs.<key>`, refusals, a result reference, and
the text a model reads.
"""
from __future__ import annotations

import threading
import uuid
from typing import Any

import pytest

from okg.substrate.mcp.aggregate_periods import (
    DATE_MEANINGS,
    PERIODS,
    PeriodReport,
    fold_period_groups,
    parse_period_field,
    split_date_filter,
)
from okg.substrate.mcp.response_bounds import InvalidOperatorArguments
# Autouse here too: the date limits out of reach of a slow runner.
from tests.substrate.mcp.test_date_conditions import (  # noqa: F401
    date_budget_for_tests,
)

# --------------------------------------------------------------------------- #
# (a) pure                                                                     #
# --------------------------------------------------------------------------- #


@pytest.mark.parametrize("period", PERIODS)
def test_period_field_parses_a_named_date_and_an_attribute(period: str) -> None:
    named = parse_period_field(f"{period}(date.created)")
    assert named is not None
    assert (named.period, named.meaning, named.attr) == (period, "created", None)
    attr = parse_period_field(f"{period}(date.attrs.opened_on)")
    assert attr is not None
    assert (attr.period, attr.meaning, attr.attr) == (period, None, "opened_on")


@pytest.mark.parametrize("plain", [
    "attrs.created", "subtype", "attrs.a.b", "date.created",
    # An attribute key may hold parentheses; it still groups by exact value.
    "attrs.Size (MB)", "attrs.count(*)", "attrs.month(date.created)",
])
def test_a_plain_field_is_not_a_period(plain: str) -> None:
    assert parse_period_field(plain) is None


@pytest.mark.parametrize(("text", "needle", "example"), [
    ("quarter(date.created)", "'quarter' is not a period", "month(date.created)"),
    ("month(created)", "'created' is not a date field", "month(date.created)"),
    ("month(date.bogus)", "'date.bogus' is not a date field", "month(date.created)"),
    ("month(attrs.created)", "date.attrs.<key>", "month(date.attrs.created)"),
    ("month(date.attrs.)", "is not a date field", "month(date.created)"),
    ("month()", "is not a date field", "month(date.created)"),
    ("month(date.created", "not one call with one argument", "month(date.created)"),
    ("month(day(date.created))", "not one call with one argument", "month(date.created)"),
    # Strict spelling: no case folding, no inner spaces.
    ("Month(date.created)", "'Month' is not a period", "month(date.created)"),
    ("WEEK(date.attrs.due)", "'WEEK' is not a period", "week(date.attrs.due)"),
    ("month( date.created )", "is not a date field", "month(date.created)"),
    (" month(date.created)", "not one call with one argument", "month(date.created)"),
])
def test_a_malformed_period_field_is_refused_with_the_form_and_an_example(
    text: str, needle: str, example: str,
) -> None:
    with pytest.raises(InvalidOperatorArguments) as excinfo:
        parse_period_field(text)
    err = excinfo.value
    assert needle in str(err)
    assert err.code == "invalid_operator_arguments"
    assert "day, week, month, year" in (err.fix_hint or "")
    assert f"by=[{example!r}]" in (err.fix_hint or "")


def _fold(rows, by=("month(date.attrs.d)",)):
    periods = {text: parse_period_field(text) for text in by if "(" in text}
    report = PeriodReport()
    groups = fold_period_groups(rows, by=list(by), periods=periods, report=report)
    return groups, report


def test_instants_are_counted_in_the_utc_calendar() -> None:
    # 23:30 on 31 December at UTC-5 is 1 January in UTC; 00:30 on 1 October
    # at UTC+2 is 30 September in UTC.
    groups, _ = _fold([
        ("2026-12-31T23:30:00-05:00", True, 2),
        ("2026-10-01T00:30:00+02:00", True, 1),
        ("2026-10-05T09:00:00Z", True, 4),
    ])
    assert groups == [(("2026-09-01",), 1), (("2026-10-01",), 4), (("2027-01-01",), 2)]


def test_dates_without_a_zone_are_read_as_utc_and_the_report_says_so() -> None:
    groups, report = _fold([
        ("2026-12-31T23:30:00", True, 2),
        ("2026-12-31", True, 1),
        ("2027-01-01", True, 5),
    ], by=("day(date.attrs.d)",))
    assert groups == [(("2026-12-31",), 3), (("2027-01-01",), 5)]
    assert report.utc_stored == ["date.attrs.d"]
    # Values that all carry a zone are not noted.
    assert _fold([("2026-10-05T09:00:00Z", True, 4)])[1].utc_stored == []


def test_weeks_start_on_monday_and_keys_are_the_first_day_oldest_first() -> None:
    # 2026-10-05 is a Monday, 2026-10-11 the Sunday of that week.
    rows = [("2026-10-12", True, 1), ("2026-10-11", True, 1), ("2026-10-05", True, 1),
            ("2025-03-09", True, 1)]
    assert _fold(rows, by=("week(date.attrs.d)",))[0] == [
        (("2025-03-03",), 1), (("2026-10-05",), 2), (("2026-10-12",), 1),
    ]
    assert _fold(rows, by=("year(date.attrs.d)",))[0] == [
        (("2025-01-01",), 1), (("2026-01-01",), 3),
    ]


def test_records_with_no_date_form_one_null_group_listed_last() -> None:
    groups, report = _fold([
        (None, False, 3), ("", True, 2), ("   ", True, 1), ("2026-02-03", True, 4),
    ])
    assert groups == [(("2026-02-01",), 4), ((None,), 6)]
    assert report.unparseable == {}


def test_a_mix_of_zoned_instants_and_plain_dates_is_counted_in_one_calendar() -> None:
    """Issue #3666: refused before (`incompatible_date_timezones`). A date
    with no zone is read as UTC, so both kinds share the UTC calendar."""
    groups, report = _fold([
        ("2026-10-05T09:00:00Z", True, 7), ("2026-10-06", True, 2),
        ("2026-12-31T23:30:00-05:00", True, 1), ("2026-12-31T23:30:00", True, 3),
        # A day in a zone is its first instant: 2026-11-01+05:00 starts on
        # 31 October in UTC.
        ("2026-11-01+05:00", True, 1),
    ])
    assert groups == [(("2026-10-01",), 10), (("2026-12-01",), 3), (("2027-01-01",), 1)]
    assert report.utc_stored == ["date.attrs.d"]


def test_values_that_are_not_dates_are_counted_and_shown_not_grouped() -> None:
    groups, report = _fold([
        ("next spring", True, 2), ("2026-02-03", True, 4), ("42", False, 1),
    ])
    assert groups == [(("2026-02-01",), 4)]
    assert report.unparseable == {
        "month(date.attrs.d)": {"count": 3},
    }
    assert report.payload()["date_unparseable"] == report.unparseable


def test_no_value_being_a_date_is_refused_with_the_exact_value_grouping() -> None:
    with pytest.raises(InvalidOperatorArguments) as excinfo:
        _fold([("next spring", True, 2), ("soon", True, 1), (None, False, 9)])
    err = excinfo.value
    assert err.data["reason"] == "not_a_date"
    # The count, never the stored value: stored text is untrusted (#3666).
    assert err.data["unparseable_count"] == 3 and "example" not in err.data
    assert "next spring" not in str(err) + (err.fix_hint or "")
    assert "by=['attrs.d']" in (err.fix_hint or "")


def test_a_named_date_with_no_date_names_the_attributes_it_read() -> None:
    field = "month(date.created)"
    report = PeriodReport(date_fields={field: {"ticket": "opened", "bug": "filed"}})
    with pytest.raises(InvalidOperatorArguments) as excinfo:
        fold_period_groups(
            [("soon", True, 2)], by=[field],
            periods={field: parse_period_field(field)}, report=report,
        )
    hint = excinfo.value.fix_hint or ""
    assert "by=['attrs.filed'] or by=['attrs.opened']" in hint
    # The record's own text is nowhere in the refusal (#3666).
    assert "soon" not in str(excinfo.value) and "example" not in excinfo.value.data


def test_a_date_at_the_edge_of_the_calendar_is_counted_as_not_a_date() -> None:
    groups, report = _fold([
        ("0001-01-01T00:00:00+01:00", True, 2), ("9999-12-31T23:59:59-05:00", True, 1),
        ("2026-02-03T00:00:00Z", True, 4),
    ])
    assert groups == [(("2026-02-01",), 4)]
    assert report.unparseable["month(date.attrs.d)"]["count"] == 3


def test_a_masked_value_counts_as_no_value() -> None:
    from okg.substrate.nomos.read_masking import MASK_TOKEN

    groups, report = _fold([(MASK_TOKEN, True, 3), ("2026-02-03", True, 4)])
    assert groups == [(("2026-02-01",), 4), ((None,), 3)]
    assert report.unparseable == {}


def test_plain_keys_that_are_lists_or_look_alike_stay_separate_groups() -> None:
    by = ("attrs.tags", "month(date.attrs.d)")
    groups, _ = _fold([
        (["a", "b"], "2026-02-03", True, 1), (["a", "b"], "2026-02-09", True, 2),
        ({"k": 1}, "2026-02-03", True, 1), (True, "2026-02-03", True, 1),
        (1, "2026-02-03", True, 1), (1.0, "2026-02-03", True, 1),
    ], by=by)
    assert sorted(groups, key=repr) == sorted([
        ((["a", "b"], "2026-02-01"), 3), (({"k": 1}, "2026-02-01"), 1),
        ((True, "2026-02-01"), 1), ((1, "2026-02-01"), 1), ((1.0, "2026-02-01"), 1),
    ], key=repr)
    assert [type(key[0]) for key, _ in groups].count(bool) == 1


def test_with_two_period_fields_a_record_that_is_not_a_date_is_not_read_further() -> None:
    by = ("month(date.attrs.a)", "month(date.attrs.b)")
    # Row 1 is not a date in `a`; its zoned `b` must not make `b` look mixed
    # with the plain dates of the rows that are grouped.
    groups, report = _fold([
        ("garbage", True, "2026-03-01T00:00:00Z", True, 2),
        ("2026-01-05", True, "2026-03-09", True, 3),
        ("junk", True, "nope", True, 1),
    ], by=by)
    assert groups == [(("2026-01-01", "2026-03-01"), 3)]
    assert report.unparseable == {
        "month(date.attrs.a)": {"count": 3},
        "month(date.attrs.b)": {"count": 1},
    }


def test_a_period_folds_beside_a_plain_field() -> None:
    groups, _ = _fold([
        ("2026-02-03", True, "open", 4), ("2026-02-20", True, "open", 1),
        ("2026-02-21", True, "closed", 2), ("2026-01-09", True, "open", 3),
    ], by=("month(date.attrs.d)", "attrs.status"))
    assert groups == [
        (("2026-01-01", "open"), 3), (("2026-02-01", "closed"), 2), (("2026-02-01", "open"), 5),
    ]


# ---- a date range on the period's own date ---------------------------------

CREATED = {"month(date.created)": parse_period_field("month(date.created)")}
IN_2025 = {"op": "between", "field": "date.created", "values": ["2025-01-01", "2025-12-31"]}


def test_a_date_range_is_taken_out_of_the_filter_alone_or_inside_one_and() -> None:
    rest, ranges = split_date_filter(IN_2025, CREATED)
    assert rest is None
    assert [r.disclosure() for r in ranges["month(date.created)"]] == [
        {"op": "between", "values": ["2025-01-01", "2025-12-31"]}]
    status = {"op": "eq", "field": "attrs.status", "value": "open"}
    kind = {"op": "eq", "field": "attrs.kind", "value": "bug"}
    after = {"op": "gt", "field": "date.created", "value": "2025-06-30"}
    rest, ranges = split_date_filter({"op": "and", "clauses": [status, IN_2025, after]}, CREATED)
    assert rest == status and len(ranges["month(date.created)"]) == 2
    rest, _ = split_date_filter({"op": "and", "clauses": [status, IN_2025, kind]}, CREATED)
    assert rest == {"op": "and", "clauses": [status, kind]}
    # No date clause: the filter is returned as it came.
    assert split_date_filter(status, CREATED) == (status, {})
    assert split_date_filter(None, CREATED) == (None, {})
    # One range serves every period of that date.
    both = {t: parse_period_field(t) for t in ("year(date.created)", "month(date.created)")}
    assert set(split_date_filter(IN_2025, both)[1]) == set(both)


@pytest.mark.parametrize(("filter", "periods"), [
    # No period on that date in by, another date, another operator, or a
    # clause under or / not: refused before #3666, now left in the filter,
    # which compiles a date condition like any other clause.
    (IN_2025, {}),
    ({"op": "between", "field": "date.attrs.due", "values": ["2025-01-01", "2025-02-01"]}, CREATED),
    ({"op": "eq", "field": "date.created", "value": "2025-01-01"}, CREATED),
    ({"op": "or", "clauses": [IN_2025, {"op": "eq", "field": "attrs.a", "value": 1}]}, CREATED),
    ({"op": "not", "clause": IN_2025}, CREATED),
])
def test_any_other_date_condition_stays_in_the_filter(filter, periods) -> None:
    assert split_date_filter(filter, periods) == (filter, {})


def test_a_timestamp_bound_is_accepted_on_a_periods_own_date() -> None:
    clause = {"op": "between", "field": "date.created",
              "values": ["2025-01-01T00:00:00Z", "2025-12-31"]}
    rest, ranges = split_date_filter(clause, CREATED)
    assert rest is None
    (found,) = ranges["month(date.created)"]
    assert found.disclosure() == {"op": "between", "values": clause["values"]}
    # The day bound has no time zone; the timestamp alone would not be noted.
    assert found.bound_read_as_utc is True
    _, ranges = split_date_filter({"op": "gt", "field": "date.created",
                                   "value": "2025-06-30T12:00:00+02:00"}, CREATED)
    assert ranges["month(date.created)"][0].bound_read_as_utc is False


@pytest.mark.parametrize(("filter", "reason", "needle"), [
    ({"op": "between", "field": "date.created", "values": ["2025-12-31", "2025-01-01"]},
     "date_bounds_reversed", "low first"),
    ({"op": "between", "field": "date.created", "value": ["2025-01-01", "2025-12-31"]},
     "date_values", '"values":["2025-01-01","2025-12-31"]'),
    ({"op": "gt", "field": "date.created", "value": "last year"},
     "date_operand", '"values":["2025-01-01","2025-12-31"]'),
    ({"op": "lt", "field": "date.created", "value": 1735689600},
     "date_operand", "2025-01-01T00:00:00Z"),
])
def test_a_bound_that_is_not_a_date_is_refused_with_the_form_that_works(
    filter, reason, needle,
) -> None:
    with pytest.raises(InvalidOperatorArguments) as excinfo:
        split_date_filter(filter, CREATED)
    assert excinfo.value.data["reason"] == reason
    assert needle in str(excinfo.value) + (excinfo.value.fix_hint or "")


# An edge set declares no named date (issue #3666): a refusal over one names
# `date.attrs.<key>`, never a named-date call that set refuses in turn.
@pytest.mark.parametrize(("text", "example"), [
    ("quarter(date.created)", "month(date.attrs.<key>)"),
    ("Year(date.created)", "year(date.attrs.<key>)"),
    ("month(date.bogus)", "month(date.attrs.<key>)"),
    ("month(date.created", "month(date.attrs.<key>)"),
    # The caller's own attribute is kept.
    ("WEEK(date.attrs.due)", "week(date.attrs.due)"),
    ("month(attrs.due)", "month(date.attrs.due)"),
])
def test_a_malformed_period_over_edges_gets_an_example_edges_accept(
    text: str, example: str,
) -> None:
    with pytest.raises(InvalidOperatorArguments) as excinfo:
        parse_period_field(text, named_dates=False)
    hint = excinfo.value.fix_hint or ""
    assert f"by=[{example!r}]" in hint
    # Neither the form line, the example nor data.expected names a date an
    # edge set refuses; both name the one it accepts.
    expected = excinfo.value.data["expected"]
    for told in (hint, expected):
        assert "date.attrs.<key>" in told
        for meaning in DATE_MEANINGS:
            assert f"date.{meaning}" not in told, told
    assert "day, week, month, year" in hint
    # Over records the form still lists the named dates.
    with pytest.raises(InvalidOperatorArguments) as records:
        parse_period_field(text)
    assert "date.created" in records.value.data["expected"]
    # The example is itself a field an edge set takes.
    parsed = parse_period_field(example, named_dates=False)
    assert parsed is not None and parsed.meaning is None
    # A well-formed field parses the same either way.
    assert parse_period_field("month(date.created)", named_dates=False) == CREATED[
        "month(date.created)"]


@pytest.mark.parametrize("periods", [{}, {"month(date.attrs.due)": parse_period_field("month(date.attrs.due)")}])
def test_a_named_date_filter_over_edges_is_refused_with_the_attribute_form(periods) -> None:
    with pytest.raises(InvalidOperatorArguments) as excinfo:
        split_date_filter(IN_2025, periods, named_dates=False)
    err = excinfo.value
    assert err.data == {"field": "date.created", "reason": "date_filter_named_date"}
    assert "edges have none" in str(err)
    hint = err.fix_hint or ""
    assert '"field":"date.attrs.<key>"' in hint and '"year(date.attrs.<key>)"' in hint
    assert "date.created" not in hint
    # A name that is no date field at all is told so, with the edge form
    # alone: it does not "name a date declared per record type".
    bogus = {"op": "between", "field": "date.bogus", "values": ["2025-01-01", "2025-12-31"]}
    with pytest.raises(InvalidOperatorArguments) as unknown:
        split_date_filter(bogus, periods, named_dates=False)
    assert unknown.value.data == {"field": "date.bogus", "reason": "unknown_date_field"}
    assert "'date.bogus' is not a date field" in str(unknown.value)
    assert unknown.value.fix_hint == "name the edge attribute as date.attrs.<key>"
    # An attribute date range over edges is taken as before.
    due = {"op": "between", "field": "date.attrs.due", "values": ["2025-01-01", "2025-02-01"]}
    rest, ranges = split_date_filter(due, periods or {"day(date.attrs.due)": parse_period_field("day(date.attrs.due)")}, named_dates=False)
    assert rest is None and len(ranges) == 1


def test_the_fold_keeps_only_days_inside_the_range_and_reports_what_it_left_out() -> None:
    field = "month(date.created)"
    _, ranges = split_date_filter(IN_2025, CREATED)
    report = PeriodReport()
    groups = fold_period_groups([
        ("2024-12-31T23:59:59Z", True, 5),       # the day before
        ("2025-01-01T00:00:00Z", True, 2),       # first day, included
        ("2025-12-31T23:30:00-05:00", True, 3),  # 2026-01-01 in UTC: outside
        ("2025-12-31T23:59:59Z", True, 4),       # last day, included
        (None, False, 7),                        # no date: outside any range
        ("soon", True, 1),                       # not a date: counted apart
    ], by=[field], periods=CREATED, report=report, ranges=ranges)
    assert groups == [(("2025-01-01",), 2), (("2025-12-01",), 4)]
    assert report.payload()["date_filter"] == {field: {
        "ranges": [{"op": "between", "values": ["2025-01-01", "2025-12-31"]}],
        "compared": "instant (a date with no time zone is read as UTC)",
        "excluded_outside": 8, "excluded_no_date": 7,
    }}
    assert report.unparseable[field]["count"] == 1
    # The day bounds had no time zone; every stored value had one.
    assert (report.utc_bounds, report.utc_stored) == (["date.created"], [])
    # gt and lt are strict: a day is its first instant.
    _, ranges = split_date_filter({"op": "and", "clauses": [
        {"op": "gt", "field": "date.created", "value": "2025-01-01"},
        {"op": "lt", "field": "date.created", "value": "2025-12-31"},
    ]}, CREATED)
    assert fold_period_groups(
        [("2025-01-01", True, 1), ("2025-01-02", True, 1), ("2025-12-31", True, 1)],
        by=[field], periods=CREATED, report=PeriodReport(), ranges=ranges,
    ) == [(("2025-01-01",), 1)]


# --------------------------------------------------------------------------- #
# (b) database                                                                 #
# --------------------------------------------------------------------------- #

psycopg = pytest.importorskip("psycopg")


def _seed(clean_runtime: str) -> tuple[str, str]:
    from okg.substrate.attributes import (
        materialize_effective_attributes_for_generation,
    )
    from okg.substrate.graph_store import ChronosGraphStore
    from okg.substrate.library.sources.base import NodeFact
    from okg.substrate.publisher import Publisher
    from tests.pytest.catalog_fixture_builders import (
        SubtypeSpec,
        bind_test_catalog_writer,
        seed_catalog,
    )

    suffix = uuid.uuid4().hex[:8]
    # ticket declares created -> `created` (instants with a zone) and
    # modified -> `updated` (calendar days). `opened` mixes both kinds.
    # 2026-10-05 is a Monday, 2026-10-11 a Sunday, 2026-10-12 the next
    # Monday; 2026-12-31T23:30-05:00 is 2027-01-01 in UTC.
    tickets = [
        {"created": "2026-10-05T09:00:00Z", "updated": "2026-10-06", "opened": "2026-10-05", "status": "open"},
        {"created": "2026-10-11T23:59:59+00:00", "updated": "2026-11-02", "opened": "2026-10-11T10:00:00Z", "status": "open"},
        {"created": "2026-10-12T00:00:00Z", "updated": "2026-11-30", "status": "closed"},
        {"created": "2026-12-31T23:30:00-05:00", "updated": "2027-01-04", "status": "closed"},
        {"created": "", "status": "open"},
        {"status": "open"},
    ]
    # release declares created -> `release_date` (a different attribute);
    # one value is not a date.
    releases = [
        {"release_date": "2026-10-20T12:00:00Z"},
        {"release_date": "next spring"},
    ]
    # memo declares no date at all; `due` holds no date.
    memos = [{"due": "soon"}, {"due": "later"}]
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        seed_catalog(conn, subtypes=[
            SubtypeSpec(name="ticket", metadata={
                "created_at_from": "created", "last_modified_at_from": "updated",
                "searchable_text_fields": ["text"],
            }),
            SubtypeSpec(name="release", metadata={"created_at_from": "release_date"}),
            "memo",
        ])
        store = ChronosGraphStore.from_connection(conn)
        facts = [
            NodeFact(f"{kind}:{i}:{suffix}", kind,
                     {**attrs, "text": f"periodword {kind} {i}", "rank": i + 1}
                     if kind == "ticket" else attrs,
                     {"id": f"{kind}:{i}"}, {"v": 1})
            for kind, rows in (("ticket", tickets), ("release", releases), ("memo", memos))
            for i, attrs in enumerate(rows)
        ]
        store.apply_facts(facts, source="default", strict_endpoints=True)
    published = Publisher(
        clean_runtime, "default",
        catalog_identity=bind_test_catalog_writer(clean_runtime, "default"),
    ).cycle()
    assert published["status"] == "published", published
    gen = str(published["generation_id"])
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        materialize_effective_attributes_for_generation(conn, generation_id=gen)
    return gen, suffix


@pytest.fixture
def served(clean_runtime: str):
    from okg.substrate.mcp.server import build_server

    gen, suffix = _seed(clean_runtime)
    mcp = build_server(dsn=clean_runtime)
    try:
        yield mcp._tool_manager._tools, gen, suffix
    finally:
        mcp._okg_pool.close()
        if mcp._okg_write_pool:
            mcp._okg_write_pool.close()


def _groups(result: dict, field: str) -> list[tuple]:
    return [(g[field], g["count"]) for g in result["groups"]]


def _aggregate(served, by, input=None, **kwargs):
    tools, gen, _ = served
    return tools["aggregate"].fn(
        input=input or {"kind": "nodes", "subtype": "ticket"},
        by=by, metrics=["count"], generation_id=gen, **kwargs,
    )


def _refused(served, by, input=None) -> InvalidOperatorArguments:
    with pytest.raises(InvalidOperatorArguments) as excinfo:
        _aggregate(served, by, input)
    return excinfo.value


@pytest.mark.postgres
def test_a_named_date_counts_per_period_in_utc_oldest_first(served) -> None:
    expected = {
        "day": [("2026-10-05", 1), ("2026-10-11", 1), ("2026-10-12", 1),
                ("2027-01-01", 1), (None, 2)],
        "week": [("2026-10-05", 2), ("2026-10-12", 1), ("2026-12-28", 1), (None, 2)],
        "month": [("2026-10-01", 3), ("2027-01-01", 1), (None, 2)],
        "year": [("2026-01-01", 3), ("2027-01-01", 1), (None, 2)],
    }
    for period, groups in expected.items():
        field = f"{period}(date.created)"
        result = _aggregate(served, [field])
        assert _groups(result, field) == groups, (period, result)
        assert result["total_count"] == len(groups)
        assert result["total_count_basis"] == "exact"
        assert result["truncated"] is False
        # The reply says which attribute the named date read.
        assert result["date_fields"] == {field: {"ticket": "created"}}
        assert "date_unparseable" not in result
        assert sum(count for _, count in groups) == 6


@pytest.mark.postgres
def test_a_named_date_reads_each_types_own_attribute(served) -> None:
    """Over every record: tickets by `created`, releases by `release_date`;
    memos declare no creation date and join the null group. The release
    whose value is not a date is counted and shown, not grouped."""
    field = "month(date.created)"
    result = _aggregate(served, [field], input={"kind": "nodes"})
    assert _groups(result, field) == [("2026-10-01", 4), ("2027-01-01", 1), (None, 4)]
    assert result["date_fields"] == {field: {"release": "release_date", "ticket": "created"}}
    assert result["date_unparseable"] == {field: {"count": 1}}
    # Every selected record is in a group or in the not-a-date count.
    assert sum(g["count"] for g in result["groups"]) + 1 == 10


@pytest.mark.postgres
def test_calendar_days_are_counted_as_written(served) -> None:
    field = "month(date.modified)"
    result = _aggregate(served, [field])
    assert _groups(result, field) == [
        ("2026-10-01", 1), ("2026-11-01", 2), ("2027-01-01", 1), (None, 2),
    ]
    assert result["date_fields"] == {field: {"ticket": "updated"}}


@pytest.mark.postgres
def test_an_attribute_is_read_by_name_on_every_record(served) -> None:
    field = "year(date.attrs.updated)"
    result = _aggregate(served, [field])
    assert _groups(result, field) == [("2026-01-01", 3), ("2027-01-01", 1), (None, 2)]
    assert result["date_fields"] == {field: {"*": "updated"}}


@pytest.mark.postgres
def test_a_field_mixing_zoned_and_plain_dates_is_counted_with_the_utc_note(served) -> None:
    """Issue #3666: refused before; one rule now, with the note."""
    field = "month(date.attrs.opened)"
    result = _aggregate(served, [field])
    assert _groups(result, field) == [("2026-10-01", 2), (None, 4)]
    assert result["dates_read_as_utc"] == {"stored": ["date.attrs.opened"]}
    # A field whose values all carry a zone has no note.
    assert "dates_read_as_utc" not in _aggregate(served, ["month(date.created)"])


@pytest.mark.postgres
def test_a_field_holding_no_date_is_refused_with_count_example_and_fix(served) -> None:
    err = _refused(served, ["year(date.attrs.due)"], input={"kind": "nodes", "subtype": "memo"})
    assert err.data["reason"] == "not_a_date"
    assert err.data["unparseable_count"] == 2
    assert "example" not in err.data and "soon" not in str(err) and "later" not in str(err)
    assert "by=['attrs.due']" in (err.fix_hint or "")


@pytest.mark.postgres
def test_a_named_date_no_selected_type_declares_is_refused_with_the_fix(served) -> None:
    err = _refused(served, ["month(date.created)"], input={"kind": "nodes", "subtype": "memo"})
    assert err.data["reason"] == "field_not_declared"
    assert "date.attrs.<key>" in (err.fix_hint or "")
    # Declared by no type at all.
    err = _refused(served, ["month(date.resolved)"])
    assert err.data["reason"] == "field_not_declared"


@pytest.mark.postgres
def test_a_period_combines_with_a_plain_field_a_filter_and_paging(served) -> None:
    field = "year(date.created)"
    result = _aggregate(served, [field, "attrs.status"], input={
        "kind": "nodes", "subtype": "ticket",
        "filter": {"field": "attrs.status", "op": "eq", "value": "closed"},
    })
    assert [(g[field], g["attrs.status"], g["count"]) for g in result["groups"]] == [
        ("2026-01-01", "closed", 1), ("2027-01-01", "closed", 1),
    ]
    day = "day(date.created)"
    first = _aggregate(served, [day], limit=2)
    assert _groups(first, day) == [("2026-10-05", 1), ("2026-10-11", 1)]
    assert first["truncated"] is True and first["next_offset"] == 2 and first["total_count"] == 5
    rest = _aggregate(served, [day], limit=10, offset=2)
    assert _groups(rest, day) == [("2026-10-12", 1), ("2027-01-01", 1), (None, 2)]


@pytest.mark.postgres
def test_a_date_range_filter_counts_per_month_inside_it(served) -> None:
    """The natural call for "per month in October 2026" (pilot question
    pq08): refused before with no working form named; now served, exactly."""
    field = "day(date.created)"
    october = {"op": "between", "field": "date.created", "values": ["2026-10-06", "2026-10-31"]}
    result = _aggregate(served, [field], input={
        "kind": "nodes", "subtype": "ticket", "filter": october})
    assert _groups(result, field) == [("2026-10-11", 1), ("2026-10-12", 1)]
    assert result["date_filter"] == {field: {
        "ranges": [{"op": "between", "values": ["2026-10-06", "2026-10-31"]}],
        "compared": "instant (a date with no time zone is read as UTC)",
        "excluded_outside": 2, "excluded_no_date": 2,
    }}
    assert result["dates_read_as_utc"] == {"bounds": ["date.created"]}
    # A timestamp bound is accepted on the period's own date (#3666).
    stamped = _aggregate(served, [field], input={"kind": "nodes", "subtype": "ticket", "filter": {
        "op": "between", "field": "date.created",
        "values": ["2026-10-11T23:59:59Z", "2026-10-12T00:00:00Z"]}})
    assert _groups(stamped, field) == [("2026-10-11", 1), ("2026-10-12", 1)]
    assert "dates_read_as_utc" not in stamped
    # Beside another clause, inside one top-level and.
    result = _aggregate(served, [field], input={"kind": "nodes", "subtype": "ticket", "filter": {
        "op": "and", "clauses": [october, {"op": "eq", "field": "attrs.status", "value": "closed"}],
    }})
    assert _groups(result, field) == [("2026-10-12", 1)]
    # Without the period in by the same range works too (#3666; it was
    # refused), and counts the same two records.
    plain = _aggregate(served, ["attrs.status"], input={
        "kind": "nodes", "subtype": "ticket", "filter": october})
    assert sorted(_groups(plain, "attrs.status")) == [("closed", 1), ("open", 1)]
    assert "date_filter" not in plain
    # With a period beside another field its groups add up to the range.
    fixed = _aggregate(served, ["year(date.created)", "attrs.status"], input={
        "kind": "nodes", "subtype": "ticket", "filter": october})
    assert sum(g["count"] for g in fixed["groups"]) == 2


@pytest.mark.postgres
def test_plain_attribute_grouping_is_unchanged(served) -> None:
    result = _aggregate(served, ["attrs.status"])
    assert _groups(result, "attrs.status") == [("open", 4), ("closed", 2)]
    assert "date_fields" not in result
    # A key with parentheses is still a plain attribute, not a period.
    result = _aggregate(served, ["attrs.Size (MB)"])
    assert _groups(result, "attrs.Size (MB)") == [(None, 6)]


@pytest.mark.postgres
def test_a_plain_field_before_a_named_date_over_several_types(served) -> None:
    """The plain column comes first and the named date adds a type column:
    every key still lands in its own field."""
    field = "month(date.created)"
    result = _aggregate(served, ["subtype", field], input={"kind": "nodes"})
    assert [(g["subtype"], g[field], g["count"]) for g in result["groups"]] == [
        ("memo", None, 2), ("release", "2026-10-01", 1),
        ("ticket", "2026-10-01", 3), ("ticket", "2027-01-01", 1), ("ticket", None, 2),
    ]
    assert result["date_unparseable"] == {field: {"count": 1}}


@pytest.mark.postgres
def test_two_period_fields_in_one_call(served) -> None:
    by = ["year(date.created)", "month(date.modified)"]
    result = _aggregate(served, by)
    assert [(g[by[0]], g[by[1]], g["count"]) for g in result["groups"]] == [
        ("2026-01-01", "2026-10-01", 1), ("2026-01-01", "2026-11-01", 2),
        ("2027-01-01", "2027-01-01", 1), (None, None, 2),
    ]
    assert result["date_fields"] == {by[0]: {"ticket": "created"}, by[1]: {"ticket": "updated"}}


@pytest.mark.postgres
def test_a_number_under_a_period_is_not_a_date(served) -> None:
    err = _refused(served, ["year(date.attrs.rank)"])
    assert err.data["reason"] == "not_a_date" and err.data["unparseable_count"] == 6
    assert "by=['attrs.rank']" in (err.fix_hint or "")


@pytest.mark.postgres
def test_more_distinct_dates_than_the_bound_is_refused_not_partly_counted(
    clean_runtime: str, monkeypatch: pytest.MonkeyPatch,
) -> None:
    from okg.substrate.mcp import aggregate_periods
    from okg.substrate.mcp.server import build_server

    gen, _ = _seed(clean_runtime)
    monkeypatch.setattr(aggregate_periods, "MAX_DISTINCT_DATE_ROWS", 3)
    mcp = build_server(dsn=clean_runtime)
    try:
        with pytest.raises(InvalidOperatorArguments) as excinfo:
            mcp._tool_manager._tools["aggregate"].fn(
                input={"kind": "nodes", "subtype": "ticket"}, by=["day(date.created)"],
                metrics=["count"], generation_id=gen,
            )
        assert excinfo.value.data == {"limit": 3}
        assert "narrow the input" in (excinfo.value.fix_hint or "")
    finally:
        mcp._okg_pool.close()
        if mcp._okg_write_pool:
            mcp._okg_write_pool.close()


@pytest.mark.postgres
def test_a_result_reference_counts_per_period(served) -> None:
    tools, gen, _ = served
    found = tools["search"].fn(query="periodword", retain=True, limit=10, generation_id=gen)
    ref = found["result_reference"]["id"]
    members = sorted(int(row["node_id"].split(":")[1]) for row in found["results"])
    assert len(members) >= 3, found["results"]
    # Tickets 0-2 were created in October 2026, ticket 3 in January 2027
    # (UTC); tickets 4 and 5 have no creation date.
    month_of = {0: "2026-10-01", 1: "2026-10-01", 2: "2026-10-01", 3: "2027-01-01", 4: None, 5: None}
    expected: dict = {}
    for index in members:
        expected[month_of[index]] = expected.get(month_of[index], 0) + 1
    field = "month(date.created)"
    result = tools["aggregate"].fn(input={"ref": ref}, by=[field], metrics=["count"])
    assert _groups(result, field) == sorted(
        expected.items(), key=lambda item: (item[0] is None, item[0] or ""))
    assert result["date_fields"] == {field: {"ticket": "created"}}
    # The reply says it counted the reference's rows, not the whole graph.
    assert result["reference_members"] == len(members)
    assert sum(g["count"] for g in result["groups"]) == len(members)
    from okg.substrate.mcp import result_text

    text = result_text.render(result, tool="aggregate")
    assert (f"! covers only the {len(members)} record(s) held by the result reference; "
            "the search that made it may match more") in text
    status = text.split("\n", 1)[0]
    assert "exhaustive" not in status
    assert f"over the reference's {len(members)} row(s) only" in status
    # A count over a set says nothing of the kind.
    assert "reference_members" not in _aggregate(served, [field])


@pytest.mark.postgres
def test_wrong_uses_are_refused_with_the_call_that_works(served) -> None:
    tools, gen, suffix = served
    # One record's edges cannot be counted per date.
    with pytest.raises(InvalidOperatorArguments) as excinfo:
        tools["aggregate"].fn(
            input={"node_id": f"ticket:0:{suffix}"}, by=["month(date.created)"],
            metrics=["count"], generation_id=gen,
        )
    assert 'by=["month(date.created)"]' in (excinfo.value.fix_hint or "")
    assert '"kind":"nodes"' in (excinfo.value.fix_hint or "")
    # Edges declare no named date.
    with pytest.raises(InvalidOperatorArguments) as excinfo:
        tools["aggregate"].fn(
            input={"kind": "edges"}, by=["month(date.created)"],
            metrics=["count"], generation_id=gen,
        )
    assert "date.attrs.<key>" in (excinfo.value.fix_hint or "")
    # A misspelled period is refused before anything is read.
    err = _refused(served, ["monthly(date.created)"])
    assert "by=['month(date.created)']" in (err.fix_hint or "")


@pytest.mark.postgres
def test_a_refused_period_over_edges_names_a_call_edges_accept(served) -> None:
    """Issue #3666: over an edge set, no refusal names a named-date period
    (`year(date.created)`), which that set refuses in turn."""
    edges = {"kind": "edges"}
    # A misspelled period: the example is an attribute period.
    err = _refused(served, ["monthly(date.created)"], edges)
    assert "by=['month(date.attrs.<key>)']" in (err.fix_hint or "")
    # A date range on a named date, with no period in by.
    in_2025 = {"op": "between", "field": "date.created",
               "values": ["2025-01-01", "2025-12-31"]}
    err = _refused(served, ["edge_type"], {**edges, "filter": in_2025})
    assert err.data == {"field": "date.created", "reason": "date_filter_named_date"}
    hint = err.fix_hint or ""
    assert '"year(date.attrs.<key>)"' in hint and "date.created" not in hint
    # The named fix is a call an edge set accepts: the attribute range is
    # compiled like any condition, with or without a period (#3666 step 4a).
    due = {"op": "between", "field": "date.attrs.due",
           "values": ["2025-01-01", "2025-12-31"]}
    assert split_date_filter(due, {}, named_dates=False) == (due, {})
    # The same range over records is served: a range needs no period in by
    # since step 4a (it was refused `date_filter_without_period`). No seeded
    # record was created in 2025.
    served_now = _aggregate(served, ["subtype"], {"kind": "nodes", "filter": in_2025})
    assert served_now["groups"] == [] and served_now["total_count_basis"] == "exact"
    # And it counts what is there: three tickets and one release were
    # created in 2026 (the fourth ticket's instant is in 2027 in UTC); the
    # release whose value is no date is counted apart; the day bounds had
    # no time zone and every stored value had one.
    in_2026 = {"op": "between", "field": "date.created",
               "values": ["2026-01-01", "2026-12-31"]}
    counted = _aggregate(served, ["subtype"], {"kind": "nodes", "filter": in_2026})
    assert {g["subtype"]: g["count"] for g in counted["groups"]} == {"ticket": 3, "release": 1}
    assert counted["total_count"] == 2 and counted["total_count_basis"] == "exact"
    assert counted["date_unparseable"] == {
        "date.created": {"count": 1}}
    assert counted["dates_read_as_utc"] == {"bounds": ["date.created"]}
    assert "date_filter" not in counted


@pytest.mark.postgres
def test_filter_and_map_over_part_of_the_matches_never_say_exhaustive(clean_runtime: str) -> None:
    """Issue #3666: a saved result that holds 3 of the 6 matching tickets.
    `filter` and `map` over it say, in JSON and in text, that they cover
    those 3 rows; neither says the retrieval was exhaustive."""
    import json

    import anyio
    from mcp.shared.memory import create_connected_server_and_client_session

    from okg.substrate.mcp.server import build_server
    from tests.substrate.mcp._result_payload import JSON_META, payload, result_text

    gen, _ = _seed(clean_runtime)
    server = build_server(dsn=clean_runtime)
    is_open = {"op": "eq", "field": "attrs.status", "value": "open"}
    no_match = {"op": "eq", "field": "attrs.status", "value": "no-such-status"}

    async def run() -> None:
        async with create_connected_server_and_client_session(server) as client:
            everything = payload(await client.call_tool("search", {
                "query": "periodword", "limit": 10, "generation_id": gen}, meta=JSON_META))
            assert len(everything["results"]) == 6
            saved = payload(await client.call_tool("search", {
                "query": "periodword", "limit": 3, "retain": True, "generation_id": gen,
            }, meta=JSON_META))
            ref = saved["result_reference"]["id"]
            held = [str(row["node_id"]).strip() for row in saved["results"]]
            assert len(held) == 3
            # Whatever order the search ranked ties in: one held row by its
            # id matches exactly 1 of the 3, and no row has the made-up status.
            one_held = {"op": "in", "field": "node_id", "values": [held[0]]}
            calls = [
                ("filter", {"input": {"ref": ref}, "predicate": one_held}, 1),
                ("filter", {"input": {"ref": ref}, "predicate": no_match}, 0),
                ("map", {"input": {"ref": ref},
                         "projection": ["node_id", "attrs.status"]}, 3),
            ]
            for tool, args, expected in calls:
                data = payload(await client.call_tool(tool, args, meta=JSON_META))
                assert data["total_count"] == expected == len(data["items"]), (tool, data)
                # The bound is the reference's row count, not the rows that matched.
                assert data["reference_members"] == 3, (tool, data)
                retrieval = data["coverage"]["retrieval"]
                assert retrieval["status"] == "bounded", (tool, retrieval)
                assert "result_reference" in retrieval["bounds"], (tool, retrieval)
                assert "exhaustive" not in json.dumps(data), tool
                text = result_text(await client.call_tool(tool, args))
                status = text.split("\n", 1)[0]
                assert "over the reference's 3 row(s) only" in status, (tool, text)
                assert "exhaustive" not in text, (tool, text)
                assert "! covers only the 3 record(s) held by the result reference" in text
            # The same reads over the whole set still say what they say today.
            whole = payload(await client.call_tool("filter", {
                "input": {"kind": "nodes", "subtype": "ticket"}, "predicate": is_open,
                "generation_id": gen}, meta=JSON_META))
            assert len(whole["items"]) == 4, whole
            assert "reference_members" not in whole
            assert "result_reference" not in whole["coverage"]["retrieval"].get("bounds", [])

    try:
        anyio.run(run)
    finally:
        server._okg_pool.close()


@pytest.mark.postgres
def test_a_count_over_part_of_the_matches_never_says_exhaustive(clean_runtime: str) -> None:
    """A saved result that holds 3 of the 6 matching tickets: the count over
    it says, in JSON and in text, that it covers those 3 rows, and nowhere
    says the retrieval was exhaustive (an agent reported 30 for 233)."""
    import json

    import anyio
    from mcp.shared.memory import create_connected_server_and_client_session

    from okg.substrate.mcp.server import build_server
    from tests.substrate.mcp._result_payload import JSON_META, payload, result_text

    gen, _ = _seed(clean_runtime)
    server = build_server(dsn=clean_runtime)

    async def run() -> None:
        async with create_connected_server_and_client_session(server) as client:
            everything = payload(await client.call_tool("search", {
                "query": "periodword", "limit": 10, "generation_id": gen}, meta=JSON_META))
            assert len(everything["results"]) == 6
            saved = payload(await client.call_tool("search", {
                "query": "periodword", "limit": 3, "retain": True, "generation_id": gen,
            }, meta=JSON_META))
            ref = saved["result_reference"]["id"]
            args = {"input": {"ref": ref}, "by": ["subtype"], "metrics": ["count"]}
            data = payload(await client.call_tool("aggregate", args, meta=JSON_META))
            assert data["groups"] == [{"subtype": "ticket", "count": 3}]
            assert data["reference_members"] == 3
            retrieval = data["coverage"]["retrieval"]
            assert retrieval["status"] == "bounded", retrieval
            assert "result_reference" in retrieval["bounds"], retrieval
            assert "exhaustive" not in json.dumps(data)
            text = result_text(await client.call_tool("aggregate", args))
            status = text.split("\n", 1)[0]
            assert "over the reference's 3 row(s) only" in status
            assert "exhaustive" not in text
            assert "! covers only the 3 record(s) held by the result reference" in text
            # A count over the whole set still says what it says today.
            whole = payload(await client.call_tool("aggregate", {
                "input": {"kind": "nodes", "subtype": "ticket"}, "by": ["subtype"],
                "metrics": ["count"], "generation_id": gen}, meta=JSON_META))
            assert whole["groups"] == [{"subtype": "ticket", "count": 6}]
            assert whole["coverage"]["retrieval"]["status"] == "exhaustive"
            assert "reference_members" not in whole

    try:
        anyio.run(run)
    finally:
        server._okg_pool.close()


@pytest.mark.postgres
def test_the_text_a_model_reads_shows_periods_the_null_group_and_non_dates(clean_runtime: str) -> None:
    import anyio
    from mcp.shared.memory import create_connected_server_and_client_session

    from okg.substrate.mcp.server import build_server
    from tests.substrate.mcp._result_payload import JSON_META, payload, result_text

    gen, _ = _seed(clean_runtime)
    server = build_server(dsn=clean_runtime)

    async def run() -> None:
        async with create_connected_server_and_client_session(server) as client:
            args = {"input": {"kind": "nodes"}, "by": ["month(date.created)"],
                    "metrics": ["count"], "generation_id": gen}
            text = result_text(await client.call_tool("aggregate", args))
            assert text.split("\n", 1)[0].startswith("aggregate ok")
            for needle in ("2026-10-01", "2027-01-01", "release_date", "date_unparseable"):
                assert needle in text, text
            # The value that is not a date is counted, not shown (#3666).
            assert "next spring" not in text
            data = payload(await client.call_tool("aggregate", args, meta=JSON_META))
            assert [g["count"] for g in data["groups"]] == [4, 1, 4]
            assert data["groups"][-1]["month(date.created)"] is None
            refused = await client.call_tool(
                "aggregate", {**args, "by": ["month(attrs.created)"]})
            assert refused.isError
            assert "month(date.attrs.created)" in result_text(refused)

    try:
        anyio.run(run)
    finally:
        server._okg_pool.close()


# --------------------------------------------------------------------------- #
# (c) database: one group per list element, each(attrs.<key>) (#3666)         #
# --------------------------------------------------------------------------- #

#: (labels, team) per bundle record. `labels` holds lists, a repeated
#: element, JSON null elements, an empty list, no value, and one scalar.
_BUNDLES = [
    (["a", "b", "a"], "x"),
    (["b", None], "x"),
    ([], "x"),
    (None, "x"),
    ("a", "y"),
    ([None], "y"),
    (["a", 1], "y"),
]


def _seed_lists(clean_runtime: str) -> str:
    from okg.substrate.attributes import (
        materialize_effective_attributes_for_generation,
    )
    from okg.substrate.graph_store import ChronosGraphStore
    from okg.substrate.library.sources.base import EdgeFact, NodeFact
    from okg.substrate.publisher import Publisher
    from tests.pytest.catalog_fixture_builders import (
        bind_test_catalog_writer,
        seed_catalog,
    )

    facts: list = []
    for i, (labels, team) in enumerate(_BUNDLES):
        attrs: dict = {"team": team}
        if labels is not None:
            attrs["labels"] = labels
        facts.append(NodeFact(f"bundle:{i}", "bundle", attrs, {"id": f"bundle:{i}"}, {"v": 1}))
    for src, dst, tags in (("bundle:0", "bundle:1", ["p", "q", "p"]),
                           ("bundle:1", "bundle:2", ["p"]),
                           ("bundle:2", "bundle:3", "p")):
        facts.append(EdgeFact(
            src=src, dst=dst, edge_type="relates", attrs={"tags": tags},
            source_record_id={"id": f"{src}|relates|{dst}"}, source_revision={"v": 1},
        ))
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        seed_catalog(conn, subtypes=["bundle"], narrowings=[("bundle", "relates", "bundle")])
        ChronosGraphStore.from_connection(conn).apply_facts(
            facts, source="default", strict_endpoints=True,
        )
    published = Publisher(
        clean_runtime, "default",
        catalog_identity=bind_test_catalog_writer(clean_runtime, "default"),
    ).cycle()
    assert published["status"] == "published", published
    gen = str(published["generation_id"])
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        materialize_effective_attributes_for_generation(conn, generation_id=gen)
    return gen


@pytest.mark.postgres
def test_each_counts_one_group_per_list_element_as_a_client_reads_it(
    clean_runtime: str,
) -> None:
    import anyio
    from mcp.shared.memory import create_connected_server_and_client_session

    from okg.substrate.mcp.server import build_server
    from tests.substrate.mcp._result_payload import JSON_META, payload, result_text

    gen = _seed_lists(clean_runtime)
    server = build_server(dsn=clean_runtime)
    records = {"input": {"kind": "nodes", "subtype": "bundle"}, "metrics": ["count"],
               "generation_id": gen}

    async def run() -> None:
        async with create_connected_server_and_client_session(server) as client:
            async def call(arguments: dict, fmt: str = "json") -> tuple:
                result = await client.call_tool(
                    "aggregate", arguments, meta=JSON_META if fmt == "json" else None,
                )
                return result, (payload(result) if fmt == "json" else result_text(result))

            # An element counts once per row; rows with no element are one
            # null group; the scalar is in no group, and counted.
            _, data = await call({**records, "by": ["each(attrs.labels)"]})
            assert [(g["each(attrs.labels)"], g["count"]) for g in data["groups"]] == [
                (None, 3), ("a", 2), ("b", 2), (1, 1),
            ]
            assert (data["rows_counted"], data["rows_with_none"],
                    data["rows_not_a_list"], data["groups_overlap"]) == (7, 3, 1, True)
            assert data["total_count"] == 4
            # The same facts in the text a model reads.
            _, text = await call({**records, "by": ["each(attrs.labels)"]}, "text")
            assert text.split("\n", 1)[0].startswith("aggregate ok"), text
            for needle in ("rows_counted: 7", "rows_with_none: 3",
                           "rows_not_a_list: 1", "groups_overlap: true"):
                assert needle in text, text
            # Beside a plain key: the elements per team.
            _, data = await call({**records, "by": ["attrs.team", "each(attrs.labels)"]})
            assert sorted(
                ((g["attrs.team"], str(g["each(attrs.labels)"])), g["count"])
                for g in data["groups"]
            ) == [(("x", "None"), 2), (("x", "a"), 1), (("x", "b"), 2),
                  (("y", "1"), 1), (("y", "None"), 1), (("y", "a"), 1)]
            # Plain grouping by the list keeps its meaning: one group per list.
            _, data = await call({**records, "by": ["attrs.labels"]})
            assert "rows_counted" not in data
            assert ["a", "b", "a"] in [g["attrs.labels"] for g in data["groups"]]
            # Over links: the tags of each link, once per link.
            _, data = await call({"input": {"kind": "edges", "edge_type": "relates"},
                                  "by": ["each(attrs.tags)"], "metrics": ["count"],
                                  "generation_id": gen})
            assert [(g["each(attrs.tags)"], g["count"]) for g in data["groups"]] == [
                ("p", 2), ("q", 1),
            ]
            assert (data["rows_counted"], data["rows_not_a_list"]) == (3, 1)
            # A field that holds no list is refused, with the plain grouping.
            refused, data = await call({**records, "by": ["each(attrs.team)"]})
            assert refused.isError, data
            assert data["error"]["data"]["reason"] == "not_a_list"
            assert "by=['attrs.team']" in data["error"]["fix_hint"]
            # A malformed each() is refused by name before any read.
            refused, text = await call({**records, "by": ["each(team)"]}, "text")
            assert refused.isError and "each(attrs.team)" in text, text
            assert "is not a period" not in text

    try:
        anyio.run(run)
    finally:
        server._okg_pool.close()


@pytest.mark.postgres
def test_a_count_past_its_time_budget_is_refused_and_the_session_is_left_as_it_was(
    clean_runtime: str,
) -> None:
    """The statement a link or list-element count runs under: past the budget
    it is cancelled and refused by name; the caller's transaction and the
    session's own timeout come through either way."""
    from okg.substrate.mcp.aggregate_each import run_within_budget

    with psycopg.connect(clean_runtime) as conn:
        conn.execute("SET statement_timeout = '45s'")
        rows, facts = run_within_budget(
            conn, "SELECT 1 AS n", {}, "SELECT 2", links=True, budget_ms=2_000,
        )
        assert (rows, tuple(facts)) == ([(1,)], (2,))
        assert conn.execute("SHOW statement_timeout").fetchone()[0] == "45s"
        with pytest.raises(InvalidOperatorArguments) as refused:
            run_within_budget(conn, "SELECT pg_sleep(2)", {}, links=True, budget_ms=50)
        assert str(refused.value) == "aggregate: counting these links did not finish in 0.05 s"
        assert refused.value.data == {"reason": "count_time_budget", "budget_ms": 50}
        assert "add a condition on the links" in (refused.value.fix_hint or "")
        with pytest.raises(InvalidOperatorArguments) as elements:
            run_within_budget(conn, "SELECT pg_sleep(2)", {}, links=False, budget_ms=50)
        assert "these list elements" in str(elements.value)
        # The cancelled statement's savepoint was rolled back: the caller's
        # transaction still runs, under its own timeout.
        assert conn.execute("SELECT 3").fetchone()[0] == 3
        assert conn.execute("SHOW statement_timeout").fetchone()[0] == "45s"
        # A cancel that is not the timeout (a client or an administrator) is
        # raised as it came, never reported as the count running out of time.
        pid = conn.info.backend_pid
        cancel = threading.Timer(0.5, lambda: _cancel_backend(clean_runtime, pid))
        cancel.start()
        try:
            with pytest.raises(psycopg.errors.QueryCanceled):
                run_within_budget(conn, "SELECT pg_sleep(3)", {}, links=True)
        finally:
            cancel.join()
        assert conn.execute("SELECT 4").fetchone()[0] == 4


def _cancel_backend(dsn: str, pid: int) -> None:
    with psycopg.connect(dsn, autocommit=True) as other:
        other.execute("SELECT pg_cancel_backend(%s)", (pid,))


# --------------------------------------------------------------------------- #
# (d) database: each() and link counts with a date condition (#3666)          #
# --------------------------------------------------------------------------- #

#: One long tag list, so a page of its groups is larger than the smallest
#: response budget and is read again at a smaller limit.
_LONG_TAGS = [f"tag-{n:03d}-" + "x" * 72 for n in range(150)]
#: (when, tags) per item; items 0 to 2 fall in the first half of 2026.
_ITEMS = [
    ("2026-01-15T00:00:00Z", _LONG_TAGS + ["a"]),
    ("2026-02-15T00:00:00Z", ["a", "b"]),
    ("2026-03-15T00:00:00Z", ["a"]),
    ("2026-07-15T00:00:00Z", ["a", "z"]),
    ("2026-08-15T00:00:00Z", ["z"]),
    (None, ["a"]),
]
#: (src, dst, at) of the `cites` links; three are in the first half of 2026.
_CITES = [
    ("item:1", "item:0", "2026-02-01T00:00:00Z"),
    ("item:2", "item:0", "2026-03-01T00:00:00Z"),
    ("item:3", "item:0", "2026-07-01T00:00:00Z"),
    ("item:2", "item:1", "2026-05-01T00:00:00Z"),
]
_FIRST_HALF = {"op": "between", "values": ["2026-01-01", "2026-06-30"]}


def _seed_dated(clean_runtime: str) -> str:
    from okg.substrate.attributes import (
        materialize_effective_attributes_for_generation,
    )
    from okg.substrate.graph_store import ChronosGraphStore
    from okg.substrate.library.sources.base import EdgeFact, NodeFact
    from okg.substrate.publisher import Publisher
    from tests.pytest.catalog_fixture_builders import (
        bind_test_catalog_writer,
        seed_catalog,
    )

    facts: list = []
    for i, (when, tags) in enumerate(_ITEMS):
        attrs: dict = {"tags": tags}
        if when is not None:
            attrs["when"] = when
        facts.append(NodeFact(f"item:{i}", "item", attrs, {"id": f"item:{i}"}, {"v": 1}))
    for src, dst, at in _CITES:
        facts.append(EdgeFact(
            src=src, dst=dst, edge_type="cites", attrs={"at": at},
            source_record_id={"id": f"{src}|cites|{dst}"}, source_revision={"v": 1},
        ))
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        seed_catalog(conn, subtypes=["item"], narrowings=[("item", "cites", "item")])
        ChronosGraphStore.from_connection(conn).apply_facts(
            facts, source="default", strict_endpoints=True,
        )
    published = Publisher(
        clean_runtime, "default",
        catalog_identity=bind_test_catalog_writer(clean_runtime, "default"),
    ).cycle()
    assert published["status"] == "published", published
    gen = str(published["generation_id"])
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        materialize_effective_attributes_for_generation(conn, generation_id=gen)
    return gen


@pytest.mark.postgres
def test_each_and_link_counts_with_a_date_condition_as_a_client_reads_them(
    clean_runtime: str, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A count with a date condition runs under the date budget, not the
    count budget; its each() facts survive the rerun that fits max_bytes
    (the cached group rows carry them); a link count takes a date condition
    on the link; and a record set grouped by a link key with a date
    condition is refused in words, since the date cannot move onto the link."""
    import anyio
    from mcp.shared.memory import create_connected_server_and_client_session

    from okg.substrate.mcp import aggregate_each
    from okg.substrate.mcp.server import build_server
    from tests.substrate.mcp._result_payload import JSON_META, payload

    count_budget_calls: list[bool] = []
    real = aggregate_each.run_within_budget

    def recorded(*args: Any, **kwargs: Any) -> Any:
        count_budget_calls.append(bool(kwargs.get("links")))
        return real(*args, **kwargs)

    monkeypatch.setattr(aggregate_each, "run_within_budget", recorded)
    gen = _seed_dated(clean_runtime)
    server = build_server(dsn=clean_runtime)

    async def run() -> None:
        async with create_connected_server_and_client_session(server) as client:
            async def call(arguments: dict) -> tuple:
                result = await client.call_tool(
                    "aggregate", {**arguments, "metrics": ["count"], "generation_id": gen},
                    meta=JSON_META,
                )
                return result, payload(result)

            # each() with a date condition, at the smallest budget: the page
            # is read again at a smaller limit, and the facts come with it.
            _, data = await call({
                "where": {"type": "item", "date.attrs.when": _FIRST_HALF},
                "by": ["each(attrs.tags)"], "max_bytes": 4096,
            })
            assert data.get("status") != "refused", data
            assert (data["rows_counted"], data["rows_with_none"],
                    data["rows_not_a_list"], data["groups_overlap"]) == (3, 0, 0, True)
            assert data["total_count"] == 152
            assert data["groups"][0] == {"each(attrs.tags)": "a", "count": 3}
            assert len(data["groups"]) < 50 and data["truncated"] is True, data
            assert count_budget_calls == [], "a dated count runs under the date budget"
            # The same each() without a date condition runs under the count
            # budget, and counts every item.
            _, data = await call({"where": {"type": "item"}, "by": ["each(attrs.tags)"],
                                  "limit": 3})
            assert data["rows_counted"] == 6 and count_budget_calls == [False]
            # A link count with a date condition on the link.
            _, data = await call({
                "input": {"kind": "edges", "edge_type": "cites"},
                "where": {"date.attrs.at": _FIRST_HALF}, "by": ["dst"],
            })
            assert {g["dst"]: g["count"] for g in data["groups"]} == {"item:0": 2, "item:1": 1}
            assert count_budget_calls == [False], "a dated link count uses the date budget"
            # A record set grouped by a link key with a date condition: the
            # date is on the records and cannot move onto the links.
            refused, data = await call({
                "where": {"type": "item", "date.attrs.when": _FIRST_HALF},
                "by": ["link:cites:out"],
            })
            assert refused.isError and "retry_call" not in data["error"], data
            assert "cannot be carried over" in data["error"]["fix_hint"]

    try:
        anyio.run(run)
    finally:
        server._okg_pool.close()


@pytest.mark.postgres
def test_a_count_past_the_count_budget_reaches_the_client_as_a_refusal(
    clean_runtime: str, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The count budget's refusal as a client reads it: a link count whose
    statement is slowed past a 100 ms budget is cancelled by PostgreSQL and
    refused by name, in JSON and in text."""
    import anyio
    from mcp.shared.memory import create_connected_server_and_client_session

    from okg.substrate.mcp import aggregate_each
    from okg.substrate.mcp.server import build_server
    from tests.substrate.mcp._result_payload import JSON_META, payload, result_text

    real = aggregate_each.run_within_budget

    def slowed(conn: Any, sql: str, params: Any, facts_sql: str = "", **kwargs: Any) -> Any:
        slow = f"SELECT counted.* FROM ({sql}) AS counted CROSS JOIN (SELECT pg_sleep(2)) AS slow"
        return real(conn, slow, params, facts_sql, **{**kwargs, "budget_ms": 100})

    monkeypatch.setattr(aggregate_each, "run_within_budget", slowed)
    gen = _seed_dated(clean_runtime)
    server = build_server(dsn=clean_runtime)
    arguments = {"input": {"kind": "edges", "edge_type": "cites"}, "by": ["dst"],
                 "metrics": ["count"], "generation_id": gen}

    async def run() -> None:
        async with create_connected_server_and_client_session(server) as client:
            refused = await client.call_tool("aggregate", arguments, meta=JSON_META)
            assert refused.isError
            error = payload(refused)["error"]
            assert error["code"] == "invalid_operator_arguments"
            assert error["message"] == "aggregate: counting these links did not finish in 0.1 s"
            assert error["data"]["reason"] == "count_time_budget"
            text = result_text(await client.call_tool("aggregate", arguments))
            assert "counting these links did not finish in 0.1 s" in text, text
            assert "add a condition on the links" in text, text

    try:
        anyio.run(run)
    finally:
        server._okg_pool.close()
