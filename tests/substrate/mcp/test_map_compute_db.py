"""`map(compute=...)` through the tools (PACT read-map-compute, PR 3c).

`map` adds computed columns (`calc.<name>`) to every row of a live set, a live
handle, a groups handle or a chain handle; a later `filter` or `aggregate` over
the map's handle reads them. Every assertion counts the adapter calls a
client's call really made, against the recording fake system of
`test_live_graph_keep.py` (typed sites, here with a usage answer per site),
`test_live_result_sets.py` (raw `expand(live=...)` answers) and
`test_live_handle_lists_db.py` (chain handles), through the server's own
`call_tool`.

MF marks a test that fails on the base (`4bee16134486`, PR #4156) for the
reason its docstring gives; on the base `map` takes no `compute` argument.
"""
from __future__ import annotations

import contextlib
import functools
import json
from typing import Any, Iterator

import pytest

from okg.substrate.external_backed.live_dispatch import LiveAdapterError
from okg.substrate.mcp import live_result_sets
from tests.substrate.mcp.test_live_graph_keep import (
    SESSION,
    STATELESS,
    Clock,
    Example,
    build_lab,
    site,
    unfence,
)
from tests.substrate.mcp.test_live_result_sets import (  # noqa: F401 - fixtures
    _ok,
    _refused,
    _serve,
    live_store,
    world,
)

pytestmark = pytest.mark.postgres

SITES = {"where": {"type": "site"}}
USAGE = "example_get_site_usage_by_source"
LISTING = "example_list_all_sites"
FILL = {"fill": "attrs.static_used / attrs.static_total"}


class Sites(Example):
    """The fake system with one usage answer per site (`usage[site]`)."""

    def __init__(self) -> None:
        super().__init__()
        self.usage: dict[str, tuple[Any, Any]] = {}

    def __call__(self, relation, *, config, args, node_id, deadline_seconds):
        if relation != USAGE:
            return super().__call__(
                relation, config=config, args=args, node_id=node_id,
                deadline_seconds=deadline_seconds,
            )
        with self.lock:
            self.calls.append((relation, dict(args)))
        name = args.get("site")
        if name in self.fail:
            raise LiveAdapterError("upstream_unavailable")
        used, total = self.usage.get(name, (7, 100))
        return [{"site": name, "source": args["source"], "total": total, "used": used}]


@pytest.fixture
def clock() -> Clock:
    return Clock()


@pytest.fixture
def lab(clean_runtime: str, tmp_path, monkeypatch, clock) -> Iterator[Any]:
    previous = live_result_sets.install_live_store(live_result_sets.new_store(clock=clock))
    try:
        with build_lab(clean_runtime, tmp_path, monkeypatch) as built:
            sites = Sites()
            runners = built.mcp._okg_live_relation_runners  # type: ignore[attr-defined]
            built.mcp._okg_live_relation_runners = {  # type: ignore[attr-defined]
                name: functools.partial(sites, name) for name in runners
            }
            built.example = sites
            built.mcp._okg_live_session = lambda context=None: (  # type: ignore[attr-defined]
                None if STATELESS.get() else SESSION.get(), STATELESS.get(),
            )
            yield built
    finally:
        live_result_sets.install_live_store(previous)


def _handle(body: dict[str, Any]) -> str:
    handle = body.get("handle")
    assert isinstance(handle, dict) and str(handle.get("id", "")).startswith("l1."), (
        json.dumps(body)[:1500]
    )
    return str(handle["id"])


def _text(lab: Any, tool: str, arguments: dict[str, Any]) -> str:
    from tests.substrate.mcp._result_payload import result_text

    result = lab.call(tool, arguments, fmt="text")
    assert result.isError is False, result.content[0].text[:2000]
    return result_text(result)


def _usage(lab: Any, count: int, *, zero_total: tuple[int, ...] = ()) -> None:
    """Sites 0..count-1 with used = 10 * i + 3, total = 1000 (0 for `zero_total`)."""
    lab.example.set_sites(count)
    for i in range(count):
        lab.example.usage[site(i)] = (10 * i + 3, 0 if i in zero_total else 1000)


def _fill(i: int) -> float:
    return (10 * i + 3) / 1000


# ============================================================ 1: the same rows


def test_map_adds_calc_columns_and_never_drops_or_reorders_rows(lab) -> None:
    """MF 1: every row gets calc.fill, in the listing's order; a row whose
    value is an error stays in with its state. Base: `compute` is not an
    argument of map."""
    _usage(lab, 10, zero_total=(4,))
    listed = lab.ok("filter", {**SITES, "select": ["node_id"]})
    body = lab.ok("map", {**SITES, "compute": FILL})
    assert [i["node_id"] for i in body["items"]] == [i["node_id"] for i in listed["items"]]
    assert body["total_count"] == 10
    values = [i["calc_fill"] for i in body["items"]]
    assert values[4] is None
    assert [v for k, v in enumerate(values) if k != 4] == [
        _fill(i) for i in range(10) if i != 4
    ]
    states = body["computed"]["calc.fill"]["states"]
    assert states == {"ok": 9, "error:division_by_zero": 1}
    assert body["computed"]["calc.fill"]["not_computed"] == [
        {"node_id": f"site:{site(4)}", "state": "error:division_by_zero",
         "why": "error:division_by_zero"},
    ]
    assert "calc.fill" in body["handle"]["columns"] and body["handle"]["rows"] == 10
    # Two listings and one usage call per site (both fields share one call).
    assert lab.example.made(LISTING) == 2 and lab.example.made(USAGE) == 10


# ============================================================ 2: refusals, zero work


@pytest.mark.parametrize("arguments,reason_text", [
    ({"where": {"type": "site", "calc.fill": {"op": "gt", "value": 0.5}}},
     "filter(input="),
    ({"order_by": "-calc.fill"}, "filter(input="),
    ({"projection": {"f": "attrs.kind"}}, "select"),
])
def test_map_where_on_calc_and_map_order_by_are_refused_with_the_filter_call(
    lab, arguments, reason_text,
) -> None:
    """MF 2 (failure case): `calc.` in `where`, `order_by` on map and
    `compute` beside `projection` are refused by name before any upstream
    call, naming the call to send. Base: `compute` is not an argument."""
    _usage(lab, 5)
    call = {**SITES, "compute": FILL, **arguments}
    error = lab.refused("map", call)
    assert reason_text in json.dumps(error), error
    assert lab.example.total() == 0


def test_a_refused_expression_makes_no_upstream_call(lab) -> None:
    """MF 2b (failure case): an unknown function, on a first read and on a
    handle that would grow, is refused before any call. Base: no compute."""
    _usage(lab, 5)
    error = lab.refused("map", {**SITES, "compute": {"x": "squareroot(attrs.static_used)"}})
    assert error["data"]["reason"] == "unknown_function", error
    assert lab.example.total() == 0
    ref = _handle(lab.ok("filter", SITES))
    calls = lab.example.total()
    error = lab.refused("map", {"input": ref, "compute": {"x": "squareroot(attrs.static_used)"}})
    assert error["data"]["reason"] == "unknown_function", error
    assert lab.example.total() == calls


# ============================================================ 3: bounds


def test_map_over_more_than_the_row_bound_is_refused_with_the_exact_count(
    lab, monkeypatch,
) -> None:
    """MF 3 (failure case): past the row bound the map is refused with the
    exact count, never computed on part. The bound is lowered from 50,000 to
    5 so a 6-row handle passes it. Base: no compute."""
    from okg.substrate.mcp import map_compute

    _usage(lab, 6)
    ref = _handle(lab.ok("filter", SITES))
    monkeypatch.setattr(map_compute, "MAX_ROWS", 5)
    error = lab.refused("map", {"input": ref, "compute": {"k": "lower(attrs.kind)"}})
    assert error["data"]["reason"] == "too_many_rows" and error["data"]["rows"] == 6, error
    assert "6 rows" in error["message"]


# ============================================================ 4: select and where on a handle


def test_map_compute_with_select_and_where_on_a_handle(lab) -> None:
    """MF 4 (M5): `map(input=h, where=..., compute=..., select=[...])` keeps
    `select` (not rewritten into `projection`) and narrows the handle's rows
    before computing. Base: `where` beside a kept live answer is refused."""
    _usage(lab, 10)
    ref = _handle(lab.ok("filter", SITES))
    body = lab.ok("map", {
        "input": ref, "where": {"attrs.kind": "COLD"}, "compute": FILL,
        "select": ["node_id", "calc.fill"],
    })
    assert body["items"] == [
        {"node_id": f"site:{site(0)}", "calc_fill": _fill(0)},
        {"node_id": f"site:{site(5)}", "calc_fill": _fill(5)},
    ]
    assert body["map_input"]["rows"] == 2 and body["map_input"]["of"] == 10
    text = _text(lab, "map", {
        "input": ref, "where": {"attrs.kind": "COLD"}, "compute": FILL,
        "select": ["node_id", "calc.fill"],
    })
    assert "input: 2 of 10 rows of l1." in text


# ============================================================ 5: sums of a computed column


def test_map_then_aggregate_sum_of_calc_is_exact_for_whole_values(lab) -> None:
    """MF 5 (scope: today's sum model, 6.4 out): a whole-number computed
    column sums exactly; a non-whole one sums as any float column does, its
    float part apart and named in `sum_not_exact`. Zero upstream calls after
    the map. Base: no compute."""
    _usage(lab, 10)
    mapped = lab.ok("map", {**SITES, "compute": {
        "big": "attrs.static_used * 1000000000000", "fill": FILL["fill"],
    }})
    handle = _handle(mapped)
    calls = lab.example.total()
    whole = lab.ok("aggregate", {"input": handle, "by": [], "metrics": ["sum(calc.big)"]})
    assert whole["groups"][0]["sum(calc.big)"] == sum(
        (10 * i + 3) * 10 ** 12 for i in range(10)
    )
    assert "sum_not_exact" not in whole
    split = lab.ok("aggregate", {"input": handle, "by": [], "metrics": ["sum(calc.fill)"]})
    group = split["groups"][0]
    assert group["sum(calc.fill)"] == 0
    assert group["sum(calc.fill) floats"] == pytest.approx(sum(_fill(i) for i in range(10)))
    assert split["sum_not_exact"] == ["sum(calc.fill)"]
    assert lab.example.total() == calls


# ============================================================ 7, 8: groups handles


def test_map_over_a_groups_handle_reads_metrics_by_fields_rank_and_col(lab) -> None:
    """MF 7 (M11): over a ranked groups handle a metric is its label, a by
    field its name, `rank` and any column `col['...']`. Base: no compute."""
    _usage(lab, 10)
    grouped = lab.ok("aggregate", {
        **SITES, "by": ["attrs.kind"], "metrics": ["count", "sum(attrs.static_used)"],
        "order_by": "-count",
    })
    groups = _handle(grouped)
    calls = lab.example.total()
    body = lab.ok("map", {"input": groups, "compute": {
        "share": "count / 10",
        "kb": "round(to_unit(sum(attrs.static_used), 'B', 'KB'), 3)",
        "place": "col['rank']",
        "kind": "lower(attrs.kind)",
    }})
    assert lab.example.total() == calls
    rows = {unfence(item["attrs.kind"]): item for item in body["items"]}
    hot_used = sum(10 * i + 3 for i in range(10) if i % 5)
    cold_used = sum(10 * i + 3 for i in range(10) if i % 5 == 0)
    assert list(rows) == ["HOT", "COLD"]
    assert rows["HOT"]["calc.share"] == 0.8 and rows["COLD"]["calc.share"] == 0.2
    assert rows["HOT"]["calc.kb"] == round(hot_used / 1000, 3)
    assert rows["COLD"]["calc.kb"] == round(cold_used / 1000, 3)
    assert rows["HOT"]["calc.place"] == 1 and rows["COLD"]["calc.place"] == 2
    assert "hot" in rows["HOT"]["calc.kind"]
    assert body["handle"]["order"] == "rank"


def test_ranking_order_and_ranking_complete_carry_through_map(lab) -> None:
    """MF 8 (part 2): the groups keep their ranked order, and a ranking over
    a read with a site not read carries `ranking_complete: false` into the
    map's reply. Base: no compute."""
    _usage(lab, 10)
    lab.example.fail = {site(3)}
    grouped = lab.ok("aggregate", {
        **SITES, "by": ["attrs.kind"], "metrics": ["count", "sum(attrs.static_used)"],
        "order_by": "-sum(attrs.static_used)",
    })
    assert grouped["ranking_complete"] is False
    body = lab.ok("map", {"input": _handle(grouped), "compute": {"x": "count * 2"}})
    assert [unfence(i["attrs.kind"]) for i in body["items"]] == [
        unfence(g["attrs.kind"]) for g in grouped["groups"]
    ]
    assert body["ranking_complete"] is False


# ============================================================ 9, 10, 11: per-row lookups


def test_map_on_a_live_handle_fetches_only_missing_fields_into_a_new_handle(lab) -> None:
    """MF 9: a handle without the usage fields grows by one usage call per
    row into a new handle; a second map over the new handle makes no call.
    Base: no compute."""
    _usage(lab, 12)
    ref = _handle(lab.ok("filter", SITES))
    assert lab.example.total() == 1
    store = live_result_sets.live_store()
    held = store.count()
    mapped = lab.ok("map", {"input": ref, "compute": FILL})
    assert lab.example.made(USAGE) == 12 and lab.example.total() == 13
    grown = _handle(mapped)
    assert grown != ref
    # #4181 review L7: the grown set and the map's handle, as on dev; the
    # map's handle is from the grown set, and no reservation is left.
    assert store.count() == held + 2 and store.pending_count() == 0
    assert mapped["handle"]["from"] == mapped["input"]["ref"] and mapped["input"]["ref"] != ref
    assert mapped["grown_from"] == ref
    again = lab.ok("map", {"input": grown, "compute": {
        "free": "attrs.static_total - attrs.static_used",
    }})
    assert lab.example.total() == 13, "the grown handle holds the fields"
    assert [i["calc_free"] for i in again["items"]] == [1000 - (10 * i + 3) for i in range(12)]


def test_a_failed_per_row_call_makes_calc_not_read_and_names_the_row(lab) -> None:
    """MF 10 (failure case): a site whose usage call failed is `not read`
    for calc.fill, counted and named with its field, never computed from
    null. Base: no compute."""
    _usage(lab, 6)
    lab.example.fail = {site(2)}
    body = lab.ok("map", {**SITES, "compute": {
        "fill": FILL["fill"], "unset": "coalesce(attrs.static_used, 0)",
    }})
    assert body["computed"]["calc.fill"]["states"] == {"ok": 5, "not read": 1}
    assert body["computed"]["calc.unset"]["states"] == {"ok": 5, "not read": 1}
    missing = body["computed"]["calc.fill"]["not_computed"]
    assert missing[0]["node_id"] == f"site:{site(2)}" and missing[0]["state"] == "not read"
    assert "unavailable" in missing[0]["why"]
    assert body["items"][2]["calc_unset"] is None, "coalesce never turns not read into 0"
    text = _text(lab, "map", {**SITES, "compute": FILL})
    assert f"calc.fill: 5 ok · 1 not read (site:{site(2)}: attrs.static_" in text


def test_a_planned_overrun_of_a_map_lookup_is_refused_before_any_call(lab) -> None:
    """MF 11 (failure case): growing 1,000 sites by a usage field and an
    open field plans 2,000 per-node calls, past the 1,500 a call makes; it
    is refused before any per-node call. Base: no compute."""
    lab.example.set_sites(1000)
    ref = _handle(lab.ok("filter", SITES))
    calls = lab.example.total()
    error = lab.refused("map", {"input": ref, "compute": {
        "x": "attrs.static_used + number(attrs.tier)",
    }})
    assert error["data"]["reason"] == "planned_overrun", error
    assert error["data"]["planned"] == 2000 and error["data"]["made"] == 0
    assert lab.example.total() == calls


# ============================================================ 13, 14: no extra calls


def test_a_bare_name_equal_to_a_calc_column_reads_it_with_zero_adapter_calls(lab) -> None:
    """MF 13 (M6): over a map's handle `order_by="-fill"`, `select=["fill"]`
    and `by=["band"]` read the computed columns, with a note, and fetch
    nothing upstream. Base: no compute."""
    _usage(lab, 10)
    mapped = _handle(lab.ok("map", {**SITES, "compute": {
        "fill": FILL["fill"], "band": "if(attrs.static_used > 50, 'high', 'low')",
    }}))
    calls = lab.example.total()
    ranked = lab.ok("filter", {
        "input": mapped, "order_by": "-fill", "select": ["node_id", "fill"], "limit": 3,
    })
    assert [i["calc_fill"] for i in ranked["items"]] == [_fill(9), _fill(8), _fill(7)]
    assert any(n.get("rule") == "bare_name_is_calc_column" for n in ranked.get("adjusted", []))
    grouped = lab.ok("aggregate", {"input": mapped, "by": ["band"], "metrics": ["count"]})
    counts = {unfence(g["calc.band"]): g["count"] for g in grouped["groups"]}
    assert counts == {"high": 5, "low": 5}
    assert lab.example.total() == calls


def test_byte_fit_reruns_make_no_extra_calls_with_compute(lab) -> None:
    """MF 14: a map whose page is cut by `max_bytes` (the byte-fitting
    reruns) makes one listing and one usage call per site, as the uncut
    call does. Base: no compute."""
    _usage(lab, 30)
    body = lab.ok("map", {**SITES, "compute": FILL, "max_bytes": 2500})
    assert body["truncated"] is True
    assert lab.example.made(LISTING) == 1 and lab.example.made(USAGE) == 30
    nxt = body["next_call"]
    assert nxt["tool"] == "filter" and nxt["arguments"]["input"]["ref"] == body["handle"]["id"]
    page = lab.ok("filter", nxt["arguments"])
    assert page["items"] and lab.example.made(USAGE) == 30


# ============================================================ 15: fencing


def test_calc_text_built_from_upstream_text_is_fenced(lab) -> None:
    """MF 15: text built from an upstream value is served as untrusted text
    of the live system; text built only from numbers is plain. Base: no
    compute."""
    _usage(lab, 3)
    body = lab.ok("map", {**SITES, "compute": {
        "kind_l": "lower(attrs.kind)", "used": "string(attrs.static_used)",
    }})
    first = body["items"][0]
    assert "UNTRUSTED_DATA" in first["calc_kind_l"] and "cold" in first["calc_kind_l"]
    assert first["calc_used"] == "3"


# ============================================================ 16: totals over partial sets


def test_totals_over_rows_a_computed_column_left_out_use_the_cut_wording(lab) -> None:
    """MF 16 (L4, the cut wording of #4155): an aggregate over a map handle
    where one row's computed column is an error counts over 9 of 10 rows;
    the count is "at least", the sum "over the rows received only". Base:
    no compute."""
    _usage(lab, 10, zero_total=(6,))
    mapped = _handle(lab.ok("map", {**SITES, "compute": FILL}))
    body = lab.ok("aggregate", {
        "input": mapped, "by": [], "metrics": ["count", "sum(calc.fill)"],
    })
    cover = body["totals_cover"]
    assert "over 9 of 10 rows; 1 not computed" in cover
    assert "count is at least the value shown" in cover
    assert "sum(calc.fill) is over the rows received only" in cover
    assert body["not_computed"]["count"] == 1
    assert body["groups"][0]["count"] == 9


def test_filter_on_a_calc_column_never_matches_an_error_row_even_under_not(lab) -> None:
    """MF 16b: a row whose calc.fill is an error matches no condition on
    it, `not` included, and is counted. Base: no compute."""
    _usage(lab, 5, zero_total=(1,))
    mapped = _handle(lab.ok("map", {**SITES, "compute": FILL}))
    body = lab.ok("filter", {"input": mapped, "where": {
        "op": "not", "clause": {"op": "gt", "field": "calc.fill", "value": 0.5},
    }})
    assert [i["node_id"] for i in body["items"]] == [
        f"site:{site(i)}" for i in (0, 2, 3, 4)
    ]
    assert body["not_computed"]["count"] == 1


# ============================================================ 17: too large


def test_map_output_past_the_byte_bound_is_refused_by_name(lab, monkeypatch) -> None:
    """MF 17 (M13): computed bytes past the bound refuse the whole map by
    name (`map_output_too_large`), never a partial answer. The bound is
    lowered to 64 bytes. Base: no compute."""
    from okg.substrate.mcp import map_compute

    _usage(lab, 10)
    ref = _handle(lab.ok("filter", {**SITES, "select": ["node_id", "attrs.static_used"]}))
    monkeypatch.setattr(map_compute, "MAX_OUTPUT_BYTES", 64)
    error = lab.refused("map", {"input": ref, "compute": {"k": "concat(node_id, '-', attrs.kind)"}})
    assert error["data"]["reason"] == "map_output_too_large", error


def test_a_map_too_large_to_keep_is_served_uncut_without_a_handle(lab, monkeypatch) -> None:
    """MF 17b (6.6): an uncut reply whose rows are too large to keep is
    served with `handle: null, handle_reason: too_large`; a cut one is
    refused `kept_set_too_large`. Base: no compute."""
    _usage(lab, 10)
    ref = _handle(lab.ok("filter", {**SITES, "select": ["node_id", "attrs.static_used"]}))
    monkeypatch.setattr(live_result_sets, "MAX_SET_BYTES", 200)
    whole = lab.ok("map", {"input": ref, "compute": {"k": "lower(attrs.kind)"}})
    assert whole["handle"] is None and whole["handle_reason"] == "too_large"
    assert len(whole["items"]) == 10
    error = lab.refused("map", {"input": ref, "compute": {"k": "lower(attrs.kind)"}, "limit": 3})
    assert error["data"]["reason"] == "kept_set_too_large", error


# ============================================================ 18, 19: stored, access rules


def test_map_on_a_stored_set_or_r1_handle_is_refused_until_stored_handles(lab) -> None:
    """MF 18 (failure case): a stored set and an `r1.` handle are refused by
    name, announced, never served as a column-only map. Base: no compute."""
    error = lab.refused("map", {"where": {"type": "page"}, "compute": {"x": "len(node_id)"}})
    assert error["data"]["reason"] == "compute_on_stored_set", error
    assert "stored handles" in error["message"]
    # A well-formed stored handle: refused by its prefix, before any lookup.
    reference = "r1." + "e" * 8 + "." + "2" * 10 + "." + "a" * 16 + "." + "b" * 22
    error = lab.refused("map", {"input": reference, "compute": {"x": "len(node_id)"}})
    assert error["data"]["reason"] == "compute_on_stored_set", error
    error = lab.refused("map", {
        "input": {"kind": "edges"}, "compute": {"x": "len(node_id)"},
    })
    assert error["data"]["reason"] == "compute_on_edges", error


def test_map_compute_is_refused_under_access_rules_with_zero_reads(lab) -> None:
    """MF 19 (H2, failure case): under access rules (or when their state
    cannot be told) `map(compute)` is refused before any read: no adapter
    call, on a first read, on a handle and on a stored set. Base: no
    compute."""
    _usage(lab, 4)
    ref = _handle(lab.ok("filter", SITES))
    calls = lab.example.total()
    for state in (True, None):
        lab.mcp._okg_access_rules_state = lambda state=state: state  # type: ignore[attr-defined]
        try:
            for arguments in (
                {**SITES, "compute": FILL},
                {"input": ref, "compute": FILL},
                {"where": {"type": "page"}, "compute": {"x": "len(node_id)"}},
            ):
                error = lab.refused("map", arguments)
                assert error["data"].get("access_rules") in ("on", "unknown"), error
        finally:
            del lab.mcp._okg_access_rules_state  # type: ignore[attr-defined]
    assert lab.example.total() == calls


# ============================================================ 20, 21: today's map


def test_old_map_without_compute_serves_the_same_items(lab) -> None:
    """20 (L6, regression): `map` without `compute` serves the items it served
    on the base, at a `max_bytes` that does not bind, with no note (a call
    served as written carries none)."""
    _usage(lab, 4)
    body = lab.ok("map", {**SITES, "select": ["node_id", "attrs.kind"], "max_bytes": 60000})
    assert [{k: unfence(v) for k, v in item.items()} for item in body["items"]] == [
        {"node_id": f"site:{site(0)}", "attrs_kind": "COLD"},
        {"node_id": f"site:{site(1)}", "attrs_kind": "HOT"},
        {"node_id": f"site:{site(2)}", "attrs_kind": "HOT"},
        {"node_id": f"site:{site(3)}", "attrs_kind": "HOT"},
    ]
    assert "adjusted" not in body


def test_old_map_projection_renames_still_work(lab) -> None:
    """21 (regression): `map(projection={alias: field})` still renames."""
    _usage(lab, 3)
    body = lab.ok("map", {**SITES, "projection": {"k": "attrs.kind", "id": "node_id"}})
    assert {k: unfence(v) for k, v in body["items"][0].items()} == {
        "k": "COLD", "id": f"site:{site(0)}",
    }


# ============================================================ 23, 25, 26: numbers


def test_integers_above_two_to_the_53_print_exactly_in_json(lab) -> None:
    """MF 23: a whole computed value past 2^53 prints as an exact JSON
    integer. Base: no compute."""
    _usage(lab, 2)
    result = lab.call("map", {**SITES, "compute": {
        "big": "attrs.static_total * 100000000000000000",
    }})
    text = result.content[0].text
    assert "100000000000000000000" in text
    assert json.loads(text)["items"][0]["calc_big"] == 10 ** 20


def test_a_map_handle_holds_no_decimal_and_mints(lab) -> None:
    """MF 25 (N12): a map with `/` mints a handle; every kept computed value
    is an int, a float or null. Base: no compute."""
    _usage(lab, 5)
    handle = _handle(lab.ok("map", {**SITES, "compute": {
        "fill": FILL["fill"], "third": "attrs.static_used / 3",
    }}))
    store = live_result_sets.live_store()
    entry = store._entries[handle]  # noqa: SLF001
    rows = live_result_sets.decode_rows(entry.payload.rows_json)
    values = [row["\x00calc.fill"] for row in rows] + [row["\x00calc.third"] for row in rows]
    assert all(type(v) in (int, float) for v in values)
    assert repr(rows[1]["\x00calc.third"]) == "4.33333333333333"


def test_filter_on_a_calc_decimal_matches_across_calls(lab) -> None:
    """MF 26 (N12): the printed value of calc.third, copied into a later
    `filter`, matches its row; `order_by="-calc.third"` sorts by it.
    Base: no compute."""
    _usage(lab, 6)
    mapped = lab.ok("map", {**SITES, "compute": {"third": "attrs.static_used / 3"}})
    printed = mapped["items"][4]["calc_third"]
    handle = _handle(mapped)
    body = lab.ok("filter", {"input": handle, "where": {"calc.third": printed}})
    assert [i["node_id"] for i in body["items"]] == [f"site:{site(4)}"]
    ranked = lab.ok("filter", {"input": handle, "order_by": "-calc.third", "limit": 2,
                               "select": ["node_id", "calc.third"]})
    assert [i["node_id"] for i in ranked["items"]] == [f"site:{site(5)}", f"site:{site(4)}"]


# ============================================================ 24: served examples


def test_served_map_examples_execute(lab) -> None:
    """MF 24: the `map` description's example chain runs against the fake
    system (its fields named as this fixture names them). Base: the
    description has no compute example."""
    import re

    import anyio

    _usage(lab, 6)
    tools = {t.name: t.description or "" for t in anyio.run(lab.mcp.list_tools)}
    description = tools["map"]
    found = re.search(r"compute=(\{[^}]*\})", description)
    assert found, description
    compute = json.loads(found.group(1).replace("attrs.used", "attrs.static_used")
                         .replace("attrs.total", "attrs.static_total"))
    handle = _handle(lab.ok("map", {**SITES, "compute": compute}))
    order = re.search(r'order_by="([^"]+)"', description).group(1)
    ranked = lab.ok("filter", {"input": handle, "order_by": order, "limit": 5})
    assert ranked["items"][0]["node_id"] == f"site:{site(5)}"
    metric = re.search(r'metrics=\["([^"]+)"\]', description).group(1).replace("calc.x", "calc.fill")
    total = lab.ok("aggregate", {"input": handle, "by": [], "metrics": [metric]})
    assert total["groups"][0]["sum(calc.fill) floats"] == pytest.approx(
        sum(_fill(i) for i in range(6)),
    )


# ============================================================ raw rows, chains


def test_map_over_a_raw_expand_answer_and_its_record_column_refusal(world) -> None:  # noqa: F811
    """MF raw: a kept `expand(live=...)` answer takes computed columns over
    its own fields; `node_id` (raw rows have none) is refused by name.
    Base: no compute."""
    world["adapter"].rows = [{"name": f"r{i}", "size": i * 1.5} for i in range(4)]
    body = _ok(world["mcp"], "expand", {
        "input": {"node_id": world["hub"]}, "direction": "in",
        "live": {"relation": "lrs_rows", "args": {"q": "x"}},
    })
    ref = body["external_live"]["result_reference"]["id"]
    mapped = _ok(world["mcp"], "map", {"input": ref, "compute": {"double": "attrs.size * 2"}})
    assert [i["calc_double"] for i in mapped["items"]] == [0, 3, 6, 9]
    page = _ok(world["mcp"], "filter", {
        "input": mapped["handle"]["id"], "where": {"calc.double": {"op": "gt", "value": 4}},
    })
    assert [unfence(i["attrs"]["name"]) for i in page["items"]] == ["r2", "r3"]
    assert all("\x00calc" not in json.dumps(i) for i in page["items"])
    error = _refused(world["mcp"], "map", {"input": ref, "compute": {"n": "len(node_id)"}})
    assert error["data"]["reason"] == "record_column_on_raw_rows", error
    assert len(world["adapter"].calls) == 1


def test_a_float_condition_still_matches_an_upstream_float(world) -> None:  # noqa: F811
    """28 (R4, regression): part 1's `{"attrs.x": 0.1}` on an upstream 0.1
    matches, and `order_by` sorts as before."""
    world["adapter"].rows = [{"name": "a", "x": 0.1}, {"name": "b", "x": 0.3},
                             {"name": "c", "x": 0.2}]
    body = _ok(world["mcp"], "expand", {
        "input": {"node_id": world["hub"]}, "direction": "in",
        "live": {"relation": "lrs_rows", "args": {"q": "x"}},
    })
    ref = body["external_live"]["result_reference"]["id"]
    hit = _ok(world["mcp"], "filter", {"input": ref, "where": {"attrs.x": 0.1}})
    assert [unfence(i["attrs"]["name"]) for i in hit["items"]] == ["a"]
    ranked = _ok(world["mcp"], "filter", {"input": ref, "order_by": "attrs.x"})
    assert [unfence(i["attrs"]["name"]) for i in ranked["items"]] == ["a", "c", "b"]


@pytest.fixture
def chain_lab(clean_runtime: str, tmp_path, monkeypatch) -> Iterator[Any]:
    """The chain-handle fixture of `test_live_handle_lists_db.py`, built from
    its own helpers."""
    from okg.substrate.mcp.server import build_server
    from tests.substrate.mcp import test_live_handle_lists_db as chains

    clock = chains.Clock()
    previous = live_result_sets.install_live_store(live_result_sets.new_store(clock=clock))
    try:
        hub = chains._publish(clean_runtime)  # noqa: SLF001
        chains._deployment(tmp_path)  # noqa: SLF001
        monkeypatch.setenv("OKG_DEPLOYMENTS_DIR", str(tmp_path))
        monkeypatch.setenv("OKG_LIVEBIND_DSN", clean_runtime)
        upstream = chains.Upstream()

        def runner(**kwargs: Any) -> Any:
            return upstream(**kwargs)

        mcp = build_server(dsn=clean_runtime, live_adapters={"http": runner},
                           auto_advance="always", deployment_name="default")
        mcp._okg_live_gate = chains.CountingGate()  # type: ignore[attr-defined]
        try:
            yield chains.Lab(mcp, upstream, hub, clean_runtime, clock)
        finally:
            for attr in ("_okg_pool", "_okg_write_pool"):
                pool = getattr(mcp, attr, None)
                if pool is not None:
                    pool.close()
    finally:
        live_result_sets.install_live_store(previous)


def test_map_over_a_chain_handle_computes_over_every_answer(chain_lab) -> None:
    """MF chain: a chain handle (#4156) is a map input; its combined rows
    get the computed column with no upstream call, and the map's handle
    names the chain as `from`. Base: no compute."""
    from tests.substrate.mcp.test_live_handle_lists_db import _chain_of, _hint

    lab = chain_lab
    lab.upstream.sizes.update({"a": 7, "b": 11, "c": 5})

    async def script(client: Any) -> Any:
        answers = await _chain_of(client, lab, ["a", "b", "c"])
        chain = _hint(answers[-1])["chain"]
        calls = lab.upstream.total()
        mapped = await client.ok("map", {"input": chain, "compute": {
            "mb": "attrs.size * attrs.files",
        }})
        total = await client.ok("aggregate", {
            "input": mapped["handle"]["id"], "by": [], "metrics": ["sum(calc.mb)"],
        })
        return chain, calls, lab.upstream.total(), mapped, total

    chain, before, after, mapped, total = lab.sessions(1, script)
    assert before == after
    assert sorted(i["calc_mb"] for i in mapped["items"]) == [15, 21, 33]
    assert mapped["handle"]["from"] == chain
    assert total["groups"][0]["sum(calc.mb)"] == 69


def test_a_column_whose_inputs_were_read_minutes_apart_says_so(lab, monkeypatch) -> None:
    """MF 9b (6.2 read times): a column that reads a listing field and a
    field grown two minutes later names both read times. Base: no compute."""
    import datetime as dt

    from okg.substrate.mcp import live_resolver

    _usage(lab, 4)
    ref = _handle(lab.ok("filter", SITES))
    later = (dt.datetime.now(dt.timezone.utc) + dt.timedelta(seconds=120)).strftime(
        "%Y-%m-%dT%H:%M:%SZ",
    )
    monkeypatch.setattr(live_resolver, "now_text", lambda: later)
    body = lab.ok("map", {"input": ref, "compute": {
        "cold_used": "if(attrs.kind == 'COLD', attrs.static_used, 0)",
    }})
    mixed = body["computed"]["calc.cold_used"]["mixes_reads"]
    assert mixed[1] == later and mixed[0] < later
    text = _text(lab, "map", {"input": ref, "compute": {
        "cold_used": "if(attrs.kind == 'COLD', attrs.static_used, 0)",
    }})
    assert f"calc.cold_used mixes values read at {mixed[0][11:16]} and {later[11:16]}" in text


def test_a_select_naming_an_unknown_computed_column_is_refused_before_any_call(lab) -> None:
    """MF 4b (failure case): `select=["calc.fil"]` beside compute={"fill": ...}
    is refused by name before the listing call, and before growth on a
    handle. Base: no compute."""
    _usage(lab, 4)
    error = lab.refused("map", {**SITES, "compute": FILL, "select": ["node_id", "calc.fil"]})
    assert error["data"]["reason"] == "calc_column_unknown", error
    assert lab.example.total() == 0
    ref = _handle(lab.ok("filter", SITES))
    error = lab.refused("map", {"input": ref, "compute": FILL, "select": ["calc.fil"]})
    assert error["data"]["reason"] == "calc_column_unknown", error
    assert lab.example.total() == 1


# ============================================================ code review fixes


def test_a_bare_self_name_over_a_map_handle_reads_the_input_column(lab) -> None:
    """MF review H1(a): over a handle holding calc.fill,
    compute={"fill": "fill * 100"} scales the input's fill (never reads an
    absent attrs.fill as null with state ok); no upstream call. Fails on the
    pre-review head: every value null, state ok."""
    _usage(lab, 5)
    first = _handle(lab.ok("map", {**SITES, "compute": FILL}))
    calls = lab.example.total()
    body = lab.ok("map", {"input": first, "compute": {"fill": "fill * 100"}})
    assert [i["calc_fill"] for i in body["items"]] == [0.3, 1.3, 2.3, 3.3, 4.3]
    assert body["computed"]["calc.fill"]["states"] == {"ok": 5}
    assert lab.example.total() == calls


def test_an_expression_field_no_row_holds_is_refused_before_any_call(world) -> None:  # noqa: F811
    """MF review H1(b) (failure case): an `attrs.` field no row of a kept
    raw answer holds is refused by name with the closest keys, never
    computed as null with state ok; no upstream call. Fails on the
    pre-review head: served as null, ok."""
    world["adapter"].rows = [{"name": f"r{i}", "size": i} for i in range(3)]
    body = _ok(world["mcp"], "expand", {
        "input": {"node_id": world["hub"]}, "direction": "in",
        "live": {"relation": "lrs_rows", "args": {"q": "x"}},
    })
    ref = body["external_live"]["result_reference"]["id"]
    error = _refused(world["mcp"], "map", {"input": ref, "compute": {"x": "attrs.sise * 2"}})
    assert error["data"]["reason"] == "key_absent_on_every_row", error
    assert error["data"]["key"] == "sise" and "size" in error["data"]["closest"]
    assert len(world["adapter"].calls) == 1


def test_a_column_named_like_a_held_or_declared_field_is_refused(lab) -> None:
    """MF review M1 (failure case): a computed column named like the open
    field `tier` the handle holds, or like a declared field, is refused by
    name with zero upstream calls. Fails on the pre-review head: served, and
    `tier` then hidden from every later read."""
    _usage(lab, 4)
    held = _handle(lab.ok("filter", {**SITES, "select": ["node_id", "attrs.tier"]}))
    calls = lab.example.total()
    error = lab.refused("map", {"input": held, "compute": {"tier": "lower(attrs.kind)"}})
    assert error["data"]["reason"] == "column_shadows_field", error
    error = lab.refused("map", {**SITES, "compute": {"static_used": "attrs.static_total"}})
    assert error["data"]["reason"] == "column_shadows_field", error
    assert lab.example.total() == calls


def test_a_date_meaning_on_a_handle_that_would_grow_is_refused_before_any_call(lab) -> None:
    """MF review M2 (failure case): `date.created` over a handle that lacks
    the usage fields is refused before growth fetches them. Fails on the
    pre-review head: one usage call per row, then the refusal."""
    _usage(lab, 6)
    ref = _handle(lab.ok("filter", SITES))
    calls = lab.example.total()
    error = lab.refused("map", {"input": ref, "compute": {
        "x": "days_between(date.created, '2026-10-01') + attrs.static_used",
    }})
    assert error["data"]["reason"] == "date_on_live_rows", error
    assert lab.example.total() == calls


def test_a_map_step_with_compute_inside_run_runs_as_its_own_call(lab) -> None:
    """Review L7 (pin of what is true): a live `map` step carrying `compute`
    inside `run` is sent through the tool, as each step is, so it computes
    exactly as the call alone does (one listing, one usage call per site).
    Steps over a run's held rows still refuse `compute` (3a-ii is paused)."""
    _usage(lab, 3)
    body = lab.ok("run", {"steps": [
        {"map": {"where": {"type": "site"}, "compute": FILL}},
    ]})
    step = body["steps"][0]
    assert step["status"] == "ok" and step["tool"] == "map", step
    assert [i["calc_fill"] for i in step["result"]["items"]] == [_fill(i) for i in range(3)]
    assert lab.example.made(LISTING) == 1 and lab.example.made(USAGE) == 3


# ============================================================ #4181: a refusal after growth


#: Needs `attrs.static_used`, which a plain `filter` handle lacks (growth),
#: and computes text, so a lowered output bound refuses it after the fetch.
GROW = {"k": "concat(node_id, '-', string(attrs.static_used))"}
#: Reads only the field GROW fetched.
USED = {"u": "attrs.static_used * 2"}


@contextlib.contextmanager
def _session(name: Any) -> Iterator[None]:
    token = SESSION.set(name)
    try:
        yield
    finally:
        SESSION.reset(token)


def _refused_after_growth(lab: Any, monkeypatch: Any, ref: str) -> dict[str, Any]:
    """`map(input=ref, compute=GROW)` with the computed-bytes bound lowered to
    64 bytes: growth fetches the usage fields, then evaluation refuses."""
    from okg.substrate.mcp import map_compute

    bound = map_compute.MAX_OUTPUT_BYTES
    monkeypatch.setattr(map_compute, "MAX_OUTPUT_BYTES", 64)
    try:
        return lab.refused("map", {"input": ref, "compute": GROW})
    finally:
        monkeypatch.setattr(map_compute, "MAX_OUTPUT_BYTES", bound)


def test_a_refusal_after_growth_names_the_kept_grown_handle(lab, monkeypatch) -> None:
    """MF #4181 (failure case): growth makes one usage call per row, then
    the computed output is refused (`map_output_too_large`). The refusal
    names the grown set, kept (`error.kept_handle`, its call and the retry
    hint), and a narrower map over it makes zero upstream calls. Base: the
    grown set is released with the refusal; no `kept_handle`."""
    _usage(lab, 12)
    ref = _handle(lab.ok("filter", SITES))
    assert lab.example.total() == 1
    store = live_result_sets.live_store()
    held = store.count()
    error = _refused_after_growth(lab, monkeypatch, ref)
    assert error["data"]["reason"] == "map_output_too_large", error
    assert lab.example.made(USAGE) == 12 and lab.example.total() == 13
    kept = error.get("kept_handle")
    assert isinstance(kept, dict), json.dumps(error)[:2000]
    grown = kept["id"]
    assert grown.startswith("l1.") and grown != ref
    assert kept["rows"] == 12 and kept["upstream_calls"] == 12
    assert "attrs.static_used" in kept["fields"]
    assert kept["template"] == {"tool": "map", "arguments": {"input": {"ref": grown}, "compute": GROW}}
    assert "refused again if sent unchanged" in kept["note"]
    assert error["data"]["kept_handle"] == grown
    assert error["fix_hint"].startswith(f"map over the kept handle {grown} (no upstream call): ")
    # Committed like any kept set (one more entry), with no reservation left.
    assert store.count() == held + 1 and store.pending_count() == 0
    narrower = lab.ok("map", {"input": grown, "where": {"attrs.kind": "COLD"}, "compute": GROW})
    assert len(narrower["items"]) == 3 and narrower["handle"]["from"] == grown
    full = lab.ok("map", {"input": grown, "compute": USED})
    assert [i["calc_u"] for i in full["items"]] == [2 * (10 * i + 3) for i in range(12)]
    assert lab.example.total() == 13, "the kept grown set holds the fetched fields"


def test_the_refusal_text_names_the_kept_handle_in_its_retry_line(lab, monkeypatch) -> None:
    """MF #4181: the text form says where the fetched fields are kept on the
    line a reader meets first. Base: the retry line is the bound's hint."""
    from okg.substrate.mcp import map_compute

    _usage(lab, 6)
    ref = _handle(lab.ok("filter", SITES))
    monkeypatch.setattr(map_compute, "MAX_OUTPUT_BYTES", 64)
    result = lab.call("map", {"input": ref, "compute": GROW}, fmt="text")
    assert result.isError is True
    text = result.content[0].text
    assert "retry: map over the kept handle l1." in text, text[:2000]
    assert "(no upstream call)" in text and "kept_handle" in text


def test_a_field_absent_from_every_row_after_growth_keeps_the_grown_handle(lab) -> None:
    """MF #4181 (failure case): an open field misspelled (`tierr`) is
    fetched by one open-fields call per row, then refused as absent from
    every row (`key_absent_on_every_row`); the refusal names the kept grown
    set, and the corrected map over it makes zero upstream calls. Base: no
    `kept_handle`; a retry pays the calls again."""
    _usage(lab, 6)
    ref = _handle(lab.ok("filter", SITES))
    calls = lab.example.total()
    error = lab.refused("map", {"input": ref, "compute": {"x": "is_null(attrs.tierr)"}})
    assert error["data"]["reason"] == "key_absent_on_every_row", error
    made = lab.example.total() - calls
    assert made == 6, "one open-fields call per row"
    kept = error.get("kept_handle")
    assert isinstance(kept, dict), json.dumps(error)[:2000]
    assert kept["upstream_calls"] == made and kept["rows"] == 6
    assert "attrs.tierr" in kept["fields_on_no_row"] and "attrs.tierr" not in kept["fields"]
    before = lab.example.total()
    body = lab.ok("map", {"input": kept["id"], "compute": {"x": "is_null(attrs.tier)"}})
    assert body["computed"]["calc.x"]["states"] == {"ok": 6}
    assert lab.example.total() == before


def test_the_kept_grown_handle_is_bound_to_its_session_and_expires(lab, monkeypatch, clock) -> None:
    """MF #4181 (N5): the kept grown set is a live handle like any other:
    another logical session hears `unknown`, its own session reads it with
    zero upstream calls, and after its 600 s it refuses `expired`. Base: no
    `kept_handle` in the refusal."""
    _usage(lab, 5)
    with _session("s1"):
        ref = _handle(lab.ok("filter", SITES))
        grown = _refused_after_growth(lab, monkeypatch, ref)["kept_handle"]["id"]
    calls = lab.example.total()
    with _session("s2"):
        error = lab.refused("map", {"input": grown, "compute": USED})
        assert error["data"]["reason"] == "unknown", error
    with _session("s1"):
        body = lab.ok("map", {"input": grown, "compute": USED})
        assert [i["calc_u"] for i in body["items"]] == [2 * (10 * i + 3) for i in range(5)]
    assert lab.example.total() == calls
    clock.now += 601
    with _session("s1"):
        error = lab.refused("map", {"input": grown, "compute": USED})
        assert error["data"]["reason"] == "expired", error


def _store_with(lab: Any, clock: Any, **limits: Any) -> Any:
    """A fresh live store with lowered limits, installed for this test."""
    import dataclasses

    from okg.substrate.mcp import result_references as rr

    store = rr.ResultReferenceStore(
        clock=clock, limits=dataclasses.replace(live_result_sets.LIMITS, **limits),
    )
    live_result_sets.install_live_store(store)
    return store


def test_a_full_store_at_mint_names_the_grown_set_as_not_kept(lab, monkeypatch, clock) -> None:
    """MF #4181 review M1 (failure case): the store really refuses the grown
    set (its rows pass the per-handle byte cap, which the source set fits).
    The refusal says the fetched fields were not kept and why, names no id,
    and the store holds what it held, with no reservation left. Base: no
    `kept_handle`."""
    import dataclasses

    _usage(lab, 4)
    store = _store_with(lab, clock)
    ref = _handle(lab.ok("filter", SITES))
    held, source_bytes = store.count(), store.total_bytes()
    store.limits = dataclasses.replace(store.limits, max_reference_bytes=source_bytes + 8)
    error = _refused_after_growth(lab, monkeypatch, ref)
    assert lab.example.made(USAGE) == 4
    kept = error.get("kept_handle")
    assert isinstance(kept, dict), json.dumps(error)[:2000]
    assert kept["id"] is None and kept["reason"] == "reference_bytes", kept
    assert kept["upstream_calls"] == 4 and "could not be kept" in kept["note"]
    assert "kept handle" not in str(error.get("fix_hint"))
    assert store.count() == held and store.pending_count() == 0
    assert len(lab.ok("filter", {"input": ref})["items"]) == 4, "nothing was evicted"


def test_a_full_store_at_commit_names_the_grown_set_as_not_kept(lab, monkeypatch, clock) -> None:
    """MF #4181 review M1 (failure case): the mint succeeds and the commit is
    refused (the caller's byte quota falls below the set between the two, as
    a concurrent change can make it): the reservation is given back, nothing
    is evicted, and the refusal names no id. Base: no `kept_handle`."""
    import dataclasses

    _usage(lab, 4)
    store = _store_with(lab, clock)
    ref = _handle(lab.ok("filter", SITES))
    held = store.count()
    commit = store.commit
    refused_commits: list[str] = []

    def commit_after_a_change(draft: Any) -> Any:
        if refused_commits:
            return commit(draft)
        limits = store.limits
        store.limits = dataclasses.replace(limits, max_quota_bytes=draft.canonical_bytes - 1)
        try:
            return commit(draft)
        except Exception as exc:
            refused_commits.append(type(exc).__name__)
            raise
        finally:
            store.limits = limits

    monkeypatch.setattr(store, "commit", commit_after_a_change)
    error = _refused_after_growth(lab, monkeypatch, ref)
    assert refused_commits == ["ResultReferenceCapacity"]
    kept = error.get("kept_handle")
    assert isinstance(kept, dict), json.dumps(error)[:2000]
    assert kept["id"] is None and kept["reason"] == "reader_bytes", kept
    assert store.count() == held and store.pending_count() == 0
    assert len(lab.ok("filter", {"input": ref})["items"]) == 4, "nothing was evicted"


def test_at_the_cap_the_kept_set_evicts_only_the_callers_own_oldest_handle(
    lab, monkeypatch, clock,
) -> None:
    """MF #4181 review M3: a refused call that grew takes one handle slot.
    At two handles per session, s1's kept set evicts s1's oldest handle;
    s1's other handle and s2's handle are untouched. Base: the refusal kept
    nothing and evicted nothing."""
    _usage(lab, 6)
    _store_with(lab, clock, max_per_reader=2)
    with _session("s1"):
        oldest = _handle(lab.ok("filter", SITES))
        clock.now += 1
        other = _handle(lab.ok("filter", {**SITES, "select": ["node_id", "attrs.kind"]}))
    with _session("s2"):
        theirs = _handle(lab.ok("filter", SITES))
    clock.now += 1
    with _session("s1"):
        kept = _refused_after_growth(lab, monkeypatch, other)["kept_handle"]["id"]
        assert kept is not None
        assert lab.refused("filter", {"input": oldest})["data"]["reason"] == "evicted"
        assert len(lab.ok("filter", {"input": other})["items"]) == 6
        assert len(lab.ok("map", {"input": kept, "compute": USED})["items"]) == 6
    with _session("s2"):
        assert len(lab.ok("filter", {"input": theirs})["items"]) == 6


def test_another_reader_or_a_caller_who_lost_access_cannot_read_the_kept_handle(
    lab, monkeypatch,
) -> None:
    """MF #4181 review M2: another reader (another authority) hears
    `unknown` for the kept id with zero upstream calls; a caller who lost
    access is refused by the caller re-check. Base: no `kept_handle`."""
    from tests.substrate.mcp.test_live_graph_keep import _deny_expand_only, _gate

    _usage(lab, 5)
    ref = _handle(lab.ok("filter", SITES))
    kept = _refused_after_growth(lab, monkeypatch, ref)["kept_handle"]["id"]
    calls = lab.example.total()
    error = lab.refused("map", {"input": kept, "compute": USED, "authority": {"note": "other"}})
    assert error["data"]["reason"] == "unknown", error
    with _gate(lab, _deny_expand_only):
        error = lab.refused("map", {"input": kept, "compute": USED})
    assert error["code"] == "nomos_identity_denied", error
    assert lab.example.total() == calls
    assert len(lab.ok("map", {"input": kept, "compute": USED})["items"]) == 5


def test_a_server_fault_after_growth_names_the_kept_handle(lab, monkeypatch) -> None:
    """MF #4181 review L1 (failure case): an untyped fault in the tool body
    after growth is served as `operator_execution_failed`, and still names
    the kept set, which a later map reads with zero upstream calls. Base: no
    `kept_handle`."""
    from okg.substrate.mcp import map_compute

    _usage(lab, 4)
    ref = _handle(lab.ok("filter", SITES))
    computed_rows = map_compute.computed_rows

    def fault(*args: Any, **kwargs: Any) -> Any:
        raise RuntimeError("simulated fault after growth")

    monkeypatch.setattr(map_compute, "computed_rows", fault)
    error = lab.refused("map", {"input": ref, "compute": USED})
    monkeypatch.setattr(map_compute, "computed_rows", computed_rows)
    assert error["code"] == "operator_execution_failed", error
    kept = error.get("kept_handle")
    assert isinstance(kept, dict) and kept["id"], json.dumps(error)[:2000]
    calls = lab.example.total()
    assert len(lab.ok("map", {"input": kept["id"], "compute": USED})["items"]) == 4
    assert lab.example.total() == calls
