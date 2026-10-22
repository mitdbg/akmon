"""Two read fixes: compute over live rows, and read matching lines.

1. Compute over live rows. ``expand(live={..., "each": {arg: [values]}})``
   asks a relation once per value in one kept answer, and ``aggregate`` over
   that answer's ref adds ``sum``/``min``/``max`` of a column, with ``by``
   optional for one total (an agent that added 30 sizes by hand).
2. Read matching lines. ``inspect(target={"kind": "text", ...,
   "matching": ..., "context": n})`` serves only the lines that match and
   ``n`` lines around each, numbered as ``lines`` takes them (the shell's ``rg -n`` then ``sed -n``).

The first part of each is hermetic; the served reads over a real publish are
Postgres-marked.
"""
from __future__ import annotations

from typing import Any

import pytest

from okg.substrate.external_backed.live_dispatch import (
    EACH_WORKERS,
    MAX_EACH_VALUES,
    LiveAdapterError,
    LiveArgumentsInvalid,
    dispatch_live_relation,
    each_deadline,
    live_reserve_envelope,
    validate_each,
)
from okg.substrate.external_backed.live_relations import compile_args_model
from okg.substrate.mcp import live_result_sets, result_text
from okg.substrate.mcp.live_result_sets import LiveResultPredicateUnsupported
from okg.substrate.mcp.record_text import (
    MAX_LINE_RANGES,
    MAX_MATCHING_CONTEXT,
    read_record_text,
)
from okg.substrate.mcp.response_bounds import InvalidOperatorArguments
from tests.substrate.mcp.test_live_result_sets import (  # noqa: F401 (fixtures)
    CountingAdapter,
    RecordingSampler,
    _bound,
    _gate,
    _ok,
    _refused,
    _rows,
    live_store,
    world,
)
from tests.substrate.mcp.test_record_text import SESSION, _install, _windows

# `live_store` and `world` are pytest fixtures imported from test_live_result_sets;
# the tests below take them by parameter name. Naming them here once keeps ruff
# from reading each parameter as a redefinition of an unused import.
_IMPORTED_FIXTURES = (live_store, world)

# ============================================================ 1. compute


def test_metrics_parse_count_and_sum_min_max_of_one_column() -> None:
    assert live_result_sets.check_metrics(None) == [("count", None)]
    assert live_result_sets.check_metrics(["count", "sum(attrs.size)", "max(attrs.size)"]) == [
        ("count", None), ("sum", "size"), ("max", "size"),
    ]


@pytest.mark.parametrize("bad", [
    ["avg(attrs.size)"], ["sum(size)"], ["sum(attrs.a.b)"], ["count", "count"],
    ["sum(attrs.size)"] * 2, ["count", "sum(attrs.a)", "min(attrs.a)", "max(attrs.a)",
                                  "sum(attrs.b)"],
])
def test_a_bad_metric_is_refused_by_name(bad: list[str]) -> None:
    with pytest.raises(LiveResultPredicateUnsupported) as excinfo:
        live_result_sets.check_metrics(bad)
    assert excinfo.value.data["argument"] == "metrics"


def test_a_sum_is_exact_skips_what_is_not_a_number_and_says_how_many() -> None:
    sizes = [75555308598524, 75546107841017, 75511158138987]
    rows = [{"item": f"/D{i}", "file_size": size} for i, size in enumerate(sizes)]
    rows += [{"item": "/none"}, {"item": "/null", "file_size": None},
             {"item": "/text", "file_size": "big"}, {"item": "/str", "file_size": "5"}]
    metrics = live_result_sets.check_metrics(
        ["count", "sum(attrs.file_size)", "min(attrs.file_size)", "max(attrs.file_size)"],
    )
    groups, skipped = live_result_sets.group_metrics(rows, [], metrics)
    assert groups == [((), {
        "count": 7,
        "sum(attrs.file_size)": sum(sizes) + 5,
        "min(attrs.file_size)": 5,
        "max(attrs.file_size)": max(sizes),
    })]
    assert skipped == {"attrs.file_size": {"missing": 2, "not_numeric": 1}}


def test_metrics_per_group_and_a_group_with_no_number_gets_null() -> None:
    rows = [{"k": "a", "v": 1}, {"k": "a", "v": 2}, {"k": "b", "v": None}]
    groups, _ = live_result_sets.group_metrics(
        rows, ["attrs.k"], live_result_sets.check_metrics(["count", "sum(attrs.v)"]),
    )
    assert groups == [(("a",), {"count": 2, "sum(attrs.v)": 3}),
                      (("b",), {"count": 1, "sum(attrs.v)": None})]


def test_by_may_be_omitted_but_not_over_four_fields() -> None:
    assert live_result_sets.check_by(None) == []
    with pytest.raises(LiveResultPredicateUnsupported):
        live_result_sets.check_by([f"attrs.f{i}" for i in range(5)])


def test_each_validates_every_value_and_lists_them_in_the_query() -> None:
    bound = _bound()
    model = compile_args_model(bound.relation)
    key, per_value = validate_each(bound, {"size": 3}, {"q": ["a", "b"]}, args_model=model)
    assert key == "q"
    assert per_value == [{"q": "a", "size": 3}, {"q": "b", "size": 3}]
    reserve = live_reserve_envelope(
        bound=bound, args_model=model, args={"size": 3}, each={"q": ["a", "b"]},
    )
    assert reserve["query"] == {"q": ["a", "b"], "size": 3} and reserve["each"] == "q"


@pytest.mark.parametrize("each, args, said", [
    ({"q": "a"}, {}, "list of 1-"),
    ({"q": ["a"], "size": [1]}, {}, "list of 1-"),
    ({"q": []}, {}, "list of 1-"),
    ({"q": [str(i) for i in range(MAX_EACH_VALUES + 1)]}, {}, "list of 1-"),
    ({"nope": ["a"]}, {}, "does not declare"),
    ({"q": ["a"]}, {"q": "b"}, "both give"),
    ({"q": ["a", "a"]}, {}, "twice"),
    ({"q": ["x" * 201]}, {}, "refused its arguments"),
])
def test_a_bad_each_is_refused_before_any_call(each, args, said) -> None:
    bound = _bound()
    adapter = CountingAdapter(rows=[{"n": 1}])
    with pytest.raises(LiveArgumentsInvalid) as excinfo:
        dispatch_live_relation(
            bound=bound, args_model=compile_args_model(bound.relation), args=args,
            node_id="node-1", gate=_gate(True), adapter=adapter,
            write_sample=RecordingSampler(), generation_id="7", room=None, each=each,
        )
    assert said in str(excinfo.value)
    assert adapter.calls == []


def _each_dispatch(adapter: Any, values: list[str]) -> dict[str, Any]:
    bound = _bound()
    return dispatch_live_relation(
        bound=bound, args_model=compile_args_model(bound.relation), args={},
        node_id="node-1", gate=_gate(True), adapter=adapter,
        write_sample=RecordingSampler(), generation_id="7", room=None,
        each={"q": values},
    )


def test_each_calls_once_per_value_and_tags_rows_in_order() -> None:
    class PerValue:
        def __init__(self) -> None:
            self.calls: list[dict[str, Any]] = []

        def __call__(self, **kwargs: Any) -> Any:
            self.calls.append(kwargs)
            q = kwargs["args"]["q"]
            if q == "two":
                return [{"n": 2}, {"n": 3, "q": "kept"}]
            return {"n": len(q)}

    adapter = PerValue()
    envelope = _each_dispatch(adapter, ["a", "two", "abc"])
    assert envelope["state"] == "ok"
    assert sorted(call["args"]["q"] for call in adapter.calls) == ["a", "abc", "two"]
    assert all(call["deadline_seconds"] <= 5.0 for call in adapter.calls)
    unfenced = [
        {k: (v.split("\n")[1] if isinstance(v, str) and "\n" in v else v) for k, v in row.items()}
        for row in envelope["rows"]
    ]
    assert unfenced == [
        {"q": "a", "n": 1}, {"q": "two", "n": 2}, {"n": 3, "q": "kept"}, {"q": "abc", "n": 3},
    ]
    assert envelope["query"]["q"] == ["a", "two", "abc"] and envelope["each"] == "q"


def test_one_failed_value_fails_the_answer_and_names_the_value() -> None:
    def adapter(**kwargs: Any) -> Any:
        if kwargs["args"]["q"] == "bad":
            raise LiveAdapterError("upstream_error", excerpt="503 from upstream")
        return [{"n": 1}]

    envelope = _each_dispatch(adapter, ["ok", "bad"])
    assert envelope["state"] == "error"
    assert "q='bad'" in envelope["error"]["excerpt"]


def test_the_fan_out_deadline_grows_per_round_up_to_a_cap() -> None:
    assert each_deadline(5, 1) == 5.0
    assert each_deadline(5, EACH_WORKERS) == 5.0
    assert each_deadline(5, EACH_WORKERS + 1) == 10.0
    assert each_deadline(10, MAX_EACH_VALUES) == 180.0


def test_a_kept_fanned_out_answer_names_the_total_call() -> None:
    live = {
        "relation": "hits", "state": "ok", "each": "q", "rows": [{"n": 1}],
        "result_reference": {"retained": True, "id": "l1.a.b.c.d"},
    }
    text = result_text.render(
        {"status": "ok", "neighbors": [], "external_live": live}, tool="expand",
    )
    assert 'aggregate(input={"ref": "l1.a.b.c.d"}, by=[], metrics=["sum(attrs.<field>)"])' in text


def test_redo_call_puts_a_listed_argument_back_in_each() -> None:
    call = live_result_sets.redo_call("n", "hits", {"q": ["a", "b"], "size": 3}, "q")
    assert call["arguments"]["live"] == {
        "relation": "hits", "each": {"q": ["a", "b"]}, "args": {"size": 3},
    }


@pytest.mark.postgres
def test_a_fanned_out_answer_is_kept_and_summed_exactly(world) -> None:
    mcp = world["mcp"]
    body = _ok(mcp, "expand", {
        "input": {"node_id": world["hub"]}, "direction": "in",
        "live": {"relation": "lrs_rows", "each": {"q": ["x", "y", "z"]}},
        "max_bytes": 16_384,
    })
    live = body["external_live"]
    assert len(world["adapter"].calls) == 3
    set_id = live["result_reference"]["id"]
    total = _ok(mcp, "aggregate", {
        "input": {"ref": set_id}, "by": [], "metrics": ["count", "sum(attrs.size)", "max(attrs.size)"],
    })
    (group,) = total["groups"]
    expected = 3 * sum(row["size"] for row in _rows(2_000))
    assert group["count"] == 6_000 and group["sum(attrs.size)"] == expected
    assert group["max(attrs.size)"] == 1_999
    assert total["rows_counted"] == 6_000 and total["by"] == []
    assert total["totals_cover"].startswith(
        "6000 rows of lrs_rows(q=<3 values>, size=10), one call per q (3 values)",
    )
    per_value = _ok(mcp, "aggregate", {
        "input": {"ref": set_id}, "by": ["attrs.q"], "metrics": ["count"],
    })
    assert sorted(g["count"] for g in per_value["groups"]) == [2_000] * 3
    # The adapter is not called again by either read.
    assert len(world["adapter"].calls) == 3


@pytest.mark.postgres
def test_graph_aggregate_still_needs_by_and_counts_only(world) -> None:
    mcp = world["mcp"]
    no_by = _refused(mcp, "aggregate", {
        "input": {"kind": "nodes", "subtype": "page"}, "by": [], "metrics": ["count"],
    })
    assert "by must name" in no_by["message"]
    summed = _refused(mcp, "aggregate", {
        "input": {"kind": "nodes", "subtype": "page"}, "by": ["subtype"],
        "metrics": ["sum(attrs.size)"],
    })
    assert "kept live answer" in summed["message"]


# ============================================================ 2. matching lines

#: Sixty numbered lines; every tenth says "ten".
SOURCE = "".join(
    f"line {i:02d}{' ten' if i % 10 == 0 else ''} of the file\n" for i in range(1, 61)
)


def _page(monkeypatch: pytest.MonkeyPatch, text: str = SOURCE, **kwargs: Any) -> dict[str, Any]:
    _install(monkeypatch, _windows(text, 137, 0))
    return read_record_text(None, SESSION, "doc:1", max_bytes=65536, **kwargs)


def _text(page: dict[str, Any]) -> str:
    return "".join(item["text"] for item in page["items"])


def test_matching_serves_each_match_with_its_context_as_source_lines(monkeypatch) -> None:
    page = _page(monkeypatch, matching="TEN", context=1)
    assert page["lines"]["served"] == [[9, 11], [19, 21], [29, 31], [39, 41], [49, 51], [59, 60]]
    assert page["matching"] == {
        "pattern": "TEN", "context": 1, "matched": 6,
        "matched_lines": [10, 20, 30, 40, 50, 60], "windows": 6, "windows_shown": 6,
    }
    assert _text(page).startswith("line 09 of the file\nline 10 ten of the file\n")
    head = result_text.render(page, tool="inspect", kind="text").split("\n", 1)[0]
    assert "source lines 9-11, 19-21, 29-31, 39-41, 49-51, 59-60 of 60" in head


def test_matching_windows_that_touch_merge_and_context_defaults_to_two(monkeypatch) -> None:
    page = _page(monkeypatch, matching="re:/line 0[3-5] /")
    assert page["lines"]["served"] == [[1, 7]]
    assert page["matching"]["matched_lines"] == [3, 4, 5]
    assert page["matching"]["context"] == 2


def test_a_regex_match_is_case_insensitive_and_context_zero_is_the_line(monkeypatch) -> None:
    page = _page(monkeypatch, matching="re:/LINE (07|33) /", context=0)
    assert page["lines"]["served"] == [[7, 7], [33, 33]]
    assert _text(page) == "line 07 of the file\nline 33 of the file\n"


def test_no_match_says_so_and_serves_no_text(monkeypatch) -> None:
    page = _page(monkeypatch, matching="no such words")
    assert page["items"] == []
    assert page["matching"]["matched"] == 0
    assert page["outcome"]["code"] == "no_line_matches"
    assert page["lines"]["total"] == 60


def test_past_the_window_cap_the_page_names_the_first_unshown_match(monkeypatch) -> None:
    text = "".join(f"row {i:03d}{' hit' if i % 3 == 0 else ''}\n" for i in range(1, 121))
    page = _page(monkeypatch, text, matching="hit", context=0)
    block = page["matching"]
    assert block["matched"] == 40 and block["windows"] == 40
    assert block["windows_shown"] == MAX_LINE_RANGES == len(page["lines"]["served"])
    assert block["unshown_from_line"] == 63 and block["unshown_matches"] == 20


def test_a_small_page_continues_through_the_matching_windows(monkeypatch) -> None:
    _install(monkeypatch, _windows(SOURCE, 137, 0))
    first = read_record_text(None, SESSION, "doc:1", matching="ten", context=3, max_bytes=4096)
    seen = _text(first)
    token = first.get("continuation_token")
    while token:
        page = read_record_text(
            None, SESSION, "doc:1", matching="ten", context=3,
            continuation_token=token, max_bytes=4096,
        )
        seen += _text(page)
        token = page.get("continuation_token")
    lines = SOURCE.splitlines(keepends=True)
    wanted = [n for n in range(1, 61) if any(abs(n - m) <= 3 for m in (10, 20, 30, 40, 50, 60))]
    assert seen == "".join(lines[n - 1] for n in wanted)


@pytest.mark.parametrize("kwargs, field", [
    ({"matching": "x", "lines": [[1, 2]]}, "target.lines"),
    ({"context": 2}, "target.context"),
    ({"matching": "x", "context": MAX_MATCHING_CONTEXT + 1}, "target.context"),
    ({"matching": "x", "context": True}, "target.context"),
    ({"matching": ""}, "target.matching"),
    ({"matching": "x" * 201}, "target.matching"),
    ({"matching": "re:/(unclosed/"}, "target.matching"),
    ({"matching": "re:/(a+)+/"}, "target.matching"),
])
def test_a_bad_matching_read_is_refused_by_field(monkeypatch, kwargs, field) -> None:
    _install(monkeypatch, _windows(SOURCE, 137, 0))
    with pytest.raises(InvalidOperatorArguments) as excinfo:
        read_record_text(None, SESSION, "doc:1", max_bytes=65536, **kwargs)
    assert field in excinfo.value.data["invalid_fields"]


# ============================================================ follow-up (cut reads)


def test_a_fanned_out_reply_shows_at_most_twenty_rows_and_keeps_all(live_store) -> None:
    from okg.substrate.external_backed.live_dispatch import EACH_ROWS_SHOWN
    from okg.substrate.mcp import result_references
    from tests.substrate.mcp.test_live_result_sets import _hook, _room

    bound = _bound()
    values = [f"v{i:03d}" for i in range(60)]
    with result_references.draft_scope() as scope:
        envelope = dispatch_live_relation(
            bound=bound, args_model=compile_args_model(bound.relation), args={},
            node_id="node-1", gate=_gate(True), adapter=lambda **kw: [{"n": 1}],
            write_sample=RecordingSampler(), generation_id="7", room=_room(1_000_000),
            retain=_hook(live_store, bound), each={"q": values},
        )
        scope.finish({"external_live": envelope})
    assert envelope["state"] == "ok"
    assert len(envelope["rows"]) == EACH_ROWS_SHOWN
    assert envelope["result_reference"]["rows"] == 60
    assert "next" in envelope


def test_a_cut_page_read_says_which_lines_it_did_not_read() -> None:
    page = {
        "status": "ok", "node_id": "doc:1", "items": [], "truncated": True,
        "complete": False, "coverage": "complete",
        "lines": {"basis": "source", "total": 1856, "first": 1, "last": 353},
    }
    text = result_text.render(page, tool="inspect", kind="text")
    assert "! not read: lines 354-1856" in text
    page["truncated"] = False
    page["lines"]["last"] = 1856
    assert "not read" not in result_text.render(page, tool="inspect", kind="text")


@pytest.mark.postgres
def test_each_from_a_kept_column_refuses_past_the_value_cap(world) -> None:
    mcp = world["mcp"]
    kept = _ok(mcp, "expand", {
        "input": {"node_id": world["hub"]}, "direction": "in",
        "live": {"relation": "lrs_rows", "args": {"q": "x"}, "retain": True},
        "max_bytes": 16_384,
    })["external_live"]["result_reference"]["id"]
    refused = _refused(mcp, "expand", {
        "input": {"node_id": world["hub"]}, "direction": "in",
        "live": {"relation": "lrs_rows", "each": {"q": {"ref": kept, "field": "name"}}},
    })
    assert f"1-{MAX_EACH_VALUES} values" in refused["message"]
    bad = _refused(mcp, "expand", {
        "input": {"node_id": world["hub"]}, "direction": "in",
        "live": {"relation": "lrs_rows", "each": {"q": {"ref": kept, "field": "nope"}}},
    })
    assert "no value in 'nope'" in bad["message"]
    assert len(world["adapter"].calls) == 1


def test_a_misplaced_wildcard_is_not_called_no_wildcards() -> None:
    """`/*/Beta2026-v1/KIND` was refused as "wildcards are not
    accepted" although `/AB*/...` is accepted; the refusal says where."""
    from okg.substrate.external_backed.live_relations import (
        ANNOTATION_KEY,
        bind_live_relations,
        parse_live_bindings,
        parse_live_relations,
    )
    from tests.substrate.mcp.test_live_result_sets import RELATION_BODY

    body = {**RELATION_BODY, "args": {"item": {
        "type": "string", "required": True, "maxLength": 350,
        "pattern": "^/[A-Za-z0-9][A-Za-z0-9_.*-]{0,98}/[A-Za-z0-9_.*-]{1,199}/[A-Z0-9*-]{1,49}$",
    }}}
    declared = parse_live_relations({ANNOTATION_KEY: {"ds": body}}, subtype="page")
    bound = bind_live_relations(declared, parse_live_bindings(
        {"ds": {"kind": "http", "config": {"url": "http://localhost"}}},
    ))["ds"]
    model = compile_args_model(bound.relation)
    with pytest.raises(LiveArgumentsInvalid) as excinfo:
        live_reserve_envelope(bound=bound, args_model=model,
                              args={"item": "/*/Beta2026-v1/KIND"})
    assert "a wildcard is not accepted where it stands in item" in str(excinfo.value)
    live_reserve_envelope(bound=bound, args_model=model, args={"item": "/AB*/Beta2026-v1/KIND"})


# ============================================================ matching and context fixes


def test_a_plain_bar_reads_either_text_and_a_double_bar_is_literal(monkeypatch) -> None:
    page = _page(monkeypatch, matching="line 07|line 33", context=0)
    assert page["lines"]["served"] == [[7, 7], [33, 33]]
    text = "a || b\nplain a\n"
    literal = _page(monkeypatch, text, matching="a || b", context=0)
    assert literal["lines"]["served"] == [[1, 1]]


def test_matching_and_context_beside_target_move_into_it() -> None:
    from okg.substrate.mcp.read_grammar import rewrite_call

    call = rewrite_call("inspect", {
        "target": {"kind": "text", "node_id": "doc:1"}, "matching": "x", "context": 3,
    })
    args = call.arguments if hasattr(call, "arguments") else call["arguments"]
    assert args["target"] == {"kind": "text", "node_id": "doc:1", "matching": "x", "context": 3}
    assert "matching" not in args and "context" not in args


def test_where_ref_to_a_kept_answer_is_read_as_input() -> None:
    from okg.substrate.mcp.read_grammar import rewrite_call

    ref = "l1.aaaaaaaa.1792360456.bbbbbbbbbbbbbbbb.cccccccccccccccccccccc"
    call = rewrite_call("aggregate", {"where": {"ref": ref}, "by": [], "metrics": ["count"]})
    args = call.arguments if hasattr(call, "arguments") else call["arguments"]
    assert args["input"] == {"ref": ref} and "where" not in args


# ============================================================ what a total covers


def _kept(query: dict, each=None, each_from=None):
    return live_result_sets.RetainedSet(
        rows_json=b"[]", row_count=0, relation=_bound().relation, node_id="n",
        subject="", args_fingerprint="", observed_at="", source="mcp",
        deployment_name="d", session_id="s", query=query, each=each, each_from=each_from,
    )


def test_a_total_over_a_listing_fan_out_names_the_listing_pattern() -> None:
    """A /AB*/ listing's 142 items read as all 223."""
    kept = _kept(
        {"item": [f"/D{i}/Beta2026-v1/KIND" for i in range(142)], "valid_only": 1},
        each="item",
        each_from={"relation": "list_items",
                   "query": {"item": "/AB*/Beta2026-v1/KIND", "access_type": "VALID"}},
    )
    assert live_result_sets.totals_cover(kept, 142) == (
        "142 rows of hits(item=<142 values>, valid_only=1), one call per item "
        "(142 values from list_items(item=/AB*/Beta2026-v1/KIND, "
        "access_type=VALID)); nothing outside these calls is counted"
    )


def test_a_total_over_one_call_names_that_call() -> None:
    assert live_result_sets.totals_cover(_kept({"q": "x", "size": 10}), 40) == (
        "40 rows of hits(q=x, size=10); nothing outside these calls is counted"
    )


def test_the_cover_line_prints_first() -> None:
    page = {"status": "ok", "groups": [{"count": 3}], "by": [], "metrics": ["count"],
            "totals_cover": "3 rows of hits(q=x)"}
    text = result_text.render(page, tool="aggregate")
    assert "! totals cover: 3 rows of hits(q=x)" in text.split("count")[0]
    assert text.count("3 rows of hits(q=x)") == 1
