"""`run`'s steps that hold rows, hermetic (#3666, lane ri).

The step grammar and its limits, the totals over held rows (exact integer
sums, the `(absent)` group), minus and intersect, the call-by-call fan-out
of a live step (each value's outcome, the stop at the deadline, the shared
upstream cap, the byte bound), the H3 hint and the operator switch. The
server-driven tests are in ``test_run_live_db.py``.
"""
from __future__ import annotations

import threading
import time
from typing import Any

import pytest

from okg.substrate.external_backed import live_dispatch
from okg.substrate.external_backed.live_dispatch import (
    LiveAdapterError,
    LiveConcurrencyExhausted,
    UpstreamCutRows,
    each_adapter,
    run_live_calls,
)
from okg.substrate.external_backed.live_relations import (
    ANNOTATION_KEY,
    bind_live_relations,
    parse_live_bindings,
    parse_live_relations,
)
from okg.substrate.mcp import live_result_sets, live_run_hint, run_live, run_steps
from okg.substrate.mcp.run_steps import RunRefusal


def _steps(raw: list[Any]) -> list[run_steps.Step]:
    steps = run_steps.parse_steps(raw)
    run_steps.check_references(steps)
    run_steps.classify_steps(steps)
    return steps


ANCHOR = {"node_id": "n:hub"}
LIST = {"expand": {"input": ANCHOR, "live": {"relation": "fx_list", "args": {"pattern": "*"}}}}


def _each(source: Any) -> dict[str, Any]:
    return {"expand": {"input": ANCHOR, "live": {
        "relation": "fx_summary", "each": {"dataset": source},
    }}}


# ============================================================ grammar


def test_step_kinds_are_classified_before_any_step_runs() -> None:
    steps = _steps([
        LIST,
        _each("$1.dataset"),
        {"values": {"dataset": ["/A", "/B"]}},
        {"minus": ["$1", "$3"], "on": "dataset"},
        {"aggregate": {"input": "$2", "by": [], "metrics": ["count", "sum(file_size)"]}},
        {"filter": {"where": {"type": "page"}}},
    ])
    assert [s.kind for s in steps] == ["live", "live", "values", "set", "rows", "graph"]
    assert steps[3].arguments == {"inputs": [1, 3], "on": ["dataset", "dataset"]}
    assert run_steps.row_references(steps[1]) == {1}
    assert run_steps.row_references(steps[3]) == {1, 3}
    assert run_steps.row_references(steps[4]) == {2}
    assert run_steps.each_source(steps[1]) == ("dataset", (1, "dataset"))
    # `attrs.<column>` and the bare column are one column.
    (step,) = _steps([{"values": {"attrs.site": ["X"]}}])
    assert step.arguments == {"column": "site", "values": ["X"]}


@pytest.mark.parametrize("raw,needle", [
    ([LIST] * 9, "at most 8 steps"),
    ([LIST, _each("$1.dataset"), _each("$1.dataset"), _each("$1.dataset")],
     "at most 2 steps with live.each"),
    ([_each([f"/D{i}" for i in range(1_501)])], "1,501 values"),
    ([{"values": {"x": list(range(1_001))}}], "1 to 1,000 values"),
    ([LIST, {"minus": ["$1"], "on": "dataset"}], "2 to 6 steps"),
    ([LIST] * 7 + [{"minus": ["$1", "$2", "$3", "$4", "$5", "$6", "$7"], "on": "d"}],
     "2 to 6 steps"),
    ([LIST, LIST, {"minus": ["$1", "$2"]}], "compares rows on one column"),
    ([LIST, {"filter": {"input": "$1"}, "on": "x"}], "`on` belongs to a minus"),
    ([{"expand": {"input": ANCHOR, "live": {"relation": "fx_list", "retain": True}}}],
     "drop retain"),
    ([{"expand": {"input": ANCHOR, "edge_type": "cites",
                  "live": {"relation": "fx_list"}}}], "does not take edge_type"),
    ([LIST, _each({"ref": "l1.x", "field": "dataset"})], "names a kept answer"),
    ([{"filter": {"where": {"type": "page"}}}, _each("$1.attrs.dataset")],
     "graph set"),
    ([{"filter": {"where": {"type": "page"}}},
      {"aggregate": {"input": "$1", "by": [], "metrics": ["count"]}}],
     'write "where": "$1"'),
    ([_each("$2.dataset"), LIST], "names only earlier steps"),
    ([LIST, {"filter": {"input": "$1", "where": {"attrs.k": "$1.k"}}}], "names a set"),
    ([LIST, {"expand": {"input": "$1", "live": {"relation": "fx_list"}}}], "by id"),
])
def test_a_run_that_cannot_run_whole_is_refused_before_any_step(raw, needle) -> None:
    with pytest.raises(RunRefusal) as caught:
        _steps(raw)
    assert needle in caught.value.message + " " + caught.value.fix_hint


def test_the_wall_clock_is_100_s_and_capped_below_the_client_timeout() -> None:
    assert run_steps.run_wall_seconds({}) == 100.0
    assert run_steps.run_wall_seconds({"OKG_RUN_WALL_SECONDS": "30"}) == 30.0
    # The client's tool timeout is 120 s; the run answers 10 s before it.
    assert run_steps.run_wall_seconds({"OKG_RUN_WALL_SECONDS": "300"}) == 110.0
    for bad in ("abc", "0", "-5", "nan"):
        with pytest.raises(RunRefusal):
            run_steps.run_wall_seconds({"OKG_RUN_WALL_SECONDS": bad})


def test_the_operator_switch_reads_only_its_own_words() -> None:
    from okg.substrate.mcp.server import run_live_steps_enabled

    assert run_live_steps_enabled({}) is True
    assert run_live_steps_enabled({"OKG_MCP_RUN_LIVE_STEPS": "on"}) is True
    assert run_live_steps_enabled({"OKG_MCP_RUN_LIVE_STEPS": "0"}) is False
    assert run_live_steps_enabled({"OKG_MCP_RUN_LIVE_STEPS": "off"}) is False
    with pytest.raises(ValueError):
        run_live_steps_enabled({"OKG_MCP_RUN_LIVE_STEPS": "maybe"})


# ============================================================ totals


def test_integer_sums_are_exact_past_float_precision() -> None:
    sizes = [168189048125252, 168174283593834, 9_007_199_254_740_993]
    rows = [{"d": f"/D{i}", "size": s} for i, s in enumerate(sizes)]
    report = live_result_sets.group_report(
        rows, [], live_result_sets.check_metrics(["count", "sum(attrs.size)"]),
    )
    ((_key, computed),) = report.groups
    assert computed["sum(attrs.size)"] == sum(sizes)
    assert isinstance(computed["sum(attrs.size)"], int)
    assert report.inexact == []


def test_a_float_is_added_apart_with_fsum_and_flagged() -> None:
    """FAILS BEFORE (review M4): the integer part was added to the float
    sum, so 10**17 + 3 came back as a rounded float."""
    rows = [{"v": 10**17}, {"v": 0.1}, {"v": 0.2}, {"v": 3}]
    report = live_result_sets.group_report(
        rows, [], live_result_sets.check_metrics(["sum(attrs.v)"]),
    )
    ((_key, computed),) = report.groups
    assert report.inexact == ["sum(attrs.v)"]
    # The exact integer part, and the float part apart.
    assert computed["sum(attrs.v)"] == 10**17 + 3
    assert isinstance(computed["sum(attrs.v)"], int)
    assert computed["sum(attrs.v) floats"] == 0.30000000000000004
    assert "sum(attrs.v) floats" in live_result_sets.inexact_sentence(report.inexact)
    # The same values in any order: one rounding, the same sum.
    again = live_result_sets.group_report(
        list(reversed(rows)), [], live_result_sets.check_metrics(["sum(attrs.v)"]),
    )
    assert again.groups[0][1] == computed


def test_a_whole_number_float_is_added_as_an_integer_and_not_flagged() -> None:
    """FAILS BEFORE (review M4): 2.0 made the sum a float, flagged inexact."""
    rows = [{"v": 10**17}, {"v": 2.0}, {"v": "3.0"}, {"v": 3}]
    report = live_result_sets.group_report(
        rows, [], live_result_sets.check_metrics(["sum(attrs.v)"]),
    )
    ((_key, computed),) = report.groups
    assert computed == {"sum(attrs.v)": 10**17 + 8}
    assert isinstance(computed["sum(attrs.v)"], int)
    assert report.inexact == []


def test_a_row_without_the_key_is_the_absent_group_apart_from_null() -> None:
    rows = [{"k": True}] * 27 + [{"k": False}] * 3 + [{"k": None}] * 2 + [{"other": 1}] * 231
    rows += [{"k": "True"}]
    report = live_result_sets.group_report(
        rows, ["attrs.k"], live_result_sets.check_metrics(["count"]),
    )
    served = live_result_sets.served_groups(report.groups)
    counts = {key: computed["count"] for (key,), computed in served}
    assert counts == {"(absent)": 231, True: 27, False: 3, None: 2, "True": 1}
    assert report.absent == {"attrs.k": 231}
    assert live_result_sets.group_counts(rows, ["attrs.k"])[0] == (("(absent)",), 231)
    fenced = live_result_sets.fenced_groups(report.groups, ["attrs.k"], "fx")
    assert fenced[0]["attrs.k"] == "(absent)"  # the server's word, not wrapped
    assert "(absent)" in live_result_sets.absent_sentence(report.absent)


# ============================================================ held rows


def _held(rows: list[dict[str, Any]], source: str = "external_live:fx") -> run_live.HeldRows:
    return run_live.HeldRows(rows=rows, sources=[source] * len(rows), head="", cover="fx")


def test_minus_keeps_the_first_row_per_key_and_counts_what_it_drops() -> None:
    step = run_steps.parse_steps([
        {"values": {"site": ["A"]}}, {"values": {"site": ["B"]}},
        {"minus": ["$1", "$2"], "on": "site"},
    ])[2]
    first = _held([{"site": "A", "n": 1}, {"site": "B"}, {"site": "A", "n": 2},
                   {"other": 1}, {"site": "C"}])
    second = _held([{"site": "B"}], source="run_values")
    out = run_live.set_step(step, [(1, first), (2, second)])
    assert out.rows == [{"site": "A", "n": 1}, {"site": "C"}]
    assert out.sources == ["external_live:fx"] * 2
    assert out.facts["removed"] == 1 and out.facts["duplicates_dropped"] == 1
    assert out.facts["without_key"] == 1
    assert "1 duplicates of a kept site dropped" in out.head


@pytest.mark.parametrize("operator,said", [
    ("minus", "rows it would have removed may remain"),
    ("intersect", "rows it would have kept may be missing"),
])
def test_a_partial_later_input_says_which_way_the_set_errs(operator, said) -> None:
    """FAILS BEFORE (review M7): the note said rows of the values not read
    "are not compared", which reads as rows missing; a short exclusion
    list leaves extra rows in a minus."""
    step = run_steps.parse_steps([
        {"values": {"site": ["A"]}}, {"values": {"site": ["B"]}},
        {operator: ["$1", "$2"], "on": "site"},
    ])[2]
    later = _held([{"site": "B"}])
    later.partial = True
    out = run_live.set_step(step, [(1, _held([{"site": "A"}, {"site": "B"}])), (2, later)])
    assert out.partial
    assert out.notes == [f"! partial: step 2 did not read every value; {said}"]
    first = _held([{"site": "A"}])
    first.partial = True
    out = run_live.set_step(step, [(1, first), (2, _held([{"site": "A"}]))])
    missing = "its rows of the values not read are missing here"
    assert out.notes == [f"! partial: step 1 did not read every value; {missing}"]


def test_intersect_keeps_rows_in_every_other_step_and_json_equal_keys() -> None:
    step = run_steps.parse_steps([
        {"values": {"k": [1]}}, {"values": {"k": [1]}}, {"values": {"k": [1]}},
        {"intersect": ["$1", "$2", "$3"], "on": "k"},
    ])[3]
    first = _held([{"k": 1}, {"k": "1"}, {"k": 2}])
    out = run_live.set_step(
        step, [(1, first), (2, _held([{"k": 1.0}, {"k": 2}])), (3, _held([{"k": 1}]))],
    )
    assert out.rows == [{"k": 1}]  # "1" is not 1; 1.0 is


def test_a_set_column_no_row_carries_is_refused_with_the_columns_there() -> None:
    step = run_steps.parse_steps([
        {"values": {"site": ["A"]}}, {"values": {"site": ["B"]}},
        {"minus": ["$1", "$2"], "on": "site_name"},
    ])[2]
    with pytest.raises(run_steps.RunLiveStepRefused) as caught:
        run_live.set_step(step, [(1, _held([{"site": "A"}])), (2, _held([{"site": "B"}]))])
    assert "did you mean site?" in caught.value.fix_hint


def test_aggregate_over_held_rows_totals_them_and_names_the_cover() -> None:
    step = run_steps.parse_steps([
        {"values": {"x": [1]}},
        {"aggregate": {"input": "$1", "by": ["keep"], "metrics": ["count", "sum(size)"]}},
    ])[1]
    rows = [{"keep": True, "size": 2}, {"keep": True, "size": 3}, {"size": 5}]
    out = run_live.rows_step(step, 1, _held(rows), lambda row, predicate: True)
    assert out.facts["groups"] == 2 and out.facts["absent_keys"] == {"attrs.keep": 1}
    assert [g["count"] for g in out.groups] == [2, 1]
    assert out.groups[0]["sum(attrs.size)"] == 5
    assert out.notes[0].startswith("! totals cover: 3 rows of step 1")


def test_a_where_over_held_rows_reads_bare_columns_as_attrs() -> None:
    step = run_steps.parse_steps([
        {"values": {"x": [1]}},
        {"filter": {"input": "$1", "where": {"site": {"op": "suffix", "value": "_Test"}}}},
    ])[1]
    seen: list[Any] = []
    run_live.rows_step(step, 1, _held([{"site": "A_Test"}]), lambda r, p: seen.append(p) or True)
    assert seen and "attrs.site" in repr(seen[0])


# ============================================================ live calls


def _bound(deadline: float = 5.0):
    body = {
        "action": "external.fixture.read", "principal_mode": "service",
        "sample_policy": "metadata", "retention_class": "warm",
        "deadline_seconds": deadline,
        "args": {"dataset": {"type": "string", "required": True, "maxLength": 300}},
    }
    declared = parse_live_relations({ANNOTATION_KEY: {"fx_summary": body}}, subtype="page")
    bindings = parse_live_bindings(
        {"fx_summary": {"kind": "http", "config": {"url": "http://localhost:1"}}},
    )
    return bind_live_relations(declared, bindings)["fx_summary"]


class Upstream:
    """One row per value; a value's behaviour is set per test."""

    def __init__(self, delay: float = 0.0, behaviour: dict[str, Any] | None = None):
        self.delay = delay
        self.behaviour = behaviour or {}
        self.calls: list[str] = []
        self.lock = threading.Lock()
        self.inflight = 0
        self.most = 0

    def __call__(self, *, config, args, node_id, deadline_seconds):
        value = args["dataset"]
        with self.lock:
            self.calls.append(value)
            self.inflight += 1
            self.most = max(self.most, self.inflight)
        try:
            if self.delay:
                time.sleep(self.delay)
            how = self.behaviour.get(value)
            if how == "empty":
                raise LiveAdapterError("upstream_empty_result")
            if how == "not_found":
                raise LiveAdapterError("upstream_not_found")
            if how == "fail":
                raise LiveAdapterError("upstream_unavailable")
            if how == "boom":
                raise RuntimeError("not a live failure")
            if how == "cut":
                return UpstreamCutRows([{"file_size": 1}], received=1)
            return [{"file_size": 10}]
        finally:
            with self.lock:
                self.inflight -= 1


def _calls(values: list[str], upstream: Upstream, *, wall: float = 30.0,
           stop: threading.Event | None = None, max_bytes: int = 1 << 20):
    return run_live_calls(
        bound=_bound(), adapter=upstream,
        per_value=[{"dataset": v} for v in values], each_key="dataset",
        node_id="n:hub", deadline=time.monotonic() + wall,
        stop=stop or threading.Event(), max_bytes=max_bytes,
    )


def test_each_value_outcome_is_counted_and_an_empty_answer_is_not_a_failure() -> None:
    upstream = Upstream(behaviour={
        "/E": "empty", "/N": "not_found", "/F": "fail", "/B": "boom", "/C": "cut",
    })
    result = _calls(["/A", "/E", "/N", "/F", "/B", "/C"], upstream)
    assert result.outcomes == ["ok", "empty", "not_found", "failed", "failed", "cut"]
    assert result.reasons[3] == "upstream_unavailable"
    assert result.reasons[4] == "adapter_raised:RuntimeError"
    assert result.counts()["ok"] == 1 and result.partial
    # Rows of the answered values, each tagged with its value.
    assert result.rows == [
        {"dataset": "/A", "file_size": 10}, {"dataset": "/C", "file_size": 1},
    ]
    whole = _calls(["/A", "/E", "/N"], Upstream(behaviour={"/E": "empty", "/N": "not_found"}))
    assert not whole.partial


def test_no_call_starts_after_the_deadline_and_the_count_stays_flat() -> None:
    upstream = Upstream(delay=0.3)
    started = time.monotonic()
    result = _calls([f"/D{i}" for i in range(200)], upstream, wall=1.0)
    assert time.monotonic() - started < 1.0 + 0.6
    assert result.stopped == "deadline"
    counts = result.counts()
    assert counts["not_started"] > 100
    # Every call counted as started had passed the stop check before the
    # step returned; none passes it afterwards.
    assert len(upstream.calls) <= result.started
    time.sleep(1.0)
    assert len(upstream.calls) == result.started
    time.sleep(0.5)
    assert len(upstream.calls) == result.started  # flat
    assert "not_started" in result.outcomes and result.partial


def test_the_stop_flag_stops_a_step_from_outside() -> None:
    stop = threading.Event()
    upstream = Upstream(delay=0.2)
    timer = threading.Timer(0.5, stop.set)
    timer.start()
    result = _calls([f"/D{i}" for i in range(100)], upstream, stop=stop)
    timer.join()
    assert result.stopped == "cancelled"
    time.sleep(0.5)
    assert len(upstream.calls) == result.started


@pytest.mark.parametrize("cause", ["cancelled", "deadline"])
def test_a_stop_is_named_when_every_call_has_returned_before_the_step_looks(
    monkeypatch, cause: str,
) -> None:
    # Once the stop (or the deadline) is seen, the queued calls return at
    # once. A slow-to-wake step thread can then find every call done and no
    # call pending; the stop must still be named (CI run 39543299969 saw
    # `stopped=None`). The step's wait is made to return only when every
    # call has, so the slow wake happens on every run.
    import concurrent.futures as futures_module

    real_wait = futures_module.wait
    waiting = threading.Event()

    def wait_for_all(fs, timeout=None, return_when=None):
        waiting.set()
        return real_wait(fs, timeout=10.0, return_when=futures_module.ALL_COMPLETED)

    monkeypatch.setattr(futures_module, "wait", wait_for_all)
    stop = threading.Event()
    if cause == "cancelled":
        class StopsTheRun(Upstream):
            # The stop comes while the step thread waits, not before it
            # first looks at `stop`.
            def __call__(self, **kwargs):
                assert waiting.wait(timeout=10.0)
                stop.set()
                return super().__call__(**kwargs)

        upstream: Upstream = StopsTheRun()
        result = _calls([f"/D{i}" for i in range(100)], upstream, stop=stop)
    else:
        upstream = Upstream(delay=0.5)
        result = _calls([f"/D{i}" for i in range(100)], upstream, wall=0.2)
    assert result.stopped == cause
    assert result.counts()["not_started"] > 0 and result.partial
    assert cause in result.reasons
    assert len(upstream.calls) == result.started


def test_past_the_byte_bound_no_further_value_is_called() -> None:
    upstream = Upstream()
    result = _calls([f"/D{i}" for i in range(50)], upstream, max_bytes=200)
    assert result.stopped == "memory"
    assert result.counts()["not_started"] > 0 and result.partial
    assert result.held_bytes > 200


def test_a_live_step_takes_a_live_slot_like_a_direct_call(monkeypatch) -> None:
    slots = threading.BoundedSemaphore(1)
    monkeypatch.setattr(live_dispatch, "_LIVE_SLOTS", slots)
    assert slots.acquire(blocking=False)
    upstream = Upstream()
    try:
        with pytest.raises(LiveConcurrencyExhausted):
            _calls(["/A"], upstream)
    finally:
        slots.release()
    assert upstream.calls == []
    _calls(["/A"], upstream)
    time.sleep(0.1)
    assert slots.acquire(blocking=False)  # given back once its calls ended
    slots.release()


def test_one_upstream_cap_per_system_binds_run_and_direct_fan_outs(monkeypatch) -> None:
    monkeypatch.setattr(live_dispatch, "UPSTREAM_CALLS_PER_BINDING", 2)
    monkeypatch.setattr(live_dispatch, "_UPSTREAM_SLOTS", {})
    upstream = Upstream(delay=0.05)
    slots = live_dispatch.upstream_slots(_bound())
    direct = each_adapter(
        upstream, "dataset", [{"dataset": f"/X{i}"} for i in range(10)], 5.0, slots=slots,
    )
    thread = threading.Thread(
        target=lambda: direct(config={}, args={}, node_id="n", deadline_seconds=5.0),
    )
    thread.start()
    _calls([f"/D{i}" for i in range(10)], upstream)
    thread.join()
    assert len(upstream.calls) == 20
    assert upstream.most <= 2
    # Every relation of one system names one server: one system, one cap.
    other = _bound()
    assert live_dispatch.binding_key(other) == live_dispatch.binding_key(_bound())


def _direct(bound: Any, adapter: Any) -> dict[str, Any]:
    """One direct `expand(live=...)` call, as the server dispatches it."""
    from okg.substrate.external_backed.live_dispatch import GateOutcome, dispatch_live_relation
    from okg.substrate.external_backed.live_relations import compile_args_model

    def gate(relation, principal, node_id):
        return GateOutcome(allowed=True, reason="allow", principal=principal or "svc",
                           action=relation.action)

    return dispatch_live_relation(
        bound=bound, args_model=compile_args_model(bound.relation),
        args={"dataset": "/direct"}, node_id="n:hub", gate=gate, adapter=adapter,
        write_sample=lambda **kwargs: object(), generation_id="7", room=None,
    )


def test_direct_calls_to_a_system_complete_while_a_run_fans_out_to_it(monkeypatch) -> None:
    """FAILS BEFORE (review H1): the run's threads released and took the
    system's slots again before a waiting direct call could (5 of 6 direct
    calls timed out at their 2 s deadline)."""
    monkeypatch.setattr(live_dispatch, "_UPSTREAM_SLOTS", {})
    upstream = Upstream(delay=0.1)
    stop = threading.Event()
    out: dict[str, Any] = {}
    runner = threading.Thread(target=lambda: out.update(result=_calls(
        [f"/D{i}" for i in range(1_000)], upstream, wall=12.0, stop=stop,
    )))
    runner.start()
    try:
        time.sleep(0.5)
        waits = []
        for _ in range(6):
            started = time.monotonic()
            envelope = _direct(_bound(deadline=2.0), upstream)
            waits.append(time.monotonic() - started)
            assert envelope["state"] == "ok", envelope.get("error")
        # The run still fans out: the direct calls did not wait for its end.
        assert runner.is_alive()
        assert max(waits) < 1.0, waits
    finally:
        stop.set()
        runner.join(30)
    assert out["result"].started > 6


def test_a_direct_call_waiting_for_its_system_holds_no_live_slot(monkeypatch) -> None:
    """FAILS BEFORE (review H1): a direct call took a live slot first and
    then waited for its system's slot, so calls waiting on one slow system
    used up the live slots and a call to another system was refused."""
    monkeypatch.setattr(live_dispatch, "UPSTREAM_CALLS_PER_BINDING", 1)
    monkeypatch.setattr(live_dispatch, "_UPSTREAM_SLOTS", {})
    monkeypatch.setattr(live_dispatch, "_LIVE_SLOTS", threading.BoundedSemaphore(2))
    gate = threading.Event()

    def slow(**_kwargs: Any) -> Any:
        gate.wait(10)
        return [{"file_size": 1}]

    slow_system = _bound(deadline=5.0)
    waiting = [threading.Thread(target=_direct, args=(slow_system, slow)) for _ in range(3)]
    for thread in waiting:
        thread.start()
    try:
        time.sleep(0.5)  # one holds the system's slot; two wait for it
        body = {
            "action": "external.fixture.read", "principal_mode": "service",
            "sample_policy": "metadata", "retention_class": "warm",
            "deadline_seconds": 5.0,
            "args": {"dataset": {"type": "string", "required": True, "maxLength": 300}},
        }
        declared = parse_live_relations({ANNOTATION_KEY: {"fx_other": body}}, subtype="page")
        bindings = parse_live_bindings(
            {"fx_other": {"kind": "http", "config": {"url": "http://localhost:2"}}},
        )
        other = bind_live_relations(declared, bindings)["fx_other"]
        assert live_dispatch.binding_key(other) != live_dispatch.binding_key(slow_system)
        envelope = _direct(other, Upstream())
        assert envelope["state"] == "ok", envelope.get("error")
    finally:
        gate.set()
        for thread in waiting:
            thread.join(30)


def test_one_fan_out_run_per_session_and_two_per_reader() -> None:
    """FAILS BEFORE (review M3): the key was the reader alone, so on a
    single-user server a second agent's fan-out run was refused."""
    claim = live_dispatch.claim_live_heavy_run
    release = live_dispatch.release_live_heavy_run
    assert claim("reader-x", "s1")
    assert claim("reader-x", "s2")  # a second session of the reader runs
    try:
        assert not claim("reader-x", "s1")  # one per session
        assert not claim("reader-x", "s3")  # a third: two per reader
        assert claim("reader-y", "s1")
        release("reader-y", "s1")
    finally:
        release("reader-x", "s1")
        release("reader-x", "s2")
    # No stable session: the reader alone is the key.
    assert claim("reader-x", None)
    try:
        assert not claim("reader-x", None)
    finally:
        release("reader-x", None)
    assert claim("reader-x", "s3")
    release("reader-x", "s3")


# ============================================================ hint H3


def test_the_third_call_differing_in_one_argument_names_the_each_form() -> None:
    session = f"s-{time.monotonic_ns()}"
    note = live_run_hint.note_call
    assert note(session, "n:hub", "fx_summary", {"dataset": "/A", "v": 1}, {"dataset": "/A"}) is None
    assert note(session, "n:hub", "fx_summary", {"dataset": "/B", "v": 1}, {"dataset": "/B"}) is None
    hint = note(session, "n:hub", "fx_summary", {"dataset": "/C", "v": 1}, {"dataset": "/C", "v": 1})
    assert hint is not None and hint["argument"] == "dataset" and hint["values"] == 3
    (step,) = hint["call"]["arguments"]["steps"]
    assert step == {"expand": {"input": {"node_id": "n:hub"}, "live": {
        "relation": "fx_summary", "args": {"v": 1}, "each": {"dataset": ["/A", "/B", "/C"]},
    }}}
    # The hinted call is a run the grammar takes.
    _steps(hint["call"]["arguments"]["steps"])


def test_no_hint_when_calls_differ_in_two_arguments_or_relations() -> None:
    session = f"s-{time.monotonic_ns()}"
    note = live_run_hint.note_call
    note(session, "n:hub", "r", {"a": 1, "b": 1}, {})
    note(session, "n:hub", "r", {"a": 2, "b": 2}, {})
    assert note(session, "n:hub", "r", {"a": 3, "b": 3}, {}) is None
    note(session, "n:hub", "s", {"a": 4, "b": 3}, {})
    assert note(session, "n:other", "r", {"a": 5, "b": 3}, {}) is None
