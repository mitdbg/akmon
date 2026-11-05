"""The empty-page explanation's branches, with scripted probes (lane ri F6).

`empty_why.explain` decides a cause from probe outcomes; these tests script
the outcomes (no database) to reach the branches the DB tests do not: a
semantic search, a one-condition lookup, unproven and unchecked probes, the
path_prefix and filter.source parts, and the scope a claim is made within.
"""
from __future__ import annotations

from typing import Any, Callable

from okg.substrate.mcp import empty_why as why

TYPES = ["ticket", "ticket_comment", "wiki_page"]


class Scripted(why.Prober):
    """A prober whose outcomes come from `script(call)`; no database."""

    def __init__(self, script: Callable[[why.Call], why.Probe], *, readable: bool = True):
        super().__init__(None, lambda call, limit: None, every_row_readable=readable)
        self.script = script
        self.calls: list[why.Call] = []

    def _probe(self, call: why.Call, limit: int) -> why.Probe:
        self.calls.append(call)
        return self.script(call)


def _explain(call: why.Call, script: Callable[[why.Call], why.Probe], *,
             page: str = why.NONE, readable: bool = True,
             values: Any = None) -> tuple[dict, Scripted]:
    prober = Scripted(script, readable=readable)
    block = why.explain(
        call, types=TYPES, prober=prober,
        values=values or (lambda field, without, seconds: None),
        page=why.Probe(page),
    )
    return block, prober


def _never(call: why.Call) -> why.Probe:
    raise AssertionError(f"no probe expected: {call}")


def test_a_semantic_search_is_not_probed() -> None:
    block, prober = _explain(why.Call(method="semantic", query="x", where={
        "op": "eq", "field": "attrs.k", "value": "v"}), _never)
    assert block["cause"] == "not_checked"
    assert why.render(block) == ["! 0 rows: cause not checked (semantic search)"]
    block, _ = _explain(why.Call(method="semantic", query="x", subtype_filter="tickets"), _never)
    assert block["cause"] == "unknown_type"
    assert block["loosened"]["status"] == why.SKIPPED


def test_a_one_condition_lookup_names_it_without_a_probe() -> None:
    call = why.Call(method="attrs", where={"op": "eq", "field": "attrs.k", "value": "v"})
    block, _ = _explain(call, _never, page=why.NONE_UNPROVEN,
                        values=lambda field, without, seconds: ["v", "w"])
    assert block["cause"] == "condition"
    assert block["removed_by"] == ['attrs.k = "v"']
    assert block["values_seen"] == {"field": "attrs.k", "values": ["w"]}
    assert why.render(block) == [
        '! 0 rows because of attrs.k = "v"; other values of attrs.k seen: "w" '
        "(a bounded read: not proven complete)"]


def test_lookup_conditions_unproven_and_unchecked() -> None:
    call = why.Call(method="attrs", where={"op": "and", "clauses": [
        {"op": "eq", "field": "attrs.a", "value": 1},
        {"op": "eq", "field": "attrs.b", "value": 2}]})
    block, _ = _explain(call, lambda c: why.Probe(why.NONE_UNPROVEN))
    assert block["cause"] == "conditions" and block["proven"] is False
    assert why.render(block)[0].endswith(
        "dropping any one still leaves 0 rows (a bounded read: not proven complete)")
    block, _ = _explain(call, lambda c: why.Probe(why.TIMED_OUT))
    assert block["cause"] == "not_found"


def test_path_prefix_and_source_are_parts_and_a_query_claim_names_its_scope() -> None:
    call = why.Call(method="grep", query="alpha beta", path_prefix="src/",
                    filter={"source": "git.repo"}, scope='profile "work"')
    labels = [part.label for part in why.parts(call)]
    assert labels == ['source = "git.repo"', 'path_prefix "src/"']

    def script(probed: why.Call) -> why.Probe:
        if probed.query == "alpha beta" and probed.conditions() and not probed.scope:
            raise AssertionError("the scope is never dropped")
        if probed.query == "alpha":
            return why.Probe(why.FOUND, ["n:1"])
        if probed.query == "beta":
            return why.Probe(why.NONE)
        return why.Probe(why.NONE) if probed.query == "alpha beta" else why.Probe(
            why.FOUND, ["n:1"])

    block, prober = _explain(call, script)
    assert all(probed.scope == 'profile "work"' for probed in prober.calls)
    assert block["cause"] == "terms" and block["absent_terms"] == ["beta"]
    assert block["scope"] == 'profile "work"'
    assert why.render(block)[0] == (
        '! 0 rows: word "beta" alone matches no record within profile "work"')


def test_a_timed_out_page_with_an_unknown_type_runs_no_probe() -> None:
    call = why.Call(method="grep", query="x", where={"op": "eq", "field": "type",
                                                     "value": "tickets"})
    block, _ = _explain(call, _never, page=why.TIMED_OUT)
    assert block["cause"] == "unknown_type"
    assert block["loosened"]["status"] == why.SKIPPED
    block, _ = _explain(why.Call(method="grep", query="x"), _never, page=why.TIMED_OUT)
    assert block == {"cause": "page_timed_out"}


def test_a_lookup_part_that_cannot_run_supports_no_claim() -> None:
    """Review L6: a filter.source part leaves nothing a lookup can run."""
    call = why.Call(method="attrs", filter={"source": "s", "subtype": "ticket"})
    block, _ = _explain(call, lambda c: why.Probe(why.NONE))
    assert block["cause"] == "not_found"


def test_the_loosened_search_call_keeps_the_pin_and_drops_paging() -> None:
    sent = {"query": "x", "where": {"type": "tickets"}, "limit": 5, "offset": 0,
            "generation_id": "g:1", "authority": {"a": 1}, "profile": "ticket"}
    arguments = why.search_call_arguments(
        sent, why.Call(method="lexical", query="x"), {"profile": None})
    assert arguments == {"query": "x", "generation_id": "g:1", "authority": {"a": 1}}
