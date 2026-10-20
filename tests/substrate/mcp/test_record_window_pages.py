"""Actual SDK byte fitting of finite window parts, independent of acquisition."""

from copy import deepcopy

import pytest

from okg.substrate.mcp.continuation import ContinuationTokenInvalid, issue_continuation
from okg.substrate.mcp.record_window_pages import (
    PlannedRecord,
    PlannedWindow,
    WindowBudgetTooSmall,
    WindowPart,
    page_windows,
)
from okg.substrate.mcp.response_bounds import public_call_tool_result_bytes
from okg.substrate.mcp.result_text import ResultContext, result_context


BINDING = {"kind": "record_windows", "generation": "published:test",
           "authority": "reader:test", "projection": {"budget": 4096, "query": "word"}}


def fragment(part, start, end):
    result = {**part.value, "text": part.text[start:end]}
    result["range"] = [part.value["range"][0] + start, part.value["range"][0] + end]
    return result


def plans(text="🙂甲é\n" * 8000, events=80, aliases=80):
    return [PlannedRecord("record", {"node_id": "record", "subtype": "document"}, (
        PlannedWindow("window", {"chain_range": [0, len(text)]}, tuple(
            [WindowPart("match", {"match_id": f"m:{i}", "range": [i, i]}) for i in range(events)]
            + [WindowPart("location", {"edge_id": f"e:{i}", "range": [0, len(text)]}) for i in range(aliases)]
            + [WindowPart("fragment", {"chunk_id": "chunk", "range": [0, len(text)]}, text)]
        )),
    ))]


def page(records, **kwargs):
    return page_windows(records, envelope={"operator_name": "search", "view": "windows"},
                        binding=BINDING, limit=2, max_bytes=4096,
                        measure=lambda value: public_call_tool_result_bytes(value, tool="search"),
                        render_fragment=fragment, **kwargs)


@pytest.mark.parametrize("fmt", ["text", "json"])
def test_large_window_all_text_events_aliases_reachable_in_actual_sdk_budget(fmt):
    records = plans()
    before = deepcopy(records)
    token = None
    matches, aliases, texts, ranges = [], [], [], []
    pages = 0
    with result_context(ResultContext(format=fmt, tool="search")):
        while True:
            result = page(records, continuation_token=token)
            assert public_call_tool_result_bytes(result, tool="search") <= 4096
            assert len(result["results"]) == 1
            window = result["results"][0]["windows"][0]
            matches.extend(item["match_id"] for item in window["matches"])
            aliases.extend(item["edge_id"] for item in window["locations"])
            texts.extend(item["text"] for item in window["fragments"])
            ranges.extend(item["range"] for item in window["fragments"])
            next_token = result["continuation_token"]
            if next_token is None:
                break
            assert next_token != token
            assert page(records, continuation_token=token) == result  # deterministic retry
            token = next_token
            pages += 1
            assert pages < 500
    assert matches == [f"m:{i}" for i in range(80)]
    assert aliases == [f"e:{i}" for i in range(80)]
    assert "".join(texts) == "🙂甲é\n" * 8000
    assert ranges[0][0] == 0 and ranges[-1][1] == 32000
    assert all(left[1] == right[0] for left, right in zip(ranges, ranges[1:]))
    assert records == before


def test_zero_width_match_is_delivered_without_fabricated_text():
    result = page(plans("", events=1, aliases=0))
    window = result["results"][0]["windows"][0]
    assert window["matches"] == [{"match_id": "m:0", "range": [0, 0]}]
    assert window["fragments"] == [{"chunk_id": "chunk", "range": [0, 0], "text": ""}]
    assert result["continuation_token"] is None


def test_record_limit_does_not_limit_windows_or_skip_later_records():
    records = plans("abc", events=1, aliases=0)
    records.append(PlannedRecord("second", {"node_id": "second"}, records[0].windows))
    arguments = dict(envelope={"operator_name": "search"}, binding=BINDING, limit=1,
                     max_bytes=4096, measure=lambda p: public_call_tool_result_bytes(p, tool="search"),
                     render_fragment=fragment)
    first = page_windows(records, **arguments)
    second = page_windows(records, **arguments, continuation_token=first["continuation_token"])
    assert [r["record_id"] for r in first["results"]] == ["record"]
    assert [r["record_id"] for r in second["results"]] == ["second"]
    assert second["continuation_token"] is None


def test_a_windowless_record_is_listed_and_paged_like_any_other():
    records = [PlannedRecord("undecided", {"node_id": "undecided", "decided": False}, ())]
    records += plans("abc", events=1, aliases=0)
    arguments = dict(envelope={"operator_name": "search"}, binding=BINDING, limit=1,
                     max_bytes=4096, measure=lambda p: public_call_tool_result_bytes(p, tool="search"),
                     render_fragment=fragment)
    first = page_windows(records, **arguments)
    assert first["results"] == [{"node_id": "undecided", "decided": False,
                                 "record_id": "undecided", "windows": []}]
    assert "delivery_last_page" not in first
    second = page_windows(records, **arguments, continuation_token=first["continuation_token"])
    assert [r["record_id"] for r in second["results"]] == ["record"]
    assert second["continuation_token"] is None
    # Only the page with nothing left carries the producer's last-page marker.
    assert second["delivery_last_page"] is True


@pytest.mark.parametrize("position", [[0], [-1, 0, 0, 0], [True, 0, 0, 0], [1, 1, 1, 1], [0, 0, 0, 99]])
def test_invalid_position_refuses(position):
    token = issue_continuation(BINDING, cursor={"position": position})
    with pytest.raises(ContinuationTokenInvalid):
        page(plans("abc", events=1), continuation_token=token)


def test_changed_binding_cannot_resume():
    token = issue_continuation({**BINDING, "authority": "other"}, cursor={"position": [0, 0, 0, 0]})
    with pytest.raises(ContinuationTokenInvalid):
        page(plans("abc"), continuation_token=token)


def test_unservable_metadata_refuses_instead_of_skipping_part():
    records = [PlannedRecord("r", {}, (PlannedWindow("w", {}, (
        WindowPart("match", {"match_id": "x" * 10000}),
    )),))]
    with pytest.raises(WindowBudgetTooSmall):
        page(records)
