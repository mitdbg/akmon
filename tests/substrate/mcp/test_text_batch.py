"""Deterministic allocation/state regressions; real wire coverage lives in DB tests."""
from __future__ import annotations

import copy
import json
import hashlib
import subprocess
import sys
from types import SimpleNamespace

import pytest
from pydantic import ValidationError

from okg.substrate.errors import NodeNotFound
from okg.substrate.mcp import text_batch as batch
from okg.substrate.mcp.continuation import ContinuationTokenInvalid, open_continuation
from okg.substrate.mcp.operator_models import TTexts
from okg.substrate.mcp.record_text import PreparedRecordText, RecordTextTooLarge, _Segment


def measure(value):
    return len(json.dumps(value, ensure_ascii=False, separators=(",", ":")).encode())


@pytest.fixture
def harness(monkeypatch):
    sources, reads = {}, []
    session = SimpleNamespace(generation_id="g", deployment_name="d", authority="reader")

    def binding(conn, session, **kwargs):
        return {**kwargs, "generation": session.generation_id, "authority": session.authority}

    def prepare(conn, session, node_id, **kwargs):
        reads.append(node_id)
        value = sources[node_id]
        if isinstance(value, Exception):
            raise value
        return value

    monkeypatch.setattr(batch, "continuation_binding", binding)
    monkeypatch.setattr(batch, "prepare_record_text", prepare)
    monkeypatch.setattr(batch, "classify_sources", lambda *a, **k: {"s": "trusted"})
    return sources, reads, session


def prepared(node_id, text):
    segment = _Segment(node_id + ":c", text, "s", None, 0, 1, 0, len(text), None, False)
    return PreparedRecordText(node_id, {"subtype": "document", "coverage": "complete",
                              "order_basis": "char_offset"}, (segment,) if text else (), {}, {}, (0, 0))


@pytest.mark.parametrize("bad", [[], [{"key": "", "node_id": "a"}],
    [{"key": "x" * 129, "node_id": "a"}], [{"key": "a"}],
    [{"key": "x", "node_id": "a"}, {"key": "x", "node_id": "b"}],
    [{"key": str(i), "node_id": "a"} for i in range(9)]])
def test_item_contract_rejects_invalid_and_never_truncates(bad):
    with pytest.raises(ValidationError):
        TTexts.model_validate({"kind": "texts", "items": bad})


def test_distinct_keys_same_node_locations_valid_and_schema_key_limit():
    target = TTexts.model_validate({"kind": "texts", "items": [
        {"key": "x", "node_id": "a", "at": {"chunk": "c", "offset": 0}},
        {"key": "y", "node_id": "a", "at": {"chunk": "c", "offset": 1}},
    ]})
    assert len(target.items) == 2
    schema = TTexts.model_json_schema()
    item = schema["$defs"]["TextBatchItem"]["properties"]["key"]
    assert item["minLength"] == 1 and item["maxLength"] == 128


def test_rotation_reassembly_retry_and_prepare_once(harness):
    sources, reads, session = harness
    texts = {"a": "α" * 8000, "b": "β" * 6000, "c": "short"}
    sources.update({key: prepared(key, value) for key, value in texts.items()})
    snapshot = copy.deepcopy(sources)
    items = [{"key": key, "node_id": key} for key in texts]
    assembled = {key: "" for key in texts}
    token = None
    first_order = []
    for page_index in range(50):
        reads.clear()
        page = batch.read_text_batch(None, session, items, max_bytes=4096, measure=measure,
                                     continuation_token=token)
        assert measure(page) <= 4096
        assert len(reads) == len(set(reads)), "allocation must not rehydrate sources"
        if token:
            retry = batch.read_text_batch(None, session, items, max_bytes=4096, measure=measure,
                                          continuation_token=token)
            assert retry == page
        first_order.append(page["items"][0]["key"])
        for entry in page["items"]:
            assembled[entry["key"]] += "".join(segment["text"] for segment in entry["segments"])
        token = page.get("continuation_token")
        if not token:
            break
    else:
        pytest.fail("batch did not terminate")
    assert assembled == texts
    assert len(set(first_order[:3])) >= 2
    assert sources == snapshot, "redistribution must not mutate prepared segments"


@pytest.mark.parametrize("refused", [False, True])
def test_all_terminal_zero_character_page_is_progress(harness, refused):
    sources, reads, session = harness
    sources["a"] = NodeNotFound("missing") if refused else prepared("a", "")
    page = batch.read_text_batch(None, session, [{"key": "a", "node_id": "a"}],
                                max_bytes=4096, measure=measure)
    assert page["complete"] and not page.get("continuation_token")
    assert page["items"][0]["state"] == ("refused" if refused else "delivered")


def test_failed_first_item_does_not_starve_remaining_and_is_not_replayed(harness):
    sources, reads, session = harness
    sources.update(a=NodeNotFound("missing"), b=prepared("b", "text" * 3000))
    items = [{"key": key, "node_id": key} for key in sources]
    first = batch.read_text_batch(None, session, items, max_bytes=4096, measure=measure)
    assert first["items"][0]["state"] == "refused"
    assert first["items"][1]["segments"]
    reads.clear()
    second = batch.read_text_batch(None, session, items, max_bytes=4096, measure=measure,
                                  continuation_token=first["continuation_token"])
    assert reads == ["b"] and [entry["key"] for entry in second["items"]] == ["b"]


def test_late_source_failure_is_terminal_without_changing_other_extent(harness):
    sources, reads, session = harness
    sources.update(a=prepared("a", "a" * 8000), b=prepared("b", "b" * 8000))
    items = [{"key": key, "node_id": key} for key in sources]
    first = batch.read_text_batch(None, session, items, max_bytes=4096, measure=measure)
    prior_b = "".join(row["text"] for item in first["items"] if item["key"] == "b" for row in item["segments"])
    sources["a"] = NodeNotFound("source unavailable after prior page")
    token = first["continuation_token"]
    remaining_b = ""
    saw_failure = False
    for _ in range(20):
        page = batch.read_text_batch(None, session, items, max_bytes=4096, measure=measure,
                                    continuation_token=token)
        for item in page["items"]:
            if item["key"] == "a":
                assert not saw_failure and item["state"] == "refused"
                saw_failure = True
            else:
                remaining_b += "".join(row["text"] for row in item["segments"])
        token = page.get("continuation_token")
        if not token:
            break
    assert saw_failure and prior_b + remaining_b == "b" * 8000


@pytest.mark.parametrize("change", ["authority", "generation_id"])
def test_authority_and_generation_mismatch_refuse_before_source_read(harness, change):
    sources, reads, session = harness
    sources["a"] = prepared("a", "text" * 4000)
    items = [{"key": "a", "node_id": "a"}]
    first = batch.read_text_batch(None, session, items, max_bytes=4096, measure=measure)
    setattr(session, change, "changed")
    reads.clear()
    with pytest.raises(ContinuationTokenInvalid):
        batch.read_text_batch(None, session, items, max_bytes=4096, measure=measure,
                              continuation_token=first["continuation_token"])
    assert not reads


def test_huge_compressible_state_refuses_before_read_without_echo(harness):
    sources, reads, session = harness
    with pytest.raises(batch.BatchStateTooLarge) as caught:
        batch.read_text_batch(None, session, [{"key": "a", "node_id": "界" * 30000}],
                              max_bytes=131072, measure=measure)
    assert not reads and "界" not in str(caught.value)


def test_metadata_budget_refusal_has_real_hint_and_no_source_read(harness):
    sources, reads, session = harness
    with pytest.raises(batch.BatchBudgetTooSmall) as caught:
        batch.read_text_batch(None, session, [{"key": "a", "node_id": "界" * 2000}],
                              max_bytes=4096, measure=measure)
    assert not reads and caught.value.data["required_budget_bytes"] > 4096


def test_issued_state_roundtrips_parser_and_measures_complete_body(harness):
    sources, reads, session = harness
    sources["a"] = prepared("a", "text" * 4000)
    first = batch.read_text_batch(None, session, [{"key": "a", "node_id": "a"}],
                                max_bytes=4096, measure=measure)
    body = open_continuation(first["continuation_token"])
    assert body["binding"]["projection"]["max_bytes"] == 4096
    assert body["cursor"]["positions"][0]["char"] > 0


def test_full_decoded_state_boundary_even_when_highly_compressible():
    binding = {"kind": "inspect_text_batch", "padding": ""}
    cursor = {"positions": [{"i": 0, "start": True}], "rotation": 0, "reserve": 0}
    base = batch.continuation_body_bytes(binding, cursor)
    binding["padding"] = "x" * (batch.MAX_DECODED_TOKEN_BYTES - base - 1)
    token = batch._next_token(binding, {0: None}, 0, reserve=0)
    assert open_continuation(token)["binding"] == binding
    binding["padding"] += "x"
    with pytest.raises(batch.BatchStateTooLarge):
        batch._next_token(binding, {0: None}, 0, reserve=0)


def test_encoded_limit_refuses_incompressible_state():
    padding = "".join(hashlib.sha256(str(i).encode()).hexdigest() for i in range(500))
    binding = {"kind": "inspect_text_batch", "padding": padding}
    assert batch.continuation_body_bytes(binding, {}) < batch.MAX_DECODED_TOKEN_BYTES
    with pytest.raises(batch.BatchStateTooLarge):
        batch._next_token(binding, {0: None}, 0, reserve=0)


def test_metadata_fit_without_text_progress_refuses_honestly(harness):
    sources, reads, session = harness
    sources["a"] = prepared("a", "long text")
    # Deliberately model a renderer whose segment framing exceeds the
    # entire budget although metadata fits. No successful empty cursor loop.
    def expensive(value):
        return measure(value) + (5000 if any(row["segments"] for row in value["items"]) else 0)
    with pytest.raises(batch.BatchBudgetTooSmall) as caught:
        batch.read_text_batch(None, session, [{"key": "a", "node_id": "a"}],
                              max_bytes=4096, measure=expensive)
    assert caught.value.data["required_budget_bytes"] > 4096


def test_a_batch_budget_retry_restarts_at_the_pin_without_echoing_items():
    """A batch token binds its budget with its items, so the retry a lowered
    ceiling names is a restart at the pinned generation (PACT
    read-model-s6-page-next `retry_call`). Huge ids are not echoed: the
    refusal must fit the smallest response budget."""
    from okg.substrate.mcp.continuation import budget_retry_call
    from okg.substrate.mcp.response_bounds import MIN_RESPONSE_BYTES, _name_budget_retry
    from okg.substrate.mcp.result_text import is_block

    items = [{"key": f"k{i}" * 60, "node_id": "n" * 5000} for i in range(8)]
    call = budget_retry_call({
        "kind": "inspect_text_batch", "generation": "gen:pinned",
        "projection": {"items": items, "max_bytes": 16384},
    })
    assert call == {
        "tool": "inspect", "mode": "restart",
        "arguments": {"target": {"kind": "texts"}, "generation_id": "gen:pinned"},
        "repeat": ["target.items", "authority"],
    }
    error: dict = {}
    _name_budget_retry(error, call, low=MIN_RESPONSE_BYTES, high=8192)
    assert not is_block(error["fix_hint"]), error["fix_hint"]
    assert error["fix_hint"].startswith("restart this batch without a token")
    assert "same items" in error["remedy"] and "same authority" in error["remedy"]
    assert '"gen:pinned"' in error["remedy"]
    assert error["retry_call"]["max_bytes"] == {"min": MIN_RESPONSE_BYTES, "max": 8192}
    assert len(json.dumps(error)) < 2048
    # A subtype restart keeps its own wording.
    subtype: dict = {}
    _name_budget_retry(subtype, budget_retry_call({
        "kind": "inspect_subtype", "subtype": "document", "projection": {},
    }), low=MIN_RESPONSE_BYTES, high=8192)
    assert subtype["fix_hint"].startswith("restart this subtype read without a token")


def test_refusal_hint_fits_its_cause(harness):
    """A record too large to read whole is refused alone too: its hint names
    the narrower reads, not a separate read of the same source."""
    sources, reads, session = harness
    sources.update(
        big=RecordTextTooLarge("too many", data={"node_id": "big", "max_occurrences": 1}),
        gone=NodeNotFound("missing"),
    )
    page = batch.read_text_batch(None, session, [{"key": key, "node_id": key} for key in ("big", "gone")],
                                 max_bytes=4096, measure=measure)
    errors = {item["key"]: item["error"] for item in page["items"]}
    assert errors["big"]["code"] == "record_text_too_large"
    assert "separately" not in errors["big"]["fix_hint"]
    assert "search within it" in errors["big"]["fix_hint"] and "expand" in errors["big"]["fix_hint"]
    assert errors["gone"]["fix_hint"] == "use a fresh search location or read the source separately"


def test_partial_cut_is_reapplied_even_when_asserts_are_stripped():
    """`python -O` strips asserts; the chosen cut must still be written."""
    script = """
import json
from types import SimpleNamespace
from okg.substrate.mcp import text_batch as batch
from okg.substrate.mcp.record_text import PreparedRecordText, _Segment
assert False, "asserts must be stripped in this child"
text = "word " * 3000
segment = _Segment("a:c", text, "s", None, 0, 1, 0, len(text), None, False)
source = PreparedRecordText("a", {"subtype": "document", "coverage": "complete",
                            "order_basis": "char_offset"}, (segment,), {}, {}, (0, 0))
batch.continuation_binding = lambda conn, session, **k: {**k, "generation": "g", "authority": "r"}
batch.prepare_record_text = lambda *a, **k: source
batch.classify_sources = lambda *a, **k: {"s": "trusted"}
measure = lambda v: len(json.dumps(v, ensure_ascii=False, separators=(",", ":")).encode())
page = batch.read_text_batch(None, SimpleNamespace(generation_id="g", deployment_name="d", authority="r"),
                             [{"key": "a", "node_id": "a"}], max_bytes=4096, measure=measure)
(item,) = page["items"]
delivered = "".join(row["text"] for row in item["segments"])
print(json.dumps({"fits": measure(page) <= 4096, "state": item["state"],
                  "delivered": len(delivered), "prefix": text.startswith(delivered),
                  "cursor": batch.open_continuation(page["continuation_token"])["cursor"]["positions"]}))
"""
    done = subprocess.run([sys.executable, "-O", "-c", script], capture_output=True, text=True, check=True)
    out = json.loads(done.stdout)
    assert out["fits"] and out["state"] == "partial" and out["prefix"]
    assert out["delivered"] > 0
    assert out["cursor"] == [{"i": 0, "segment": 0, "char": out["delivered"]}]


def test_nondeterministic_reapplication_fails_loudly(harness):
    """If a cut that fit stops fitting when re-applied, the batch raises
    instead of returning a page over budget or with a stale cursor."""
    sources, reads, session = harness
    sources["a"] = prepared("a", "word " * 3000)
    seen = set()

    def flaky(value):
        size = measure(value)
        if any(row["segments"] for row in value["items"]):
            key = json.dumps(value, sort_keys=True)
            if key in seen:
                return size + 10**6
            seen.add(key)
        return size

    with pytest.raises(RuntimeError, match="not deterministic"):
        batch.read_text_batch(None, session, [{"key": "a", "node_id": "a"}],
                              max_bytes=4096, measure=flaky)


def test_a_cut_batch_page_names_its_byte_budget(harness):
    """A cut batch page says which bound it reached: the signed `max_bytes`.

    Before the fix `_bound` found no field it knew on a batch page and said
    `bound reached (page)`, which names no budget a caller could raise.
    """
    from okg.substrate.mcp.result_text import render

    sources, _reads, session = harness
    sources["a"] = prepared("a", "α" * 8000)
    page = batch.read_text_batch(None, session, [{"key": "a", "node_id": "a"}],
                                 max_bytes=4096, measure=measure)
    token = page["continuation_token"]
    head = render(page, tool="inspect", kind="texts").split("\n", 1)[0]
    assert f"bound reached (max_bytes 4096) · next: {token}" in head, head
    assert "(page)" not in head, head
    # Failure case: a finished batch page reaches no bound at all.
    sources["b"] = prepared("b", "short")
    done = batch.read_text_batch(None, session, [{"key": "b", "node_id": "b"}],
                                 max_bytes=4096, measure=measure)
    head = render(done, tool="inspect", kind="texts").split("\n", 1)[0]
    assert "complete" in head and "bound reached" not in head, head


def test_location_facts_are_carried_onto_each_batch_item(harness):
    """`read_from` and `location_check` from the single-source read reach the
    batch item, in JSON and as that item's `!` notices, and only there.

    Before the fix the batch copied only subtype, coverage and order basis,
    so a located item said `delivered` with no sign its earlier text was
    omitted, and an unhashed location was served without the weak-check notice.
    """
    from dataclasses import replace

    from okg.substrate.mcp.result_text import render

    sources, _reads, session = harness
    plain = prepared("a", "tail text")
    sources["a"] = replace(plain, payload={
        **plain.payload,
        "read_from": {"chunk": "a:c", "offset": 800, "text_before_sent": False},
        "location_check": "offset_only",
    })
    sources["b"] = prepared("b", "plain text")
    items = [{"key": "a", "node_id": "a", "at": {"chunk": "a:c", "offset": 1000}},
             {"key": "b", "node_id": "b"}]
    page = batch.read_text_batch(None, session, items, max_bytes=4096, measure=measure)
    first, second = page["items"]
    assert first["state"] == "delivered"
    assert first["read_from"] == {"chunk": "a:c", "offset": 800, "text_before_sent": False}
    assert first["location_check"] == "offset_only"
    # Failure case: a plain read carries neither field.
    assert "read_from" not in second and "location_check" not in second, second
    assert "read_from" not in page and "location_check" not in page
    text = render(page, tool="inspect", kind="texts")
    section_a, section_b = text.split('\n"b" · ', 1)
    assert "! read starts at a:c @800; earlier text was not sent" in section_a, section_a
    assert "! location checked by offset only (no hash)" in section_a, section_a
    assert "read starts" not in section_b and "offset only" not in section_b, section_b
    assert text.count("read starts at") == 1 and text.count("offset only") == 1, text


def test_retry_keeps_the_token_reserve_when_stamps_change_size(harness):
    """The cut depends on the page, the token and max_bytes, not on this
    call's stamps, while they still fit the reserve the token carries."""
    sources, _reads, session = harness
    sources["a"] = prepared("a", "word " * 3000)
    items = [{"key": "a", "node_id": "a"}]
    first = batch.read_text_batch(None, session, items, max_bytes=4096, measure=measure,
                                  reserve_bytes=600)
    token = first["continuation_token"]
    reserve = open_continuation(token)["cursor"]["reserve"]
    assert reserve == 600 + batch.TEXT_READ_RESERVE_MARGIN + batch.TEXT_BATCH_DRIFT_BYTES
    page = batch.read_text_batch(None, session, items, max_bytes=4096, measure=measure,
                                 continuation_token=token, reserve_bytes=600)
    for stamps in (0, 590, 600 + batch.TEXT_BATCH_DRIFT_BYTES):
        retry = batch.read_text_batch(None, session, items, max_bytes=4096, measure=measure,
                                      continuation_token=token, reserve_bytes=stamps)
        assert retry == page, stamps
    # Failure case: stamps that outgrew the reserve choose a new one, so the
    # page moves (losslessly) rather than leave less than the full spare.
    grown = batch.read_text_batch(None, session, items, max_bytes=4096, measure=measure,
                                  continuation_token=token, reserve_bytes=600 + batch.TEXT_BATCH_DRIFT_BYTES + 1)
    assert grown["items"] != page["items"]
    assert measure(grown) <= 4096 - (600 + batch.TEXT_BATCH_DRIFT_BYTES + 1) - batch.TEXT_READ_RESERVE_MARGIN


def test_a_cursor_without_a_stamp_reserve_is_refused_before_any_read(harness):
    from okg.substrate.mcp.continuation import issue_continuation

    sources, reads, session = harness
    sources["a"] = prepared("a", "word " * 3000)
    items = [{"key": "a", "node_id": "a"}]
    first = batch.read_text_batch(None, session, items, max_bytes=4096, measure=measure)
    body = open_continuation(first["continuation_token"])
    for reserve in (None, -1, True, "9"):
        cursor = {key: value for key, value in body["cursor"].items() if key != "reserve"}
        if reserve is not None:
            cursor["reserve"] = reserve
        forged = issue_continuation(body["binding"], cursor=cursor)
        reads.clear()
        with pytest.raises(ContinuationTokenInvalid, match="stamp reserve"):
            batch.read_text_batch(None, session, items, max_bytes=4096, measure=measure,
                                  continuation_token=forged)
        assert not reads
