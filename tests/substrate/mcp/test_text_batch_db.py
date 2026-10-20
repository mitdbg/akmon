"""Actual published generation and MCP text/JSON wire budgets for keyed batch reads."""
from __future__ import annotations

import json
import pathlib
import re
import uuid

import jsonschema
import pytest

from tests.substrate.mcp._result_payload import JSON_META, payload, result_text
from tests.substrate.mcp.test_page_next_db import _drive, deployment  # noqa: F401

pytestmark = pytest.mark.postgres


@pytest.fixture
def short_deployment(clean_runtime, okg_mcp_can_log_in, tmp_path, monkeypatch, request):
    """The page-next fixture's document, published under a short fixed name.

    The shared fixture names its deployment by the pytest tmp path. Freshness
    stamps repeat the deployment name (an unavailable block twice), and a
    batch page reserves room for them, so a longer runner path would leave
    less text per page. Here the name is `analyst-search`, resolved through
    OKG_DEPLOYMENTS_DIR. Each test has its own tmp_path and each xdist worker
    its own database, so the fixed name cannot collide.
    """
    from okg.substrate.library.sources.base import EdgeFact
    from tests.substrate.mcp.search_fixture import make_search_deployment, node, publish_nodes
    from tests.substrate.mcp.test_page_next_db import CHUNK, CHUNK_TEXTS, DOC

    name = pathlib.Path(make_search_deployment(tmp_path, dsn=clean_runtime)).name
    monkeypatch.setenv("OKG_DEPLOYMENTS_DIR", str(tmp_path))
    texts = CHUNK_TEXTS * getattr(request, "param", 1)
    nodes = [node("doc:big", "document", "the big document", title="Big")]
    edges = []
    for i, text in enumerate(texts):
        chunk_id = f"doc:big:chunk:{i}"
        nodes.append(node(chunk_id, "document_chunk", text, chunk_index=i,
                          chunk_ordinal=i, chunk_total=len(texts)))
        edges.append(EdgeFact(
            src="doc:big", dst=chunk_id, edge_type="contains",
            source_record_id={"id": f"e{i}"}, source_revision={"v": 1},
        ))
    publish_nodes(
        clean_runtime, name, subtypes=[DOC, CHUNK],
        narrowings=[("document", "contains", "document_chunk")],
        nodes=nodes, edges=edges,
    )
    return name


def unfence(text):
    return re.sub(r"\n?⟦/UNTRUSTED_DATA⟧", "", re.sub(r"⟦UNTRUSTED_DATA[^⟧]*⟧\n?", "", text))


@pytest.mark.parametrize(("budget", "short_deployment"), [(4096, 1), (131072, 30)], indirect=["short_deployment"])
@pytest.mark.parametrize("json_format", [False, True])
def test_published_batch_schema_wire_replay_and_lossless_pages(clean_runtime, short_deployment, budget, json_format):
    async def body(client):
        tools = await client.list_tools()
        schema = next(tool.inputSchema for tool in tools.tools if tool.name == "inspect")
        initial = {"target": {"kind": "texts", "items": [
            {"key": "whole", "node_id": "doc:big"},
            {"key": "anchored", "node_id": "doc:big", "at": {"chunk": "doc:big:chunk:1", "offset": 250}},
        ]}, "max_bytes": budget}
        jsonschema.validate(initial, schema)
        meta = JSON_META if json_format else None
        assembled = {"whole": "", "anchored": ""}
        token = None
        pages = []
        for _ in range(100):
            args = initial if token is None else {"next": token}
            jsonschema.validate(args, schema)
            result = await client.call_tool("inspect", args, meta=meta)
            assert not result.isError, result_text(result)
            assert len(result.model_dump_json(by_alias=True, exclude_none=True).encode()) <= budget
            if json_format:
                page = payload(result)
                pages.append(page)
                assert page["total_count_unit"] == "item"
                assert page["total_count_set"] == "current_page"
                for item in page["items"]:
                    assembled[item["key"]] += "".join(unfence(row["text"]) for row in item["segments"])
                next_token = page.get("continuation_token")
                if token:
                    replay = payload(await client.call_tool("inspect", args, meta=meta))
                    assert replay["items"] == page["items"]
                    assert replay.get("continuation_token") == next_token
            else:
                text = result_text(result)
                pages.append({"text": text})
                sections = re.split(r'(?m)^"(whole|anchored)" · ', text)
                for index in range(1, len(sections), 2):
                    assembled[sections[index]] += "".join(re.findall(
                        r"⟦UNTRUSTED_DATA[^⟧]*⟧\n?(.*?)\n?⟦/UNTRUSTED_DATA⟧",
                        sections[index + 1], re.DOTALL,
                    ))
                match = re.search(r"next: ([^ ·\n]+)", text.split("\n", 1)[0])
                next_token = match.group(1) if match else None
                if not next_token:
                    assert "complete" in text.split("\n", 1)[0]
            if not next_token:
                break
            token = next_token
        else:
            pytest.fail("batch did not terminate")
        assert len(pages) > 1
        if json_format:
            assert {p["pinned_generation_id"] for p in pages} == {pages[0]["pinned_generation_id"]}
        for key, target in [("whole", {"kind": "text", "node_id": "doc:big"}),
                                ("anchored", {"kind": "text", "node_id": "doc:big", "at": initial["target"]["items"][1]["at"]})]:
            expected = ""
            args = {"target": target, "max_bytes": 131072}
            for _ in range(30):
                single = payload(await client.call_tool("inspect", args, meta=JSON_META))
                expected += "".join(unfence(row["text"]) for row in single["items"])
                if not single.get("continuation_token"):
                    break
                args = {"next": single["continuation_token"]}
            assert assembled[key] == expected
        if json_format:
            first_token = pages[0]["continuation_token"]
            for mixed in [{"next": first_token, "max_bytes": 100},
                          {"next": first_token, "target": initial["target"]},
                          {"next": first_token, "select": ["node_id"]}]:
                refused = await client.call_tool("inspect", mixed, meta=JSON_META)
                assert refused.isError
            # #3605 (`nextbytes A`): the signed size beside `next` is served.
            same = await client.call_tool(
                "inspect", {"next": first_token, "max_bytes": budget}, meta=JSON_META,
            )
            assert not same.isError
            assert payload(same)["items"] == pages[1]["items"]
            # #3634 (`nextresize A`): a larger size goes on from the same
            # place: the next item begins where the signed page began.
            bigger = await client.call_tool(
                "inspect", {"next": first_token, "max_bytes": min(budget * 2, 262144)},
                meta=JSON_META,
            )
            assert not bigger.isError
            first_next = pages[1]["items"][0]
            got = payload(bigger)["items"][0]
            assert got["key"] == first_next["key"]
            was = unfence(first_next["segments"][0]["text"])
            assert unfence(got["segments"][0]["text"])[:len(was)] == was
    _drive(clean_runtime, short_deployment, body)


def test_published_source_refusal_independent_and_terminal(clean_runtime, deployment):  # noqa: F811
    async def body(client):
        args = {"target": {"kind": "texts", "items": [
            {"key": "missing", "node_id": "missing"},
            {"key": "stale", "node_id": "doc:big", "at": {"chunk": "doc:big:chunk:0", "offset": 999999}},
            {"key": "good", "node_id": "doc:big:chunk:0"},
        ]}, "max_bytes": 131072}
        result = await client.call_tool("inspect", args, meta=JSON_META)
        assert not result.isError
        page = payload(result)
        assert [item["state"] for item in page["items"]] == ["refused", "refused", "delivered"]
        assert page["items"][2]["requested_extent"]["kind"] == "chunk"
        assert page["complete"] and not page.get("continuation_token")
    _drive(clean_runtime, deployment, body)


@pytest.mark.parametrize("deployment", [5], indirect=True)
def test_batch_next_under_a_lowered_ceiling_names_a_retry_that_works(clean_runtime, deployment):  # noqa: F811
    """A batch token signed at 16384 bytes; the ceiling then drops to 8192.

    The batch binding signs its budget with the items, so `next` cannot
    resume, the legacy spelling with a new budget is refused, and the
    refusal must name the call that IS accepted (PACT read-model-s6-page-next
    `retry_call`): since #3634 (`nextresize A`), `next` at a size under the
    ceiling, from the same place at the pinned generation.
    """
    import json

    from okg.substrate.mcp import continuation
    from tests.substrate.mcp.test_page_next_db import _lower_ceiling, _refusal

    items = [
        {"key": "whole", "node_id": "doc:big"},
        {"key": "anchored", "node_id": "doc:big", "at": {"chunk": "doc:big:chunk:1", "offset": 250}},
    ]
    first: dict = {}

    async def issue(client):
        page = payload(await client.call_tool("inspect", {
            "target": {"kind": "texts", "items": items}, "max_bytes": 16384,
        }, meta=JSON_META))
        assert page.get("continuation_token"), page
        first.update(page)

    _drive(clean_runtime, deployment, issue)
    _lower_ceiling(deployment, 8192)

    async def resume(client):
        token = first["continuation_token"]
        generation = continuation.open_continuation(token)["binding"]["generation"]
        text, error = await _refusal(client, "inspect", {"next": token})
        assert error["code"] == "invalid_response_byte_budget", error
        assert "16384" in error["message"] and "8192" in error["message"], error
        head = text.split("\n", 1)[0]
        # #3634 (`nextresize A`): the batch goes on by `next` at a size under
        # the new ceiling, from the same place, at its pinned generation.
        assert "retry: continue with inspect(next=<this token>" in head, head
        call = error["retry_call"]
        assert call == {
            "tool": "inspect", "mode": "continue",
            "arguments": {"next": continuation.TOKEN_PLACEHOLDER},
            "max_bytes": {"min": 4096, "max": 8192},
        }
        assert "doc:big:chunk:1" not in json.dumps(error), "items must not be echoed"
        result = await client.call_tool(
            "inspect", {"next": token, "max_bytes": 8192}, meta=JSON_META,
        )
        assert len(result.model_dump_json(by_alias=True, exclude_none=True).encode()) <= 8192
        page = payload(result)
        assert page.get("status") == "ok", page.get("error")
        assert page["pinned_generation_id"] == generation
        assert page["max_bytes"] == 8192
        if page.get("continuation_token"):
            resized = continuation.open_continuation(page["continuation_token"])
            assert resized["binding"]["projection"]["max_bytes"] == 8192

    _drive(clean_runtime, deployment, resume)


AGE = re.compile(rb'(publish_age_seconds\\*":)-?[0-9.]+')
#: #3666 (read-passage-rows): the text status line names the generation on
#: a session's first reply only, so which of two compared calls carries it
#: is call order, not content; it is left out of both before comparing.
GENERATION = re.compile(rb" \xc2\xb7 generation [A-Za-z0-9:._/\-]+")


def test_hidden_items_refuse_byte_for_byte_like_missing_ones(clean_runtime, okg_mcp_can_log_in, monkeypatch):
    """Under Nomos enforce, a batch item naming a hidden record, or a location
    at a hidden chunk, gets exactly the page a missing one gets: the same
    refusal, the same work counters, the same bytes in text and JSON.

    Each missing id has the same length as its hidden twin, so swapping the
    caller's own echoed id is the only edit before comparing whole results.
    """
    from tests.substrate.mcp.test_search_location import DEEP, TERM, _record, _seed

    sfx = uuid.uuid4().hex[:8]
    doc, hidden_doc, missing_doc = f"doc:{sfx}", f"doc:{sfx}-hidden", f"doc:{sfx}-absent"
    visible_chunk, secret_chunk, missing_chunk = f"chunk:{doc}:00", f"chunk:{doc}:01", f"chunk:{doc}:07"
    assert len(hidden_doc) == len(missing_doc) and len(secret_chunk) == len(missing_chunk)
    _seed(clean_runtime, _record(doc, ["Open text.", DEEP], secret=frozenset({1}))
          + _record(hidden_doc, [DEEP], doc_secret=True))

    def batch(probe: dict) -> dict:
        return {"target": {"kind": "texts", "items": [
            {"key": "probe", **probe},
            {"key": "good", "node_id": visible_chunk},
        ]}, "max_bytes": 131072}

    cases = {
        "record": ({"node_id": hidden_doc}, {"node_id": missing_doc}),
        "chunk": ({"at": {"chunk": secret_chunk, "offset": 5}},
                  {"at": {"chunk": missing_chunk, "offset": 5}}),
        "chunk_in_record": ({"node_id": doc, "at": {"chunk": secret_chunk, "offset": 5}},
                            {"node_id": doc, "at": {"chunk": missing_chunk, "offset": 5}}),
    }

    async def enforced(client):
        for name, (hidden, missing) in cases.items():
            for meta in (None, JSON_META):
                hidden_result = await client.call_tool("inspect", batch(hidden), meta=meta)
                missing_result = await client.call_tool("inspect", batch(missing), meta=meta)
                assert not hidden_result.isError, result_text(hidden_result)
                hidden_bytes = hidden_result.model_dump_json(by_alias=True, exclude_none=True).encode()
                for hidden_id, missing_id in ((hidden_doc, missing_doc), (secret_chunk, missing_chunk)):
                    hidden_bytes = hidden_bytes.replace(hidden_id.encode(), missing_id.encode())
                missing_bytes = missing_result.model_dump_json(by_alias=True, exclude_none=True).encode()
                # The publish age is wall-clock time between the two calls
                # (PACT: freshness ages may vary); every other byte must match.
                hidden_bytes = GENERATION.sub(b"", AGE.sub(rb"\1<age>", hidden_bytes))
                missing_bytes = GENERATION.sub(b"", AGE.sub(rb"\1<age>", missing_bytes))
                at = next((i for i, (a, b) in enumerate(zip(hidden_bytes, missing_bytes)) if a != b),
                          min(len(hidden_bytes), len(missing_bytes)))
                assert hidden_bytes == missing_bytes, (name, meta, hidden_bytes[max(0, at - 80):at + 80],
                                                       missing_bytes[max(0, at - 80):at + 80])
            hidden_page = payload(hidden_result)
            missing_page = payload(missing_result)
            probe, good = hidden_page["items"]
            assert probe["state"] == "refused" and good["state"] == "delivered", hidden_page["items"]
            assert probe["error"] == missing_page["items"][0]["error"]
            assert probe["error"]["code"] in {"node_not_found", "text_location_stale"}, probe["error"]
            assert hidden_page["work"] == missing_page["work"]
            assert hidden_page["work"]["prepared_items"] == 1, hidden_page["work"]
            assert TERM not in json.dumps(hidden_page)

    monkeypatch.setenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", "enforce")
    monkeypatch.setenv("OKG_NOMOS_SUBJECT", "subject:mcp-reader")
    _drive(clean_runtime, "default", enforced)

    # Failure case: with enforcement off the hidden items exist and read, so
    # the identical answers above come from the policy, not from bad ids.
    async def unenforced(client):
        for name, (hidden, missing) in cases.items():
            seen = payload(await client.call_tool("inspect", batch(hidden), meta=JSON_META))
            absent = payload(await client.call_tool("inspect", batch(missing), meta=JSON_META))
            assert seen["items"][0]["state"] != "refused", (name, seen["items"][0])
            assert absent["items"][0]["state"] == "refused", (name, absent["items"][0])
            assert seen["work"]["prepared_items"] == 2, (name, seen["work"])

    monkeypatch.setenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", "off")
    _drive(clean_runtime, "default", unenforced)


def _item_section(text: str, key: str) -> str:
    """One batch item's rendered lines, from its header to the next item."""
    start = text.index(f'\n"{key}" · ')
    end = text.find('\n\n"', start + 1)
    return text[start:end if end != -1 else len(text)]


def test_located_batch_items_say_where_they_start_and_how_they_were_checked(clean_runtime, deployment):  # noqa: F811
    """A located batch item carries the single read's `read_from` and
    `location_check`, in JSON and as that item's own notices.

    Before the fix an `at`-alone item at offset 1000 said `delivered` for
    the chunk although its read began 200 characters before the offset, and
    an unhashed location was served without the offset-only notice.
    """
    from okg.substrate.mcp.record_text import AT_LEAD_CHARS, chunk_text_hash
    from tests.substrate.mcp.test_page_next_db import CHUNK_TEXTS

    chunk = "doc:big:chunk:1"
    offset = AT_LEAD_CHARS + 800
    assert offset < len(CHUNK_TEXTS[1])
    items = [
        {"key": "loose", "at": {"chunk": chunk, "offset": offset}},
        {"key": "hashed", "at": {"chunk": chunk, "offset": offset,
                                 "hash": chunk_text_hash(CHUNK_TEXTS[1])}},
        {"key": "whole", "node_id": chunk},
    ]
    args = {"target": {"kind": "texts", "items": items}, "max_bytes": 131072}
    expected_from = {"chunk": chunk, "offset": offset - AT_LEAD_CHARS, "text_before_sent": False}

    async def body(client):
        result = await client.call_tool("inspect", args, meta=JSON_META)
        assert not result.isError, result_text(result)
        assert len(result.model_dump_json(by_alias=True, exclude_none=True).encode()) <= 131072
        page = payload(result)
        loose, hashed, whole = page["items"]
        assert [item["state"] for item in page["items"]] == ["delivered"] * 3, page["items"]
        assert loose["requested_extent"]["kind"] == "chunk"
        assert loose["read_from"] == expected_from, loose
        assert loose["location_check"] == "offset_only", loose
        assert hashed["read_from"] == expected_from, hashed
        # Failure cases: a hashed location was checked by its hash, and a
        # whole-chunk read starts at the first character.
        assert "location_check" not in hashed, hashed
        assert "read_from" not in whole and "location_check" not in whole, whole
        sent = unfence("".join(row["text"] for row in loose["segments"]))
        assert sent == CHUNK_TEXTS[1][offset - AT_LEAD_CHARS:]

        text = result_text(await client.call_tool("inspect", args))
        starts = f"! read starts at {chunk} @{offset - AT_LEAD_CHARS}; earlier text was not sent"
        weak = "! location checked by offset only (no hash)"
        assert starts in _item_section(text, "loose"), text
        assert weak in _item_section(text, "loose"), text
        assert starts in _item_section(text, "hashed"), text
        assert weak not in _item_section(text, "hashed"), text
        assert "read starts" not in _item_section(text, "whole"), text
        assert text.count(weak) == 1 and text.count(starts) == 2, text
    _drive(clean_runtime, deployment, body)


#: Freshness stamped on (page one, page two's first serve, page two's replay).
#: "age" moves the publish age from seconds to seven digits; "unavailable"
#: fails the freshness read, so both blocks take their unavailable shape.
STAMP_SCENARIOS = {
    "age_digits_on_replay": ("normal", "normal", "age"),
    "unavailable_at_issue": ("unavailable", "normal", "unavailable"),
    "unavailable_on_replay": ("normal", "normal", "unavailable"),
    "unavailable_on_first_serve": ("normal", "unavailable", "normal"),
}


@pytest.mark.parametrize("json_format", [True, False], ids=["json", "text"])
@pytest.mark.parametrize("scenario", list(STAMP_SCENARIOS))
def test_replayed_batch_token_cuts_the_same_page_when_stamps_change_size(
    clean_runtime, short_deployment, monkeypatch, scenario, json_format,
):
    """The same token gives the same page whatever freshness stamps each serve.

    The server stamps freshness on every page after the batch reader cuts it:
    the publish age gains digits, and a failed freshness read stamps the
    unavailable blocks instead -- in JSON a larger block, in text notice
    lines where a fresh block prints nothing. Before the fix the cut was
    measured with this call's stamps, so a replay could end the text at a
    different character. A freshness failure must never move the cut.
    """
    import datetime

    from okg.substrate.mcp import freshness
    from okg.substrate.mcp import server as server_module

    real_lag = freshness.summarize_publish_lag
    real_outcomes = server_module._stamp_freshness_outcomes
    stamps = {"now": "normal"}
    seen = []

    def lag(**kwargs):
        if stamps["now"] == "age":
            # The publish age jumps from a few seconds to seven digits.
            kwargs["now"] = kwargs["now"] + datetime.timedelta(seconds=1_000_000)
            seen.append("age")
        return real_lag(**kwargs)

    def outcomes(*args, **kwargs):
        if stamps["now"] == "unavailable":
            seen.append("unavailable")
            raise RuntimeError("freshness query failed")
        return real_outcomes(*args, **kwargs)

    monkeypatch.setattr(freshness, "summarize_publish_lag", lag)
    monkeypatch.setattr(server_module, "_stamp_freshness_outcomes", outcomes)
    budget = 4096
    initial = {"target": {"kind": "texts", "items": [
        {"key": "whole", "node_id": "doc:big"},
        {"key": "anchored", "node_id": "doc:big", "at": {"chunk": "doc:big:chunk:1", "offset": 250}},
    ]}, "max_bytes": budget}
    meta = JSON_META if json_format else None

    async def call(client, args, state):
        stamps["now"] = state
        seen.clear()
        result = await client.call_tool("inspect", args, meta=meta)
        assert not result.isError, result_text(result)
        served = len(result.model_dump_json(by_alias=True, exclude_none=True).encode())
        assert served <= budget, served
        # The stamps did change, or this proves nothing.
        assert (state in seen) == (state != "normal"), (state, seen)
        if json_format:
            page = payload(result)
            if state == "unavailable":
                assert page["publish_freshness"]["status"] == "unavailable", page["publish_freshness"]
            return page["items"], page.get("continuation_token")
        text = result_text(result)
        # D8 (read-passage-rows-3666): publish freshness, unavailable or
        # not, is no read reply's notice; the JSON form keeps the block.
        assert "publish freshness" not in text, text.split("\n\n", 1)[0]
        head, body = text.split("\n\n", 1)
        match = re.search(r"next: ([^ ·\n]+)", head.split("\n", 1)[0])
        # The page itself: every item, its text, coverage and work. Only the
        # status line and its notices carry the per-call stamps.
        return body, match.group(1) if match else None

    async def body(client):
        issue, serve, replay = STAMP_SCENARIOS[scenario]
        _first, token = await call(client, initial, issue)
        assert token, "page one must be cut"
        page, next_token = await call(client, {"next": token}, serve)
        assert next_token, "page two must be cut, or nothing is tested"
        again, again_token = await call(client, {"next": token}, replay)
        assert again == page
        assert again_token == next_token
    _drive(clean_runtime, short_deployment, body)
