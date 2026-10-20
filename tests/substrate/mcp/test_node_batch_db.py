"""The batch record read through the real MCP call on a published generation (#3657).

Text and JSON wire sizes, each record against the single `inspect(node)` at
the same pin, continuation, refusals and the access check.
"""
from __future__ import annotations

import json
import re
import uuid

import jsonschema
import pytest

from tests.pytest.catalog_fixture_builders import SubtypeSpec
from tests.substrate.mcp._result_payload import JSON_META, payload, result_text
from tests.substrate.mcp.search_fixture import make_search_deployment, node, publish_nodes
from tests.substrate.mcp.test_page_next_db import _drive

pytestmark = pytest.mark.postgres

TICKET = SubtypeSpec("ticket", metadata={"searchable_text_fields": ["summary", "text"]})
#: Six tickets of about 300 bytes, one of 6 KB and one of 20 KB.
SIZES = {"ticket:1": 120, "ticket:2": 6000, "ticket:3": 150, "ticket:4": 20000,
         "ticket:5": 90, "ticket:6": 200, "ticket:7": 110, "ticket:8": 140}
IDS = list(SIZES)
#: The publish age is wall-clock time between two calls; it may differ.
AGE = re.compile(rb'(publish_age_seconds\\*":)-?[0-9.]+')
#: #3666 (read-passage-rows): the text status line names the generation on
#: a session's first reply only, so which of two compared calls carries it
#: is call order, not content; it is left out of both before comparing.
GENERATION = re.compile(rb" \xc2\xb7 generation [A-Za-z0-9:._/\-]+")


def _body(node_id: str) -> str:
    word = f"{node_id.replace(':', '')}word "
    return (word * (SIZES[node_id] // len(word) + 1))[:SIZES[node_id]]


@pytest.fixture
def tickets(clean_runtime, okg_mcp_can_log_in, tmp_path):
    deployment = make_search_deployment(tmp_path, dsn=clean_runtime)
    publish_nodes(
        clean_runtime, deployment, subtypes=[TICKET],
        nodes=[
            node(node_id, "ticket", _body(node_id), summary=f"Summary of {node_id}",
                 status="Open" if index % 2 else "Closed", priority=index)
            for index, node_id in enumerate(IDS)
        ],
    )
    return deployment


def _unfenced(value: str) -> str:
    """A served text value without its untrusted-data fence lines."""
    value = re.sub(r"\A⟦UNTRUSTED_DATA[^⟧]*⟧\n?", "", value)
    return re.sub(r"\n?⟦/UNTRUSTED_DATA⟧\s*\Z", "", value)


def _wire_bytes(result) -> int:
    return len(result.model_dump_json(by_alias=True, exclude_none=True).encode())


async def _single(client, node_id: str, select=None) -> dict:
    args = {"target": {"kind": "node", "node_id": node_id}, "max_bytes": 262144}
    if select is not None:
        args["select"] = select
    result = await client.call_tool("inspect", args, meta=JSON_META)
    assert not result.isError, result_text(result)
    return payload(result)


@pytest.mark.parametrize("budget", [4096, 131072])
def test_batch_reads_each_record_as_the_single_read_does_within_one_budget(clean_runtime, tickets, budget):
    async def body(client):
        tools = await client.list_tools()
        schema = next(tool.inputSchema for tool in tools.tools if tool.name == "inspect")
        first = {"target": {"kind": "nodes", "node_ids": IDS}, "max_bytes": budget}
        jsonschema.validate(first, schema)
        singles = {node_id: await _single(client, node_id) for node_id in IDS}
        seen, args, pages = [], first, []
        for _ in range(20):
            result = await client.call_tool("inspect", args, meta=JSON_META)
            assert not result.isError, result_text(result)
            assert _wire_bytes(result) <= budget
            page = payload(result)
            pages.append(page)
            assert page["request_count"] == len(IDS)
            for record in page["records"]:
                single = singles[record["node_id"]]
                assert record["subtype"] == "ticket"
                if record["status"] == "ok":
                    assert record["attrs"] == single["attrs"]
                    assert "attrs_cut" not in record
                else:
                    assert record["status"] == "cut"
                    # The hidden search-text copy and the source lineage a
                    # single read leaves out are never served or listed.
                    hidden = {"okg_producer_text", "nomos_source_lineage"}
                    assert not hidden & set(record["attrs_cut"]), record["attrs_cut"]
                    assert not hidden & set(record["attrs"])
                    cut = record["attrs_cut"]["text"]
                    assert cut["chars"] > cut["served_chars"] >= 0
                    assert record["read_whole"]["target"] == {"kind": "text", "node_id": record["node_id"]}
                    for key, value in record["attrs"].items():
                        if key not in record["attrs_cut"]:
                            assert value == single["attrs"][key]
            seen += [record["node_id"] for record in page["records"]]
            assert page.get("omitted", []) == IDS[len(seen):][:len(page.get("omitted", []))]
            token = page.get("continuation_token")
            if not token:
                assert page["complete"] is True and seen == IDS
                break
            assert page["truncated"] is True and page["remaining_count"] == len(IDS) - len(seen)
            replay = payload(await client.call_tool("inspect", {"next": token}, meta=JSON_META))
            args = {"next": token}
            jsonschema.validate(args, schema)
            assert replay["records"] == payload(await client.call_tool("inspect", args, meta=JSON_META))["records"]
        else:
            pytest.fail("the batch did not finish")
        statuses = [record["status"] for page in pages for record in page["records"]]
        if budget == 131072:
            assert len(pages) == 1 and set(statuses) == {"ok"}
        else:
            # The two large tickets do not fit 4096 bytes whole.
            assert "cut" in statuses and "ok" in statuses

        # The same pages as a model reads them: within budget, every id shown.
        text_seen, args = [], first
        for _ in range(20):
            result = await client.call_tool("inspect", args)
            assert not result.isError, result_text(result)
            assert _wire_bytes(result) <= budget
            text = result_text(result)
            status_line = text.split("\n", 1)[0]
            assert status_line.startswith("inspect ok"), status_line
            text_seen += re.findall(r"(?m)^(ticket:\d+) \(ticket\) · (?:ok|cut)$", text)
            cut_here = len(re.findall(r"(?m)^ticket:\d+ \(ticket\) · cut$", text))
            # A page with shortened records says how many in its status line.
            assert (f" · {cut_here} cut · " in status_line) is bool(cut_here), status_line
            # Each cut record says what was cut and how to read it whole.
            assert text.count("\n! attributes cut to fit max_bytes: ") == cut_here
            if "bound reached" in status_line:
                assert "\nomitted: [" in text and "\nremaining_count: " in text
            match = re.search(r"next: ([^ ·\n]+)", status_line)
            if not match:
                break
            assert "bound reached (max_bytes" in status_line
            args = {"next": match.group(1)}
        assert text_seen == IDS
    _drive(clean_runtime, tickets, body)


def test_select_narrows_every_record_as_it_narrows_a_single_read(clean_runtime, tickets):
    async def body(client):
        select = ["node_id", "subtype"]
        result = await client.call_tool("inspect", {
            "target": {"kind": "nodes", "node_ids": IDS}, "select": select, "max_bytes": 4096,
        }, meta=JSON_META)
        assert not result.isError, result_text(result)
        assert _wire_bytes(result) <= 4096
        page = payload(result)
        assert page["complete"] is True and [r["node_id"] for r in page["records"]] == IDS
        for record in page["records"]:
            single = await _single(client, record["node_id"], select)
            assert record["status"] == "ok" and "attrs" not in record and "attrs" not in single
            assert record["subtype"] == single["subtype"] == "ticket"
        # A field the single read does not know is refused the same way,
        # with the same fields to choose from.
        bad = ["attrs", "nope"]
        refused = await client.call_tool("inspect", {
            "target": {"kind": "nodes", "node_ids": IDS[:2]}, "select": bad,
        }, meta=JSON_META)
        single = await client.call_tool("inspect", {
            "target": {"kind": "node", "node_id": IDS[0]}, "select": bad,
        }, meta=JSON_META)
        assert refused.isError and single.isError
        batch_error, single_error = payload(refused)["error"], payload(single)["error"]
        assert batch_error["code"] == single_error["code"] == "select_unknown_field"
        assert batch_error["data"]["valid_fields"] == single_error["data"]["valid_fields"]
        # The batch refusal names no record of the batch.
        assert "ticket:" not in result_text(refused)
        text_field = await client.call_tool("inspect", {
            "target": {"kind": "nodes", "node_ids": IDS[:2]}, "select": ["text"],
        })
        assert text_field.isError and "ticket:" not in result_text(text_field)
    _drive(clean_runtime, tickets, body)


def test_a_missing_id_is_marked_and_the_rest_are_read(clean_runtime, tickets):
    async def body(client):
        ids = ["ticket:1", "ticket:404", "ticket:3"]
        result = await client.call_tool("inspect", {
            "target": {"kind": "nodes", "node_ids": ids}, "max_bytes": 16384,
        }, meta=JSON_META)
        assert not result.isError, result_text(result)
        page = payload(result)
        assert [(r["node_id"], r["status"]) for r in page["records"]] == [
            ("ticket:1", "ok"), ("ticket:404", "not_visible"), ("ticket:3", "ok"),
        ]
        assert page["records"][1] == {"node_id": "ticket:404", "status": "not_visible"}
        text = result_text(await client.call_tool("inspect", {
            "target": {"kind": "nodes", "node_ids": ids}, "max_bytes": 16384,
        }))
        assert "\nticket:404 · not_visible\n" in text
    _drive(clean_runtime, tickets, body)


@pytest.mark.parametrize(("target", "says"), [
    ({"kind": "nodes", "node_ids": [f"ticket:{i}" for i in range(51)]},
     ["at most 50 `node_ids` per call, got 51", "pass the first 50 ids now and the other 1 in another call"]),
    ({"kind": "nodes", "node_ids": ["ticket:1"], "ref": "r1"},
     ["exactly one of `node_ids` or `ref`, got both", '{"kind":"nodes","ref":']),
    ({"kind": "nodes"},
     ["exactly one of `node_ids` or `ref`, got neither", '{"kind":"nodes","node_ids":[...]}']),
    ({"kind": "nodes", "node_ids": []}, ["at least one id", "1 to 50 ids"]),
    ({"kind": "nodes", "node_ids": ["ticket:1", "ticket:1"]}, ["unique", "pass each id once"]),
    ({"kind": "nodes", "node_id": "ticket:1"},
     ["takes `node_ids` (a list of ids), not `node_id`", '{"kind":"nodes","node_ids":[...]}']),
    ({"kind": "node", "node_ids": ["ticket:1", "ticket:2"]},
     ["reads one record", '{"kind":"nodes","node_ids":[...]}']),
    ({"kind": "node", "node_id": ["ticket:1", "ticket:2"]},
     ["reads one record", '{"kind":"nodes","node_ids":[...]}']),
    ({"kind": "nodes", "node_ids": "ticket:1"}, ["as a list of ids", "a list even for one id"]),
    ({"kind": "nodes", "node_ids": ["ticket:1"], "hydrate": True},
     ["does not take `hydrate`", '{"kind":"node","node_id":"<id>","hydrate":true}']),
    # Beside a different top-level value a nested one is still refused; alone
    # or equal it is lifted out and served with a note (issue #3666, below).
    ({"kind": "nodes", "node_ids": ["ticket:1"], "max_bytes": 6000},
     ["max_bytes goes at the top level"]),
], ids=["51-ids", "both", "neither", "empty", "duplicate", "node_id-on-nodes",
        "node_ids-on-node", "list-on-node", "string-node_ids", "hydrate",
        "max_bytes-inside-target-differs"])
def test_a_wrong_shape_is_refused_with_the_form_that_is_accepted(clean_runtime, tickets, target, says):
    async def body(client):
        for meta in (None, JSON_META):
            result = await client.call_tool("inspect", {"target": target, "max_bytes": 8192}, meta=meta)
            assert result.isError, result_text(result)
            text = result_text(result).replace('\\"', '"')
            assert "invalid_operator_arguments" in text
            for word in says:
                assert word in text, text
            # The caller's ids are not echoed back.
            assert "ticket:" not in text, text
    _drive(clean_runtime, tickets, body)


@pytest.mark.parametrize(("nested", "top", "sent"), [
    ({"max_bytes": 8192}, {"max_bytes": 8192}, "max_bytes inside target"),
    ({"max_bytes": 8192}, {}, "max_bytes inside target"),
    ({"select": ["attrs"]}, {"max_bytes": 8192}, "select inside target"),
], ids=["max_bytes-inside-target", "max_bytes-only-inside-target", "select-inside-target"])
def test_an_argument_inside_the_target_is_lifted_out_with_a_note(
        clean_runtime, tickets, nested, top, sent):
    """Issue #3666: the call the old refusal named is the call now served.
    The reply is that corrected call's, plus the note saying what moved."""
    target = {"kind": "nodes", "node_ids": ["ticket:1", "ticket:2"]}

    async def body(client):
        lifted = await client.call_tool(
            "inspect", {"target": {**target, **nested}, **top}, meta=JSON_META)
        corrected = await client.call_tool(
            "inspect", {"target": target, **nested, **top}, meta=JSON_META)
        assert not lifted.isError, result_text(lifted)
        assert not corrected.isError, result_text(corrected)
        noted, plain = payload(lifted), payload(corrected)
        assert noted["adjusted"] == [{
            "rule": "argument_lifted", "sent": sent,
            "served": "top-level " + sent.split(" ", 1)[0],
        }]
        assert "adjusted" not in plain
        assert _wire_bytes(lifted) <= 8192 and _wire_bytes(corrected) <= 8192
        # The note's bytes come out of the same budget, so the noted reply
        # may cut a record earlier, or leave one for the next page. It is
        # never different in any other way.
        for page in (noted, plain):
            listed = [r["node_id"] for r in page["records"]] + page.get("omitted", [])
            assert listed == target["node_ids"], page
        assert 1 <= len(noted["records"]) <= len(plain["records"])
        for mine, theirs in zip(noted["records"], plain["records"]):
            if mine["status"] != "cut":
                assert mine == theirs
                continue
            # Cut here: whole or cut there, and what is left is said.
            assert theirs["status"] in {"ok", "cut"}
            for key, entry in mine["attrs_cut"].items():
                if "chars" not in entry:
                    # A value dropped whole, not shortened.
                    assert entry["served_chars"] == 0
                    continue
                assert isinstance(mine["attrs"][key], str)
                served = _unfenced(mine["attrs"][key])
                assert len(served) == entry["served_chars"] < entry["chars"]
                assert _unfenced(theirs["attrs"][key]).startswith(served)
            for key, value in mine["attrs"].items():
                if key not in mine["attrs_cut"]:
                    assert value == theirs["attrs"][key]
            assert mine.get("read_whole", {}).get("target", {}).get("node_id") in (
                None, mine["node_id"])
        if noted.get("omitted"):
            # A record left out is named, and the reply says how to read it.
            assert noted["truncated"] is True and noted["continuation_token"]
            assert noted["remaining_count"] >= len(noted["omitted"])
        text = result_text(await client.call_tool(
            "inspect", {"target": {**target, **nested}, **top}))
        assert f"! adjusted: {sent} read as top-level" in text
    _drive(clean_runtime, tickets, body)


def test_a_result_reference_is_read_in_its_order(clean_runtime, tickets):
    async def body(client):
        found = payload(await client.call_tool("search", {
            "query": "Summary", "method": "lexical", "retain": True, "limit": 8,
        }, meta=JSON_META))
        ref = found["result_reference"]["id"]
        ranked = [row["node_id"] for row in found["results"]]
        assert len(ranked) >= 2
        result = await client.call_tool("inspect", {
            "target": {"kind": "nodes", "ref": ref}, "select": ["node_id", "subtype"], "max_bytes": 16384,
        }, meta=JSON_META)
        assert not result.isError, result_text(result)
        page = payload(result)
        assert [r["node_id"] for r in page["records"]] == ranked
        assert all(r["status"] == "ok" and r["subtype"] == "ticket" for r in page["records"])
        # The reply says it covers the reference's rows, not the whole graph.
        assert page["reference_members"] == len(ranked)
        text = result_text(await client.call_tool("inspect", {
            "target": {"kind": "nodes", "ref": ref}, "select": ["node_id", "subtype"],
            "max_bytes": 16384,
        }))
        assert (f"! covers only the {len(ranked)} record(s) held by the result reference; "
                "the search that made it may match more") in text
        unknown = await client.call_tool("inspect", {
            "target": {"kind": "nodes", "ref": "no-such-reference"}, "max_bytes": 16384,
        }, meta=JSON_META)
        assert unknown.isError
    _drive(clean_runtime, tickets, body)


def test_a_reference_of_more_than_fifty_records_is_paged_by_next(clean_runtime, okg_mcp_can_log_in, tmp_path):
    deployment = make_search_deployment(tmp_path, dsn=clean_runtime)
    publish_nodes(clean_runtime, deployment, subtypes=[TICKET], nodes=[
        node(f"ticket:{i:03d}", "ticket", "paging", summary=f"S{i}") for i in range(60)
    ])

    async def body(client):
        found = payload(await client.call_tool("search", {
            "query": "paging", "method": "lexical", "retain": True, "limit": 60,
            "select": ["node_id"], "max_bytes": 65536,
        }, meta=JSON_META))
        ref = found["result_reference"]["id"]
        members = [row["node_id"] for row in found["results"]]
        assert len(members) == 60
        args = {"target": {"kind": "nodes", "ref": ref}, "select": ["node_id", "subtype"],
                "max_bytes": 65536}
        seen, sizes = [], []
        for _ in range(5):
            result = await client.call_tool("inspect", args, meta=JSON_META)
            assert not result.isError, result_text(result)
            page = payload(result)
            assert page["request_count"] == 60
            sizes.append(len(page["records"]))
            seen += [r["node_id"] for r in page["records"]]
            if not page.get("continuation_token"):
                break
            args = {"next": page["continuation_token"]}
        assert seen == members and sizes == [50, 10]
        # A reference id from nowhere is refused.
        refused = await client.call_tool("inspect", {
            "target": {"kind": "nodes", "ref": "no-such-reference"}, "max_bytes": 8192},
            meta=JSON_META)
        assert refused.isError
        # A live result set holds external rows, not records: refused before
        # anything is opened, with the tools that do read it.
        from okg.substrate.mcp.live_result_sets import ID_PREFIX

        live = await client.call_tool("inspect", {
            "target": {"kind": "nodes", "ref": ID_PREFIX + "0123456789abcdef"}, "max_bytes": 8192})
        text = result_text(live)
        assert live.isError and "invalid_operator_arguments" in text
        assert "a live result set holds external rows, not records" in text
        assert 'filter/map/aggregate input={"ref": id}' in text.replace('\\"', '"')
    _drive(clean_runtime, deployment, body)


def test_next_carries_the_batch_and_takes_only_a_new_size(clean_runtime, tickets):
    async def body(client):
        start = {"target": {"kind": "nodes", "node_ids": IDS, }, "max_bytes": 4096}
        first = payload(await client.call_tool("inspect", start, meta=JSON_META))
        token = first["continuation_token"]
        rest = IDS[len(first["records"]):]
        for mixed in ({"next": token, "target": start["target"]},
                      {"next": token, "select": ["node_id"]},
                      {"next": token, "max_bytes": 100}):
            refused = await client.call_tool("inspect", mixed, meta=JSON_META)
            assert refused.isError, mixed
        same = payload(await client.call_tool("inspect", {"next": token}, meta=JSON_META))
        assert same["records"][0]["node_id"] == rest[0] and same["offset"] == len(first["records"])
        # A larger size goes on from the same record and serves the rest whole.
        bigger = await client.call_tool("inspect", {"next": token, "max_bytes": 131072}, meta=JSON_META)
        assert not bigger.isError, result_text(bigger)
        page = payload(bigger)
        assert [r["node_id"] for r in page["records"]] == rest
        assert {r["status"] for r in page["records"]} == {"ok"} and page["complete"] is True
    _drive(clean_runtime, tickets, body)


def test_no_single_record_fits_names_the_budget_that_does(clean_runtime, okg_mcp_can_log_in, tmp_path):
    deployment = make_search_deployment(tmp_path, dsn=clean_runtime)
    wide = {f"field_{k:03d}": 10**9 + k for k in range(300)}
    publish_nodes(clean_runtime, deployment, subtypes=[TICKET],
                  nodes=[node("ticket:wide", "ticket", "wide", **wide)])

    async def body(client):
        args = {"target": {"kind": "nodes", "node_ids": ["ticket:wide"]}, "max_bytes": 4096}
        refused = await client.call_tool("inspect", args, meta=JSON_META)
        assert refused.isError
        error = payload(refused)["error"]
        assert error["code"] == "node_batch_budget_too_small", error
        needed = error["data"]["required_budget_bytes"]
        assert needed > 4096
        text = result_text(await client.call_tool("inspect", args))
        assert "max_bytes" in text and "select" in text
        served = await client.call_tool("inspect", {**args, "max_bytes": needed}, meta=JSON_META)
        assert not served.isError, result_text(served)
        assert _wire_bytes(served) <= needed
        # The named budget is the least that serves the record: its numbers
        # whole, its text cut.
        assert payload(served)["records"][0]["status"] in {"ok", "cut"}
        assert payload(served)["records"][0]["attrs"]["field_000"] == 10**9
        # Narrowing select, the other named fix, also serves it.
        narrow = await client.call_tool("inspect", {**args, "select": ["node_id", "subtype"]}, meta=JSON_META)
        assert not narrow.isError and payload(narrow)["records"][0]["status"] == "ok"
    _drive(clean_runtime, deployment, body)


def test_a_hidden_record_reads_exactly_like_a_missing_one(clean_runtime, okg_mcp_can_log_in, monkeypatch):
    """Under access enforcement a hidden id and a missing id of the same
    length give the same bytes, in text and JSON, so the batch cannot be used
    to learn that a hidden record exists."""
    from tests.substrate.mcp.test_search_location import DEEP, TERM, _record, _seed

    sfx = uuid.uuid4().hex[:8]
    doc, hidden_doc, missing_doc = f"doc:{sfx}", f"doc:{sfx}-hidden", f"doc:{sfx}-absent"
    assert len(hidden_doc) == len(missing_doc)
    _seed(clean_runtime, _record(doc, ["Open text."]) + _record(hidden_doc, [DEEP], doc_secret=True))

    def batch(probe: str) -> dict:
        return {"target": {"kind": "nodes", "node_ids": [doc, probe]}, "max_bytes": 16384}

    async def enforced(client):
        for meta in (None, JSON_META):
            hidden = await client.call_tool("inspect", batch(hidden_doc), meta=meta)
            missing = await client.call_tool("inspect", batch(missing_doc), meta=meta)
            assert not hidden.isError, result_text(hidden)
            hidden_bytes = hidden.model_dump_json(by_alias=True, exclude_none=True).encode()
            hidden_bytes = hidden_bytes.replace(hidden_doc.encode(), missing_doc.encode())
            missing_bytes = missing.model_dump_json(by_alias=True, exclude_none=True).encode()
            hidden_bytes = GENERATION.sub(b"", AGE.sub(rb"\1<age>", hidden_bytes))
            missing_bytes = GENERATION.sub(b"", AGE.sub(rb"\1<age>", missing_bytes))
            at = next((i for i, (a, b) in enumerate(zip(hidden_bytes, missing_bytes)) if a != b),
                      min(len(hidden_bytes), len(missing_bytes)))
            assert hidden_bytes == missing_bytes, (
                meta, hidden_bytes[max(0, at - 80):at + 80], missing_bytes[max(0, at - 80):at + 80])
        page = payload(hidden)
        assert [r["status"] for r in page["records"]] == ["ok", "not_visible"]
        assert TERM not in json.dumps(page)

    monkeypatch.setenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", "enforce")
    monkeypatch.setenv("OKG_NOMOS_SUBJECT", "subject:mcp-reader")
    _drive(clean_runtime, "default", enforced)

    # Failure case: with enforcement off the hidden record is read, so the
    # identical answers above come from the policy, not from a bad id.
    async def unenforced(client):
        page = payload(await client.call_tool("inspect", batch(hidden_doc), meta=JSON_META))
        assert [r["status"] for r in page["records"]] == ["ok", "ok"]

    monkeypatch.setenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", "off")
    _drive(clean_runtime, "default", unenforced)
