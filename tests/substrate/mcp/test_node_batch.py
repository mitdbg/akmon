"""Input, sharing and token rules of the batch record read (#3657).

The real MCP call, its text and JSON wire sizes and the access checks are in
`test_node_batch_db.py`.
"""
from __future__ import annotations

import json
from types import SimpleNamespace

import pytest
from pydantic import ValidationError

from okg.substrate.errors import NodeNotFound, SubstrateError
from okg.substrate.mcp import node_batch as batch
from okg.substrate.mcp.continuation import ContinuationTokenInvalid
from okg.substrate.mcp.operator_models import MAX_NODE_BATCH, TNodes


def measure(value):
    return len(json.dumps(value, ensure_ascii=False, separators=(",", ":")).encode())


def record(node_id, text="", **attrs):
    return {
        "node_id": node_id, "subtype": "ticket",
        "attrs": {"summary": f"about {node_id}", "text": text, **attrs},
        "generation_id": "g", "pinned_generation_id": "g",
    }


@pytest.fixture
def harness(monkeypatch):
    """`read(ids, ...)` over an in-memory record store; `reads` logs each
    single-record read."""
    store, reads = {}, []
    session = SimpleNamespace(generation_id="g", deployment_name="d", authority="reader")

    def binding(conn, session, **kwargs):
        return {**kwargs, "generation": session.generation_id, "authority": session.authority}

    monkeypatch.setattr(batch, "continuation_binding", binding)

    def read_node(node_id):
        reads.append(node_id)
        value = store.get(node_id)
        if isinstance(value, Exception):
            raise value
        return value

    def read(ids=None, *, ref=None, members=None, max_bytes=4096, select=None, token=None):
        return batch.read_node_batch(
            None, session, node_ids=ids, ref=ref,
            resolve_ref=lambda _ref: list(members or []), read_node=read_node,
            select=select, max_bytes=max_bytes, measure=measure,
            continuation_token=token,
        )

    return store, reads, read


@pytest.mark.parametrize("bad", [
    {"node_ids": []},
    {"node_ids": [""]},
    {"node_ids": ["a", "a"]},
    {"node_ids": [f"n{i}" for i in range(MAX_NODE_BATCH + 1)]},
    {"node_ids": ["a"], "ref": "r1"},
    {},
    {"node_id": "a"},
    {"node_ids": "a"},
    {"node_ids": ["a"], "select": ["attrs"]},
    {"node_ids": ["a"], "max_bytes": 4096},
])
def test_target_takes_one_source_of_at_most_fifty_unique_ids_and_no_aliases(bad):
    with pytest.raises(ValidationError):
        TNodes.model_validate({"kind": "nodes", **bad})


def test_target_schema_states_the_limit_and_the_one_source_rule():
    assert MAX_NODE_BATCH == 50
    TNodes.model_validate({"kind": "nodes", "node_ids": [f"n{i}" for i in range(50)]})
    TNodes.model_validate({"kind": "nodes", "ref": "r1"})
    schema = TNodes.model_json_schema()
    ids = schema["properties"]["node_ids"]
    ids = next((option for option in ids.get("anyOf", [ids]) if option.get("type") == "array"), ids)
    assert ids["maxItems"] == 50 and ids["minItems"] == 1
    assert schema["oneOf"] == [{"required": ["node_ids"]}, {"required": ["ref"]}]
    assert schema["additionalProperties"] is False


def test_shares_serve_small_records_whole_and_split_the_rest_equally():
    # 1000 bytes for records costing 100, 2000, 100, 3000: the two small
    # ones are whole, the two large ones get (1000 - 200) / 2 each.
    assert batch._shares([100, 2000, 100, 3000], 1000) == [None, 400, None, 400]
    assert batch._shares([10, 20, 30], 1000) == [None, None, None]
    # A record that fits only after smaller ones return their unused share.
    assert batch._shares([100, 450, 2000], 1000) == [None, None, 450]
    assert batch._shares([], 1000) == []


def test_every_record_fits_in_request_order_as_the_single_read_returns_it(harness):
    store, reads, read = harness
    ids = ["t:3", "t:1", "t:2"]
    store.update({i: record(i, "short text") for i in ids})
    page = read(ids, max_bytes=16384)
    assert measure(page) <= 16384
    assert [r["node_id"] for r in page["records"]] == ids
    assert {r["status"] for r in page["records"]} == {"ok"}
    assert page["complete"] is True and page["truncated"] is False
    assert "continuation_token" not in page and "omitted" not in page
    assert page["request_count"] == 3
    assert reads == ids, "each record is read once"
    for served in page["records"]:
        single = store[served["node_id"]]
        assert served["attrs"] == single["attrs"] and served["subtype"] == single["subtype"]
        # The page states the generation once.
        assert "generation_id" not in served and "attrs_cut" not in served


def test_large_records_are_cut_with_the_single_read_markers_and_small_ones_stay_whole(harness):
    store, reads, read = harness
    store.update({
        "t:small": record("t:small", "tiny"),
        "t:big": record("t:big", "B" * 9000),
        "t:huge": record("t:huge", "H" * 30000),
    })
    page = read(["t:small", "t:big", "t:huge"], max_bytes=4096)
    assert measure(page) <= 4096
    small, big, huge = page["records"]
    assert small["status"] == "ok" and small["attrs"]["text"] == "tiny"
    for cut, full in ((big, 9000), (huge, 30000)):
        assert cut["status"] == "cut"
        marker = cut["attrs_cut"]["text"]
        assert marker["chars"] == full
        assert 0 < marker["served_chars"] == len(cut["attrs"]["text"]) < full
        assert cut["read_whole"] == {
            "tool": "inspect", "target": {"kind": "text", "node_id": cut["node_id"]},
        }
        # The cutter's page-level stamps belong to the page, not each record.
        assert not {"truncated", "truncation_reason", "response_byte_budget"} & set(cut)
        assert cut["attrs"]["summary"] == f"about {cut['node_id']}"
    # Fair: the two cut records get the same share, give or take one cut step.
    assert abs(len(big["attrs"]["text"]) - len(huge["attrs"]["text"])) <= 512
    # Every requested record was served, so nothing is left to continue;
    # the page still says two of them were shortened.
    assert page["complete"] is True and "continuation_token" not in page
    assert page["cut_count"] == 2


def test_each_cut_record_in_the_text_page_says_what_was_cut_and_how_to_read_it_whole(harness):
    """A batch page in text: every cut record carries its own plain note,
    naming the cut value with the characters served of how many and the
    call that reads that record whole; an uncut record carries none, and no
    record dumps its cut markers as raw fields."""
    from okg.substrate.mcp.result_text import render

    store, _, read = harness
    store.update({
        "t:small": record("t:small", "tiny"),
        "t:big": record("t:big", "B" * 9000),
        "t:huge": record("t:huge", "H" * 30000),
    })
    page = read(["t:small", "t:big", "t:huge"], max_bytes=4096)
    text = render(page, tool="inspect", kind="nodes")
    blocks = text.split("\n\n")[1:]
    assert [block.split("\n", 1)[0] for block in blocks] == [
        "t:small (ticket) · ok", "t:big (ticket) · cut", "t:huge (ticket) · cut",
    ]
    assert "! attributes cut" not in blocks[0]
    for block, cut in zip(blocks[1:], page["records"][1:]):
        served = cut["attrs_cut"]["text"]["served_chars"]
        full = cut["attrs_cut"]["text"]["chars"]
        assert (
            f"! attributes cut to fit max_bytes: text {served:,} of {full:,} chars; "
            "read the record's whole text with "
            f'inspect(target={{"kind":"text","node_id":"{cut["node_id"]}"}})'
        ) in block, block
    assert text.count("\n! attributes cut to fit max_bytes: ") == page["cut_count"] == 2
    assert "attrs_cut:" not in text and "read_whole:" not in text


def test_a_cut_record_with_nothing_named_still_prints_its_whole_read():
    """Review nit on lane RD: a record marked cut whose `attrs_cut` is empty
    lost its `read_whole` call in text (the key was consumed, no note said
    it). The call now prints on its own note."""
    from okg.substrate.mcp.result_text import render

    cut = batch._cut_form(record("t:1", "tiny"), 64, frozenset())
    assert cut["status"] == "cut" and cut["attrs_cut"] == {}
    page = {
        "status": "ok", "result_kind": "node_batch", "generation_id": "g",
        "pinned_generation_id": "g", "max_bytes": 4096, "request_count": 1, "offset": 0,
        "records": [cut], "complete": True, "truncated": False, "cut_count": 1,
    }
    text = render(page, tool="inspect", kind="nodes")
    assert (
        '! cut to fit max_bytes; read the record\'s whole text with '
        'inspect(target={"kind":"text","node_id":"t:1"})'
    ) in text, text


def test_a_list_left_out_of_a_batch_record_names_the_read_that_returns_it(harness):
    """Review P2-3 on lane RD, batch form: a list left out whole names the
    record's select=["attrs"] read, not its text."""
    from okg.substrate.mcp.result_text import render

    store, _, read = harness
    store["t:list"] = record("t:list", "x", labels=["l" * 400] * 20)
    page = read(["t:list"], max_bytes=4096)
    (cut,) = page["records"]
    entry = cut["attrs_cut"]["labels"]
    assert entry["served_chars"] == 0
    assert entry["read"]["select"] == ["attrs"] and entry["read"]["max_bytes"] > entry["bytes"]
    text = render(page, tool="inspect", kind="nodes")
    assert (
        'labels left out (' f"{entry['bytes']:,}" ' B; read it with '
        'inspect(target={"kind":"node","node_id":"t:list"}, select=["attrs"], '
        f'max_bytes={entry["read"]["max_bytes"]}))'
    ) in text, text


def test_two_equally_large_fields_share_the_room_and_each_keeps_its_start(harness):
    store, reads, read = harness
    store["t:twin"] = record("t:twin", "A" * 100_000, notes="N" * 100_000, tags=["x"] * 50)
    page = read(["t:twin"], max_bytes=8192)
    assert measure(page) <= 8192
    twin = page["records"][0]
    assert twin["status"] == "cut" and page["cut_count"] == 1
    kept = {key: twin["attrs_cut"][key]["served_chars"] for key in ("text", "notes")}
    assert kept["text"] == kept["notes"] > 1000
    assert twin["attrs"]["text"] == "A" * kept["text"] and twin["attrs"]["notes"] == "N" * kept["notes"]
    # The small list did not have to go to make room.
    assert twin["attrs"]["tags"] == ["x"] * 50 and "tags" not in twin["attrs_cut"]


def test_a_list_is_left_out_only_when_emptying_the_text_is_not_enough(harness):
    store, reads, read = harness
    store["t:list"] = record("t:list", "T" * 500, items=[f"item {i}" for i in range(2000)])
    page = read(["t:list"], max_bytes=4096)
    assert measure(page) <= 4096
    served = page["records"][0]
    assert served["attrs"]["items"] is None
    assert served["attrs_cut"]["items"]["served_chars"] == 0 and served["attrs_cut"]["items"]["bytes"] > 4096
    # With the list gone the text fits whole and is not marked.
    assert served["attrs"]["text"] == "T" * 500 and "text" not in served["attrs_cut"]


def test_cutting_takes_a_bounded_number_of_measures(harness, monkeypatch):
    store, reads, read = harness
    ids = [f"t:{i:02d}" for i in range(50)]
    store.update({i: record(i, "A" * 60_000, notes="N" * 60_000) for i in ids})
    calls = 0

    def counting(value):
        nonlocal calls
        calls += 1
        return measure(value)

    session = SimpleNamespace(generation_id="g", deployment_name="d", authority="reader")
    page = batch.read_node_batch(
        None, session, node_ids=ids, ref=None, resolve_ref=lambda _ref: [],
        read_node=lambda node_id: store[node_id], select=None, max_bytes=65536,
        measure=counting,
    )
    assert measure(page) <= 65536 and len(page["records"]) == 50
    # About 20 measures per cut record and try, never thousands per record.
    assert calls < 50 * 80


def test_a_small_budget_over_a_long_list_reads_only_the_records_it_could_serve(harness):
    store, reads, read = harness
    ids = [f"t:{i:02d}" for i in range(50)]
    store.update({i: record(i, "x" * 2000, **{f"n{k}": 10**9 + k for k in range(40)}) for i in ids})
    # A failure far down the list is not this page's business.
    store["t:40"] = SubstrateError("should not be read on the first page")
    page = read(ids, max_bytes=4096)
    assert 1 <= len(page["records"]) < 10
    assert len(reads) <= len(page["records"]) + 2 and "t:40" not in reads
    assert page["omitted"] == ids[len(page["records"]):]


def test_records_that_cannot_fit_even_cut_are_listed_and_next_continues_them(harness):
    store, reads, read = harness
    # 40 numbers per record cannot be cut, so about four records fit 4096 bytes.
    ids = [f"t:{i:02d}" for i in range(12)]
    store.update({i: record(i, "x" * 2000, **{f"n{k}": 10**9 + k for k in range(40)}) for i in ids})
    seen, token, pages = [], None, 0
    while True:
        page = read(ids, max_bytes=4096, token=token)
        pages += 1
        assert measure(page) <= 4096
        served = [r["node_id"] for r in page["records"]]
        assert served, "a page always serves at least one record"
        assert served == ids[len(seen):len(seen) + len(served)]
        seen += served
        rest = ids[len(seen):]
        assert page.get("omitted", []) == rest
        assert page.get("remaining_count", 0) == len(rest)
        assert page["truncated"] is bool(rest) and page["complete"] is (not rest)
        if token is not None:
            assert read(ids, max_bytes=4096, token=token) == page, "a retried token reads the same page"
        token = page.get("continuation_token")
        if token is None:
            break
        assert pages < 20
    assert seen == ids and pages > 1


def test_missing_or_hidden_records_are_marked_and_do_not_stop_the_others(harness):
    store, reads, read = harness
    store.update({"t:1": record("t:1", "one"), "t:gone": NodeNotFound("no such node"),
                  "t:2": record("t:2", "two")})
    page = read(["t:1", "t:gone", "t:absent", "t:2"], max_bytes=8192)
    assert [(r["node_id"], r["status"]) for r in page["records"]] == [
        ("t:1", "ok"), ("t:gone", "not_visible"), ("t:absent", "not_visible"), ("t:2", "ok"),
    ]
    # A not-visible record says nothing but its requested id and status.
    assert page["records"][1] == {"node_id": "t:gone", "status": "not_visible"}
    assert page["records"][2] == {"node_id": "t:absent", "status": "not_visible"}
    assert page["complete"] is True


def test_an_error_that_is_not_about_one_record_refuses_the_batch(harness):
    store, reads, read = harness

    class PinLost(SubstrateError):
        code = "generation_pin_lost"

    store.update({"t:1": record("t:1"), "t:2": PinLost("the pin is gone")})
    with pytest.raises(PinLost):
        read(["t:1", "t:2"], max_bytes=8192)


def test_refusal_names_a_budget_that_then_serves_the_first_record(harness):
    store, reads, read = harness
    store["t:wide"] = record("t:wide", "w" * 500, **{f"n{k}": 10**9 + k for k in range(400)})
    with pytest.raises(batch.NodeBatchBudgetTooSmall) as refused:
        read(["t:wide"], max_bytes=4096)
    data = refused.value.data
    assert data["max_bytes"] == 4096 and data["required_budget_bytes"] > 4096
    assert "max_bytes" in refused.value.fix_hint and "select" in refused.value.fix_hint
    page = read(["t:wide"], max_bytes=data["required_budget_bytes"])
    assert page["records"][0]["node_id"] == "t:wide"
    assert measure(page) <= data["required_budget_bytes"]


def test_a_reference_is_read_in_its_order_fifty_records_a_page(harness):
    store, reads, read = harness
    members = [f"m:{i:03d}" for i in range(120)]
    store.update({i: {"node_id": i, "subtype": "ticket", "attrs": {"k": 1}} for i in members})
    seen, token, sizes = [], None, []
    while True:
        page = read(ref="r1", members=members, max_bytes=65536, token=token)
        assert page["request_count"] == 120
        sizes.append(len(page["records"]))
        seen += [r["node_id"] for r in page["records"]]
        token = page.get("continuation_token")
        if token is None:
            break
    assert seen == members and sizes == [50, 50, 20]
    # Every page of a reference read says how many rows the reference holds;
    # a read by ids does not.
    assert page["reference_members"] == 120
    store["t:1"] = record("t:1")
    assert "reference_members" not in read(["t:1"], max_bytes=8192)


def test_a_token_is_bound_to_its_ids_select_and_budget(harness):
    store, reads, read = harness
    ids = [f"t:{i:02d}" for i in range(12)]
    store.update({i: record(i, "x" * 2000, **{f"n{k}": 10**9 + k for k in range(40)}) for i in ids})
    token = read(ids, max_bytes=4096)["continuation_token"]
    for changed in (
        {"ids": list(reversed(ids))},
        {"ids": ids[:-1]},
        {"ids": ids, "select": ["attrs.summary"]},
        {"ids": ids, "max_bytes": 8192},
    ):
        reads.clear()
        with pytest.raises(ContinuationTokenInvalid):
            read(changed["ids"], max_bytes=changed.get("max_bytes", 4096),
                 select=changed.get("select"), token=token)
        assert reads == [], "the binding is compared before any record is read"
    with pytest.raises(ContinuationTokenInvalid):
        read(ids, max_bytes=4096, token="not-a-token")


def test_ids_too_long_for_a_continuation_are_refused_only_when_one_is_needed(harness):
    import random

    store, reads, read = harness
    rng = random.Random(3657)
    # 50 ids of 700 random characters: more than a token can carry.
    ids = ["t:" + "".join(rng.choice("0123456789abcdef") for _ in range(700)) for _ in range(50)]
    store.update({i: {"node_id": i, "subtype": "ticket", "attrs": {"k": 1}} for i in ids})
    # Everything fits one page: no token is needed and the read is served.
    page = read(ids, max_bytes=262144)
    assert page["complete"] is True and len(page["records"]) == 50
    # A page that must continue cannot sign these ids into its token.
    with pytest.raises(batch.NodeBatchStateTooLarge) as refused:
        read(ids, max_bytes=16384)
    assert refused.value.code == "node_batch_state_too_large"
    assert refused.value.fix_hint == "pass fewer or shorter node_ids per call"
    assert set(refused.value.data) <= {"max_decoded_token_bytes", "max_encoded_token_chars"}
    # The named fix works: fewer ids per call.
    assert read(ids[:10], max_bytes=16384)["records"]


@pytest.mark.parametrize(("target", "fields"), [
    ({"kind": "nodes", "node_ids": [str(i) for i in range(51)]}, ["target.nodes.node_ids"]),
    ({"kind": "nodes", "node_ids": ["a"], "ref": "r"}, ["target.nodes.node_ids", "target.nodes.ref"]),
    ({"kind": "nodes"}, ["target.nodes.node_ids", "target.nodes.ref"]),
    ({"kind": "nodes", "node_ids": []}, ["target.nodes.node_ids"]),
    ({"kind": "nodes", "node_ids": ["a", "a"]}, ["target.nodes.node_ids"]),
    ({"kind": "nodes", "node_id": "a"}, ["target.nodes.node_id"]),
    ({"kind": "nodes", "node_ids": "a"}, ["target.nodes.node_ids"]),
    ({"kind": "nodes", "node_ids": ["a", 7]}, ["target.nodes.node_ids"]),
    ({"kind": "nodes", "node_ids": ["a", ""]}, ["target.nodes.node_ids"]),
    ({"kind": "nodes", "node_ids": ["a"], "hydrate": True}, ["target.nodes.hydrate"]),
    ({"kind": "node", "node_ids": ["a"]}, ["target.node.node_ids"]),
    ({"kind": "node", "node_id": ["a", "b"]}, ["target.node.node_id"]),
])
def test_a_wrong_shape_gets_a_message_and_fix_that_do_not_echo_ids(target, fields):
    message, fix_hint, named = batch.shape_refusal(target)
    assert named == fields
    assert message and fix_hint.startswith(("pass ", "to read several", "remove hydrate"))
    assert "'a'" not in message + fix_hint


@pytest.mark.parametrize("target", [
    None, "nodes", {"kind": "text", "node_id": "a"}, {"kind": "node", "node_id": "a"},
    {"kind": "nodes", "node_ids": ["a", "b"]}, {"kind": "nodes", "ref": "r"},
    {"kind": "nodes", "node_ids": ["a"], "max_bytes": 1},
])
def test_other_shapes_are_left_to_the_general_refusal(target):
    assert batch.shape_refusal(target) is None
