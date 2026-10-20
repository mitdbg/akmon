"""Search hits carry a location; `inspect` text reads at it (PACT read-model-s5-location).

PostgreSQL tests on a published graph, through the served tools. A lexical
hit on a `document_chunk` (its own row, a record it folds into, or a
passage-ranked parent) carries `location`: the chunk, the character offset
where its excerpt starts in the chunk's text, and the text's hash. `inspect`
kind text with `at` reads from there; a location that no longer holds is
refused as `text_location_stale`, and under Nomos enforce a hidden chunk, a
chunk of a hidden record and a missing chunk give one answer.
"""
from __future__ import annotations

import json
import re
import uuid

import psycopg
import pytest

from okg.substrate.graph_store import ChronosGraphStore
from okg.substrate.library.sources.base import EdgeFact, NodeFact
from okg.substrate.mcp.record_text import AT_LEAD_CHARS, TextLocationStale
from okg.substrate.mcp.response_bounds import InvalidOperatorArguments
from okg.substrate.mcp.result_text import render
from okg.substrate.mcp.server import build_server
from okg.substrate.nomos import activate_policy_bundle, compile_policy_graph
from okg.substrate.publisher import Publisher
from tests.pytest.catalog_fixture_builders import SubtypeSpec, bind_test_catalog_writer, seed_catalog

pytestmark = pytest.mark.postgres

TERM = "zirconium"
SENTENCE = f"{TERM} cladding failed at the weld."
#: Long enough that the excerpt window does not start at 0.
DEEP = ("filler " * 400) + SENTENCE + (" tail" * 1200)
#: A hash in the form search returns that matches no text.
WRONG_HASH = "sha256:" + "0" * 64

_FENCE = re.compile(r"\A⟦UNTRUSTED_DATA[^⟧]*⟧\n(.*)\n⟦/UNTRUSTED_DATA⟧\Z", re.S)


def _plain(text: str) -> str:
    match = _FENCE.match(text)
    return match.group(1) if match else text


def _policy() -> dict:
    return {
        "policy_bundle_id": f"nomos:test:location:{uuid.uuid4().hex[:8]}",
        "policy_version": "v1",
        "default_effect": "allow",
        "rules": [
            {
                "rule_id": "hide_secret", "effect": "deny", "priority": 300,
                "subjects": ["subject:mcp-reader"], "actions": ["graph.read.mcp"],
                "match": {"classification": "secret"},
            },
            {
                "rule_id": "allow_read", "effect": "allow", "priority": 100,
                "subjects": ["subject:mcp-reader"], "actions": ["graph.read.mcp"],
            },
        ],
    }


def _record(doc: str, texts: list[str], *, secret: frozenset[int] = frozenset(),
            doc_secret: bool = False) -> list:
    facts: list = [NodeFact(
        doc, "document",
        {"classification": "secret" if doc_secret else "internal",
         "text": "Quarterly review.", "path": f"{doc}.md"},
        {"id": doc}, {"v": 1},
    )]
    for i, text in enumerate(texts):
        chunk = f"chunk:{doc}:{i:02d}"
        cls = "secret" if i in secret else "internal"
        facts += [
            NodeFact(chunk, "document_chunk", {"classification": cls, "text": text},
                     {"id": chunk}, {"v": 1}),
            EdgeFact(chunk, doc, "member_of",
                     {"classification": cls, "chunk_ordinal": i, "chunk_total": len(texts)},
                     source_record_id={"id": f"{chunk}-m"}, source_revision={"v": 1}),
            EdgeFact(doc, chunk, "contains", {"classification": cls},
                     source_record_id={"id": f"{chunk}-c"}, source_revision={"v": 1}),
        ]
    return facts


def _seed(dsn: str, facts: list) -> None:
    with psycopg.connect(dsn, autocommit=True) as conn:
        seed_catalog(
            conn,
            subtypes=[
                SubtypeSpec("document", metadata={
                    "searchable_text_fields": ["text"], "path_from": "path",
                    "passage_rollup": {"edge_type": "contains",
                                       "child_subtype": "document_chunk"},
                }),
                SubtypeSpec("document_chunk", metadata={
                    "searchable_text_fields": ["text"],
                    "pii_classes": {"classification": "non_pii", "text": "free_text"},
                }),
            ],
            narrowings=[
                ("document", "contains", "document_chunk"),
                ("document_chunk", "member_of", "document"),
            ],
        )
        activate_policy_bundle(
            conn, deployment_name="default", bundle=compile_policy_graph(_policy()),
            scope="deployment:default", activated_by="test:operator", bootstrap=True,
        )
        ChronosGraphStore.from_connection(conn).apply_facts(
            facts, source="default", strict_endpoints=True,
        )
    published = Publisher(
        dsn, "default", catalog_identity=bind_test_catalog_writer(dsn, "default"),
    ).cycle()
    assert published["status"] == "published", published


def _tool(mcp, name: str):
    return mcp._tool_manager._tools[name].fn


def _stale(mcp, target: dict) -> TextLocationStale:
    with pytest.raises(TextLocationStale) as refused:
        _tool(mcp, "inspect")(target={"kind": "text", **target}, max_bytes=4096)
    assert refused.value.code == "text_location_stale"
    return refused.value


def test_a_hit_carries_its_location_and_inspect_reads_from_there(
    clean_runtime: str, okg_mcp_can_log_in: str,
) -> None:
    doc = f"doc:{uuid.uuid4().hex[:8]}"
    chunk = f"chunk:{doc}:01"
    _seed(clean_runtime, _record(doc, ["Opening words.", DEEP, "Closing words."]))
    mcp = build_server(dsn=clean_runtime, deployment_name="default")
    try:
        search = _tool(mcp, "search")
        page = search(query=TERM, method="lexical", limit=10)
        (row,) = [r for r in page["results"] if r["node_id"] == doc]
        location = row["location"]
        assert row["rollup_best_chunk_id"] == chunk
        assert location["chunk"] == chunk
        offset = location["offset"]
        assert offset > 0
        # The offset is where the excerpt's window starts in the chunk text.
        body = _plain(row["excerpt"]).removeprefix("...").removesuffix("...")
        assert DEEP[offset:].startswith(body)
        assert location["hash"].startswith("sha256:") and len(location["hash"]) == 71
        # #3666 (read-passage-rows): the text row shows the matched passage
        # and the reply names one read call; the location (chunk, offset,
        # hash) is in the JSON form, where `inspect` `at` takes it.
        text = render(page, tool="search")
        card = next(line for line in text.split("\n") if line.startswith("[1] "))
        assert f"@ {chunk}" not in card
        assert "sha256" not in text and f"best {chunk}" not in card

        # A self-ranked hit on the chunk points into its own text.
        selfp = search(query=TERM, method="lexical", limit=10, rank_by="self")
        (own,) = [r for r in selfp["results"] if r["node_id"] == chunk]
        assert own["location"] == location
        # A passage-ranked parent points at its winning chunk.
        passages = search(query=TERM, method="lexical", limit=10, rank_by="passages",
                          subtype_filter="document")
        (parent,) = [r for r in passages["results"] if r["node_id"] == doc]
        assert parent["location"] == location
        # A narrowed select sends none.
        narrow = search(query=TERM, method="lexical", limit=10, select=["node_id", "excerpt"])
        assert all("location" not in r for r in narrow["results"])

        # Read the record from the location: the first segment is that chunk,
        # starting AT_LEAD_CHARS before the offset, and the read goes on.
        inspect = _tool(mcp, "inspect")
        first = inspect(target={"kind": "text", "node_id": doc, "at": location}, max_bytes=4096)
        assert first.get("status") == "ok", first
        assert first["items"][0]["chunk_id"] == chunk
        assert first["items"][0]["text_range"][0] == offset - AT_LEAD_CHARS
        assert SENTENCE in _plain(first["items"][0]["text"])
        assert first["complete"] is False
        pages = [first]
        while not pages[-1]["complete"]:
            pages.append(inspect(target={
                "kind": "text", "node_id": doc, "at": location,
                "continuation_token": pages[-1]["continuation_token"],
            }, max_bytes=4096))
        read = "".join(_plain(i["text"]) for p in pages for i in p["items"])
        assert read == DEEP[offset - AT_LEAD_CHARS:] + "Closing words."
        # Review fix 2: every page, the complete last one included, says the
        # read began mid-record and the earlier text was not sent.
        for page in pages:
            assert page["read_from"] == {
                "chunk": chunk, "offset": offset - AT_LEAD_CHARS, "text_before_sent": False,
            }
        assert pages[-1]["complete"] is True
        assert (f"! read starts at {chunk} @{offset - AT_LEAD_CHARS}; earlier text was not sent"
                in render(pages[-1], tool="inspect", kind="text"))
        # Review fix 1(a): the location carried its hash, so the check was
        # not offset-only; without the hash the page says it was.
        assert all("location_check" not in page for page in pages)
        bare = {"chunk": chunk, "offset": offset}
        weak = inspect(target={"kind": "text", "node_id": doc, "at": bare}, max_bytes=4096)
        assert weak["location_check"] == "offset_only"
        assert "! location checked by offset only (no hash)" in render(
            weak, tool="inspect", kind="text")

        # `at` alone reads the chunk itself and points on through the record.
        alone = inspect(target={"kind": "text", "at": location}, max_bytes=4096)
        assert alone["node_id"] == chunk and alone["order_basis"] == "single_chunk"
        assert SENTENCE in _plain(alone["items"][0]["text"])
        hint = alone["outcome"]["next_move"]
        # Review fix 1(b): the follow-on read carries the caller's hash.
        assert json.dumps(doc) in hint and json.dumps(
            {"chunk": chunk, "offset": offset, "hash": location["hash"]}, separators=(",", ":"),
        ) in hint
        unhashed = inspect(target={"kind": "text", "at": bare}, max_bytes=4096)
        assert (f'"at":{{"chunk":{json.dumps(chunk)},"offset":{offset}}}'
                in unhashed["outcome"]["next_move"])

        # Failure cases: each stale location is refused by name.
        assert _stale(mcp, {"node_id": doc, "at": {**location, "hash": WRONG_HASH}}).data[
            "reason"] == "hash_mismatch"
        # Review LOW 4: a hash not in search's form is a malformed argument.
        for malformed in ("SHA256:AB", "sha256:00", location["hash"].upper()):
            with pytest.raises(InvalidOperatorArguments):
                inspect(target={"kind": "text", "node_id": doc,
                                "at": {**location, "hash": malformed}}, max_bytes=4096)
        assert _stale(mcp, {"node_id": doc, "at": {"chunk": chunk, "offset": len(DEEP)}}).data[
            "reason"] == "offset_out_of_range"
        assert _stale(mcp, {"node_id": doc, "at": {"chunk": f"{chunk}-gone", "offset": 0}}).data[
            "reason"] == "chunk_not_found"
        assert _stale(mcp, {"node_id": f"{doc}-gone", "at": location}).data[
            "reason"] == "chunk_not_found"
    finally:
        mcp._okg_pool.close()  # type: ignore[attr-defined]


def _answer(err: TextLocationStale, *ids: str) -> tuple:
    """The refusal with the ids it was given replaced, for comparison."""
    blob = json.dumps({"message": err.message, "data": err.data, "hint": err.fix_hint,
                       "code": err.code}, sort_keys=True)
    for i, value in enumerate(ids):
        blob = blob.replace(value, f"<id{i}>")
    return blob


def test_hidden_chunks_and_records_answer_exactly_as_missing_ones(
    clean_runtime: str, okg_mcp_can_log_in: str, monkeypatch: pytest.MonkeyPatch,
) -> None:
    sfx = uuid.uuid4().hex[:8]
    doc, hidden_doc = f"doc:{sfx}", f"doc:{sfx}-hidden"
    secret_chunk = f"chunk:{doc}:01"
    chunk_of_hidden = f"chunk:{hidden_doc}:00"
    missing_chunk, missing_doc = f"chunk:{sfx}-missing", f"doc:{sfx}-missing"
    _seed(clean_runtime,
          _record(doc, ["Open text.", DEEP], secret=frozenset({1}))
          + _record(hidden_doc, [DEEP], doc_secret=True))

    monkeypatch.setenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", "enforce")
    monkeypatch.setenv("OKG_NOMOS_SUBJECT", "subject:mcp-reader")
    mcp = build_server(dsn=clean_runtime, deployment_name="default")
    try:
        # A hidden chunk, read alone or in its visible record.
        for target in ({}, {"node_id": doc}):
            hidden = _stale(mcp, {**target, "at": {"chunk": secret_chunk, "offset": 5}})
            missing = _stale(mcp, {**target, "at": {"chunk": missing_chunk, "offset": 5}})
            assert hidden.data["reason"] == "chunk_not_found"
            assert _answer(hidden, secret_chunk) == _answer(missing, missing_chunk)
        # A visible chunk of a hidden record, with a guessed hash too.
        hidden = _stale(mcp, {"node_id": hidden_doc,
                              "at": {"chunk": chunk_of_hidden, "offset": 5, "hash": WRONG_HASH}})
        missing = _stale(mcp, {"node_id": missing_doc,
                               "at": {"chunk": missing_chunk, "offset": 5, "hash": WRONG_HASH}})
        # Chunk ids first: the hidden chunk's id contains its record's id.
        assert _answer(hidden, chunk_of_hidden, hidden_doc) == _answer(
            missing, missing_chunk, missing_doc)
        assert "Quarterly" not in str(hidden.data) and TERM not in hidden.message
        # The secret chunk never reaches a search hit's location.
        page = _tool(mcp, "search")(query=TERM, method="lexical", limit=10)
        assert secret_chunk not in json.dumps(page)
    finally:
        mcp._okg_pool.close()  # type: ignore[attr-defined]

    # With enforcement off the same location reads: the hidden one exists.
    monkeypatch.setenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", "off")
    mcp = build_server(dsn=clean_runtime, deployment_name="default")
    try:
        page = _tool(mcp, "inspect")(
            target={"kind": "text", "node_id": doc, "at": {"chunk": secret_chunk, "offset": 5}},
            max_bytes=4096,
        )
        assert page["items"][0]["chunk_id"] == secret_chunk
    finally:
        mcp._okg_pool.close()  # type: ignore[attr-defined]


def test_copied_location_continues_through_published_wire_schema(
    clean_runtime: str, okg_mcp_can_log_in: str,
) -> None:
    import anyio
    import jsonschema
    from mcp.shared.memory import create_connected_server_and_client_session
    from okg.substrate.mcp.result_text import fact_value, status_facts
    from tests.substrate.mcp._result_payload import JSON_META, payload, result_text

    doc = f"doc:{uuid.uuid4().hex[:8]}"
    _seed(clean_runtime, _record(doc, ["Opening words.", DEEP, "Closing " * 5000]))
    mcp = build_server(dsn=clean_runtime, deployment_name="default")

    async def run() -> None:
        async with create_connected_server_and_client_session(mcp) as client:
            schemas = {tool.name: tool.inputSchema for tool in (await client.list_tools()).tools}
            hit = payload(await client.call_tool("search", {
                "query": TERM, "method": "lexical", "limit": 10,
            }, meta=JSON_META))
            row = next(row for row in hit["results"] if row["node_id"] == doc)
            args = {"target": {"kind": "text", "node_id": doc, "at": row["location"]},
                    "generation_id": hit["pinned_generation_id"], "max_bytes": 4096}
            jsonschema.validate(args, schemas["inspect"])
            baseline = payload(await client.call_tool("inspect", {**args, "max_bytes": 131072}, meta=JSON_META))
            assert baseline["complete"]
            expected = "".join(_plain(item["text"]) for item in baseline["items"])
            pages = []
            current = args
            for _ in range(40):
                result = await client.call_tool("inspect", current, meta=JSON_META)
                assert len(result.model_dump_json(by_alias=True, exclude_none=True).encode()) <= 4096
                data = payload(result)
                pages.append(data)
                if data["complete"]:
                    break
                token = data["continuation_token"]
                current = {"next": token}
                jsonschema.validate(current, schemas["inspect"])
            else:
                raise AssertionError("location read did not finish")
            assert len(pages) > 1
            assert "".join(_plain(item["text"]) for page in pages for item in page["items"]) == expected
            assert {page["pinned_generation_id"] for page in pages} == {hit["pinned_generation_id"]}
            current = args
            for _ in range(40):
                result = await client.call_tool("inspect", current)
                assert len(result.model_dump_json(by_alias=True, exclude_none=True).encode()) <= 4096
                text = result_text(result)
                token = fact_value(status_facts(text), "next:", last=True)
                if token is None:
                    assert "complete" in status_facts(text)
                    break
                current = {"next": token}
            else:
                raise AssertionError("text location read did not finish")
            for invalid in (True, False, -1):
                refused = payload(await client.call_tool("inspect", {
                    "target": {"kind": "text", "node_id": doc,
                               "at": {"chunk": row["location"]["chunk"], "offset": invalid}},
                }, meta=JSON_META))
                assert refused["error"]["code"] == "invalid_operator_arguments"
    try:
        anyio.run(run)
    finally:
        mcp._okg_pool.close()


def test_budget_retry_call_continues_a_located_read_where_it_stopped(
    clean_runtime: str, okg_mcp_can_log_in: str,
) -> None:
    """A located read's token, read again under a new budget (PACT read-model-s6-page-next).

    The text binding signs the node read (the record, or the chunk for `at`
    alone) and the cursor, not `at`. So the retry call a budget refusal names
    (`continuation.budget_retry_call`), which carries no `at`, must continue
    from the token's position -- inside the located chunk -- and never restart
    at the record's top or lose the at-alone read's chunk.
    """
    import anyio
    from mcp.shared.memory import create_connected_server_and_client_session
    from okg.substrate.mcp import continuation
    from tests.substrate.mcp._result_payload import JSON_META, payload

    doc = f"doc:{uuid.uuid4().hex[:8]}"
    chunk = f"chunk:{doc}:01"
    _seed(clean_runtime, _record(doc, ["Opening words.", DEEP, "Closing " * 3000]))
    mcp = build_server(dsn=clean_runtime, deployment_name="default")

    def where(item: dict) -> tuple:
        return item["chunk_id"], item.get("text_range", [0])[0]

    async def run() -> None:
        async with create_connected_server_and_client_session(mcp) as client:
            hit = payload(await client.call_tool("search", {
                "query": TERM, "method": "lexical", "limit": 10,
            }, meta=JSON_META))
            location = next(r for r in hit["results"] if r["node_id"] == doc)["location"]
            for target, read_node in (
                ({"kind": "text", "node_id": doc, "at": location}, doc),
                ({"kind": "text", "at": location}, chunk),
            ):
                first = payload(await client.call_tool("inspect", {
                    "target": target, "max_bytes": 4096,
                }, meta=JSON_META))
                assert first["items"][0]["chunk_id"] == chunk, first
                token = first["continuation_token"]
                assert token, first
                binding = continuation.open_continuation(token)["binding"]
                call = continuation.budget_retry_call(binding)
                assert call is not None and call["mode"] == "continue", call
                assert call["arguments"]["target"]["node_id"] == read_node
                assert "at" not in call["arguments"]["target"]
                arguments = json.loads(json.dumps(call["arguments"]).replace(
                    json.dumps(continuation.TOKEN_PLACEHOLDER), json.dumps(token),
                ))
                by_next = payload(await client.call_tool(
                    "inspect", {"next": token}, meta=JSON_META,
                ))
                retried = payload(await client.call_tool(
                    "inspect", {**arguments, "max_bytes": 8192}, meta=JSON_META,
                ))
                assert retried.get("status", "ok") == "ok", retried.get("error")
                # Same place as `next`, under the new budget: past page 1,
                # still in the located chunk, not back at the record's top.
                assert where(retried["items"][0]) == where(by_next["items"][0])
                assert where(retried["items"][0]) > where(first["items"][0])
                assert retried["items"][0]["chunk_id"] == chunk
                if retried.get("continuation_token"):
                    # The new budget is signed into the following token.
                    renewed = continuation.open_continuation(retried["continuation_token"])
                    assert renewed["binding"]["projection"]["max_bytes"] == 8192
                else:
                    # Only the at-alone read (one chunk) can end here; the
                    # record's long closing chunk cannot fit 8192 bytes.
                    assert read_node == chunk and retried["complete"] is True, retried

            # Failure case: the at-alone token (the loop's last) under another
            # node is refused.
            wrong = payload(await client.call_tool("inspect", {
                "target": {"kind": "text", "node_id": doc, "continuation_token": token},
                "max_bytes": 8192,
            }, meta=JSON_META))
            assert wrong["error"]["code"] == "continuation_token_invalid", wrong
            assert wrong["error"]["data"]["reason"] == "binding_mismatch", wrong
    try:
        anyio.run(run)
    finally:
        mcp._okg_pool.close()  # type: ignore[attr-defined]


def test_an_over_limit_max_chars_is_lowered_and_said_not_refused(
    clean_runtime: str, okg_mcp_can_log_in: str,
) -> None:
    """Issue #3252: agents send max_chars 4,500 and 5,000; the search tool
    lowers it to the maximum, names the applied value and still answers.
    A value under the floor, or not an integer, is still refused."""
    from okg.substrate.mcp.search import (
        MAX_PREVIEW_CHARS,
        MIN_PREVIEW_CHARS,
        InvalidPreviewCharBudget,
    )

    doc = f"doc:{uuid.uuid4().hex[:8]}"
    _seed(clean_runtime, _record(doc, ["Opening words.", DEEP, "Closing words."]))
    mcp = build_server(dsn=clean_runtime, deployment_name="default")
    try:
        search = _tool(mcp, "search")
        for asked in (4500, 5000, MAX_PREVIEW_CHARS + 1):
            page = search(query=TERM, method="lexical", limit=5, max_chars=asked)
            assert page["results"], page
            assert page["max_chars_lowered"] == {
                "requested": asked, "applied": MAX_PREVIEW_CHARS,
            }
            text = render(page, tool="search")
            assert f"! max_chars {asked} lowered to {MAX_PREVIEW_CHARS}" in text
            assert "max_chars_lowered" not in text  # said once, by the notice
            # The page is the one the maximum serves.
            capped = search(
                query=TERM, method="lexical", limit=5, max_chars=MAX_PREVIEW_CHARS,
            )
            assert "max_chars_lowered" not in capped
            assert [r["excerpt"] for r in page["results"]] == [
                r["excerpt"] for r in capped["results"]
            ]
        for bad in (MIN_PREVIEW_CHARS - 1, 0, "5000", 1.5, True):
            with pytest.raises(InvalidPreviewCharBudget):
                search(query=TERM, method="lexical", limit=5, max_chars=bad)

        # Through the served wire, as a client receives it: lexical and grep,
        # in the JSON format a program reads and the text a model reads.
        import anyio
        from mcp.shared.memory import create_connected_server_and_client_session
        from tests.substrate.mcp._result_payload import JSON_META, payload, result_text

        async def run() -> None:
            async with create_connected_server_and_client_session(mcp) as client:
                for method in ("lexical", "grep"):
                    args = {"query": TERM, "method": method, "limit": 5, "max_chars": 5000}
                    served = await client.call_tool("search", args, meta=JSON_META)
                    assert not served.isError, served
                    data = payload(served)
                    assert data["results"], data
                    assert data["max_chars_lowered"] == {
                        "requested": 5000, "applied": MAX_PREVIEW_CHARS,
                    }
                    text = result_text(await client.call_tool("search", args))
                    assert f"! max_chars 5000 lowered to {MAX_PREVIEW_CHARS}" in text
                refused = await client.call_tool(
                    "search", {"query": TERM, "method": "lexical", "max_chars": 63},
                    meta=JSON_META,
                )
                assert refused.isError
                assert "invalid_preview_char_budget" in result_text(refused)

        anyio.run(run)
    finally:
        mcp._okg_pool.close()  # type: ignore[attr-defined]


# ---------- issue #3002: a later page reuses the record text it built ----------

#: Long enough that a 4,096-byte page holds only a few chunks.
PAGE_CHUNK = "routine inspection reading recorded without findings. " * 12


def _walk_text(mcp, doc: str) -> list[dict]:
    inspect = _tool(mcp, "inspect")
    pages, token = [], None
    while True:
        target = {"kind": "text", "node_id": doc}
        if token:
            target["continuation_token"] = token
        page = inspect(target=target, max_bytes=4096)
        pages.append(page)
        token = page.get("continuation_token")
        if not token:
            return pages


def _count_record_reads(monkeypatch) -> list[int]:
    from okg.substrate.mcp import record_text

    calls = [0]
    real = record_text._occurrence_edges

    def counted(*args, **kwargs):
        calls[0] += 1
        return real(*args, **kwargs)

    monkeypatch.setattr(record_text, "_occurrence_edges", counted)
    return calls


def _text_of(pages: list[dict]) -> list:
    return [[(item.get("chunk_id"), item.get("text")) for item in p["items"]] for p in pages]


def test_later_pages_of_a_record_reuse_its_text_and_serve_the_same_pages(
    clean_runtime: str, okg_mcp_can_log_in: str, monkeypatch,
) -> None:
    monkeypatch.setenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", "off")
    doc = f"doc:{uuid.uuid4().hex[:8]}"
    _seed(clean_runtime, _record(doc, [f"{i:03d} {PAGE_CHUNK}" for i in range(40)]))
    mcp = build_server(dsn=clean_runtime, deployment_name="default")
    try:
        monkeypatch.setenv("OKG_RECORD_TEXT_CACHE_ENTRIES", "0")
        reads = _count_record_reads(monkeypatch)
        uncached = _walk_text(mcp, doc)
        assert len(uncached) >= 3
        assert reads[0] == len(uncached)  # the old path: every page reads it all

        monkeypatch.setenv("OKG_RECORD_TEXT_CACHE_ENTRIES", "8")
        reads[0] = 0
        cached = _walk_text(mcp, doc)
        # One read of the record for the whole walk, not one per page.
        assert reads[0] == 1, reads
        assert _text_of(cached) == _text_of(uncached)
        assert [p["complete"] for p in cached] == [p["complete"] for p in uncached]
        assert cached[-1]["complete"] is True
    finally:
        mcp._okg_pool.close()  # type: ignore[attr-defined]


@pytest.mark.parametrize("mode", ["audit", "enforce"])
def test_under_nomos_a_walk_reads_the_record_once_and_serves_the_uncached_pages(
    clean_runtime: str, okg_mcp_can_log_in: str, monkeypatch, mode: str,
) -> None:
    """PACT record-text-nomos-page-cache-3002: with a policy bundle active, a
    walk's first page builds and filters the record once and later pages reuse
    it. The pages match the cache-off walk, and under enforce the secret chunk's
    text is served on no page, cached or not."""
    monkeypatch.setenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", mode)
    monkeypatch.setenv("OKG_NOMOS_SUBJECT", "subject:mcp-reader")
    doc = f"doc:{uuid.uuid4().hex[:8]}"
    texts = [f"{i:03d} {PAGE_CHUNK}" for i in range(30)]
    texts[12] = f"012 {SENTENCE} {PAGE_CHUNK}"
    _seed(clean_runtime, _record(doc, texts, secret=frozenset({12})))
    mcp = build_server(dsn=clean_runtime, deployment_name="default")
    try:
        monkeypatch.setenv("OKG_RECORD_TEXT_CACHE_ENTRIES", "0")
        reads = _count_record_reads(monkeypatch)
        uncached = _walk_text(mcp, doc)
        assert len(uncached) >= 3
        assert reads[0] == len(uncached)  # cache off: every page reads it all

        monkeypatch.setenv("OKG_RECORD_TEXT_CACHE_ENTRIES", "8")
        reads[0] = 0
        cached = _walk_text(mcp, doc)
        assert reads[0] == 1, reads  # one build for the whole walk
        assert _text_of(cached) == _text_of(uncached)
        assert [p["complete"] for p in cached] == [p["complete"] for p in uncached]
        assert cached[-1]["complete"] is True
        served = json.dumps(cached)
        if mode == "enforce":
            assert TERM not in served and TERM not in json.dumps(uncached)
            assert f"chunk:{doc}:12" not in served
        else:  # audit records decisions but hides nothing
            assert TERM in served
    finally:
        mcp._okg_pool.close()  # type: ignore[attr-defined]


def test_a_chunk_reads_through_whichever_edge_the_reader_may_see(
    clean_runtime: str, okg_mcp_can_log_in: str, monkeypatch,
) -> None:
    """Issue #3002: a record read decides member_of edges first and contains
    edges only for chunks no visible member_of places. A chunk whose member_of
    is hidden still reads through a visible contains edge; a hidden chunk, or
    one whose two edges are both hidden, is neither shown nor counted."""
    monkeypatch.setenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", "enforce")
    monkeypatch.setenv("OKG_NOMOS_SUBJECT", "subject:mcp-reader")
    doc = f"doc:{uuid.uuid4().hex[:8]}"
    # (chunk, member_of, contains) classifications per chunk.
    plan = [
        ("internal", "internal", "internal"),
        ("internal", "secret", "internal"),   # reads through contains
        ("secret", "internal", "internal"),   # hidden chunk
        ("internal", "secret", "secret"),     # no visible edge
        ("internal", "internal", "secret"),   # member_of is enough
    ]
    facts: list = [NodeFact(doc, "document", {"classification": "internal",
                                              "text": "Quarterly review."},
                            {"id": doc}, {"v": 1})]
    for i, (node_cls, member_cls, contains_cls) in enumerate(plan):
        chunk = f"chunk:{doc}:{i:02d}"
        facts += [
            NodeFact(chunk, "document_chunk",
                     {"classification": node_cls, "text": f"text-{i}-{TERM}"},
                     {"id": chunk}, {"v": 1}),
            EdgeFact(chunk, doc, "member_of",
                     {"classification": member_cls, "chunk_ordinal": i,
                      "chunk_total": len(plan)},
                     source_record_id={"id": f"{chunk}-m"}, source_revision={"v": 1}),
            EdgeFact(doc, chunk, "contains", {"classification": contains_cls},
                     source_record_id={"id": f"{chunk}-c"}, source_revision={"v": 1}),
        ]
    _seed(clean_runtime, facts)
    mcp = build_server(dsn=clean_runtime, deployment_name="default")
    try:
        page = _tool(mcp, "inspect")(target={"kind": "text", "node_id": doc},
                                     max_bytes=65536)
        assert page["complete"] is True
        served = json.dumps(page)
        assert sorted(item["chunk_id"] for item in page["items"]) == [
            f"chunk:{doc}:{i:02d}" for i in (0, 1, 4)]
        for i in (2, 3):
            assert f"text-{i}-" not in served
            assert f"chunk:{doc}:{i:02d}" not in served
    finally:
        mcp._okg_pool.close()  # type: ignore[attr-defined]


def test_a_changed_overlay_rebuilds_the_cached_record_text(
    clean_runtime: str, okg_mcp_can_log_in: str, monkeypatch,
) -> None:
    """An effective-attr row written after the first page is served on the
    next page: the cache re-checks overlays and rebuilds."""
    monkeypatch.setenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", "off")
    monkeypatch.setenv("OKG_RECORD_TEXT_CACHE_ENTRIES", "8")
    doc = f"doc:{uuid.uuid4().hex[:8]}"
    _seed(clean_runtime, _record(doc, [f"{i:03d} {PAGE_CHUNK}" for i in range(30)]))
    mcp = build_server(dsn=clean_runtime, deployment_name="default")
    try:
        reads = _count_record_reads(monkeypatch)
        inspect = _tool(mcp, "inspect")
        first = inspect(target={"kind": "text", "node_id": doc}, max_bytes=4096)
        last_chunk = f"chunk:{doc}:29"
        with psycopg.connect(clean_runtime, autocommit=True) as conn:
            conn.execute(
                "INSERT INTO okg.attribute_effective_attrs (target_kind, target_id, "
                "generation_id, namespace, attr_key, observation_id, value, value_hash, "
                "producer) VALUES ('node', %s, %s, 'default', 'text', 'obs:lanerp', "
                "'\"OVERRIDDEN TAIL\"'::jsonb, 'h:lanerp', 'test')",
                (last_chunk, str(first["pinned_generation_id"])),
            )
        pages, token = [first], first.get("continuation_token")
        while token:
            page = inspect(
                target={"kind": "text", "node_id": doc, "continuation_token": token},
                max_bytes=4096,
            )
            pages.append(page)
            token = page.get("continuation_token")
        assert reads[0] == 2, reads  # the first page, then one rebuild
        served = "".join(_plain(item["text"]) for p in pages[1:] for item in p["items"])
        assert "OVERRIDDEN TAIL" in served
    finally:
        mcp._okg_pool.close()  # type: ignore[attr-defined]


def test_a_trust_change_reaches_the_next_cached_page(
    clean_runtime: str, okg_mcp_can_log_in: str, monkeypatch,
) -> None:
    """Review blocker on #3002: source trust is producer policy that can
    change at any time, so a cached record still classifies it per page."""
    from okg.substrate.mcp import record_text

    monkeypatch.setenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", "off")
    monkeypatch.setenv("OKG_RECORD_TEXT_CACHE_ENTRIES", "8")
    doc = f"doc:{uuid.uuid4().hex[:8]}"
    _seed(clean_runtime, _record(doc, [f"{i:03d} {PAGE_CHUNK}" for i in range(30)]))
    mcp = build_server(dsn=clean_runtime, deployment_name="default")
    label = ["trusted"]
    monkeypatch.setattr(
        record_text, "classify_sources",
        lambda conn, *, deployment_name, sources: {s: label[0] for s in sources},
    )
    try:
        reads = _count_record_reads(monkeypatch)
        inspect = _tool(mcp, "inspect")
        first = inspect(target={"kind": "text", "node_id": doc}, max_bytes=4096)
        assert not any("UNTRUSTED_DATA" in item["text"] for item in first["items"])
        label[0] = "untrusted"
        second = inspect(
            target={"kind": "text", "node_id": doc,
                    "continuation_token": first["continuation_token"]},
            max_bytes=4096,
        )
        assert reads[0] == 1  # the second page came from the cache
        assert second["items"]
        assert all("UNTRUSTED_DATA" in item["text"] for item in second["items"])
    finally:
        mcp._okg_pool.close()  # type: ignore[attr-defined]


# ---------- issue #3404: the newest-generation walk reads no metadata it skips ----------

def test_latest_generation_reads_no_checkpoint_metadata_once_warm(
    clean_runtime: str,
) -> None:
    """Every MCP call asks for the newest published generation. Checkpoint
    metadata embeds the generation's invariant summary (MBs), so the walk
    must not read it again for rows it already knows; an UPDATE to a
    checkpoint (a new row version) is still seen."""
    doc = f"doc:{uuid.uuid4().hex[:8]}"
    _seed(clean_runtime, _record(doc, ["Opening words.", "Closing words."]))
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        store = ChronosGraphStore.from_connection(conn, ensure_schema=False)
        db = store.context.db
        real_execute = db.execute
        seen: list[str] = []

        def recording(sql, *args, **kwargs):
            seen.append(str(sql))
            return real_execute(sql, *args, **kwargs)

        db.execute = recording  # type: ignore[method-assign]
        expected = store.list_generations(deployment_name="default", status="published")[-1]
        first = store.latest_generation(deployment_name="default")
        assert first is not None and first.checkpoint_id == expected.checkpoint_id
        assert first.metadata == expected.metadata
        seen.clear()
        second = store.latest_generation(deployment_name="default")
        assert second is not None and second.checkpoint_id == expected.checkpoint_id
        assert second.metadata == expected.metadata
        assert not [s for s in seen if "metadata" in s], seen
        # Positively the version walk, one statement, not a fallback path.
        assert len(seen) == 1 and "xmin" in seen[0], seen

        # A changed checkpoint is a new row version: the walk reads it again.
        conn.execute(
            "UPDATE _chronos_branch_interval_checkpoints "
            "SET metadata = (metadata::jsonb || '{\"status\": \"blocked\"}'::jsonb)::text "
            "WHERE checkpoint_id = %s",
            (expected.checkpoint_id,),
        )
        third = store.latest_generation(deployment_name="default")
        fresh = store.list_generations(deployment_name="default", status="published")
        assert (third.checkpoint_id if third else None) == (
            fresh[-1].checkpoint_id if fresh else None
        )
        assert third is None or third.checkpoint_id != expected.checkpoint_id
