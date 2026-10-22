"""`next` continues a paged read through a real server (PACT read-model-s6-page-next).

Design `read-model` section 3.5. One published fixture: a document with three
long chunks. A text read, a subtype summary and a path expansion each end
`bound reached (...) · next: <token>`; passing that token back as `next`,
alone, continues the same read until a page ends `complete`. Refusals are
checked by name: a token for another tool, `next` beside read arguments, a
forged token, a token signed before a restart, and no target at all.
"""
from __future__ import annotations

import json
import pathlib
import re
from typing import Any

import anyio
import psycopg
import pytest
from mcp.shared.memory import create_connected_server_and_client_session

from okg.substrate.graph_store import ChronosGraphStore
from okg.substrate.library.sources.base import EdgeFact
from okg.substrate.mcp import continuation
from okg.substrate.mcp.result_text import fact_value, status_facts
from okg.substrate.mcp.server import build_server
from okg.substrate.publisher import Publisher
from tests.pytest.catalog_fixture_builders import SubtypeSpec, bind_test_catalog_writer
from tests.substrate.mcp._result_payload import (
    JSON_META,
    payload as json_payload,
    result_text,
)
from tests.substrate.mcp.search_fixture import make_search_deployment, node, publish_nodes


pytestmark = pytest.mark.postgres

DOC = SubtypeSpec("document", metadata={
    "searchable_text_fields": ["title", "text"],
    "passage_rollup": {"edge_type": "contains", "child_subtype": "document_chunk"},
})
CHUNK = SubtypeSpec("document_chunk", metadata={"searchable_text_fields": ["text"]})
#: Three chunks of about 2,600 characters: a 4,096-byte page holds about one.
CHUNK_TEXTS = [f"Part {i}: " + (f"word{i} " * 370) for i in range(3)]


@pytest.fixture
def deployment(
    clean_runtime: str, okg_mcp_can_log_in: str, tmp_path: pathlib.Path,
    request: pytest.FixtureRequest,
) -> str:
    deployment = make_search_deployment(tmp_path, dsn=clean_runtime)
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
        clean_runtime, deployment, subtypes=[DOC, CHUNK],
        narrowings=[("document", "contains", "document_chunk")],
        nodes=nodes, edges=edges,
    )
    return deployment


def _drive(dsn: str, deployment: str, body) -> None:
    server = build_server(dsn=dsn, deployment_name=deployment)

    async def run() -> None:
        async with create_connected_server_and_client_session(server) as client:
            await body(client)

    try:
        anyio.run(run)
    finally:
        server._okg_pool.close()  # type: ignore[attr-defined]


def _ending(text: str) -> list[str]:
    """The ending: the last fact or two before `generation`, which stays last."""
    facts = status_facts(text)
    if facts and facts[-1].startswith("generation "):
        facts = facts[:-1]
    if not facts[-1].startswith("next: "):
        return facts[-1:]
    # A cut text page puts what is left (#3634) between the bound and next.
    bound = next(f for f in reversed(facts) if f.startswith("bound reached"))
    return [bound, facts[-1]]


async def _follow(client: Any, tool: str, first: dict, *, bound: str) -> list[dict]:
    """Read every page twice, as a model and as a program.

    In text, each cut page ends `bound reached (<bound>) · next: <token>`,
    and that token alone fetches the next page, until one ends `complete`.
    The JSON chain follows `continuation_token` through `next` the same
    way; it is the one returned, for its rows. (Each format sizes its own
    pages, so the two chains need not cut at the same place.)
    """
    result = await client.call_tool(tool, first)
    budget = first.get("max_bytes")
    if budget is not None:
        assert len(result.model_dump_json(by_alias=True, exclude_none=True).encode()) <= budget
    text = result_text(result)
    for followed in range(20):
        ending = _ending(text)
        if ending == ["complete"]:
            assert followed >= 1, "the first page was already whole"
            break
        assert ending[0] == f"bound reached ({bound})", text.split("\n", 1)[0]
        token = fact_value(status_facts(text), "next:", last=True)
        # Lane ri, tokens: a reply prints a short handle for its token.
        assert isinstance(token, str) and (
            token.startswith(continuation.HANDLE_PREFIX) or token.count(".") == 1
        ), ending
        assert len(token) <= 40 or token.count(".") == 1, token
        result = await client.call_tool(tool, {"next": token})
        if budget is not None:
            assert len(result.model_dump_json(by_alias=True, exclude_none=True).encode()) <= budget
        text = result_text(result)
    else:
        raise AssertionError("the text read did not end complete within 20 pages")
    pages: list[dict] = []
    result = await client.call_tool(tool, first, meta=JSON_META)
    if budget is not None:
        assert len(result.model_dump_json(by_alias=True, exclude_none=True).encode()) <= budget
    data = json_payload(result)
    for _ in range(20):
        pages.append(data)
        token = data.get("continuation_token")
        if not token:
            return pages
        result = await client.call_tool(tool, {"next": token}, meta=JSON_META)
        if budget is not None:
            assert len(result.model_dump_json(by_alias=True, exclude_none=True).encode()) <= budget
        data = json_payload(result)
    raise AssertionError("the JSON read did not end within 20 pages")


# Lane ri, tokens: 6144, not 8192: with a 40-character handle in place of a
# 500-character token, the whole three-chunk record fits one 8192-byte page.
@pytest.mark.parametrize(("budget", "deployment"), [(4096, 1), (6144, 1), (131072, 30)], indirect=["deployment"])
def test_inspect_text_reads_the_whole_record_by_next(
    clean_runtime: str, deployment: str, budget: int,
) -> None:
    async def body(client: Any) -> None:
        pages = await _follow(client, "inspect", {
            "target": {"kind": "text", "node_id": "doc:big"}, "max_bytes": budget,
        }, bound="max_bytes")
        assert len(pages) >= 2
        assert {page["pinned_generation_id"] for page in pages} == {
            pages[0]["pinned_generation_id"],
        }
        # Each served piece is fenced as untrusted text; unfenced and
        # joined, the pages are the record's text with nothing lost.
        text = "".join(
            re.sub(r"\n?⟦/UNTRUSTED_DATA⟧", "", re.sub(
                r"⟦UNTRUSTED_DATA[^⟧]*⟧\n?", "", item["text"],
            ))
            for page in pages for item in page["items"]
        )
        for part in CHUNK_TEXTS:
            assert part.strip() in text
        # #3605 (`nextbytes A`): `next` with the budget it was signed under
        # reads the same page as `next` alone.
        same = json_payload(await client.call_tool("inspect", {
            "next": pages[0]["continuation_token"], "max_bytes": budget,
        }, meta=JSON_META))
        assert same.get("status") != "refused", same
        assert same["items"] == pages[1]["items"]
        # #3634: every cut page says how much text is left: exactly the
        # characters the later pages serve. The last page says nothing.
        def unfenced(item: dict) -> str:
            return re.sub(r"\n?⟦/UNTRUSTED_DATA⟧", "", re.sub(
                r"⟦UNTRUSTED_DATA[^⟧]*⟧\n?", "", item["text"]))

        served = [sum(len(unfenced(item)) for item in page["items"]) for page in pages]
        for k, page in enumerate(pages[:-1]):
            left = page["text_left"]
            assert left["chars"] == sum(served[k + 1:]), (k, left, served)
            assert left["segments"] >= 1 and left["max_bytes_for_rest"] % 1024 == 0
        assert "text_left" not in pages[-1]
        # #3634 (`nextresize A`): for EVERY cut page, next with exactly the
        # size it names reads the rest from there, complete, in one page;
        # the size is one `next` accepts.
        for k, page in enumerate(pages[:-1]):
            left = page["text_left"]
            assert 4096 <= left["max_bytes_for_rest"] <= 262144, left
            if left.get("more_than_one_page"):
                continue
            rest = json_payload(await client.call_tool("inspect", {
                "next": page["continuation_token"],
                "max_bytes": left["max_bytes_for_rest"],
            }, meta=JSON_META))
            assert rest.get("complete") is True, (k, left, rest.get("text_left"))
            assert sum(len(unfenced(item)) for item in rest["items"]) == sum(served[k + 1:])
        rendered = result_text(await client.call_tool("inspect", {
            "target": {"kind": "text", "node_id": "doc:big"}, "max_bytes": budget,
        }))
        head = rendered.split("\n", 1)[0]
        # The text form sizes its own pages, so only the facts' shape is
        # compared, and their place before `next:`.
        assert re.search(r"· [\d,]+ chars left( in \d+ segments)? · max_bytes \d+ reads the "
                         r"(rest|most per page) · next: ", head), head
        assert head.index("chars left") < head.index("next: "), head

    _drive(clean_runtime, deployment, body)



def test_historical_text_token_keeps_explicit_legacy_budget(
    clean_runtime: str, deployment: str,
) -> None:
    async def body(client: Any) -> None:
        start = {"target": {"kind": "text", "node_id": "doc:big"}, "max_bytes": 4096}
        first = json_payload(await client.call_tool("inspect", start, meta=JSON_META))
        body = continuation.open_continuation(first["continuation_token"])
        body["binding"]["projection"].pop("max_bytes")
        old = continuation.issue_continuation(body["binding"], cursor=body["cursor"])
        result = await client.call_tool("inspect", {
            **start, "target": {**start["target"], "continuation_token": old},
        }, meta=JSON_META)
        assert len(result.model_dump_json(by_alias=True, exclude_none=True).encode()) <= 4096
        legacy = json_payload(result)
        assert legacy.get("items")
        # Old next-only has no original budget to restore and uses default.
        defaulted = json_payload(await client.call_tool("inspect", {"next": old}, meta=JSON_META))
        assert defaulted.get("items")
        assert defaulted["items"][0]["text_range"][0] == legacy["items"][0]["text_range"][0]

    _drive(clean_runtime, deployment, body)



@pytest.mark.parametrize("deployment", [5], indirect=True)
@pytest.mark.parametrize("historical", [False, True])
@pytest.mark.parametrize("budget", [None, 4096, 8192, 3000, 20_000_000])
@pytest.mark.parametrize("json_format", [False, True])
def test_legacy_text_budget_matrix(
    clean_runtime: str, deployment: str, historical: bool, budget: int | None,
    json_format: bool,
) -> None:
    async def body(client: Any) -> None:
        first = json_payload(await client.call_tool("inspect", {
            "target": {"kind": "text", "node_id": "doc:big"}, "max_bytes": 4096,
        }, meta=JSON_META))
        token = first["continuation_token"]
        original = continuation.open_continuation(token)
        if historical:
            original["binding"]["projection"].pop("max_bytes")
            token = continuation.issue_continuation(original["binding"], cursor=original["cursor"])
        args = {"target": {"kind": "text", "node_id": "doc:big", "continuation_token": token}}
        if budget is not None:
            args["max_bytes"] = budget
        result = await client.call_tool("inspect", args, meta=JSON_META if json_format else None)
        if budget == 20_000_000:
            assert result.isError
            assert "refused" in result_text(result)
            return
        # Issue #3153: 3000 is raised to the 4096 floor, not refused.
        chosen = max(budget, 4096) if budget is not None else 65536 if historical else 4096
        assert not result.isError, result_text(result)
        if budget == 3000:
            # Over the wire, in both formats, the raise is said.
            if json_format:
                assert json_payload(result)["max_bytes_raised"] == {
                    "requested": 3000, "applied": 4096,
                }
            else:
                assert "! max_bytes 3000 raised to 4096" in result_text(result)
        assert len(result.model_dump_json(by_alias=True, exclude_none=True).encode()) <= chosen
        if json_format:
            data = json_payload(result)
            assert data.get("items")
            following = data.get("continuation_token")
        else:
            following = fact_value(status_facts(result_text(result)), "next:", last=True)
        if chosen < 65536:
            assert following, "fixture must exercise a further signed continuation"
        if following:
            assert continuation.open_continuation(following)["binding"]["projection"]["max_bytes"] == chosen
            resumed = await client.call_tool("inspect", {"next": following}, meta=JSON_META if json_format else None)
            assert not resumed.isError
            assert len(resumed.model_dump_json(by_alias=True, exclude_none=True).encode()) <= chosen

    _drive(clean_runtime, deployment, body)


def test_inspect_subtype_summary_continues_by_next(
    clean_runtime: str, deployment: str,
) -> None:
    async def body(client: Any) -> None:
        import jsonschema

        schemas = {tool.name: tool.inputSchema for tool in (await client.list_tools()).tools}
        for name in ("inspect", "expand"):
            jsonschema.validate({"next": "a.signed-token"}, schemas[name])
            # The schema publishes no top-level anyOf (Claude Code drops a
            # tool whose input schema has one), so it no longer rejects a
            # call with neither argument; the server refuses it, by name.
            assert "anyOf" not in schemas[name]
            for missing in ({}, {"next": None}):
                _, error = await _refusal(client, name, missing)
                assert error["code"] == "invalid_operator_arguments", (name, missing)
        pages = await _follow(client, "inspect", {
            "target": {"kind": "subtype", "subtype": "document", "page_size": 1},
            "max_bytes": 131072,
        }, bound="page_size 1")
        assert len(pages) >= 2
        token = pages[0]["continuation_token"]
        for conflict in ({"select": ["node_id"]}, {"max_bytes": 100}):
            _, error = await _refusal(client, "inspect", {"next": token, **conflict})
            assert error["code"] == "next_with_read_arguments"
        # #3605 (`nextbytes A`): the token's own page size beside `next`
        # changes nothing and is read as omitted; another names that size.
        same = json_payload(await client.call_tool(
            "inspect", {"next": token, "max_bytes": 131072}, meta=JSON_META,
        ))
        assert same.get("status") != "refused", same
        assert same["attr_keys"] == pages[1]["attr_keys"]
        # #3634 (`nextresize A`): another valid size goes on at that size.
        resized = json_payload(await client.call_tool(
            "inspect", {"next": token, "max_bytes": 65536}, meta=JSON_META,
        ))
        assert resized.get("status") != "refused", resized
        assert resized["attr_keys"][0] == pages[1]["attr_keys"][0]
        _, error = await _refusal(client, "inspect", {"next": token, "max_bytes": 100})
        assert error["signed_max_bytes"] == 131072
        assert "must be an integer from 4096 to 262144, got 100" in error["message"]
        names = [item["name"] for page in pages for item in page["attr_keys"]]
        assert names == sorted(set(names)) and {"text", "title"} <= set(names)

    _drive(clean_runtime, deployment, body)


def test_expand_path_continues_by_next_as_continuation_token_does(
    clean_runtime: str, deployment: str,
) -> None:
    async def body(client: Any) -> None:
        start = {"input": {"node_id": "doc:big"}, "path": ["contains:out"], "limit": 1}
        pages = await _follow(client, "expand", start, bound="page")
        by_next = [row["node_id"] for page in pages for row in page["results"]]
        assert by_next == [f"doc:big:chunk:{i}" for i in range(3)]
        # The old spelling still works and reads the same second page.
        old = json_payload(await client.call_tool("expand", {
            **start, "continuation_token": pages[0]["continuation_token"],
        }, meta=JSON_META))
        assert old["results"] == pages[1]["results"]

    _drive(clean_runtime, deployment, body)


async def _refusal(client: Any, tool: str, arguments: dict) -> tuple[str, dict]:
    text = result_text(await client.call_tool(tool, arguments))
    data = json_payload(await client.call_tool(tool, arguments, meta=JSON_META))
    error = data.get("error") if isinstance(data.get("error"), dict) else {}
    return text, error


def test_next_refusals_say_why_and_how_to_retry(
    clean_runtime: str, deployment: str, monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("OKG_MCP_CONTINUATION_SIGNING_KEY", raising=False)

    async def body(client: Any) -> None:
        start = {"input": {"node_id": "doc:big"}, "path": ["contains:out"], "limit": 1}
        first = json_payload(await client.call_tool("expand", start, meta=JSON_META))
        token = first["continuation_token"]

        text, error = await _refusal(client, "inspect", {"next": token})
        assert error["code"] == "continuation_token_invalid"
        assert error["data"]["reason"] == "wrong_tool"
        assert " · retry: call expand(next=<this token>)" in text.split("\n", 1)[0]

        # Lane ri, tokens: the read's own arguments beside its token are
        # read as `next` alone, with a note; another value is still refused.
        repeated = result_text(await client.call_tool("expand", {"next": token, **start}))
        assert repeated.startswith("expand ok"), repeated.split("\n", 1)[0]
        assert "beside `next` read as `next` alone" in repeated
        text, error = await _refusal(client, "expand", {"next": token, **start, "limit": 2})
        assert error["code"] == "next_with_read_arguments"
        assert text.startswith("expand refused next_with_read_arguments")
        assert " · retry: call expand(next=<token>) alone" in text.split("\n", 1)[0]

        body_, signature = continuation.full_token(token).split(".")
        forged = body_ + "." + ("A" if signature[0] != "A" else "B") + signature[1:]
        _, error = await _refusal(client, "expand", {"next": forged})
        assert error["data"]["reason"] == "signature_invalid"
        # A miscopied handle reads the same way.
        if token.startswith(continuation.HANDLE_PREFIX):
            miscopied = token[:-1] + ("A" if token[-1] != "A" else "B")
            _, error = await _refusal(client, "expand", {"next": miscopied})
            assert error["data"]["reason"] == "signature_invalid"

        text, error = await _refusal(client, "inspect", {})
        # Neither `target` nor `next`: refused as validation refused it before.
        assert error["code"] == "invalid_operator_arguments"
        assert "next=<token>" in text.split("\n", 1)[0]

        # A restart regenerates the per-process signing key.
        monkeypatch.setattr(continuation, "_PROCESS_SIGNING_KEY", b"\x07" * 32)
        text, error = await _refusal(client, "expand", {"next": token})
        assert error["data"]["reason"] == "signing_key_changed"
        assert "restarted" in text

    _drive(clean_runtime, deployment, body)


# ── `next` alone rebuilds the original path call exactly ─────────────────

#: Two levels below doc:root: depth 2 reaches the three chunks.
TWO_LEVEL_CHUNKS = ["doc:a:chunk:0", "doc:a:chunk:1", "doc:b:chunk:0"]


@pytest.fixture
def two_level(
    clean_runtime: str, okg_mcp_can_log_in: str, tmp_path: pathlib.Path,
    request: pytest.FixtureRequest,
) -> str:
    """doc:root contains doc:a and doc:b, which contain three chunks.

    ``request.param`` says whether the catalog declares narrowings. With
    narrowings, a literal `*` edge type is refused as absent from the
    catalog; without, it would match no edge, so a later page reads empty.
    """
    deployment = make_search_deployment(tmp_path, dsn=clean_runtime)
    nodes = [node(doc, "document", doc, title=doc)
             for doc in ("doc:root", "doc:a", "doc:b")]
    pairs = [("doc:root", "doc:a"), ("doc:root", "doc:b")]
    for chunk_id in TWO_LEVEL_CHUNKS:
        nodes.append(node(chunk_id, "document_chunk", f"text of {chunk_id}"))
        pairs.append((chunk_id.rsplit(":chunk:", 1)[0], chunk_id))
    edges = [
        EdgeFact(src=src, dst=dst, edge_type="contains",
                 source_record_id={"id": f"{src}>{dst}"}, source_revision={"v": 1})
        for src, dst in pairs
    ]
    narrowed = getattr(request, "param", True)
    publish_nodes(
        clean_runtime, deployment, subtypes=[DOC, CHUNK],
        narrowings=(
            [("document", "contains", "document"),
             ("document", "contains", "document_chunk")]
            if narrowed else None
        ),
        nodes=nodes, edges=edges,
    )
    return deployment


async def _legacy_pages(client: Any, start: dict) -> list[dict]:
    """Every page by the old spelling: the first call again, plus its token."""
    pages: list[dict] = []
    data = json_payload(await client.call_tool("expand", start, meta=JSON_META))
    for _ in range(20):
        assert data.get("status") == "ok", data
        pages.append(data)
        token = data.get("continuation_token")
        if not token:
            return pages
        data = json_payload(await client.call_tool(
            "expand", {**start, "continuation_token": token}, meta=JSON_META,
        ))
    raise AssertionError("the legacy read did not end within 20 pages")


@pytest.mark.parametrize("two_level", [True, False], indirect=True,
                         ids=["narrowings", "no-narrowings"])
def test_any_edge_depth_two_continues_by_next_as_continuation_token_does(
    clean_runtime: str, two_level: str,
) -> None:
    """Any-edge hops are echoed `*:out`; `next` must not replay that as a path."""
    async def body(client: Any) -> None:
        start = {"input": {"node_id": "doc:root"}, "depth": 2, "limit": 1}
        legacy = await _legacy_pages(client, start)
        by_legacy = [row["node_id"] for page in legacy for row in page["results"]]
        assert by_legacy == TWO_LEVEL_CHUNKS
        pages = await _follow(client, "expand", start, bound="page")
        for page in pages:
            assert page.get("status") == "ok", page
        assert [page["results"] for page in pages] == [page["results"] for page in legacy]

    _drive(clean_runtime, two_level, body)


@pytest.mark.parametrize("deployment", [30], indirect=True)
def test_large_path_budget_survives_next(clean_runtime: str, deployment: str) -> None:
    """A path page served under 262144 bytes resumes under 262144, not 65536."""
    async def body(client: Any) -> None:
        start = {"input": {"node_id": "doc:big"}, "path": ["contains:out"],
                 "limit": 50, "max_bytes": 262144}
        first = await client.call_tool("expand", start, meta=JSON_META)
        assert len(first.model_dump_json(by_alias=True, exclude_none=True).encode()) > 65536, (
            "the fixture must need more than the default budget"
        )
        pages = await _follow(client, "expand", start, bound="page")
        ids = [row["node_id"] for page in pages for row in page["results"]]
        assert ids == sorted(f"doc:big:chunk:{i}" for i in range(90))
        # The old spelling with max_bytes omitted resumes under the signed
        # budget too; with the same explicit budget it reads the same page.
        token = pages[0]["continuation_token"]
        for extra in ({}, {"max_bytes": 262144}):
            legacy = json_payload(await client.call_tool("expand", {
                **{k: v for k, v in start.items() if k != "max_bytes"},
                **extra, "continuation_token": token,
            }, meta=JSON_META))
            assert legacy["results"] == pages[1]["results"], legacy.get("error")
        # An explicit budget beside `next` is refused as an operator refusal.
        _, error = await _refusal(client, "expand", {"next": token, "max_bytes": 262144})
        assert error["code"] == "next_with_read_arguments"
        # #3605: expand keeps the general message; only inspect accepts the
        # signed size, so expand is never told to send it.
        assert error["message"] == (
            "expand: `next` carries the whole read and cannot be combined with max_bytes")
        assert "signed_max_bytes" not in error
        refused = json_payload(await client.call_tool(
            "expand", {"next": token, "max_bytes": 262144}, meta=JSON_META,
        ))
        assert refused["operator_name"] == "expand"
        assert refused["pinned_generation_id"] == pages[0]["pinned_generation_id"]

    _drive(clean_runtime, deployment, body)


@pytest.mark.parametrize("select", [["node_id", "source_lineage"], ["source_lineage"]])
def test_lineage_select_keeps_its_row_shape_across_next_pages(
    clean_runtime: str, deployment: str, select: list[str],
) -> None:
    async def body(client: Any) -> None:
        start = {"input": {"node_id": "doc:big"}, "path": ["contains:out"],
                 "limit": 1, "select": select}
        pages = await _follow(client, "expand", start, bound="page")
        assert len(pages) == 3
        shapes = [sorted(row) for page in pages for row in page["results"]]
        assert all("source_lineage" in shape for shape in shapes), shapes
        assert all(shape == shapes[0] for shape in shapes), shapes
        legacy = await _legacy_pages(client, start)
        assert [page["results"] for page in pages] == [page["results"] for page in legacy]

    _drive(clean_runtime, deployment, body)


def test_a_path_token_without_its_hops_is_refused_by_next(
    clean_runtime: str, deployment: str,
) -> None:
    """A token signed before bindings recorded hops cannot rebuild its read."""
    async def body(client: Any) -> None:
        start = {"input": {"node_id": "doc:big"}, "path": ["contains:out"], "limit": 1}
        first = json_payload(await client.call_tool("expand", start, meta=JSON_META))
        opened = continuation.open_continuation(first["continuation_token"])
        projection = opened["binding"]["projection"]
        assert projection["hops"] == [["contains", "out"]]
        projection.pop("hops")
        projection.pop("max_bytes")
        old = continuation.issue_continuation(opened["binding"], cursor=opened["cursor"])
        text, error = await _refusal(client, "expand", {"next": old})
        assert error["code"] == "continuation_token_invalid"
        assert error["data"]["reason"] == "binding_mismatch"
        assert error["data"]["mismatched"] == ["target"]
        assert text.startswith("expand refused continuation_token_invalid")
        # The legacy spelling refuses it by the binding comparison as well.
        _, error = await _refusal(client, "expand", {**start, "continuation_token": old})
        assert error["data"]["reason"] == "binding_mismatch"

    _drive(clean_runtime, deployment, body)


# ── a page that needs another budget is read by the call its refusal names ──

def _follow_retry(error: dict, token: str, max_bytes: int) -> tuple[str, dict]:
    """The tool and arguments a refusal's `retry_call` names, made concrete."""
    call = error["retry_call"]
    low, high = call["max_bytes"]["min"], call["max_bytes"]["max"]
    assert low <= max_bytes <= high, call
    arguments = json.loads(json.dumps(call["arguments"]).replace(
        json.dumps(continuation.TOKEN_PLACEHOLDER), json.dumps(token),
    ))
    return call["tool"], {**arguments, "max_bytes": max_bytes}


@pytest.fixture
def uneven(
    clean_runtime: str, okg_mcp_can_log_in: str, tmp_path: pathlib.Path,
) -> str:
    """doc:u contains three chunks; the middle one needs about 8 KB."""
    deployment = make_search_deployment(tmp_path, dsn=clean_runtime)
    texts = ["short zero", "Part 1: " + ("bigword " * 1000), "short two"]
    nodes = [node("doc:u", "document", "an uneven document", title="Uneven")]
    edges = []
    for i, text in enumerate(texts):
        chunk_id = f"doc:u:chunk:{i}"
        nodes.append(node(chunk_id, "document_chunk", text, chunk_index=i))
        edges.append(EdgeFact(
            src="doc:u", dst=chunk_id, edge_type="contains",
            source_record_id={"id": f"u{i}"}, source_revision={"v": 1},
        ))
    publish_nodes(
        clean_runtime, deployment, subtypes=[DOC, CHUNK],
        narrowings=[("document", "contains", "document_chunk")],
        nodes=nodes, edges=edges,
    )
    return deployment


def test_oversize_path_page_is_read_by_the_legacy_call_its_refusal_names(
    clean_runtime: str, uneven: str,
) -> None:
    """Page 2 does not fit 4096 bytes; a larger legacy max_bytes reads it.

    `next` refuses `max_bytes`, and narrowing `select` would change the read
    the token is bound to, so the refusal must name the legacy spelling with
    a larger budget, and that call must be accepted (it was refused
    `binding_mismatch` while the path binding compared `max_bytes`).
    """
    async def body(client: Any) -> None:
        start = {"input": {"node_id": "doc:u"}, "path": ["contains:out"],
                 "limit": 1, "max_bytes": 4096}
        first = json_payload(await client.call_tool("expand", start, meta=JSON_META))
        assert [row["node_id"] for row in first["results"]] == ["doc:u:chunk:0"]
        token = first["continuation_token"]

        text, error = await _refusal(client, "expand", {"next": token})
        assert error["code"] == "response_byte_budget_exceeded", error
        head = text.split("\n", 1)[0]
        assert "retry: continue with expand(" in head, head
        assert "continuation_token=<this token>" in head, head
        assert error["retry_call"]["max_bytes"]["min"] == 4097

        tool, arguments = _follow_retry(error, token, 16384)
        assert arguments == {
            **{k: v for k, v in start.items() if k != "max_bytes"},
            "generation_id": continuation.open_continuation(token)["binding"]["generation"],
            "continuation_token": token, "max_bytes": 16384,
        }
        assert error["retry_call"]["repeat"] == ["authority"]
        second = json_payload(await client.call_tool(tool, arguments, meta=JSON_META))
        assert second.get("status") == "ok", second.get("error")
        assert [row["node_id"] for row in second["results"]] == ["doc:u:chunk:1"]
        # The new budget is signed into the next token: `next` resumes under it.
        opened = continuation.open_continuation(second["continuation_token"])
        assert opened["binding"]["projection"]["max_bytes"] == 16384
        third = await client.call_tool("expand", {"next": second["continuation_token"]})
        assert _ending(result_text(third)) == ["complete"]
        third_json = json_payload(await client.call_tool(
            "expand", {"next": second["continuation_token"]}, meta=JSON_META,
        ))
        assert [row["node_id"] for row in third_json["results"]] == ["doc:u:chunk:2"]

        # The legacy spelling with any valid budget reads the same page.
        for budget in (32768, 65536):
            legacy = json_payload(await client.call_tool("expand", {
                **start, "max_bytes": budget, "continuation_token": token,
            }, meta=JSON_META))
            assert legacy["results"] == second["results"], legacy.get("error")

    _drive(clean_runtime, uneven, body)


def _lower_ceiling(deployment: str, ceiling: int) -> None:
    manifest_path = pathlib.Path(deployment) / "deployment.yaml"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    manifest["agent_surface"] = {
        "served": ["search", "inspect", "expand"],
        "response_bytes": {"ceiling": ceiling},
    }
    manifest_path.write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")


@pytest.mark.parametrize("deployment", [5], indirect=True)
def test_next_refused_by_a_lowered_ceiling_names_a_retry_that_works(
    clean_runtime: str, deployment: str,
) -> None:
    """Tokens signed at 16384 bytes; the ceiling then drops to 8192.

    `next` cannot resume under 16384. Each refusal names a call that IS
    accepted: the legacy continuation with a valid budget for a path token,
    `next` with a max_bytes under the ceiling for a text or subtype token
    (#3634, `nextresize A`), from the same place.
    """
    starts = {
        "path": ("expand", {"input": {"node_id": "doc:big"}, "path": ["contains:out"],
                            "limit": 1, "max_bytes": 16384}),
        "text": ("inspect", {"target": {"kind": "text", "node_id": "doc:big"},
                             "max_bytes": 16384}),
        "subtype": ("inspect", {"target": {"kind": "subtype", "subtype": "document",
                                           "page_size": 1}, "max_bytes": 16384}),
    }
    first: dict[str, dict] = {}
    second: dict[str, dict] = {}

    async def issue(client: Any) -> None:
        for name, (tool, start) in starts.items():
            page = json_payload(await client.call_tool(tool, start, meta=JSON_META))
            assert page.get("continuation_token"), (name, page)
            first[name] = page
            second[name] = json_payload(await client.call_tool(
                tool, {"next": page["continuation_token"]}, meta=JSON_META,
            ))

    _drive(clean_runtime, deployment, issue)
    _lower_ceiling(deployment, 8192)

    async def resume(client: Any) -> None:
        for name, (tool, _start) in starts.items():
            token = first[name]["continuation_token"]
            text, error = await _refusal(client, tool, {"next": token})
            assert error["code"] == "invalid_response_byte_budget", (name, error)
            head = text.split("\n", 1)[0]
            assert "16384" in error["message"] and "8192" in error["message"], error
            call = error["retry_call"]
            assert call["max_bytes"]["max"] == 8192
            assert call["mode"] == "continue"
            assert "retry: continue with " in head, head
            if tool == "inspect":
                # #3634 (`nextresize A`): an inspect token goes on by `next`
                # at a size under the new ceiling, from the same place.
                assert call["arguments"] == {"next": continuation.TOKEN_PLACEHOLDER}
            else:
                assert "continuation_token" in json.dumps(call["arguments"])
            retry_tool, arguments = _follow_retry(error, token, 8192)
            assert retry_tool == tool
            result = await client.call_tool(retry_tool, arguments, meta=JSON_META)
            assert len(result.model_dump_json(by_alias=True, exclude_none=True).encode()) <= 8192
            page = json_payload(result)
            assert page.get("status", "ok") == "ok", (name, page.get("error"))
            if name == "path":
                assert page["results"] == second["path"]["results"]
            elif name == "text":
                # It continues where page 1 stopped, under the smaller budget.
                def where(item: dict) -> tuple:
                    return item["chunk_id"], item.get("text_range", [0])[0]

                assert where(page["items"][0]) == where(second["text"]["items"][0])
            else:
                assert page["attr_keys"] == second["subtype"]["attr_keys"]

    _drive(clean_runtime, deployment, resume)


@pytest.mark.parametrize("two_level", [True], indirect=True, ids=["narrowings"])
def test_named_edge_depth_two_continues_by_next_as_continuation_token_does(
    clean_runtime: str, two_level: str,
) -> None:
    """A named edge at depth 2 rebuilds as a typed path with the same pages."""
    async def body(client: Any) -> None:
        start = {"input": {"node_id": "doc:root"}, "edge_type": "contains",
                 "depth": 2, "limit": 1}
        legacy = await _legacy_pages(client, start)
        assert ([row["node_id"] for page in legacy for row in page["results"]]
                == TWO_LEVEL_CHUNKS)
        pages = await _follow(client, "expand", start, bound="page")
        for page in pages:
            assert page.get("status") == "ok", page
        assert [page["results"] for page in pages] == [page["results"] for page in legacy]

    _drive(clean_runtime, two_level, body)


def _publish_another_generation(dsn: str, deployment: str) -> str:
    """Publish one more document, so the session default moves past a token."""
    with psycopg.connect(dsn, autocommit=True) as conn:
        ChronosGraphStore.from_connection(conn).apply_facts(
            [node("doc:later", "document", "published after page one", title="Later")],
            source="system:analyst_search_fixture",
            strict_endpoints=True,
        )
    result = Publisher(
        dsn, deployment, catalog_identity=bind_test_catalog_writer(dsn, deployment),
    ).cycle()
    assert result["status"] == "published", result
    return str(result["generation_id"])


@pytest.mark.parametrize("deployment", [5], indirect=True)
def test_a_retry_after_a_publish_reads_the_tokens_pinned_generation(
    clean_runtime: str, deployment: str,
) -> None:
    """Page 1 is read; a publish moves the head; the ceiling drops below the
    signed budget. The retry a refusal names must read the token's pinned
    generation: without `generation_id` the legacy call opens the new head
    and is refused `binding_mismatch` (generation).
    """
    starts = {
        "path": ("expand", {"input": {"node_id": "doc:big"}, "path": ["contains:out"],
                            "limit": 1, "max_bytes": 16384}),
        "text": ("inspect", {"target": {"kind": "text", "node_id": "doc:big"},
                             "max_bytes": 16384}),
    }
    first: dict[str, dict] = {}
    second: dict[str, dict] = {}

    async def issue(client: Any) -> None:
        for name, (tool, start) in starts.items():
            page = json_payload(await client.call_tool(tool, start, meta=JSON_META))
            assert page.get("continuation_token"), (name, page)
            first[name] = page
            second[name] = json_payload(await client.call_tool(
                tool, {"next": page["continuation_token"]}, meta=JSON_META,
            ))

    _drive(clean_runtime, deployment, issue)
    pinned = {
        name: continuation.open_continuation(page["continuation_token"])["binding"]["generation"]
        for name, page in first.items()
    }
    newer = _publish_another_generation(clean_runtime, deployment)
    assert all(str(generation) != newer for generation in pinned.values()), (pinned, newer)
    _lower_ceiling(deployment, 8192)

    async def resume(client: Any) -> None:
        for name, (tool, _start) in starts.items():
            token = first[name]["continuation_token"]
            text, error = await _refusal(client, tool, {"next": token})
            assert error["code"] == "invalid_response_byte_budget", (name, error)
            head = text.split("\n", 1)[0]
            retry_tool, arguments = _follow_retry(error, token, 8192)
            if tool == "inspect":
                # #3634 (`nextresize A`): `next` carries the token's own pin,
                # so no generation_id is named and none is needed.
                assert arguments == {"next": token, "max_bytes": 8192}
            else:
                assert "generation_id=" in head and "authority" in head, head
                assert f"generation_id={json.dumps(pinned[name])}" in error["remedy"], error
                assert error["retry_call"]["repeat"] == ["authority"]
                assert arguments["generation_id"] == pinned[name]
                # Failure case: the same call without the pin opens the new head.
                unpinned = {k: v for k, v in arguments.items() if k != "generation_id"}
                refused = json_payload(await client.call_tool(retry_tool, unpinned, meta=JSON_META))
                assert refused["error"]["data"]["reason"] == "binding_mismatch", refused
                assert "generation" in refused["error"]["data"]["mismatched"], refused

            page = json_payload(await client.call_tool(retry_tool, arguments, meta=JSON_META))
            assert page.get("status", "ok") == "ok", (name, page.get("error"))
            assert str(page["pinned_generation_id"]) == str(pinned[name]), page
            if name == "path":
                assert page["results"] == second["path"]["results"]
            else:
                def where(item: dict) -> tuple:
                    return item["chunk_id"], item.get("text_range", [0])[0]

                assert where(page["items"][0]) == where(second["text"]["items"][0])

    _drive(clean_runtime, deployment, resume)


def test_any_edge_depth_retry_rebuilds_the_depth_form(
    clean_runtime: str, two_level: str,
) -> None:
    """An any-edge depth-2 token retries as `direction, depth`, never `*:out`."""
    start = {"input": {"node_id": "doc:root"}, "depth": 2, "limit": 1,
             "max_bytes": 16384}
    first: dict[str, Any] = {}
    legacy: list[dict] = []

    async def issue(client: Any) -> None:
        first.update(json_payload(await client.call_tool("expand", start, meta=JSON_META)))
        legacy.extend(await _legacy_pages(client, start))

    _drive(clean_runtime, two_level, issue)
    token = first["continuation_token"]
    _lower_ceiling(two_level, 8192)

    async def resume(client: Any) -> None:
        _, error = await _refusal(client, "expand", {"next": token})
        assert error["code"] == "invalid_response_byte_budget", error
        call = error["retry_call"]
        assert call["mode"] == "continue"
        assert "path" not in call["arguments"], call
        assert call["arguments"]["direction"] == "out"
        assert call["arguments"]["depth"] == 2
        tool, arguments = _follow_retry(error, token, 8192)
        page = json_payload(await client.call_tool(tool, arguments, meta=JSON_META))
        assert page.get("status") == "ok", page.get("error")
        assert page["results"] == legacy[1]["results"]

    _drive(clean_runtime, two_level, resume)


def test_an_oversize_path_page_at_the_ceiling_names_a_restart(
    clean_runtime: str, uneven: str,
) -> None:
    """At the ceiling no larger budget exists: the refusal names a restart
    with a smaller limit or a narrower select, and no `retry_call`."""
    _lower_ceiling(uneven, 4096)

    async def body(client: Any) -> None:
        start = {"input": {"node_id": "doc:u"}, "path": ["contains:out"], "limit": 1}
        first = json_payload(await client.call_tool("expand", start, meta=JSON_META))
        assert [row["node_id"] for row in first["results"]] == ["doc:u:chunk:0"]
        text, error = await _refusal(client, "expand", {"next": first["continuation_token"]})
        assert error["code"] == "response_byte_budget_exceeded", error
        head = text.split("\n", 1)[0]
        assert "retry: no larger max_bytes is allowed here (ceiling 4096)" in head, head
        assert "smaller limit or a narrower select" in error["fix_hint"]
        assert "retry_call" not in error, error

        # The restart it names works: a narrower select reads the big chunk.
        narrow = {**start, "select": ["node_id"]}
        page = json_payload(await client.call_tool("expand", narrow, meta=JSON_META))
        page = json_payload(await client.call_tool(
            "expand", {"next": page["continuation_token"]}, meta=JSON_META,
        ))
        assert page.get("status") == "ok", page.get("error")
        assert [row["node_id"] for row in page["results"]] == ["doc:u:chunk:1"]

    _drive(clean_runtime, uneven, body)


#: Twelve notes, more than the lowered offset cap below reaches.
NOTE_IDS = [f"note:{index:02d}" for index in range(12)]


@pytest.fixture
def notes(
    clean_runtime: str, okg_mcp_can_log_in: str, tmp_path: pathlib.Path,
    monkeypatch: pytest.MonkeyPatch,
) -> str:
    """A small set past a lowered offset cap: `filter` and `map` page it by
    offset up to the cap and by a keyset token after it (#3666 step 3b)."""
    import okg.substrate.mcp.server as server_module

    monkeypatch.setattr(server_module, "MAX_OFFSET", 5)
    deployment = make_search_deployment(tmp_path, dsn=clean_runtime)
    publish_nodes(
        clean_runtime, deployment, subtypes=["note"],
        nodes=[node(node_id, "note", f"note {node_id}", group="a") for node_id in NOTE_IDS],
    )
    return deployment


async def _read_all(client: Any, tool: str, first: dict) -> tuple[list[str], list[dict]]:
    """Every row id of a read, page by page: `offset` while the page names
    one, then the token alone."""
    pages = [json_payload(await client.call_tool(tool, first, meta=JSON_META))]
    while True:
        page = pages[-1]
        assert page.get("status") != "refused", page.get("error")
        if page.get("next_offset") is not None:
            assert page["next_offset"] <= 5, page["next_offset"]
            nxt = {**first, "offset": page["next_offset"]}
        elif page.get("continuation_token"):
            nxt = {"next": page["continuation_token"], "limit": first["limit"]}
        else:
            break
        pages.append(json_payload(await client.call_tool(tool, nxt, meta=JSON_META)))
        assert len(pages) < 20
    ids = [row["node_id"] for page in pages for row in page["items"]]
    return ids, pages


def test_filter_and_map_page_past_the_offset_cap_by_a_keyset_token(
    clean_runtime: str, notes: str,
) -> None:
    """Issue #3683 item 3: no tool passed offset 10,000. Past the cap (5
    here), the page ends with a token instead of an offset no call may
    send, and `next` alone reads on in id order: every row once, none
    twice, the last page complete, its count exact."""
    async def body(client: Any) -> None:
        scope = {"kind": "nodes", "subtype": "note"}
        reads = [
            ("filter", {"input": scope,
                        "predicate": {"op": "eq", "field": "attrs.group", "value": "a"},
                        "limit": 4}),
            # A condition on a column only: the cap moves it to the same read.
            ("filter", {"input": scope,
                        "predicate": {"op": "eq", "field": "subtype", "value": "note"},
                        "limit": 4}),
            ("map", {"input": scope, "projection": ["node_id"], "limit": 4}),
        ]
        for tool, first in reads:
            ids, pages = await _read_all(client, tool, first)
            assert ids == NOTE_IDS, (tool, ids)
            assert [page["offset"] for page in pages] == [0, 4, 8], tool
            assert pages[1]["next_offset"] is None and pages[1]["continuation_token"]
            assert pages[-1]["total_count"] == 12, pages[-1]
            assert pages[-1]["coverage"]["delivery"]["complete"] is True
            # The text form says the token as the page's `next`.
            text = result_text(await client.call_tool(tool, {**first, "offset": 4}))
            assert fact_value(status_facts(text), "next:", last=True) == pages[1]["continuation_token"]

    _drive(clean_runtime, notes, body)


def test_a_listing_ordered_by_another_column_never_changes_order_at_the_cap(
    clean_runtime: str, notes: str,
) -> None:
    """Review of 3b: a `select` that starts with another column is read in
    id order from its first page, so crossing the cap into token pages
    repeats and skips nothing. The leading column is `text_hash`, whose
    order is not the id order (the old scan program sorted by it below the
    cap; this test failed on that code, see the PACT evidence)."""
    async def body(client: Any) -> None:
        hashes = json_payload(await client.call_tool("map", {
            "input": {"kind": "nodes", "subtype": "note"},
            "projection": ["node_id", "text_hash"], "limit": 50,
        }, meta=JSON_META))["items"]
        by_hash = [row["node_id"] for row in sorted(hashes, key=lambda r: str(r["text_hash"]))]
        assert by_hash != NOTE_IDS, "the fixture must order differently by text_hash"
        for tool, first in [
            ("filter", {"input": {"kind": "nodes", "subtype": "note"},
                        "predicate": {"op": "eq", "field": "subtype", "value": "note"},
                        "select": ["text_hash", "node_id"], "limit": 4}),
            ("map", {"input": {"kind": "nodes"}, "projection": ["text_hash", "node_id"],
                     "limit": 4}),
        ]:
            ids, pages = await _read_all(client, tool, first)
            assert len(ids) == len(set(ids)), (tool, ids)
            assert [i for i in ids if i.startswith("note:")] == NOTE_IDS, (tool, ids)
            assert any(page.get("continuation_token") for page in pages), tool

    _drive(clean_runtime, notes, body)


@pytest.fixture
def wide(
    clean_runtime: str, okg_mcp_can_log_in: str, tmp_path: pathlib.Path,
) -> str:
    """One record too large for a 4,096-byte page, then two small ones."""
    deployment = make_search_deployment(tmp_path, dsn=clean_runtime)
    publish_nodes(
        clean_runtime, deployment, subtypes=["note"],
        nodes=[
            node("wide:0", "note", "the wide one", body="b" * 20_000),
            node("wide:1", "note", "small one"),
            node("wide:2", "note", "small two"),
        ],
    )
    return deployment


def test_a_cut_row_says_so_in_text_and_a_size_refusal_retry_is_served(
    clean_runtime: str, wide: str,
) -> None:
    """#3666 step 3b through a real client: the text reply names the cut
    value and its size, the next rows stay one offset away, and a size
    refusal that stands carries a call that is served as written."""
    async def body(client: Any) -> None:
        call = {"input": {"kind": "nodes", "subtype": "note"},
                "predicate": {"op": "eq", "field": "subtype", "value": "note"},
                "select": ["node_id", "attrs"], "limit": 3, "max_bytes": 4096}
        text = result_text(await client.call_tool("filter", call))
        assert text.startswith("filter ok"), text.split("\n", 1)[0]
        assert re.search(r'^! \[1\] "attrs\.body" cut: \d+ of 20000 chars$', text, re.M), text
        page = json_payload(await client.call_tool("filter", call, meta=JSON_META))
        assert [row["node_id"] for row in page["items"]] == ["wide:0"]
        assert page["items"][0]["values_cut"]["attrs.body"]["chars"] == 20_000
        assert page["next_offset"] == 1

        refused = json_payload(await client.call_tool(
            "query", {"sql": "SELECT repeat('x', 9000) AS big", "max_bytes": 4096},
            meta=JSON_META,
        ))
        assert refused["error"]["code"] == "response_byte_budget_exceeded", refused
        retry = refused["error"]["retry_call"]
        assert retry["tool"] == "query" and retry["mode"] == "corrected"
        served = json_payload(await client.call_tool(
            retry["tool"], retry["arguments"], meta=JSON_META,
        ))
        assert served.get("status") != "refused", served
        # Served as written (query's own cell bound may shorten the value).
        assert served["rows"] and str(served["rows"][0][0]).startswith("x" * 100)

    _drive(clean_runtime, wide, body)


def test_a_keyset_token_stands_alone_and_ordered_reads_say_where_they_stop(
    clean_runtime: str, notes: str,
) -> None:
    """Failure cases: a read argument beside `next` is refused, not ignored;
    a filter token is refused by map; with order_by no token is offered past
    the cap, and the page says to narrow or drop order_by."""
    async def body(client: Any) -> None:
        first = {"input": {"kind": "nodes", "subtype": "note"},
                 "predicate": {"op": "eq", "field": "attrs.group", "value": "a"},
                 "limit": 4, "offset": 4}
        page = json_payload(await client.call_tool("filter", first, meta=JSON_META))
        token = page["continuation_token"]

        other = {"op": "eq", "field": "attrs.group", "value": "b"}
        _, error = await _refusal(client, "filter", {"next": token, "predicate": other})
        assert error["code"] == "next_with_read_arguments", error
        # Lane ri, tokens: the read's own predicate beside its token is read
        # as `next` alone, with a note.
        repeated = json_payload(await client.call_tool(
            "filter", {"next": token, "predicate": first["predicate"]}, meta=JSON_META,
        ))
        assert repeated.get("status") != "refused", repeated.get("error")
        assert any(
            record.get("rule") == "next_repeats_read" for record in repeated.get("adjusted") or []
        ), repeated.get("adjusted")
        _, error = await _refusal(client, "map", {"next": token})
        assert error["data"]["reason"] == "wrong_tool", error
        # M10 (lead's ruling for experimental6, applied on read-v2):
        # `filter({})` has one meaning, every record, so it is served with a
        # note ("lenient yes"), not refused as on dev's 3b.
        served = json_payload(await client.call_tool("filter", {}, meta=JSON_META))
        assert served.get("status") != "refused", served.get("error")
        assert any(
            record.get("rule") == "scope_defaulted" for record in served.get("adjusted") or []
        ), served.get("adjusted")
        assert served["items"], "every record: the fixture's notes are listed"

        ordered = json_payload(await client.call_tool(
            "filter", {**first, "order_by": "node_id desc"}, meta=JSON_META,
        ))
        assert [row["node_id"] for row in ordered["items"]] == NOTE_IDS[::-1][4:8]
        assert ordered["next_offset"] is None
        assert "continuation_token" not in ordered
        assert ordered["pagination_capped"] is True
        assert "drop order_by" in ordered["pagination_notice"]

    _drive(clean_runtime, notes, body)
