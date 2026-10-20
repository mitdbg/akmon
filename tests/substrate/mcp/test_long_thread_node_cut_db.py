"""inspect(node) of a real long-thread ticket under a small max_bytes
(#3666, review of read stack PR 2, finding 1).

The node read fits a ticket's thread to 24 KB by keeping its NEWEST
comments; a max_bytes cut then keeps the description and the oldest of
those. The reply used to say "its first N of M chars are shown", with M the
fitted block's characters. These tests read a published 60-comment ticket
through the MCP client at max_bytes=8500 (8000 before PACT read-reply-size-3666, whose
reply_size field took the room of the one comment the JSON page held) and check that the cut line counts
the whole thread and names the comments shown by position, and that the
text form does not cut (or report cutting) attributes it never prints.
"""
from __future__ import annotations

import json
import re
from typing import Any

import anyio
import psycopg
import pytest
from mcp.shared.memory import create_connected_server_and_client_session

from okg.substrate.library.sources.base import EdgeFact, NodeFact
from okg.substrate.mcp import open_session
from okg.substrate.mcp.server import build_server
from okg.substrate.mcp.tools import inspect_node
from tests.pytest.catalog_fixture_builders import SubtypeSpec
from tests.substrate.mcp._result_payload import payload as _result_payload
from tests.substrate.mcp._result_payload import result_text as _result_text
from tests.substrate.mcp.search_fixture import make_search_deployment, publish_nodes

_COMMENTS = 60
_PER_CHUNK = 10
_DESCRIPTION = "The description of a long thread. " + "d" * 1_500
_COMMENT_TEXTS = [f"comment {n} " + "c" * 650 for n in range(1, _COMMENTS + 1)]
_WHOLE_CHARS = len(_DESCRIPTION) + sum(len(text) for text in _COMMENT_TEXTS)
_SUBTYPES = [
    SubtypeSpec("issue", metadata={
        "searchable_text_fields": ["text"], "title_from": "summary",
        "passage_rollup": {"edge_type": "contains", "child_subtype": "document_chunk"},
    }),
    SubtypeSpec("document_chunk", metadata={"searchable_text_fields": ["text"]}),
]
_NODE_ID = "issue:LONG-1"


def _comment_line(index: int) -> str:
    month, day = 1 + index // 28, 1 + index % 28
    return (
        f"User {index + 1:02d} 2026-{month:02d}-{day:02d}T09:00:00.000+0200: "
        f"{_COMMENT_TEXTS[index]}\n"
    )


def _chunks() -> list[str]:
    head = (
        f"summary: Long thread\n\ndescription: {_DESCRIPTION}\n\n"
        "status: Open\n\nrecent_comments: "
    )
    lines = [_comment_line(i) for i in range(_COMMENTS)]
    out = [head + "".join(lines[:_PER_CHUNK])]
    for start in range(_PER_CHUNK, _COMMENTS, _PER_CHUNK):
        out.append("".join(lines[start:start + _PER_CHUNK]))
    return out


def _publish(tmp_path: Any, dsn: str) -> str:
    deployment = make_search_deployment(tmp_path, dsn=dsn)
    chunks = _chunks()
    nodes = [NodeFact(
        node_id=_NODE_ID, subtype="issue",
        # The attributes repeat the thread, as a tracker's fields do: the
        # description and the recent comments, larger than the fitted thread.
        attrs={
            "text": "summary: Long thread", "summary": "Long thread", "status": "Open",
            "description": _DESCRIPTION,
            "recent_comments": "".join(_comment_line(i) for i in range(_COMMENTS)),
        },
        source_record_id={"id": _NODE_ID}, source_revision={"v": 1},
    )]
    edges = []
    offset = 0
    for index, text in enumerate(chunks):
        chunk_id = f"chunk:long1:{index}"
        nodes.append(NodeFact(
            node_id=chunk_id, subtype="document_chunk",
            attrs={"text": text, "char_offset": offset, "char_end": offset + len(text)},
            source_record_id={"id": chunk_id}, source_revision={"v": 1},
        ))
        edges.append(EdgeFact(
            src=_NODE_ID, dst=chunk_id, edge_type="contains",
            source_record_id={"id": f"{_NODE_ID}|{chunk_id}"}, source_revision={"v": 1},
        ))
        offset += len(text)
    publish_nodes(
        dsn, deployment, subtypes=_SUBTYPES,
        narrowings=[("issue", "contains", "document_chunk")],
        nodes=nodes, edges=edges,
    )
    return deployment


def _served(dsn: str, deployment: str, budget: int) -> dict[str, Any]:
    server = build_server(dsn=dsn, deployment_name=deployment)
    results: dict[str, Any] = {}

    async def drive() -> None:
        async with create_connected_server_and_client_session(server) as session:
            for fmt in ("json", "text"):
                result = await session.call_tool(
                    "inspect",
                    {"target": {"kind": "node", "node_id": _NODE_ID}, "max_bytes": budget},
                    meta={"okg/result_format": fmt},
                )
                assert result.isError is not True, _result_text(result)[:600]
                assert len(result.model_dump_json(
                    by_alias=True, exclude_none=True).encode("utf-8")) <= budget, fmt
                results[fmt] = result

    try:
        anyio.run(drive)
    finally:
        server._okg_pool.close()
        if server._okg_write_pool:
            server._okg_write_pool.close()
    return results


@pytest.mark.postgres
def test_a_long_thread_ticket_read_at_8500_bytes_names_what_it_shows(
    clean_runtime: str, okg_mcp_can_log_in: str, tmp_path: Any,
) -> None:
    deployment = _publish(tmp_path, clean_runtime)
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        session = open_session(conn, deployment_name=deployment)
        whole = inspect_node(conn, session, _NODE_ID)
    fitted = whole["record_text"]
    # The precondition: the node read's 24 KB fit already left out the
    # oldest comments of this thread.
    earlier = fitted["comments_left"]
    assert fitted["comments_total"] == _COMMENTS and earlier > 0, json.dumps(fitted)[:400]
    assert fitted["comments_shown"] + earlier == _COMMENTS

    results = _served(clean_runtime, deployment, 8_500)
    text = _result_text(results["text"])
    # Review finding 2 (inferred risk), checked on the real read: the text
    # form prints the thread, not the attributes, so the cut leaves them
    # whole and says nothing about them.
    assert "! attributes cut" not in text, text[-1_500:]
    page = _result_payload(results["json"])
    assert page["truncated"] is True and page.get("complete") is not True
    entry = page["parts_cut"]["record_text"]
    assert entry["chars"] == _WHOLE_CHARS
    comments = entry["comments"]
    shown = comments["shown"]
    assert shown >= 1
    assert comments["total"] == _COMMENTS and comments["first"] == earlier + 1
    assert comments["earlier_left"] == earlier
    assert comments["later_left"] == _COMMENTS - earlier - shown
    first_text = page["record_text"]["comments"][0]["text"]
    assert f"comment {earlier + 1} " in first_text, first_text[:160]

    assert entry["description"]["chars"] == len(_DESCRIPTION)

    # The text form is measured in its own bytes, so it serves its own cut:
    # its line is read back and checked against the thread and the
    # comments the reply prints.
    head = text.split("\n", 1)[0]
    assert "bound reached (max_bytes 8500)" in head and " complete" not in head, head
    assert "its first" not in text
    line = re.search(
        r"^! record text cut to fit max_bytes: ([\d,]+) of the whole thread's "
        r"([\d,]+) chars are shown: (the description|the description's first "
        r"[\d,]+ of [\d,]+ chars) and comments? (\d+)(?:-(\d+))? of (\d+)"
        r"(, the last one cut short)?; (.*?) comments are left out; "
        r"read it page by page with ",
        text, re.M,
    )
    assert line is not None, text[-1_500:]
    assert int(line.group(2).replace(",", "")) == _WHOLE_CHARS
    first = int(line.group(4))
    last = int(line.group(5) or first)
    assert int(line.group(6)) == _COMMENTS and first == earlier + 1
    later = _COMMENTS - last
    assert line.group(8) == (
        f"{earlier} earlier and {later} later" if later else f"{earlier} earlier"
    )
    assert f"! {last - first + 1} of {_COMMENTS} comments shown" in text
    assert f"comment {first} " in text and f"comment {last} " in text
    assert f"comment {earlier} " not in text and f"comment {last + 1} " not in text


@pytest.mark.postgres
def test_at_8000_bytes_a_page_with_no_comment_says_none_of_them(
    clean_runtime: str, okg_mcp_can_log_in: str, tmp_path: Any,
) -> None:
    """Review L5 of PACT read-reply-size-3666: at 8000 bytes the JSON page's
    `reply_size` leaves no room for a comment. Its cut line, as the text form
    prints that page, says none of the comments is shown and claims no
    range; it still counts the whole thread."""
    from okg.substrate.mcp.result_text import render

    deployment = _publish(tmp_path, clean_runtime)
    results = _served(clean_runtime, deployment, 8_000)
    page = _result_payload(results["json"])
    comments = page["parts_cut"]["record_text"]["comments"]
    assert comments["total"] == _COMMENTS
    printed = render(page, tool="inspect", kind="node", size_facts=False)
    (line,) = [ln for ln in printed.splitlines() if ln.startswith("! record text cut")]
    assert f"of the whole thread's {_WHOLE_CHARS:,} chars" in line, line
    if comments["shown"] == 0:
        assert f"none of its {_COMMENTS} comments" in line, line
        assert "left out" not in line and "comment 1" not in line, line
    else:
        assert f"of {_COMMENTS}" in line and "none of its" not in line, line
