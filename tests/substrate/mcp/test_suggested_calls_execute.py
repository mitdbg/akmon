"""Every call a reply suggests runs, and a continuation reads on (lane ri, tokens).

A reply names the calls to make next: `next: <token>`, `next:
continuation_token=<token>` or `next: offset=<n>` on its status line; on a
refusal, `retry: <call>` and, for a program, `error.retry_call`; and calls
spelled inside its `!` notices. In nine lab runs agents rarely followed them,
and 40 of the 96 continuation calls they made were refused. These tests take
every call a reply suggests from the text a model reads, send it, and require
that it is served, not refused, and that a continuation returns lines or
records no earlier page returned.

The hermetic part runs the server's own call path in process: the argument
rewrite with its continuation-token check (`_rewrite_operator_call`), the
byte-bounded tool wrapper, typed refusals (`_expected_error_refusal`) and the
text renderer, around small operators that page one record's lines, one grep
result and one filter listing with real signed tokens. The PostgreSQL part
sends the same kinds of calls to a real server.
"""
from __future__ import annotations

import json
import re
from typing import Any, Callable, Optional

import pytest

from okg.substrate.errors import SubstrateError
from okg.substrate.mcp import continuation
from okg.substrate.mcp.continuation import (
    consume_continuation,
    issue_continuation,
    route_next,
)
from okg.substrate.mcp.result_text import fact_value, status_facts

GEN = "okg:demo:branch:default:gen:20260101T000000000000Z:0123456789ab"
DOC = "doc:1"
LINES = [f"line {n:03d} " + "w" * 40 for n in range(1, 61)]
RECORDS = [f"n:{k:02d}" for k in range(1, 23)]
PAGE_LINES = 10

#: A tool name followed by `(`: where a suggested call is spelled.
_CALL_START = re.compile(r"\b(inspect|expand|filter|map|search|aggregate)\(")
_ARG_NAME = re.compile(r"\s*([A-Za-z_][A-Za-z0-9_]*)=")
#: What a hint writes for the caller's own token.
_PLACEHOLDERS = ("<this token>", "<token>")


def _binding(kind: str, *, projection: Any, sort: str = "s") -> dict:
    return {
        "kind": kind, "deployment": "d", "database": "db", "branch": "main",
        "generation": GEN, "catalog_version": 7, "subtype": None, "path": None,
        "sort": sort, "projection": projection, "authority": "auth",
        "policy": None, "key_id": continuation.signing_key_id(),
    }


def _text_binding(node_id: str) -> dict:
    return _binding("inspect_text", projection={"node_id": node_id, "page_lines": PAGE_LINES})


def _grep_binding(query: str) -> dict:
    return _binding("grep_page", projection={"query": query}, sort="grep")


def _refused(tool: str, code: str, message: str, hint: str) -> dict:
    return {"operator_name": tool, "status": "refused",
            "error": {"code": code, "message": message, "fix_hint": hint}}


# ── small operators with real tokens ─────────────────────────────────────


def _inspect(target=None, select=None, generation_id=None, max_bytes=None, next=None):
    """Pages of `PAGE_LINES` lines of one record; `next` alone continues, as
    the server's own `_next_call` requires."""
    if next is not None:
        if target is not None or select is not None:
            named = ", ".join(n for n, v in (("target", target), ("select", select)) if v)
            return _refused(
                "inspect", "next_with_read_arguments",
                f"inspect: `next` carries the whole read and cannot be combined with {named}",
                "call inspect(next=<token>) alone",
            )
        node_id = route_next(next, tool="inspect")["projection"]["node_id"]
        start = int(consume_continuation(next, expected_binding=_text_binding(node_id))["line"])
    else:
        if not isinstance(target, dict) or target.get("kind") != "text" or target.get(
            "node_id"
        ) != DOC:
            return _refused("inspect", "invalid_operator_arguments",
                            "inspect needs a text target", "pass target={...}")
        node_id, start = DOC, 0
        lines = target.get("lines")
        if lines is not None:
            # A lines read: [[first, last]], numbered from 1.
            start = int(lines[0][0]) - 1
    end = min(start + PAGE_LINES, len(LINES))
    payload = {
        "node_id": node_id, "subtype": "document", "order_basis": "chunk_ordinal",
        "coverage": "complete",
        "items": [{"chunk_id": f"c:{i}", "char_offset": i * 50, "text": LINES[i]}
                  for i in range(start, end)],
        "complete": end >= len(LINES), "truncated": end < len(LINES),
        "lines": {"basis": "source", "total": len(LINES), "first": start + 1, "last": end},
        "pinned_generation_id": GEN,
    }
    if end < len(LINES):
        payload["continuation_token"] = issue_continuation(
            _text_binding(node_id), cursor={"line": end},
        )
    return payload


def _search(query=None, method=None, where=None, limit=None, select=None, within=None,
            view=None, continuation_token=None, offset=0, max_chars=None, max_bytes=None):
    """A grep result of `RECORDS`, five a page, continued by its token."""
    if method != "grep":
        return _refused("search", "invalid_operator_arguments", "grep only here",
                        'pass method="grep"')
    start = 0
    if continuation_token is not None:
        cursor = consume_continuation(continuation_token, expected_binding=_grep_binding(query))
        start = int(cursor["after"])
    size = int(limit or 5)
    if query == "sparse" and continuation_token is None:
        # A first page that stopped at its scan bound before any record of
        # its own (`grep_searched.bound_read`), as a real grep page does.
        return {
            "query": query, "method": "grep", "results": [], "limit": size, "offset": 0,
            "next_offset": None, "total_count": None, "total_count_basis": "unknown",
            "truncated": False, "page_basis": "scan_bound_reached",
            "page_scan_bound": {"candidate_rows": 2000}, "page_scan_bound_hit": "candidate_rows",
            "grep_searched": {"bound_read": {"bound": "candidate_rows", "rows": 2000}},
            "continuation_token": issue_continuation(_grep_binding(query), cursor={"after": 0}),
            "pinned_generation_id": GEN,
        }
    page = RECORDS[start:start + size]
    end = start + len(page)
    payload = {
        "query": query, "method": "grep",
        "results": [{"node_id": node_id, "subtype": "note"} for node_id in page],
        "limit": size, "offset": start, "next_offset": None, "total_count": None,
        "total_count_basis": "unknown", "truncated": end < len(RECORDS),
        "pinned_generation_id": GEN,
    }
    if end < len(RECORDS):
        payload["continuation_token"] = issue_continuation(
            _grep_binding(query), cursor={"after": end},
        )
    return payload


def _filter(input=None, predicate=None, generation_id=None, limit=50, offset=0, select=None,
            order_by=None, authority=None, max_bytes=None, retain=False, next=None):
    """`RECORDS` by offset; a `next` here is a keyset token's."""
    if next is not None:
        route_next(next, tool="filter")
        raise AssertionError("no keyset token is issued by this listing")
    size = int(limit or 50)
    rows = RECORDS[offset:offset + size]
    more = offset + size < len(RECORDS)
    return {
        "items": [{"node_id": node_id, "subtype": "note"} for node_id in rows],
        "total_count": len(RECORDS), "total_count_basis": "exact", "truncated": more,
        "limit": size, "offset": offset, "next_offset": offset + size if more else None,
        "pinned_generation_id": GEN,
    }


_OPERATORS: dict[str, Callable[..., Any]] = {
    "inspect": _inspect, "search": _search, "filter": _filter,
}


def _serve(tool: str, arguments: dict) -> tuple[dict, str]:
    """One call as the server serves it, and the text a model reads.

    The rewrite and the token check first (their refusal is served as a
    typed refusal), then the byte-bounded tool under the call's rewrite
    state (so the reply carries its `adjusted` notes), then the text.
    """
    from okg.substrate.mcp.response_bounds import (
        _CALL_REWRITE,
        _expected_error_refusal,
        _inspect_kind,
        _rewrite_operator_call,
        bound_read_tool,
    )
    from okg.substrate.mcp.result_text import ResultContext, render, result_context

    served, rewrite = _rewrite_operator_call(tool, arguments)
    kind = _inspect_kind(tool, served if isinstance(served, dict) else {})
    if rewrite is not None and rewrite.refusal is not None:
        payload = _expected_error_refusal(tool, rewrite.refusal, budget=65536)
        return payload, render(payload, tool=tool, kind=kind)
    state = _CALL_REWRITE.set(rewrite)
    try:
        with result_context(ResultContext(format="text", tool=tool, kind=kind)):
            try:
                payload = bound_read_tool(tool, _OPERATORS[tool])(**served)
            except SubstrateError as exc:
                payload = _expected_error_refusal(tool, exc, budget=65536)
    finally:
        _CALL_REWRITE.reset(state)
    return payload, render(payload, tool=tool, kind=kind)


# ── what a reply suggests ────────────────────────────────────────────────


def _parse_call(text: str, start: int) -> tuple[Optional[dict], int]:
    """The `name=<json>, ...)` arguments from `start`, or None when the text
    there is not one call this module can send."""
    decoder = json.JSONDecoder()
    arguments: dict[str, Any] = {}
    i = start
    while True:
        if text.startswith(")", i):
            return arguments, i + 1
        named = _ARG_NAME.match(text, i)
        if named is None:
            return None, i
        i = named.end()
        placeholder = next((p for p in _PLACEHOLDERS if text.startswith(p, i)), None)
        if placeholder is not None:
            value: Any = _PLACEHOLDERS[0]
            i += len(placeholder)
        else:
            try:
                value, i = decoder.raw_decode(text, i)
            except json.JSONDecodeError:
                return None, i
        arguments[named.group(1)] = value
        if text.startswith(", ", i) and _ARG_NAME.match(text, i + 2):
            i += 2


#: A status-line fact that changes the read's size: `smaller: select=[...],
#: max_bytes=10240`, `fewer: limit=5`, `more text: max_chars=1200`.
_KNOB = re.compile(r"^(smaller|fewer|more text): (.+)$")
#: A cut text page's `max_bytes N reads the rest` (#3634).
_REST = re.compile(r"^max_bytes (\d+) reads the rest$")


def suggested_calls(
    text: str, payload: dict, tool: str, sent: dict, origin: Optional[dict] = None,
) -> list[tuple[str, str, Optional[dict]]]:
    """Every call `text` (and, for a program, `payload`) suggests after
    `tool(**sent)`, as `(how, tool, arguments)`; arguments None for a call
    that is named but cannot be read as one (a test failure). A size knob
    applies to `origin`, the read's first call (default `sent`)."""
    out: list[tuple[str, str, Optional[dict]]] = []
    origin = sent if origin is None else origin
    own_token = sent.get("next") or sent.get("continuation_token")
    facts = status_facts(text)
    value = fact_value(facts, "next:", last=True)
    page_token: Optional[str] = None
    if isinstance(value, str) and value != "none":
        rest = {k: v for k, v in sent.items() if k not in ("next", "continuation_token", "offset")}
        if value.startswith("offset="):
            out.append(("next", tool, {**rest, "offset": int(value.split("=", 1)[1])}))
        elif value.startswith("continuation_token="):
            out.append(("next", tool, {**rest, "continuation_token": value.split("=", 1)[1]}))
        else:
            page_token = value
            out.append(("next", tool, {"next": value}))
    elif isinstance(value, int):
        out.append(("next", tool, {**sent, "offset": value}))
    for fact in facts:
        knob = _KNOB.match(fact)
        if knob is not None:
            changes, _ = _parse_call(knob.group(2) + ")", 0)
            out.append(("knob", tool, None if changes is None else {**origin, **changes}))
        rest_of_text = _REST.match(fact)
        if rest_of_text is not None and page_token is not None:
            out.append(("knob", tool, {"next": page_token,
                                       "max_bytes": int(rest_of_text.group(1))}))
    plain = _unquoted(text)
    for match in _CALL_START.finditer(plain):
        arguments, _ = _parse_call(plain, match.end())
        if arguments is not None:
            arguments = {
                k: (own_token if v == _PLACEHOLDERS[0] else v) for k, v in arguments.items()
            }
        out.append(("named", match.group(1), arguments))
    error = payload.get("error") if isinstance(payload, dict) else None
    retry = error.get("retry_call") if isinstance(error, dict) else None
    if isinstance(retry, dict) and retry.get("mode") in ("corrected", "guess", "restart"):
        out.append(("retry_call", retry["tool"], dict(retry["arguments"])))
    # One send per distinct call (a hint is on the status line and again
    # on its own line).
    unique: list[tuple[str, str, Optional[dict]]] = []
    seen: set[str] = set()
    for how, name, arguments in out:
        key = name + json.dumps(arguments, sort_keys=True, default=str)
        if arguments is None or key not in seen:
            seen.add(key)
            unique.append((how, name, arguments))
    return unique


#: A hint printed as a JSON string (it holds quotes): `retry: "..."`,
#: `fix_hint: "..."`, `remedy: "..."`.
_QUOTED_HINT = re.compile(r'((?:retry|fix_hint|remedy): )("(?:[^"\\]|\\.)*")')


def _unquoted(text: str) -> str:
    """`text` with each JSON-quoted hint read as the text a model reads."""
    return _QUOTED_HINT.sub(lambda m: m.group(1) + json.loads(m.group(2)), text)


def _refused_status(payload: dict) -> bool:
    return isinstance(payload, dict) and payload.get("status") == "refused"


def _run_every_suggestion(
    serve: Callable[[str, dict], tuple[dict, str]], tool: str, sent: dict,
    payload: dict, text: str, origin: Optional[dict] = None,
) -> list[tuple[str, str, dict, dict, str]]:
    """Send every call the reply suggests; each must be served, not refused."""
    ran = []
    for how, name, arguments in suggested_calls(text, payload, tool, sent, origin):
        assert arguments is not None, f"a suggested {name}(...) call cannot be read: {text}"
        result, result_text = serve(name, arguments)
        assert not _refused_status(result), (how, name, arguments, result_text)
        ran.append((how, name, arguments, result, result_text))
    return ran


def _lines(payload: dict) -> list[str]:
    return [item["text"] for item in payload.get("items") or []]


# ── hermetic: pages ───────────────────────────────────────────────────────


def test_every_call_a_cut_text_page_suggests_runs_and_reads_on() -> None:
    sent = origin = {"target": {"kind": "text", "node_id": DOC}}
    payload, text = _serve("inspect", sent)
    seen = _lines(payload)
    for _ in range(len(LINES)):
        ran = _run_every_suggestion(_serve, "inspect", sent, payload, text, origin)
        following = [r for r in ran if r[0] == "next"]
        if payload.get("complete"):
            assert not following, text
            break
        assert len(following) == 1, text
        _, _, sent, payload, text = following[0]
        page = _lines(payload)
        assert page and not set(page) & set(seen), "a continuation returned no new lines"
        seen += page
    assert seen == LINES


def test_every_call_a_grep_page_suggests_runs_and_reads_on() -> None:
    sent = origin = {"query": "w", "method": "grep", "limit": 5}
    payload, text = _serve("search", sent)
    seen = [row["node_id"] for row in payload["results"]]
    for _ in range(len(RECORDS)):
        following = [r for r in _run_every_suggestion(_serve, "search", sent, payload, text,
                                                      origin)
                     if r[0] == "next"]
        if not following:
            break
        assert len(following) == 1, text
        _, _, sent, payload, text = following[0]
        page = [row["node_id"] for row in payload["results"]]
        assert page and not set(page) & set(seen), "a continuation returned no new records"
        seen += page
    assert seen == RECORDS


def test_every_call_a_listing_page_suggests_runs_and_reads_on() -> None:
    sent = origin = {"where": {"type": "note"}, "limit": 5}
    payload, text = _serve("filter", sent)
    seen = [row["node_id"] for row in payload["items"]]
    for _ in range(len(RECORDS)):
        following = [r for r in _run_every_suggestion(_serve, "filter", sent, payload, text,
                                                      origin)
                     if r[0] == "next"]
        if not following:
            break
        _, _, sent, payload, text = following[0]
        page = [row["node_id"] for row in payload["items"]]
        assert page and not set(page) & set(seen)
        seen += page
    assert seen == RECORDS


# ── hermetic: refusals (the lab transcripts' cases) ──────────────────────


def _first_token() -> str:
    payload, _ = _serve("inspect", {"target": {"kind": "text", "node_id": DOC}})
    return payload["continuation_token"]


def _altered(token: str, index: int) -> str:
    """One character changed: at `index` of a whole token's body, or inside
    a handle's last part (lane ri, tokens: the lab runs' miscopies)."""
    if token.startswith(continuation.HANDLE_PREFIX):
        index = len(token) - 1 - index % 16
    else:
        index = min(index, token.index(".") - 1)
    return token[:index] + ("Q" if token[index] != "Q" else "R") + token[index + 1:]


@pytest.mark.parametrize(("tool", "sent", "reason"), [
    # DeepSeek okg3-c006 try 2, call 25 (with the target, as at call 24).
    ("inspect", {"target": {"kind": "text", "node_id": DOC}, "next": "ALTERED"},
     "signature_invalid"),
    # DeepSeek okg3-q020 try 1, call 8.
    ("filter", {"where": {"type": "document"}, "select": ["node_id", "attrs.path"],
                "max_bytes": 8192, "next": "1"}, "malformed"),
    # ERAG K qst_0204 call 46: a grep page's token miscopied.
    ("search", {"query": "w", "method": "grep", "limit": 5,
                "continuation_token": "ALTERED_GREP"}, "signature_invalid"),
])
def test_every_call_a_token_refusal_suggests_runs(tool: str, sent: dict, reason: str) -> None:
    if sent.get("next") == "ALTERED":
        sent = {**sent, "next": _altered(_first_token(), 136)}
    if sent.get("continuation_token") == "ALTERED_GREP":
        token = _serve("search", {"query": "w", "method": "grep", "limit": 5})[0][
            "continuation_token"]
        sent = {**sent, "continuation_token": _altered(token, 476)}
    payload, text = _serve(tool, sent)
    assert _refused_status(payload), text
    assert payload["error"]["code"] == "continuation_token_invalid", text
    assert payload["error"]["data"]["reason"] == reason
    assert "exactly as the reply printed it" in payload["error"]["fix_hint"]
    # The status line names the restart as a call, not as quoted text.
    assert f"· retry: copy the token exactly as printed, or restart: {tool}(" in (
        text.split("\n", 1)[0]
    ), text.split("\n", 1)[0]
    ran = _run_every_suggestion(_serve, tool, sent, payload, text)
    restarts = [r for r in ran if r[0] in ("named", "retry_call")]
    assert restarts, f"no restart is suggested: {text}"
    for _, name, arguments, _, _ in restarts:
        assert name == tool
        assert "next" not in arguments and "continuation_token" not in arguments


def test_the_copy_the_refusal_asks_for_reads_on() -> None:
    """`pass the token exactly`: the token as issued continues page one."""
    token = _first_token()
    refused, _ = _serve("inspect", {"next": _altered(token, 346)})
    assert refused["error"]["data"]["reason"] == "signature_invalid"
    payload, _ = _serve("inspect", {"next": token})
    assert _lines(payload) == LINES[PAGE_LINES:2 * PAGE_LINES]


def test_a_made_up_next_names_no_call_to_send() -> None:
    """DeepSeek okg3-q020 try 1, call 13: nothing the caller sent names a read."""
    sent = {"max_bytes": 16384, "next": "x"}
    payload, text = _serve("inspect", sent)
    assert payload["error"]["code"] == "continuation_token_invalid", text
    assert payload["error"]["data"]["reason"] == "malformed"
    assert "opaque" in text
    assert suggested_calls(text, payload, "inspect", sent) == []


def test_the_target_repeated_beside_next_is_served_with_a_note() -> None:
    """DeepSeek okg3-c005 try 1, call 40: the token copied exactly, its own
    target beside it. Served as `next` alone; the reply says so."""
    token = _first_token()
    payload, text = _serve("inspect", {"next": token, "target": {"kind": "text", "node_id": DOC}})
    assert not _refused_status(payload), text
    assert _lines(payload) == LINES[PAGE_LINES:2 * PAGE_LINES]
    assert "! adjusted: `target` beside `next` read as `next` alone" in text


def test_another_target_beside_next_is_refused_and_its_retry_runs() -> None:
    token = _first_token()
    sent = {"next": token, "target": {"kind": "text", "node_id": "doc:2"}}
    payload, text = _serve("inspect", sent)
    assert payload["error"]["code"] == "next_with_read_arguments", text
    ran = _run_every_suggestion(_serve, "inspect", sent, payload, text)
    assert [r[2] for r in ran] == [{"next": token}]
    assert _lines(ran[0][3]) == LINES[PAGE_LINES:2 * PAGE_LINES]


# ── hermetic: cut notes name their calls (lane ri, tokens: fix C) ─────────


def test_a_cut_text_page_note_prints_both_calls_and_both_run() -> None:
    """crfix3: 50 `! not read: lines` notes said "`next` reads on" and 10 were
    followed; the note now prints the next call and the lines read whole."""
    sent = {"target": {"kind": "text", "node_id": DOC}}
    payload, text = _serve("inspect", sent)
    note = next(line for line in text.splitlines() if line.startswith("! not read: lines"))
    assert note.startswith(f"! not read: lines {PAGE_LINES + 1}-{len(LINES)}; read on with inspect(")
    assert f'"lines": [[{PAGE_LINES + 1}, {len(LINES)}]]' in note or (
        f'"lines":[[{PAGE_LINES + 1},{len(LINES)}]]' in note
    ), note
    assert payload["continuation_token"] in note
    ran = _run_every_suggestion(_serve, "inspect", sent, payload, text, sent)
    # The note's `next` call is the status line's own (sent once).
    both = [r for r in ran if r[1] == "inspect" and json.dumps(r[2], sort_keys=True) in {
        json.dumps({"next": payload["continuation_token"]}, sort_keys=True),
        json.dumps({"target": {"kind": "text", "node_id": DOC,
                               "lines": [[PAGE_LINES + 1, len(LINES)]]}}, sort_keys=True),
    }]
    assert len(both) == 2, [r[2] for r in ran]
    for _, _, _, result, _ in both:
        assert _lines(result)[0] == LINES[PAGE_LINES]


def test_a_grep_page_that_read_no_record_says_how_far_it_read() -> None:
    """12 grep pages that stopped at their bound with no record said only
    "bound reached, no records on this page" and none was continued."""
    sent = {"query": "sparse", "method": "grep", "limit": 5}
    payload, text = _serve("search", sent)
    head = text.split("\n", 1)[0]
    assert "no match in the first 2,000 candidate records, more may follow" in head, head
    assert "no records on this page" not in head
    following = [r for r in _run_every_suggestion(_serve, "search", sent, payload, text)
                 if r[0] == "next"]
    assert [row["node_id"] for row in following[0][3]["results"]] == RECORDS[:5]


def test_a_cut_node_read_names_the_calls_that_read_what_was_cut() -> None:
    """Sol 6.1: 92 of 98 cut node reads ended `next: none` and 90 were left;
    the page-cut line now names the calls that read the cut parts."""
    from okg.substrate.mcp.result_text import render

    page = {
        "node_id": "jira:1", "subtype": "jira_issue", "attrs": {"title": "t"},
        "truncated": True, "truncation_reason": "response_byte_budget",
        "response_byte_budget": 12000,
        "read_whole": {"tool": "inspect", "target": {"kind": "text", "node_id": "jira:1"}},
        "attrs_cut": {"text": {"bytes": 30000, "chars": 29000, "served_chars": 100}},
        "parts_cut": {"live_relations": {
            "bytes": 23417, "relations": 65, "names": ["a", "b"],
            "read_whole": {"tool": "inspect",
                           "target": {"kind": "subtype", "subtype": "jira_issue"}},
        }},
        "pinned_generation_id": GEN,
    }
    text = render(page, tool="inspect", kind="node")
    line = next(line for line in text.splitlines() if line.startswith("! page cut to fit"))
    assert "; read what was cut with inspect(" in line, line
    calls = [c for c in suggested_calls(line, {}, "inspect", {}) if c[0] == "named"]
    assert [c[2] for c in calls] == [
        {"target": {"kind": "text", "node_id": "jira:1"}},
        {"target": {"kind": "subtype", "subtype": "jira_issue"}},
    ]


# ── PostgreSQL: a real server ────────────────────────────────────────────


@pytest.fixture
def deployment(clean_runtime: str, okg_mcp_can_log_in: str, tmp_path: Any) -> str:
    from okg.substrate.library.sources.base import EdgeFact
    from tests.pytest.catalog_fixture_builders import SubtypeSpec
    from tests.substrate.mcp.search_fixture import make_search_deployment, node, publish_nodes

    doc = SubtypeSpec("document", metadata={
        "searchable_text_fields": ["title", "text"],
        "passage_rollup": {"edge_type": "contains", "child_subtype": "document_chunk"},
    })
    chunk = SubtypeSpec("document_chunk", metadata={"searchable_text_fields": ["text"]})
    note = SubtypeSpec("note", metadata={"searchable_text_fields": ["text"]})
    deployment = make_search_deployment(tmp_path, dsn=clean_runtime)
    nodes = [
        node("doc:big", "document", "the big document", title="Big"),
        # A record whose node read is cut at 4 KB: its text is ~30 KB.
        node("note:long", "note", " ".join(f"word{i % 97}" for i in range(5000)),
             title="Long"),
    ]
    edges = []
    for i in range(3):
        chunk_id = f"doc:big:chunk:{i}"
        nodes.append(node(chunk_id, "document_chunk", f"Part {i}: " + (f"word{i} " * 370),
                          chunk_index=i, chunk_ordinal=i, chunk_total=3))
        edges.append(EdgeFact(
            src="doc:big", dst=chunk_id, edge_type="contains",
            source_record_id={"id": f"e{i}"}, source_revision={"v": 1},
        ))
    publish_nodes(
        clean_runtime, deployment, subtypes=[doc, chunk, note],
        narrowings=[("document", "contains", "document_chunk")],
        nodes=nodes, edges=edges,
    )
    return deployment


def _drive(dsn: str, deployment: str, body: Callable[[Any], Any]) -> None:
    import anyio
    from mcp.shared.memory import create_connected_server_and_client_session

    from okg.substrate.mcp.server import build_server

    server = build_server(dsn=dsn, deployment_name=deployment)

    async def run() -> None:
        async with create_connected_server_and_client_session(server) as client:
            await body(client)

    try:
        anyio.run(run)
    finally:
        server._okg_pool.close()  # type: ignore[attr-defined]


async def _call(client: Any, tool: str, arguments: dict) -> tuple[dict, str]:
    """The text a model reads, and the same call's JSON for a program."""
    from tests.substrate.mcp._result_payload import JSON_META, payload, result_text

    text = result_text(await client.call_tool(tool, arguments))
    data = payload(await client.call_tool(tool, arguments, meta=JSON_META))
    return data, text


async def _run_all(
    client: Any, tool: str, sent: dict, data: dict, text: str, origin: Optional[dict] = None,
) -> list:
    ran = []
    for how, name, arguments in suggested_calls(text, data, tool, sent, origin):
        assert arguments is not None, f"a suggested {name}(...) call cannot be read: {text}"
        result, result_text = await _call(client, name, arguments)
        assert not _refused_status(result), (how, name, arguments, result_text)
        ran.append((how, name, arguments, result, result_text))
    return ran


def _body(text: str) -> str:
    return text.split("\n", 1)[1] if "\n" in text else ""


@pytest.mark.postgres
def test_a_real_text_read_follows_every_suggestion_to_the_end(
    clean_runtime: str, deployment: str,
) -> None:
    async def body(client: Any) -> None:
        sent = origin = {"target": {"kind": "text", "node_id": "doc:big"}, "max_bytes": 4096}
        data, text = await _call(client, "inspect", sent)
        bodies = [_body(text)]
        for _ in range(20):
            ran = await _run_all(client, "inspect", sent, data, text, origin)
            following = [r for r in ran if r[0] == "next"]
            if not following:
                assert "complete" in status_facts(text), text.split("\n", 1)[0]
                break
            _, _, sent, data, text = following[0]
            assert _body(text) not in bodies, "a continuation repeated a page"
            bodies.append(_body(text))
        else:
            raise AssertionError("the read did not end within 20 pages")
        assert len(bodies) >= 2

    _drive(clean_runtime, deployment, body)


@pytest.mark.postgres
def test_a_real_path_read_follows_every_suggestion_and_takes_its_own_call_back(
    clean_runtime: str, deployment: str,
) -> None:
    async def body(client: Any) -> None:
        start = {"input": {"node_id": "doc:big"}, "path": ["contains:out"], "limit": 1}
        data, text = await _call(client, "expand", start)
        seen = [row["node_id"] for row in data["results"]]
        token = data["continuation_token"]
        # The issuing call repeated beside its token: served as `next` alone.
        again, again_text = await _call(client, "expand", {"next": token, **start})
        assert not _refused_status(again), again_text
        assert "! adjusted: " in again_text and "beside `next`" in again_text
        sent = start
        for _ in range(10):
            following = [r for r in await _run_all(client, "expand", sent, data, text, start)
                         if r[0] == "next"]
            if not following:
                break
            _, _, sent, data, text = following[0]
            page = [row["node_id"] for row in data["results"]]
            assert page and not set(page) & set(seen), text
            seen += page
        assert sorted(seen) == [f"doc:big:chunk:{i}" for i in range(3)]

    _drive(clean_runtime, deployment, body)


@pytest.mark.postgres
def test_every_call_a_real_cut_node_read_suggests_runs(
    clean_runtime: str, deployment: str,
) -> None:
    """A node read cut at 4 KB ends `next: none`; every call its notices name
    (the record's text, the cut parts) is served."""
    async def body(client: Any) -> None:
        sent = {"target": {"kind": "node", "node_id": "note:long"}, "max_bytes": 4096}
        data, text = await _call(client, "inspect", sent)
        assert not _refused_status(data), text
        cut = [line for line in text.splitlines() if line.startswith("! page cut to fit")]
        assert cut and "; read what was cut with inspect(" in cut[0], text
        ran = await _run_all(client, "inspect", sent, data, text)
        assert [r for r in ran if r[0] == "named"], text

    _drive(clean_runtime, deployment, body)


@pytest.mark.postgres
def test_every_call_a_real_token_refusal_suggests_runs(
    clean_runtime: str, deployment: str,
) -> None:
    async def body(client: Any) -> None:
        target = {"kind": "text", "node_id": "doc:big"}
        first, _ = await _call(client, "inspect", {"target": target, "max_bytes": 4096})
        cases = [
            ("inspect", {"target": target, "next": _altered(first["continuation_token"], 136)},
             "signature_invalid"),
            # A one-hop read: its restart is one the retry proof takes.
            ("expand", {"input": {"node_id": "doc:big"}, "edge_type": "contains",
                        "next": "1"}, "malformed"),
            ("expand", {"input": {"node_id": "doc:big"}, "path": ["contains:out"],
                        "limit": 1, "next": "offset=100"}, "malformed"),
            ("search", {"query": "word1", "method": "grep", "limit": 5,
                        "continuation_token": "abc.def"}, "signature_invalid"),
        ]
        for tool, sent, reason in cases:
            data, text = await _call(client, tool, sent)
            assert _refused_status(data), text
            assert data["error"]["code"] == "continuation_token_invalid", text
            assert data["error"]["data"]["reason"] == reason, text
            assert "exactly as the reply printed it" in text
            ran = await _run_all(client, tool, sent, data, text)
            assert [r for r in ran if r[0] in ("named", "retry_call")], text

    _drive(clean_runtime, deployment, body)
