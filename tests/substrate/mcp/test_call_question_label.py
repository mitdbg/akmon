"""The question and try each MCP call served, in the call log (issue #3773).

A client (a benchmark harness) names the question and try per request: the
HTTP headers ``X-OKG-Question`` / ``X-OKG-Try`` or the request ``_meta`` keys
``okg/question`` / ``okg/try``. The tracer writes ``<id>#try<n>`` into
``okg.mcp_trace_events.question_label`` and the parts into
``okg.mcp_call_log.question_id`` / ``try_number``.

Two layers:

* **Hermetic**: the request parsing, the precedence (request, then the
  in-process label, then ``OKG_MCP_SESSION_QUESTION``) and every refusal,
  against a stand-in for the SDK's per-request context.
* **Postgres**: the real ``build_server`` over a real publish, served over
  streamable HTTP. Two sessions with different questions call at the same
  moment (a two-party barrier inside the tool proves the overlap); a call
  with no header keeps today's behavior; a ``_meta`` call; an invalid header
  is refused and its refusal is logged with no label.
"""

from __future__ import annotations

import asyncio
import contextlib
import threading
from contextlib import AsyncExitStack
from types import SimpleNamespace
from typing import Any, Callable, Iterator

import psycopg
import pytest

pytest.importorskip("mcp")

from mcp.server.lowlevel.server import request_ctx  # noqa: E402
from starlette.datastructures import Headers  # noqa: E402

from okg.substrate.mcp import tracing  # noqa: E402
from okg.substrate.mcp.tracing import (  # noqa: E402
    CallQuestion,
    QuestionLabelInvalid,
    call_question,
    clear_current_label,
    request_call_question,
    set_current_label,
)


@contextlib.contextmanager
def _request(
    headers: list[tuple[str, str]] | None = None,
    meta: dict[str, Any] | None = None,
) -> Iterator[None]:
    """Stand in for the SDK's per-request context of one tool call."""
    context = SimpleNamespace(
        request=None if headers is None else SimpleNamespace(
            headers=Headers(raw=[(k.lower().encode(), v.encode()) for k, v in headers])
        ),
        meta=None if meta is None else SimpleNamespace(model_extra=dict(meta)),
    )
    token = request_ctx.set(context)  # type: ignore[arg-type]
    try:
        yield
    finally:
        request_ctx.reset(token)


@pytest.fixture(autouse=True)
def _no_ambient_label(monkeypatch: pytest.MonkeyPatch) -> Iterator[None]:
    monkeypatch.delenv("OKG_MCP_SESSION_QUESTION", raising=False)
    clear_current_label()
    yield
    clear_current_label()


# ============================================================ hermetic


def test_headers_name_the_question_and_try() -> None:
    with _request([("X-OKG-Question", "bench-17"), ("X-OKG-Try", "2")]):
        question = request_call_question()
    assert question == CallQuestion("bench-17", 2, "request")
    assert question.label == "bench-17#try2"


def test_meta_names_the_question_and_try_on_any_transport() -> None:
    # No HTTP request at all: the stdio shape.
    with _request(None, {"okg/question": "bench-9", "okg/try": 3}):
        question = request_call_question()
    assert question == CallQuestion("bench-9", 3, "request")


def test_a_question_without_a_try_has_no_try_suffix() -> None:
    with _request([("X-OKG-Question", "bench-1")]):
        question = request_call_question()
    assert question is not None and question.label == "bench-1"
    assert question.try_number is None


def test_header_and_meta_that_agree_are_one_question() -> None:
    with _request(
        [("X-OKG-Question", "bench-4"), ("X-OKG-Try", "1")],
        {"okg/question": "bench-4", "okg/try": "1"},
    ):
        assert request_call_question() == CallQuestion("bench-4", 1, "request")


def test_precedence_request_then_in_process_label_then_env(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("OKG_MCP_SESSION_QUESTION", "env-question")
    assert call_question() == CallQuestion("env-question", None, "env")
    set_current_label("in-process-question")
    assert call_question() == CallQuestion("in-process-question", None, "label")
    with _request([("X-OKG-Question", "bench-2"), ("X-OKG-Try", "5")]):
        assert call_question() == CallQuestion("bench-2", 5, "request")
    # A request that names nothing falls through, unchanged from before.
    with _request([("X-Other", "x")]):
        assert call_question() == CallQuestion("in-process-question", None, "label")


def test_no_request_and_no_label_is_no_question() -> None:
    assert call_question() is None


@pytest.mark.parametrize(
    ("headers", "meta", "field", "channel"),
    [
        ([("X-OKG-Question", "two words")], None, "question", "header"),
        ([("X-OKG-Question", "q#1")], None, "question", "header"),
        ([("X-OKG-Question", "a,b")], None, "question", "header"),
        ([("X-OKG-Question", "x" * 201)], None, "question", "header"),
        ([("X-OKG-Question", "café")], None, "question", "header"),
        ([("X-OKG-Question", "  ")], None, "question", "header"),
        ([("X-OKG-Question", "a"), ("X-OKG-Question", "b")], None, "question", "header"),
        ([("X-OKG-Question", "q"), ("X-OKG-Try", "0")], None, "try", "header"),
        ([("X-OKG-Question", "q"), ("X-OKG-Try", "-1")], None, "try", "header"),
        ([("X-OKG-Question", "q"), ("X-OKG-Try", "two")], None, "try", "header"),
        ([("X-OKG-Question", "q"), ("X-OKG-Try", "1000000")], None, "try", "header"),
        ([("X-OKG-Try", "1")], None, "try", "header"),
        (None, {"okg/question": 17}, "question", "meta"),
        (None, {"okg/question": "q", "okg/try": True}, "try", "meta"),
        (None, {"okg/question": "q", "okg/try": 0}, "try", "meta"),
        ([("X-OKG-Question", "q1")], {"okg/question": "q2"}, "question", "header+meta"),
        (
            [("X-OKG-Question", "q"), ("X-OKG-Try", "1")],
            {"okg/try": 2}, "try", "header+meta",
        ),
    ],
)
def test_an_invalid_or_ambiguous_value_is_refused_by_name(
    headers: list[tuple[str, str]] | None,
    meta: dict[str, Any] | None,
    field: str,
    channel: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # A lower-precedence label is present and must NOT cover the refusal.
    monkeypatch.setenv("OKG_MCP_SESSION_QUESTION", "env-question")
    with _request(headers, meta), pytest.raises(QuestionLabelInvalid) as raised:
        call_question()
    assert raised.value.code == "mcp_question_label_invalid"
    assert raised.value.data == {"field": field, "channel": channel}
    # The caller's value is not echoed into a message that reaches logs.
    for name, value in headers or []:
        if len(value) > 3:
            assert value not in str(raised.value), name


def _capturing_tracer(
    monkeypatch: pytest.MonkeyPatch,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    monkeypatch.setenv("OKG_MCP_TRACE_ENABLED", "1")
    traced: list[dict[str, Any]] = []
    logged: list[dict[str, Any]] = []
    monkeypatch.setattr(
        tracing, "trace_tool_call", lambda *_a, **kw: traced.append(kw),
    )
    monkeypatch.setattr(
        tracing, "log_call_to_log", lambda *_a, **kw: logged.append(kw),
    )
    return traced, logged


def test_the_tracer_writes_the_request_question_into_both_rows(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    traced, logged = _capturing_tracer(monkeypatch)
    wrapped = tracing.make_tracer(object(), deployment_name="default")("search")(  # type: ignore[arg-type]
        lambda query: {"query": query, "results": []}
    )
    with _request([("X-OKG-Question", "bench-3"), ("X-OKG-Try", "4")]):
        wrapped(query="needle")
    assert [row["question_label"] for row in traced] == ["bench-3#try4"]
    assert [row["question"] for row in logged] == [CallQuestion("bench-3", 4, "request")]


def test_the_tracer_refuses_an_invalid_label_before_the_tool_runs(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("OKG_MCP_SESSION_QUESTION", "env-question")
    traced, logged = _capturing_tracer(monkeypatch)
    ran: list[str] = []

    def search(query: str) -> dict[str, Any]:
        ran.append(query)
        return {"query": query}

    wrapped = tracing.make_tracer(object(), deployment_name="default")("search")(search)  # type: ignore[arg-type]
    with _request([("X-OKG-Question", "not ok")]), pytest.raises(QuestionLabelInvalid):
        wrapped(query="needle")
    assert ran == []
    # Logged with its reason, and with NO label: not the env label either.
    assert [row["question_label"] for row in traced] == [None]
    assert isinstance(traced[0]["error"], QuestionLabelInvalid)
    assert [(row["question"], row["error_code"]) for row in logged] == [
        (None, "mcp_question_label_invalid"),
    ]


def test_trace_tool_call_still_defaults_to_the_env_label(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A direct caller that names no label keeps the env fallback."""
    monkeypatch.setenv("OKG_MCP_SESSION_QUESTION", "env-question")
    seen: list[Any] = []
    monkeypatch.setattr(
        tracing, "_write_unified_mcp_span", lambda *_a, **kw: seen.append(kw),
    )

    @contextlib.contextmanager
    def _conn() -> Iterator[Any]:
        yield SimpleNamespace()

    pool = SimpleNamespace(connection=_conn)
    # The first insert fails on the stand-in connection (swallowed); the
    # span writer then receives the resolved label.
    tracing.trace_tool_call(
        pool, deployment_name="default", tool_name="search",  # type: ignore[arg-type]
        request={}, response={},
    )
    tracing.trace_tool_call(
        pool, deployment_name="default", tool_name="search",  # type: ignore[arg-type]
        request={}, response={}, question_label=None,
    )
    assert [row["question_label"] for row in seen] == ["env-question", None]


# ============================================================ postgres

_WORDS = ("alpha", "bravo", "charlie", "delta", "echo", "foxtrot")


def _gate(mcp: Any, name: str, parties: int) -> Callable[[], None]:
    """Make ``name`` wait until ``parties`` calls are inside it; return undo."""
    tool = mcp._tool_manager._tools[name]
    original = tool.fn
    barrier = threading.Barrier(parties, timeout=30.0)

    def gated(**kwargs: Any) -> Any:
        barrier.wait()
        return original(**kwargs)

    tool.fn = gated

    def undo() -> None:
        tool.fn = original

    return undo


def _search_rows(dsn: str) -> tuple[dict[str, tuple], dict[str, tuple]]:
    with psycopg.connect(dsn, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT request_json->'kwargs'->>'query', question_label, error_text "
            "FROM okg.mcp_trace_events WHERE tool_name = 'search'"
        )
        traces = [(row[0], (row[1], row[2])) for row in cur.fetchall()]
        cur.execute(
            "SELECT args->>'query', question_id, try_number, error_code "
            "FROM okg.v_mcp_calls WHERE tool_name = 'search'"
        )
        calls = [(row[0], tuple(row[1:])) for row in cur.fetchall()]
    # One row per query in each table: a duplicate would hide a leak.
    assert len({query for query, _ in traces}) == len(traces), traces
    assert len({query for query, _ in calls}) == len(calls), calls
    return dict(traces), dict(calls)


@pytest.mark.postgres
def test_concurrent_http_calls_each_log_their_own_question(
    clean_runtime: str, okg_mcp_can_log_in: str, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """req.mcp-call-question-id.request-label, .no-leak, .unchanged, .refused."""
    from tests.substrate.mcp._result_payload import JSON_META, json_client_sessions, payload
    from tests.substrate.mcp.test_concurrent_tool_calls import _open_sessions, _serve_http
    from tests.substrate.mcp.test_result_references_db import _publish, _record, _server

    monkeypatch.setenv("OKG_MCP_TRACE_ENABLED", "1")
    facts: list[Any] = []
    for index, word in enumerate(_WORDS):
        facts += _record(f"doc:{word}", index, f"{word} cladding failed at the weld.")
    _publish(clean_runtime, facts)
    json_client_sessions(monkeypatch)

    def search(word: str) -> dict[str, Any]:
        return {"query": word, "subtype_filter": "document"}

    async def scenario(url: str, mcp: Any) -> dict[str, Any]:
        out: dict[str, Any] = {}
        async with AsyncExitStack() as stack:
            bench_a, bench_b, plain, stdio_like, bad = await _open_sessions(stack, url, [
                {"X-OKG-Question": "bench-A", "X-OKG-Try": "1"},
                {"X-OKG-Question": "bench-B", "X-OKG-Try": "2"},
                None,
                None,
                {"X-OKG-Question": "not one token"},
            ])
            # Two questions at the same moment: both calls are inside the
            # tool together, so a process-wide label would show on both.
            undo = _gate(mcp, "search", 2)
            try:
                out["alpha"], out["bravo"] = await asyncio.gather(
                    bench_a.call_tool("search", search("alpha")),
                    bench_b.call_tool("search", search("bravo")),
                )
            finally:
                undo()
            # No header and no env label: unlabeled, as before.
            out["charlie"] = await plain.call_tool("search", search("charlie"))
            # No header, env label set: today's env behavior is kept ...
            monkeypatch.setenv("OKG_MCP_SESSION_QUESTION", "env-question")
            out["delta"] = await plain.call_tool("search", search("delta"))
            # ... and a request _meta value wins over it.
            out["echo"] = await stdio_like.call_tool(
                "search", search("echo"),
                meta={**JSON_META, "okg/question": "bench-M", "okg/try": 3},
            )
            # An invalid header is refused; the env label must not cover it.
            out["foxtrot"] = await bad.call_tool("search", search("foxtrot"))
        return out

    with _server({"dsn": clean_runtime}, monkeypatch) as mcp:
        with _serve_http(mcp) as url:
            results = asyncio.run(scenario(url, mcp))

    for word in ("alpha", "bravo", "charlie", "delta", "echo"):
        body = payload(results[word])
        assert results[word].isError is False, (word, body)
        assert [str(row["node_id"]).strip() for row in body["results"]] == [f"doc:{word}"]
    refused = payload(results["foxtrot"])
    assert refused["error"]["code"] == "mcp_question_label_invalid", refused
    assert refused["error"]["data"] == {"field": "question", "channel": "header"}

    traces, calls = _search_rows(clean_runtime)
    assert {word: label for word, (label, _err) in traces.items()} == {
        "alpha": "bench-A#try1",
        "bravo": "bench-B#try2",
        "charlie": None,
        "delta": "env-question",
        "echo": "bench-M#try3",
        "foxtrot": None,
    }
    assert traces["foxtrot"][1].startswith("QuestionLabelInvalid:"), traces
    assert all(err is None for word, (_l, err) in traces.items() if word != "foxtrot")
    assert calls == {
        "alpha": ("bench-A", 1, None),
        "bravo": ("bench-B", 2, None),
        "charlie": (None, None, None),
        "delta": ("env-question", None, None),
        "echo": ("bench-M", 3, None),
        "foxtrot": (None, None, "mcp_question_label_invalid"),
    }
