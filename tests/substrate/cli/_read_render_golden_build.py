"""The recorded cases of the read-render golden test, and the runner that
records what a read command prints for each.

`tests/substrate/cli/fixtures/read_render_golden.json` was recorded by
running `python -m tests.substrate.cli._read_render_golden_build <out.json>`
on the code BEFORE the parse/render split (dev 4c28b5a92c0), so it holds
what that code printed: stdout, stderr and the exit code of `main(argv)` over
a stand-in server, and the calls the stand-in server was sent. The test
(`test_read_render.py`) runs the same cases on the current code and requires
the same result, and also requires the parse and render steps alone to give
it for the cases that make one plain call.

Each case: `name`, `kind` (`reply` a plain call and its reply, `parse_refusal`
a command line refused before any call, `failure` the call or the start-up
raised, `driver` the shell layer's flags that print rows), `argv`, and
`reply`: a dict `{text, is_error, extra_blocks}`, a dict `{raises, message}`,
or a function of `(arguments, result_format)` giving one of those.
"""
from __future__ import annotations

import contextlib
import io
import json
import os
import sys
from typing import Any
from unittest import mock

DEPLOYMENT = "golden"


def _page(offset: int, size: int, total: int) -> dict[str, Any]:
    rows = [
        {"node_id": f"doc:{i:03d}", "subtype": "doc"}
        for i in range(offset, min(offset + size, total))
    ]
    page: dict[str, Any] = {
        "status": "ok", "items": rows, "generation_id": 7, "offset": offset,
    }
    if offset + size < total:
        page["next_offset"] = offset + size
    return page


def _pages(arguments: dict[str, Any], fmt: str) -> dict[str, Any]:
    return {"text": json.dumps(_page(arguments.get("offset", 0), 2, 5))}


def _json_reply(payload: Any, **more: Any) -> dict[str, Any]:
    return {"text": json.dumps(payload), **more}


_POINTER = (
    "filter refused invalid_argument\nfix_hint: re-read filter's "
    "inputSchema in tools/list and send a valid value"
)
_RETRY = (
    'retry_call: {"tool": "filter", "mode": "corrected", '
    '"arguments": {"limit": 5}}'
)

CASES: list[dict[str, Any]] = [
    # ── a plain call, served ─────────────────────────────────────────────
    {
        "name": "text_ok", "kind": "reply",
        "argv": ["query", "select 1"],
        "reply": {"text": "query ok · rows=1\nrow 1: 1"},
    },
    {
        "name": "text_ok_ends_with_newline", "kind": "reply",
        "argv": ["inspect", "--deployment", "typed", "--target", "{\"kind\": \"nodes\"}"],
        "reply": {"text": "inspect ok\nbody\n"},
    },
    {
        "name": "text_ok_search_unscoped_note", "kind": "reply",
        "argv": ["search", "words", "--view", "list"],
        "reply": {
            "text": "search ok · 1-1 of 1\nresult 1\n--- other fields\nsearch_profile: null\n",
        },
    },
    {
        "name": "text_ok_pipe_hint", "kind": "reply",
        "argv": ["filter", "--deployment", "typed", "--input", '{"kind":"nodes"}'],
        "reply": {"text": "filter ok · 1-50 of 312 · generation_id=7\n" + "\n".join(
            f"doc:{i}" for i in range(50))},
    },
    {
        "name": "text_ok_extra_block", "kind": "reply",
        "argv": ["query", "select 1"],
        "reply": {"text": "query ok · rows=1", "extra_blocks": 2},
    },
    {
        "name": "json_ok", "kind": "reply",
        "argv": ["--json", "filter", "--input", '{"kind":"nodes"}'],
        "reply": _json_reply({"status": "ok", "items": [], "generation_id": 7}),
    },
    {
        "name": "json_ok_extra_block", "kind": "reply",
        "argv": ["--json", "query", "select 1"],
        "reply": _json_reply({"status": "ok", "rows": []}, extra_blocks=1),
    },
    # ── a plain call, refused by the tool ────────────────────────────────
    {
        "name": "text_refused_with_retry", "kind": "reply",
        "argv": ["filter", "--deployment", "typed", "--input", '{"kind":"nodes"}'],
        "reply": {
            "text": "filter refused unknown_argument\nmessage: no such argument\n" + _RETRY,
            "is_error": True,
        },
    },
    {
        "name": "text_refused_retry_without_typed_deployment", "kind": "reply",
        "argv": ["filter", "--input", '{"kind":"nodes"}'],
        "reply": {
            "text": "filter refused unknown_argument\nmessage: no such argument\n" + _RETRY,
            "is_error": True,
        },
    },
    {
        "name": "text_refused_schema_pointer", "kind": "reply",
        "argv": ["filter", "--input", '{"kind":"nodes"}'],
        "reply": {
            "text": _POINTER,
            "is_error": True,
        },
    },
    {
        "name": "text_refused_not_sql_hint", "kind": "reply",
        "argv": ["query", "recent pacts"],
        "reply": {"text": "query refused query_not_select\nmessage: not a SELECT", "is_error": True},
    },
    {
        "name": "text_refused_search_with_hint_only_for_query", "kind": "reply",
        "argv": ["search", "words", "--view", "list"],
        "reply": {"text": "search refused bad_view\nmessage: no", "is_error": True},
    },
    {
        "name": "text_server_fault_exit_1", "kind": "reply",
        "argv": ["query", "select 1"],
        "reply": {
            "text": "query error operator_execution_failed\nmessage: boom",
            "is_error": True,
        },
    },
    {
        "name": "json_refused_by_flag", "kind": "reply",
        "argv": ["--json", "filter", "--input", '{"kind":"nodes"}'],
        "reply": _json_reply(
            {"status": "refused", "error": {"code": "unknown_argument", "message": "m"}},
        ),
    },
    {
        "name": "json_refused_is_error_with_pointer", "kind": "reply",
        "argv": ["--json", "filter", "--input", '{"kind":"nodes"}'],
        "reply": _json_reply(
            {"error": {"code": "invalid", "message": "re-read filter's inputSchema in tools/list"}},
            is_error=True,
        ),
    },
    {
        "name": "json_server_fault_exit_1", "kind": "reply",
        "argv": ["--json", "query", "select 1"],
        "reply": _json_reply(
            {"status": "refused", "error": {"code": "operator_execution_failed", "message": "boom"}},
            is_error=True,
        ),
    },
    # ── the command line refused before any call ────────────────────────
    {
        "name": "parse_refusal_args_twice_text", "kind": "parse_refusal",
        "argv": ["filter", "--args", '{"limit": 1}', "--limit", "2"],
        "reply": {"text": "never called"},
    },
    {
        "name": "parse_refusal_args_twice_json", "kind": "parse_refusal",
        "argv": ["--json", "filter", "--args", '{"limit": 1}', "--limit", "2"],
        "reply": {"text": "never called"},
    },
    {
        "name": "parse_refusal_args_not_object", "kind": "parse_refusal",
        "argv": ["inspect", "--args", "[1]"],
        "reply": {"text": "never called"},
    },
    {
        "name": "parse_refusal_ids_and_jsonl", "kind": "parse_refusal",
        "argv": ["filter", "--input", '{"kind":"nodes"}', "--ids", "--jsonl"],
        "reply": {"text": "never called"},
    },
    {
        "name": "parse_refusal_json_and_ids", "kind": "parse_refusal",
        "argv": ["--json", "filter", "--input", '{"kind":"nodes"}', "--ids"],
        "reply": {"text": "never called"},
    },
    {
        "name": "parse_refusal_search_query_twice", "kind": "parse_refusal",
        "argv": ["search", "one", "--query", "two", "--view", "list"],
        "reply": {"text": "never called"},
    },
    # ── the call or the start-up raised ─────────────────────────────────
    {
        "name": "failure_runtime_text", "kind": "failure",
        "argv": ["query", "select 1"],
        "reply": {"raises": "runtime", "message": "boom in /home/someone/okg/file.py line 3"},
    },
    {
        "name": "failure_runtime_json", "kind": "failure",
        "argv": ["--json", "query", "select 1"],
        "reply": {"raises": "runtime", "message": "boom"},
    },
    {
        "name": "failure_shape_mismatch", "kind": "failure",
        "argv": ["query", "select 1"],
        "reply": {
            "raises": "runtime",
            "message": "Invalid structured content returned by tool query: rows is not a list",
        },
    },
    {
        "name": "failure_substrate_error_text", "kind": "failure",
        "argv": ["query", "select 1"],
        "reply": {"raises": "substrate", "message": "the pool is closed"},
    },
    {
        "name": "failure_substrate_error_json", "kind": "failure",
        "argv": ["--json", "query", "select 1"],
        "reply": {"raises": "substrate", "message": "the pool is closed"},
    },
    {
        "name": "failure_budget_text", "kind": "failure",
        "argv": ["query", "select 1"],
        "reply": {"raises": "budget", "message": "no room for two connections"},
    },
    {
        "name": "failure_budget_json", "kind": "failure",
        "argv": ["--json", "query", "select 1"],
        "reply": {"raises": "budget", "message": "no room for two connections"},
    },
    {
        "name": "failure_group_of_two", "kind": "failure",
        "argv": ["query", "select 1"],
        "reply": {"raises": "group", "message": "first"},
    },
    {
        "name": "failure_at_build_text", "kind": "failure",
        "argv": ["query", "select 1"],
        "build": {"raises": "substrate", "message": "the database is behind this code"},
        "reply": {"text": "never called"},
    },
    {
        "name": "failure_at_build_json", "kind": "failure",
        "argv": ["--json", "query", "select 1"],
        "build": {"raises": "substrate", "message": "the database is behind this code"},
        "reply": {"text": "never called"},
    },
    # ── the shell layer's flags (rows printed by the driver) ────────────
    {
        "name": "driver_jsonl_one_page", "kind": "driver",
        "argv": ["filter", "--input", '{"kind":"nodes"}', "--jsonl"],
        "reply": _json_reply(_page(0, 2, 2)),
    },
    {
        "name": "driver_ids_one_page", "kind": "driver",
        "argv": ["filter", "--input", '{"kind":"nodes"}', "--ids"],
        "reply": _json_reply(_page(0, 2, 2)),
    },
    {
        "name": "driver_all_follows_pages", "kind": "driver",
        "argv": ["filter", "--input", '{"kind":"nodes"}', "--limit", "2", "--all"],
        "reply": _pages,
    },
    {
        "name": "driver_all_json_joins_pages", "kind": "driver",
        "argv": ["--json", "filter", "--input", '{"kind":"nodes"}', "--limit", "2", "--all"],
        "reply": _pages,
    },
    {
        "name": "driver_where_text", "kind": "driver",
        "argv": ["filter", "--input", '{"kind":"nodes"}', "--where", "kind=doc"],
        "reply": {"text": "filter ok · 1-1 of 1\ndoc:1"},
    },
    {
        "name": "driver_jsonl_refused", "kind": "driver",
        "argv": ["filter", "--input", '{"kind":"nodes"}', "--jsonl"],
        "reply": _json_reply(
            {"status": "refused", "error": {"code": "unknown_argument", "message": "m"}},
            is_error=True,
        ),
    },
    {
        "name": "driver_all_text_refused_with_retry", "kind": "driver",
        "argv": ["filter", "--input", '{"kind":"nodes"}', "--deployment", "typed", "--where", "kind=doc"],
        "reply": {
            "text": "filter refused unknown_argument\nmessage: no such argument\n" + _RETRY,
            "is_error": True,
        },
    },
    # ── expand, aggregate and map (recorded on dev 4c28b5a, before the split) ──
    {
        "name": "expand_text_ok", "kind": "reply",
        "argv": ["expand", "--args", '{"target": {"kind": "node", "node_id": "doc:1"}}'],
        "reply": {"text": "expand ok · 1-2 of 2\nlink 1\nlink 2"},
    },
    {
        "name": "expand_json_ok", "kind": "reply",
        "argv": ["--json", "expand", "--args", '{"target": {"kind": "node", "node_id": "doc:1"}}'],
        "reply": _json_reply({"status": "ok", "neighbors": [], "generation_id": 7}),
    },
    {
        "name": "expand_text_refused_with_retry", "kind": "reply",
        "argv": ["expand", "--deployment", "typed", "--args", '{"target": {"kind": "node", "node_id": "doc:1"}}'],
        "reply": {
            "text": "expand refused unknown_argument\nmessage: no such argument\n"
            + _RETRY.replace('"filter"', '"expand"'),
            "is_error": True,
        },
    },
    {
        "name": "aggregate_text_ok", "kind": "reply",
        "argv": ["aggregate", "--args", '{"group_by": "subtype"}'],
        "reply": {"text": "aggregate ok · 2 groups\ndoc: 3\nfile: 2\n"},
    },
    {
        "name": "aggregate_json_ok", "kind": "reply",
        "argv": ["--json", "aggregate", "--args", '{"group_by": "subtype"}'],
        "reply": _json_reply({"status": "ok", "groups": [{"key": "doc", "count": 3}]}),
    },
    {
        "name": "aggregate_text_refused_schema_pointer", "kind": "reply",
        "argv": ["aggregate", "--args", '{"group_by": "subtype"}'],
        "reply": {
            "text": _POINTER.replace("filter", "aggregate"),
            "is_error": True,
        },
    },
    {
        "name": "aggregate_json_server_fault", "kind": "reply",
        "argv": ["--json", "aggregate", "--args", '{"group_by": "subtype"}'],
        "reply": _json_reply(
            {"status": "refused", "error": {"code": "operator_execution_failed", "message": "boom"}},
            is_error=True,
        ),
    },
    {
        "name": "map_text_ok", "kind": "reply",
        "argv": ["map", "--args", '{"input": {"kind": "nodes"}}'],
        "reply": {"text": "map ok · 1-2 of 2\ndoc:1\ndoc:2"},
    },
    {
        "name": "map_json_ok", "kind": "reply",
        "argv": ["--json", "map", "--args", '{"input": {"kind": "nodes"}}'],
        "reply": _json_reply({"status": "ok", "items": [], "generation_id": 7}),
    },
    {
        "name": "map_text_refused_with_retry", "kind": "reply",
        "argv": ["map", "--deployment", "typed", "--args", '{"input": {"kind": "nodes"}}'],
        "reply": {
            "text": "map refused unknown_argument\nmessage: no such argument\n"
            + _RETRY.replace('"filter"', '"map"'),
            "is_error": True,
        },
    },
    {
        "name": "map_driver_ids_one_page", "kind": "driver",
        "argv": ["map", "--args", '{"input": {"kind": "nodes"}}', "--ids"],
        "reply": _json_reply(_page(0, 2, 2)),
    },
    {
        "name": "aggregate_driver_jsonl_refused", "kind": "driver",
        "argv": ["aggregate", "--args", '{"group_by": "subtype"}', "--jsonl"],
        "reply": _json_reply(
            {"status": "refused", "error": {"code": "unknown_argument", "message": "m"}},
            is_error=True,
        ),
    },
]


class _Block:
    def __init__(self, type_: str, text: str = "") -> None:
        self.type = type_
        self.text = text


class Result:
    """What the in-memory MCP client gives back, as far as the commands read it."""

    def __init__(self, spec: dict[str, Any]) -> None:
        self.content = [_Block("text", spec["text"])] + [
            _Block("image") for _ in range(int(spec.get("extra_blocks", 0)))
        ]
        self.isError = bool(spec.get("is_error", False))


def _raised(spec: dict[str, Any]) -> BaseException:
    kind, message = spec["raises"], spec["message"]
    if kind == "runtime":
        return RuntimeError(message)
    if kind == "substrate":
        from okg.substrate.errors import SubstrateError

        err = SubstrateError(message, data={"where": "golden"}, fix_hint="try the other thing")
        err.code = "golden_failure"
        return err
    if kind == "budget":
        from okg.substrate.db.concurrency_budget import BudgetError

        return BudgetError(message)
    if kind == "group":
        return BaseExceptionGroup(
            "unhandled errors in a TaskGroup",
            [RuntimeError(message), RuntimeError("second")],
        )
    raise ValueError(kind)


def reply_spec(case: dict[str, Any], arguments: dict[str, Any], fmt: str) -> dict[str, Any]:
    spec = case["reply"]
    return spec(arguments, fmt) if callable(spec) else spec


def run_case(case: dict[str, Any]) -> dict[str, Any]:
    """`main(argv)` over a stand-in server: what it printed, its exit code,
    and the calls it sent."""
    from okg.substrate.cli import _read_operators as ops
    from okg.substrate.cli.main import main

    sent: list[list[Any]] = []

    def build(args: Any, **kwargs: Any) -> Any:
        if "build" in case:
            raise _raised(case["build"])
        return object()

    def call(server: Any, operator: str, arguments: dict[str, Any], *, result_format: str) -> Any:
        sent.append([arguments, result_format])
        spec = reply_spec(case, arguments, result_format)
        if "raises" in spec:
            raise _raised(spec)
        return Result(spec)

    out, err = io.StringIO(), io.StringIO()
    env = {"OKG_DEPLOYMENT": DEPLOYMENT}
    with contextlib.ExitStack() as stack:
        stack.enter_context(mock.patch.dict(os.environ, env))
        for name in ("OKG_READ_DEBUG", "OKG_AGENT"):
            os.environ.pop(name, None)
        stack.enter_context(mock.patch.object(ops, "build_read_server", build))
        stack.enter_context(mock.patch.object(ops, "call_operator", call))
        stack.enter_context(mock.patch.object(ops, "close_server", lambda server: None))
        stack.enter_context(contextlib.redirect_stdout(out))
        stack.enter_context(contextlib.redirect_stderr(err))
        code = main(list(case["argv"]))
    return {"stdout": out.getvalue(), "stderr": err.getvalue(), "exit": code, "sent": sent}


def main_record(path: str) -> None:
    recorded = {case["name"]: run_case(case) for case in CASES}
    with open(path, "w", encoding="utf-8") as handle:
        json.dump(recorded, handle, indent=1, sort_keys=True, ensure_ascii=False)
        handle.write("\n")
    print(f"recorded {len(recorded)} cases to {path}", file=sys.stderr)


if __name__ == "__main__":
    main_record(sys.argv[1])
