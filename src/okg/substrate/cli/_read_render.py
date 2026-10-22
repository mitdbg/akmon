"""The render step of the read commands: a tool's reply to stdout, stderr and
an exit code.

`_read_call.parse_read_call` turns a parsed command line into the tool name,
its argument object and the output choices. The transport then gets a reply:
today an in-memory MCP client (`_read_operators.call_operator`), later a call
sent to a running server. This module turns that reply, and every refusal or
failure the command makes itself, into what the command prints. It does not
touch the database and never imports the server module
(`okg.substrate.mcp.server`), so a client that only forwards a call can use it
as it is. A test pins both.

The one JSON error shape lives here (`error_document`):

* a refusal, the call as typed or as the tool refused it (exit 2):
  `{"status": "refused", "error": {"code", "message", "fix_hint"?, "data",
  "retry_call"?}}`, written on stderr (`render_refusal`);
* a failure, the command could not finish (exit 1): the same `error` object
  without `status` (`render_failure`).

A reply the TOOL refused is printed as the tool gave it (`render_reply`).
"""
from __future__ import annotations

import argparse
import json
import re
import sys
from dataclasses import dataclass
from typing import Any, Optional

#: The exit codes of a read command: served, an error, a refusal.
OK_EXIT = 0
FAILED_EXIT = 1
REFUSED_EXIT = 2

#: The code the MCP server gives an operator's untyped exception.
SERVER_FAULT_CODE = "operator_execution_failed"

#: How a refusal's fix hint sends an MCP client to the tool's input schema
#: (`response_bounds.py`: an undeclared argument, an invalid one). A shell
#: has no tools/list to re-read; `okg <tool> --help` prints the same schema
#: as one line per argument.
_SCHEMA_POINTER = re.compile(r"re-read (\w+)'s inputSchema in tools/list")


# ── what comes back, what goes out ───────────────────────────────────────


@dataclass(frozen=True)
class Reply:
    """A tool's reply, as the render step reads it.

    `text` is the first text block of the MCP `CallToolResult` content;
    `other_blocks` counts every further block (an image, a second text);
    `is_error` is the result's `isError`. `structured` is the result's
    structured content when the caller has one; the commands do not print it.
    """

    text: str
    is_error: bool = False
    other_blocks: int = 0
    structured: Any = None


def reply_of(result: Any) -> Reply:
    """The `Reply` of an MCP `CallToolResult` (or anything shaped like one:
    `content` blocks with a `type` and `text`, and `isError`)."""
    content = list(getattr(result, "content", None) or [])
    texts = [block.text for block in content if getattr(block, "type", None) == "text"]
    return Reply(
        text=texts[0] if texts else "",
        is_error=bool(getattr(result, "isError", False)),
        other_blocks=len(content) - min(len(texts), 1),
        structured=getattr(result, "structuredContent", None),
    )


@dataclass(frozen=True)
class Rendered:
    """What a command prints, and the exit code it ends with."""

    stdout: str = ""
    stderr: str = ""
    exit_code: int = OK_EXIT

    def write(self) -> int:
        """Write both streams, return the exit code."""
        if self.stdout:
            sys.stdout.write(self.stdout)
        if self.stderr:
            sys.stderr.write(self.stderr)
        return self.exit_code


def _masking() -> Any:
    # Late import: `_read_operators` imports this module at the top.
    from okg.substrate.cli import _read_operators

    return _read_operators


# ── the reply helpers ────────────────────────────────────────────────────


def text_is_refused(is_error: bool, text: str, *, as_json: bool) -> bool:
    if is_error:
        return True
    if not as_json:
        return False
    try:
        payload = json.loads(text)
    except ValueError:
        return False
    return isinstance(payload, dict) and (
        payload.get("status") == "refused" or isinstance(payload.get("error"), dict)
    )


def reply_error_code(text: str, *, as_json: bool) -> Any:
    """The error code a refused reply states, or None.

    The JSON reply's `error.code`; for a text reply, the code on its status
    line (`<tool> refused <code>`), read with the renderer's own parser. The
    code is looked for nowhere else in the reply.
    """
    if as_json:
        try:
            payload = json.loads(text)
        except ValueError:
            return None
        error = payload.get("error") if isinstance(payload, dict) else None
        return error.get("code") if isinstance(error, dict) else None
    from okg.substrate.mcp.result_text import parse_status_line

    status = parse_status_line(text)
    if status is None or status.status not in ("refused", "error"):
        return None
    return status.code


def shell_refusal_text(text: str) -> str:
    """A refused reply's text with each pointer to a tool's input schema
    replaced by the command that prints it. Nothing else changes, in the
    text or in the JSON form."""
    return _SCHEMA_POINTER.sub(lambda match: f"run `okg {match.group(1)} --help`", text)


def render_printed(reply: Reply, *, as_json: bool) -> Rendered:
    """The reply uncut. Exit 0 when served; 2, on stderr, when refused; 1
    when the tool raised something it does not name.

    A refusal is printed as the tool gave it, except that a pointer to the
    tool's input schema names `okg <tool> --help` (`shell_refusal_text`).
    """
    text = reply.text
    refused = text_is_refused(reply.is_error, text, as_json=as_json)
    if refused:
        text = shell_refusal_text(text)
    body = text if text.endswith("\n") else text + "\n"
    note = ""
    if reply.other_blocks:
        note = (
            f"note: {reply.other_blocks} more content block(s) of this reply "
            "(an image, or a further block) were not printed\n"
        )
    code = OK_EXIT
    if refused:
        # The tool raised something it does not name: a fault the caller
        # cannot fix by changing the call, so not a refusal.
        code = (
            FAILED_EXIT
            if reply_error_code(text, as_json=as_json) == SERVER_FAULT_CODE
            else REFUSED_EXIT
        )
    if refused:
        return Rendered(stderr=body + note, exit_code=code)
    return Rendered(stdout=body, stderr=note, exit_code=code)


def render_reply(
    operator: str,
    reply: Reply,
    *,
    as_json: bool,
    arguments: dict[str, Any],
    args: argparse.Namespace,
    refusal_hint: Optional[str] = None,
) -> Rendered:
    """What a read command prints for one served reply of the plain form
    (text or `--json`): the reply, then on stderr the lines after it.

    * a served text reply: the search note when no profile scoped the page,
      and, after a page of more than a few rows, the pipe hint;
    * a refused text reply: `retry: okg ...` when the refusal carries a
      corrected call, then `refusal_hint` (one masked line, for a refusal
      of the tool only).

    `arguments` is the argument object that was sent, `args` the parsed
    command line (both name the call a retry or a hint repeats).
    """
    from okg.substrate.cli import _read_shell

    text = reply.text
    shown = reply
    if not as_json:
        noted = _read_shell.with_unscoped_note(operator, text)
        if noted != text:
            shown = Reply(
                noted, reply.is_error, reply.other_blocks, reply.structured,
            )
    printed = render_printed(shown, as_json=as_json)
    stderr = printed.stderr
    code = printed.exit_code
    if code == OK_EXIT and not as_json:
        hint = _read_shell.pipe_hint(operator, text, args)
        if hint is not None:
            # Not masked: the caller's own call, to be run as printed.
            stderr += hint + "\n"
    if code == REFUSED_EXIT and not as_json:
        line = _read_shell.retry_line(
            _read_shell.retry_call_in_text(text), arguments, args,
        )
        if line is not None:
            # Not masked: the caller's own call, to be run as printed.
            stderr += line + "\n"
    if code == REFUSED_EXIT and refusal_hint and not as_json:
        stderr += _masking().one_line(refusal_hint) + "\n"
    return Rendered(stdout=printed.stdout, stderr=stderr, exit_code=code)


# ── the error shape ──────────────────────────────────────────────────────


def error_document(
    code: Any,
    message: str,
    *,
    fix_hint: Optional[str] = None,
    data: Optional[dict[str, Any]] = None,
    retry_call: Optional[dict[str, Any]] = None,
    status: Optional[str] = None,
) -> dict[str, Any]:
    """The one JSON error document of a read command (not yet masked).

    `status="refused"` for a refusal, none for a failure. `retry_call` is
    `{"tool", "mode", "arguments"}` of a whole call that is served as
    written.
    """
    error: dict[str, Any] = {
        "code": code,
        "message": message,
        **({"fix_hint": fix_hint} if fix_hint else {}),
        "data": data if data is not None else {},
    }
    if retry_call is not None:
        error["retry_call"] = retry_call
    return {"status": status, "error": error} if status else {"error": error}


def render_refusal(
    exc: Any, operator: str, args: argparse.Namespace, *, mode: str,
) -> Rendered:
    """A read command refused before any call, or a client refusing for its
    own reason (no server, a version mismatch): exit 2, the call as typed
    is the fault, not an error.

    `exc` is a `SubstrateError` (`code`, `message`, `fix_hint`, `data`; a
    `ShellRefusal` also carries `retry`, `retry_mode`, `retry_command`). The
    shape is a tool refusal's: with `mode == "json"` one JSON document
    (`error_document`, status refused) on stderr; otherwise `<op> refused
    <code>: ...`, the fix, the data and the `retry:` line.
    """
    from okg.substrate.cli import _read_shell

    ops = _masking()
    retry = getattr(exc, "retry", None)
    retry_call = (
        {
            "tool": retry[0], "mode": getattr(exc, "retry_mode", "corrected"),
            "arguments": retry[1],
        }
        if retry is not None else None
    )
    if mode == "json":
        document = error_document(
            exc.code, exc.message, fix_hint=exc.fix_hint, data=exc.data,
            retry_call=retry_call, status="refused",
        )
        return Rendered(
            stderr=json.dumps(ops.mask_value(document)) + "\n", exit_code=REFUSED_EXIT,
        )
    lines = [ops.one_line(f"{operator} refused {exc.code}: {exc.message}")]
    if exc.fix_hint:
        lines.append(ops.mask(f"  fix_hint: {exc.fix_hint}"))
    if exc.data:
        lines.append(ops.mask(f"  data: {exc.data}"))
    command = getattr(exc, "retry_command", None)
    line = (
        "retry: " + command if command
        else _read_shell.retry_line(retry_call, {}, args)
    )
    if line is not None:
        # Not masked: it is the caller's own call, to be run as printed.
        lines.append(line)
    return Rendered(stderr="".join(text + "\n" for text in lines), exit_code=REFUSED_EXIT)


def render_failure(err: Any, *, as_json: bool) -> Rendered:
    """A read command that could not finish: exit 1. One masked JSON
    document (`error_document`, no status) with `--json`; otherwise `ERROR
    [code]: message`, the fix hint and the data."""
    ops = _masking()
    if as_json:
        document = error_document(
            err.code, err.message, fix_hint=err.fix_hint, data=err.data,
        )
        return Rendered(
            stderr=json.dumps(ops.mask_value(document)) + "\n", exit_code=FAILED_EXIT,
        )
    fix = f"\n  fix_hint: {err.fix_hint}" if err.fix_hint else ""
    text = ops.mask(f"ERROR [{err.code}]: {err.message}{fix}") + "\n"
    if err.data:
        text += ops.mask(f"  data: {err.data}") + "\n"
    return Rendered(stderr=text, exit_code=FAILED_EXIT)


def render_busy(err: Any, line: str, *, as_json: bool) -> Rendered:
    """The host-wide connection ledger has no room (`BudgetError`): nothing
    was opened and the caller can try again. Exit 2. `line` is the one
    line, which with `--json` is the message of the failure document `err`."""
    if as_json:
        err.message = line
        document = error_document(
            err.code, err.message, fix_hint=err.fix_hint, data=err.data,
        )
        return Rendered(
            stderr=json.dumps(_masking().mask_value(document)) + "\n",
            exit_code=REFUSED_EXIT,
        )
    return Rendered(stderr=line + "\n", exit_code=REFUSED_EXIT)
