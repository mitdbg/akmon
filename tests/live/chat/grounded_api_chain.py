"""The pure, import-safe core of the grounded OpenWebUI API proof.

Everything here is standard library only and has no import side effects: no
server, no Docker call, no ``sys.path`` mutation. The live driver and the
hermetic unit test both import this module, so a defect in the driver can
never turn into a pytest collection error, and importing the contract can
never leak a path entry into a test session.

Vocabulary: the "causal chain" is the sequence this proof exists to
demonstrate. OpenWebUI receives one user turn, the model asks for the
``inspect`` tool, OpenWebUI calls the MCP server, the MCP server answers with
the generation, node, and source strings, OpenWebUI hands that answer back to
the model, and OpenWebUI persists the model's grounded reply. Each link is
checked against what was recorded, not against what the driver hoped for.
"""

from __future__ import annotations

import ast
import json
import re
import socket
from datetime import datetime
from pathlib import Path
from typing import Any

#: The evidence strings. Only the MCP fixture knows them; the driver imports
#: them to check the chain and must never write them into a request (see
#: :func:`driver_literal_findings`).
GENERATION = "okg:fixture:generation:7"
NODE_ID = "okg:fixture:node:alpha"
SOURCE_REF = "fixture://source/alpha"
MODEL_ID = "okg-grounded-fixture-model"
TOOL_NAME = "inspect"

#: The tool-server id the driver registers. OpenWebUI prefixes the tool name
#: with it, so the name the vendor persists is ``SERVER_ID_TOOL_NAME`` exactly.
SERVER_ID = "okg-grounded-fixture"
PERSISTED_TOOL_NAME = f"{SERVER_ID}_{TOOL_NAME}"


def call_id_for(session_id: str) -> str:
    """The tool-call id for one run.

    Per-run rather than a constant, so the function_call and
    function_call_output that OpenWebUI persists are pinned to THIS turn and
    a record from an earlier turn cannot pair with this one.
    """
    return f"fixture-call-{session_id}"

#: OpenWebUI writes a finished assistant turn through this endpoint. A driver
#: that posted to it could invent the whole answer, so the proof forbids it.
FORBIDDEN_WRITE_ENDPOINT = "/api/chat/completed"


class Refusal(RuntimeError):
    """The run cannot proceed honestly; say why rather than degrade."""


class ChainError(AssertionError):
    """A recorded link of the causal chain is missing, wrong, or refused."""


#: Shapes that must never reach a committed artifact. Checked after redaction
#: over every file written, so a leak fails the run instead of the review.
SECRET_SHAPES = (
    re.compile(r"eyJ[A-Za-z0-9_-]{8,}"),
    re.compile(r"\bsk-[A-Za-z0-9_-]{8,}"),
    re.compile(r"Bearer\s+\S{8,}"),
)
class Scrubber:
    """Replace run-generated credentials and local paths with placeholders.

    The driver creates a throwaway admin account and receives a session token
    from the container. Those values, and the absolute paths of this machine,
    are rewritten before anything is written to disk. Nothing is read from an
    operator ``.env``, so there is no durable credential to leak; this guards
    the ones this process itself minted.
    """

    def __init__(self) -> None:
        self._replacements: list[tuple[str, str]] = []

    def hide(self, value: str, placeholder: str) -> None:
        if value and len(value) >= 4:
            self._replacements.append((value, placeholder))

    def __call__(self, text: str) -> str:
        for value, placeholder in sorted(
            self._replacements, key=lambda pair: len(pair[0]), reverse=True
        ):
            text = text.replace(value, placeholder)
        return text

    def write(self, path: Path, text: str) -> None:
        path.write_text(self(text), encoding="utf-8")

    def rewrite(self, path: Path) -> None:
        if path.exists():
            path.write_text(self(path.read_text("utf-8")), encoding="utf-8")


def assert_no_secret_shapes(out: Path) -> list[str]:
    """Fail the run if a credential shape survived redaction."""
    checked: list[str] = []
    for path in sorted(out.rglob("*")):
        if not path.is_file():
            continue
        text = path.read_text("utf-8", errors="replace")
        for shape in SECRET_SHAPES:
            found = shape.search(text)
            if found:
                raise Refusal(
                    f"{path.name} still carries a credential shape "
                    f"({shape.pattern}) after redaction"
                )
        checked.append(path.relative_to(out).as_posix())
    return checked


#: The name the container uses for this host (model and MCP URLs), and the
#: flag that defines it. See docker_run_command.
HOST_GATEWAY_ALIAS = "host.docker.internal"
HOST_GATEWAY_ARGS = ("--add-host", f"{HOST_GATEWAY_ALIAS}:host-gateway")


def docker_run_command(
    *, image: str, container: str, label: str, web_port: int
) -> list[str]:
    """Every flag the container is started with, in one place.

    The manifest says the container is configured through its own HTTP API
    apart from having authentication on. That sentence is only checkable if
    the whole command is recorded, so this function IS the record -- run.json
    stores exactly what it returns. WEBUI_AUTH=True is the vendor default and
    is set explicitly so the run does not depend on the image default holding.
    No volume, no mount, no bind of a repository file, no patched layer.

    ``--add-host`` defines HOST_GATEWAY_ALIAS, the name the site's model and
    tool URLs use to reach this host. Podman defines it by itself; plain
    Docker on Linux does not, so without the flag a Docker run cannot reach
    the fixture model or the MCP server. It mirrors ``okg chat-instance up``
    (okg.substrate.chat.instance), which passes the same flag for a bridge
    network. Both runtimes accept ``host-gateway``.
    """
    return [
        "docker",
        "run",
        "-d",
        "--name",
        container,
        "--label",
        label,
        "-p",
        f"127.0.0.1:{web_port}:8080",
        *HOST_GATEWAY_ARGS,
        "-e",
        "WEBUI_AUTH=True",
        image,
    ]


def digest_of(image: str) -> str:
    """The ``sha256:...`` an ``@sha256:`` reference names, or a refusal.

    The drivers default ``--expected-digest`` from the image reference when it
    is a digest reference, so the product pin needs one flag, not two that
    could disagree. A tag reference names no digest and must be given one.
    """
    _, sep, digest = image.partition("@")
    if not sep or not re.fullmatch(r"sha256:[0-9a-f]{64}", digest):
        raise Refusal(
            f"--image {image!r} is not an @sha256: reference, so it names no "
            "registry digest; pass --expected-digest"
        )
    return digest


def parse_port_range(text: str) -> tuple[int, int]:
    """``"5600-5699"`` -> ``(5600, 5699)``, or a refusal naming the form.

    A shared host hands each run a block of loopback ports it may use, so a
    driver must be able to stay inside one instead of taking whatever the
    kernel offers.
    """
    low, sep, high = (text or "").partition("-")
    try:
        first, last = int(low), int(high)
    except ValueError:
        first = last = 0
    if not sep or not 1024 <= first <= last <= 65535:
        raise Refusal(
            f"--port-range {text!r} is not LOW-HIGH with 1024 <= LOW <= HIGH <= 65535"
        )
    return first, last


def free_port(
    port_range: tuple[int, int] | None = None, *, taken: tuple[int, ...] = ()
) -> int:
    """A loopback port nothing is bound to, inside ``port_range`` when given.

    Without a range the kernel picks, as before. With one, the first port in
    it that binds and is not in ``taken`` wins, and a range with none free
    refuses rather than falling back to a port outside it.
    """
    if port_range is None:
        with socket.socket() as sock:
            sock.bind(("127.0.0.1", 0))
            return int(sock.getsockname()[1])
    first, last = port_range
    for port in range(first, last + 1):
        if port in taken:
            continue
        with socket.socket() as sock:
            try:
                sock.bind(("0.0.0.0", port))  # noqa: S104 - a probe, closed at once
            except OSError:
                continue
            return port
    raise Refusal(f"no free port in {first}-{last} (skipped {sorted(taken)})")


def poll_until(
    fetch: Any,
    accept: Any,
    *,
    timeout: float,
    on_timeout: Any,
    clock: Any,
    sleep: Any,
    interval: float = 0.25,
) -> Any:
    """Poll ``fetch`` until ``accept`` holds, or raise what ``on_timeout`` says.

    The clock and the sleep are injected for one reason -- the deadline arms
    are part of the contract ("a timeout must fail") and a deadline that only
    exists inside a live driver is a claim no test can check. With a fake
    clock these arms are ordinary unit tests.

    ``accept`` is consulted after every fetch INCLUDING the one that runs at
    or past the deadline, so a result arriving in the last instant is taken
    rather than thrown away.

    An exception from ``fetch`` PROPAGATES unchanged. None of the three live
    callers wants it swallowed: the health probe already reports a failure by
    returning its message, ``read_jsonl`` already tolerates the one transient
    it can meet (a half-written final line), and an HTTP failure reading the
    chat back is a real refusal that must surface with its own message rather
    than be retried until a deadline hides it. A tolerate-list would be a
    silent-retry switch with no caller, so there is not one.
    """
    deadline = clock() + timeout
    while True:
        latest = fetch()
        if accept(latest):
            return latest
        if clock() >= deadline:
            raise on_timeout(latest)
        sleep(interval)


#: How much of a recorded log is kept. A whole log is mostly model download
#: and migration chatter; the tail is where a refused tool call would appear.
LOG_LINE_BUDGET = 400
LOG_CHAR_BUDGET = 200_000


def trim_recorded_log(
    text: str, *, lines: int = LOG_LINE_BUDGET, chars: int = LOG_CHAR_BUDGET
) -> str:
    """Keep the tail of a recorded log, declared normalisation and all.

    Used for both logs this proof records: the container's, and the model and
    MCP fixture processes'. Two changes are made to the recorded bytes and
    both are deliberate. Only the last ``lines`` lines are kept, bounded again
    by ``chars``, because the head is startup noise. Trailing whitespace is
    stripped from each line, because OpenWebUI pads its table and warning
    output and the fixture's server framework pads its own, and
    ``git diff --check`` reports every such line as a defect in what is really
    a verbatim record. Nothing else is rewritten.
    """
    kept = [line.rstrip() for line in text.splitlines()[-lines:]]
    return "\n".join(kept)[-chars:] + "\n"


#: How much of a server log a refusal carries. The step log is the one record
#: that survives when artifact upload fails (run 36692144664 lost its
#: mcp-serve.log to the storage quota), so the refusal itself must say why the
#: server never answered.
REFUSAL_LOG_TAIL_LINES = 40

#: Credential shapes masked in a refusal tail even when no driver registered
#: the value with the Scrubber: a connection-string password
#: (``scheme://user:SECRET@host``) and the SECRET_SHAPES the artifact check
#: refuses. A tail goes to a CI step log, where a leak cannot be recalled.
UNREGISTERED_CREDENTIAL = "<redacted:unregistered-credential>"
#: Each pattern keeps its first group and masks the rest of the match. The
#: URL password may be empty-user (``://:pw@``) or hold an unencoded ``@``
#: (greedy to the last ``@`` before the host); libpq key/value and query
#: forms, any-case ``bearer``, and ``*TOKEN*``/``*SECRET*``/``*PASSWORD*``
#: environment assignments are masked too.
_TAIL_CREDENTIALS = (
    re.compile(r"(\b[a-z][a-z0-9+.-]*://[^\s:/@]*:)[^\s/]+(?=@)", re.IGNORECASE),
    re.compile(r"([?&;]password=)[^&;\s]+", re.IGNORECASE),
    re.compile(r"(\bpassword\s*=\s*)(?!<redacted:)\S+", re.IGNORECASE),
    re.compile(r"(\bbearer\s+)(?!<redacted:)\S{8,}", re.IGNORECASE),
    re.compile(
        r"(\b[A-Z0-9_]*(?:TOKEN|SECRET|PASSWORD|API_KEY)[A-Z0-9_]*\s*=\s*)"
        r"(?!<redacted:)\S+"
    ),
)
#: A tail is capped by characters as well as lines: one unbroken log line
#: must not flood the step log (or the lifecycle verdict's ``stopped``).
REFUSAL_LOG_TAIL_LINE_CHARS = 500
REFUSAL_LOG_TAIL_CHARS = 16_000


def mask_log_tail(text: str, scrub: Scrubber) -> str:
    """``scrub`` the text, then mask any credential shape that survived."""
    text = scrub(text)
    for pattern in _TAIL_CREDENTIALS:
        text = pattern.sub(rf"\1{UNREGISTERED_CREDENTIAL}", text)
    for shape in SECRET_SHAPES:
        # A placeholder the Scrubber already wrote (``Bearer <redacted:...>``)
        # is kept, so the message still names which registered value it was.
        text = shape.sub(
            lambda found: (
                found.group(0)
                if "<redacted:" in found.group(0)
                else UNREGISTERED_CREDENTIAL
            ),
            text,
        )
    return text


def _cap(text: str, limit: int) -> str:
    """``text`` cut to ``limit`` characters, saying how much was dropped."""
    if len(text) <= limit:
        return text
    return f"{text[:limit]}... [{len(text) - limit} more characters cut]"


def refusal_with_log_tail(
    message: str,
    *,
    log_path: Path,
    scrub: Scrubber,
    process_name: str,
    exit_code: int | None = None,
    lines: int = REFUSAL_LOG_TAIL_LINES,
) -> Refusal:
    """A Refusal that carries the redacted tail of the process's own log.

    A server that died at startup leaves its cause only in its log. The tail
    goes through the drivers' shared Scrubber (the same one every artifact is
    written through) and then :func:`mask_log_tail`, so no DSN, password or
    token reaches the message. A missing, unreadable or empty log is stated
    as such rather than omitted.
    """
    state = "still running" if exit_code is None else f"exited with code {exit_code}"
    header = f"{message}\n{process_name} {state}; "
    try:
        text = log_path.read_text("utf-8", errors="replace")
    except FileNotFoundError:
        return Refusal(header + f"its log {log_path.name} does not exist")
    except OSError as exc:
        return Refusal(
            header + f"its log {log_path.name} could not be read ({type(exc).__name__})"
        )
    tail = [line.rstrip() for line in text.splitlines()[-lines:]]
    if not any(tail):
        return Refusal(header + f"its log {log_path.name} is empty")
    # Mask whole lines first, then cut: a cut must never split a secret and
    # leave a fragment too short for the credential patterns to match.
    masked = mask_log_tail("\n".join(tail), scrub).split("\n")
    body = _cap(
        "\n".join(_cap(line, REFUSAL_LOG_TAIL_LINE_CHARS) for line in masked),
        REFUSAL_LOG_TAIL_CHARS,
    )
    return Refusal(
        header + f"last {len(tail)} line(s) of {log_path.name}, redacted:\n{body}"
    )


def read_jsonl(path: Path) -> list[dict[str, Any]]:
    """Read whole JSONL rows, ignoring a trailing partially written line.

    The fixture appends to this file from another process while the driver
    polls it, so the final line can be short by a byte. A partial LAST line is
    not evidence of anything and is dropped; a malformed EARLIER line is a
    real defect and raises, because a row that was fully written and still
    unparseable means the recorder is broken.
    """
    if not path.exists():
        return []
    lines = [line for line in path.read_text("utf-8").split("\n") if line.strip()]
    rows: list[dict[str, Any]] = []
    for index, line in enumerate(lines):
        try:
            rows.append(json.loads(line))
        except json.JSONDecodeError:
            if index == len(lines) - 1:
                break
            raise
    return rows


def parse_task_acknowledgement(
    payload: Any, *, expected_chat_id: str
) -> dict[str, Any]:
    """Read what ``POST /api/chat/completions`` actually answers with.

    Measured, not assumed: given a ``chat_id`` and ``session_id``, OpenWebUI
    does not stream the turn back to the API caller (v0.11.0, and unchanged on
    the 0.11.4 compatibility image on 2026-09-30). It accepts the
    request, answers with a small JSON acknowledgement naming the background
    task it queued, and delivers the turn over its own event channel while
    persisting the result. So there is no assistant text and no citation
    ``sources`` in this response to inspect -- an earlier version of this
    proof asked the response for ``sources``, which could only ever be
    absent. The response is still worth checking: it must acknowledge the
    work for THIS chat, or nothing that follows is about our turn.
    """
    if not isinstance(payload, dict):
        raise ChainError(
            f"OpenWebUI did not acknowledge the turn in JSON: {payload!r:.200}"
        )
    if payload.get("status") is not True:
        raise ChainError(f"OpenWebUI refused the turn: {payload!r:.400}")
    task_ids = payload.get("task_ids")
    if not isinstance(task_ids, list) or not task_ids:
        raise ChainError(f"acknowledgement queued no background task: {payload!r:.400}")
    if payload.get("chat_id") != expected_chat_id:
        raise ChainError(
            f"acknowledgement names chat {payload.get('chat_id')!r}, "
            f"expected {expected_chat_id!r}"
        )
    return {
        "kind": "task_acknowledgement",
        "task_ids": len(task_ids),
        "chat_id": payload["chat_id"],
        "keys": sorted(payload),
    }


def persisted_chain(stored: dict[str, Any], *, session_id: str) -> dict[str, Any]:
    """Read OpenWebUI's own record of the turn out of the stored message.

    This is the strongest link available, because it is the vendor's account
    of its own work rather than the fixtures'. It persists an ``output`` list
    holding the function call it made, the function call's output, and the
    assistant message it composed from them. Measured on v0.11.0, and again on
    the 0.11.4 compatibility image on 2026-09-30: the same three item types,
    the output still the raw MCP content list with its text block relabelled
    ``input_text``, and each item carrying a ``status``. 0.11.4 marks a
    failed call ``status: failed`` and stores its output as
    ``{"error": ...}``, so a status other than ``completed`` refuses by name
    rather than as a missing value.
    """
    output = stored.get("output")
    if not isinstance(output, list) or not output:
        raise ChainError("stored message has no output record of the tool turn")
    calls = [
        item
        for item in output
        if isinstance(item, dict) and item.get("type") == "function_call"
    ]
    results = [
        item
        for item in output
        if isinstance(item, dict) and item.get("type") == "function_call_output"
    ]
    if len(calls) != 1:
        raise ChainError(f"stored output records {len(calls)} function calls, expected 1")
    if len(results) != 1:
        raise ChainError(
            f"stored output records {len(results)} tool results, expected 1"
        )
    call, result = calls[0], results[0]
    name = str(call.get("name") or "")
    if name != PERSISTED_TOOL_NAME:
        raise ChainError(
            f"stored function call names {name!r}, expected exactly "
            f"{PERSISTED_TOOL_NAME!r}"
        )
    if NODE_ID not in str(call.get("arguments") or ""):
        raise ChainError(
            f"stored function call arguments do not name the node: {call.get('arguments')!r}"
        )
    expected_call_id = call_id_for(session_id)
    if call.get("call_id") != expected_call_id:
        raise ChainError(
            f"stored function call id {call.get('call_id')!r} is not this run's "
            f"{expected_call_id!r}"
        )
    if call.get("call_id") != result.get("call_id"):
        raise ChainError("stored tool result does not belong to the stored call")
    for kind, item in (("function call", call), ("tool result", result)):
        if item.get("status") != "completed":
            raise ChainError(
                f"stored {kind} has status {item.get('status')!r}, not 'completed'; "
                "the vendor recorded the tool call as not answered"
            )
    result_text = json.dumps(result.get("output"), sort_keys=True)
    for value in (GENERATION, NODE_ID, SOURCE_REF):
        if value not in result_text:
            raise ChainError(f"stored tool result lost {value}")
    return {
        "function_call_name": name,
        "function_call_arguments": call.get("arguments"),
        "function_call_id": call.get("call_id"),
        "tool_result_status": result.get("status"),
        "has_sources": bool(stored.get("sources")),
    }


def _tool_result_text(row: dict[str, Any]) -> str:
    return json.dumps(row.get("result"), sort_keys=True)


def check_row_provenance(
    rows: list[dict[str, Any]],
    *,
    kind: str,
    session_id: str,
    window: tuple[datetime, datetime],
) -> None:
    """Tie every recorded row to THIS run.

    Without this the recorder files are indistinguishable between runs -- the
    first two recorded runs of this proof produced byte-identical mcp.jsonl
    and model.jsonl -- so nothing but the driver clearing its output directory
    stopped a leftover row from satisfying the validator. Each row now carries
    the run session id and a UTC timestamp, and both must match.

    The window is CLOSED at both ends: a row stamped exactly at the start or
    exactly at the end belongs to the run. The driver takes the start before
    launching the fixtures and the end after reading the rows, so a row
    written in the same clock tick as either boundary is legitimate and must
    not be refused.

    The session comparison is exact -- whole string, case sensitive. A prefix
    or case-insensitive comparison would let a neighbouring run satisfy it.
    """
    start, end = window
    for row in rows:
        session = row.get("session")
        if session != session_id:
            raise ChainError(
                f"{kind} row carries session {session!r}, not this run {session_id!r}"
            )
        stamp = row.get("at")
        try:
            at = datetime.fromisoformat(str(stamp))
        except (TypeError, ValueError) as exc:
            raise ChainError(f"{kind} row has no readable timestamp {stamp!r}") from exc
        if at.tzinfo is None:
            raise ChainError(f"{kind} row timestamp {stamp!r} names no time zone")
        if not start <= at <= end:
            raise ChainError(
                f"{kind} row recorded at {stamp} falls outside this run window "
                f"{start.isoformat()} .. {end.isoformat()}"
            )


def validate_causal_chain(
    *,
    mcp_rows: list[dict[str, Any]],
    model_rows: list[dict[str, Any]],
    stored: dict[str, Any],
    session_id: str,
    window: tuple[datetime, datetime],
) -> dict[str, Any]:
    """Check every recorded link, or raise :class:`ChainError`.

    Returns the values the record reports, taken from the recording rather
    than from the module constants, so the summary cannot describe a chain
    that did not happen.
    """
    check_row_provenance(
        mcp_rows, kind="MCP", session_id=session_id, window=window
    )
    check_row_provenance(
        model_rows, kind="model", session_id=session_id, window=window
    )
    if len(mcp_rows) != 1:
        raise ChainError(f"expected exactly one MCP call, recorded {len(mcp_rows)}")
    call = mcp_rows[0]
    if call.get("tool") != TOOL_NAME:
        raise ChainError(f"MCP call was {call.get('tool')!r}, expected {TOOL_NAME!r}")
    arguments = call.get("arguments")
    if not isinstance(arguments, dict) or arguments.get("node_id") != NODE_ID:
        raise ChainError(f"MCP call arguments did not name the node: {arguments!r}")
    result_text = _tool_result_text(call)
    for value in (GENERATION, NODE_ID, SOURCE_REF):
        if value not in result_text:
            raise ChainError(f"MCP tool result did not carry {value}")

    bodies = [
        row.get("body") or {}
        for row in model_rows
        if row.get("kind") == "model_request"
    ]
    if len(bodies) < 2:
        raise ChainError(
            f"model received {len(bodies)} request(s); the tool-result "
            "continuation never arrived"
        )
    tool_messages = [
        message
        for body in bodies
        for message in body.get("messages") or []
        if isinstance(message, dict) and message.get("role") == "tool"
    ]
    if not tool_messages:
        raise ChainError("model requests contain no tool-result message")
    handed_back = "\n".join(str(message.get("content") or "") for message in tool_messages)
    for value in (GENERATION, NODE_ID, SOURCE_REF):
        if value not in handed_back:
            raise ChainError(f"OpenWebUI lost {value} before the model continuation")

    if stored.get("error"):
        raise ChainError(f"OpenWebUI stored a refusal as the result: {stored['error']!r}")
    if stored.get("role") != "assistant":
        raise ChainError(
            f"stored result has role {stored.get('role')!r}, expected 'assistant'"
        )
    text = stored.get("content")
    if not isinstance(text, str) or not text.strip():
        raise ChainError("stored assistant answer has no text")
    for value in (GENERATION, NODE_ID, SOURCE_REF):
        if value not in text:
            raise ChainError(f"stored assistant answer lost {value}")
    persisted = persisted_chain(stored, session_id=session_id)

    return {
        "mcp_calls": len(mcp_rows),
        "mcp_tool": call["tool"],
        "mcp_arguments": arguments,
        "mcp_result": call.get("result"),
        "model_requests": len(bodies),
        "tool_messages": len(tool_messages),
        "generation": GENERATION,
        "node_id": NODE_ID,
        "source_reference": SOURCE_REF,
        "stored_answer": text,
        "session_id": session_id,
        "window": [window[0].isoformat(), window[1].isoformat()],
        "persisted": persisted,
    }


def driver_literal_findings(source: str) -> list[str]:
    """Name every way a driver's own source could fabricate the answer.

    Read through Python's parser rather than as substrings: the earlier
    version of this guard matched the two spellings one particular mistake
    happened to use. This one reports any string constant in the module that
    names the endpoint which writes a finished assistant turn, or that
    carries an evidence value the driver must only ever receive.
    """
    findings: list[str] = []
    tree = ast.parse(source)
    for node in ast.walk(tree):
        if not isinstance(node, ast.Constant) or not isinstance(node.value, str):
            continue
        literal = node.value
        if FORBIDDEN_WRITE_ENDPOINT in literal:
            findings.append(
                f"line {node.lineno}: literal names the finished-turn write "
                f"endpoint {FORBIDDEN_WRITE_ENDPOINT}"
            )
        for value in (GENERATION, NODE_ID, SOURCE_REF):
            if value in literal:
                findings.append(
                    f"line {node.lineno}: literal carries the evidence value {value}, "
                    "which the driver must receive rather than author"
                )
    return findings
