"""Answer one question with a tool-using model over MCP: one try.

Ported from the golden-bench harness (okg-deployments@40b9ba3ad2
eval/golden-bench/answer.py, ``_agent`` / ``_forced_final`` /
``answer_multi`` with the model-turn limit on), over the OpenAI Responses
client. The limits are the run file's:

* at most ``max_model_turns`` model calls per try; the tools asked for by
  calls 1 to N-1 run, and call N is a *forced final* call (the tools stay
  defined, ``tool_choice: none``, after a note that no more tools can run);
* a runaway safety cap of ``tool_call_safety_cap`` tool calls per try: a batch
  that would pass it is not run (each call gets a "not run" reply) and the
  forced final call follows;
* each tool call has ``tool_timeout_s``; each try has ``answer_timeout_s``;
* a tool reply over ``tool_reply_cap_bytes`` is cut, with a note, before the
  model sees it.

A failed try comes back labelled (``context_overflow``, ``answer_timeout``,
``step_limit``, ``api_error``, ``output_limit``, ``empty_answer``,
``other``). An answer failure is scored 0; ``api_error`` and ``other`` are
failures of the runner or provider and leave the run incomplete (measure.py
``RUN_FAILURE_LABELS``). Spend stops, provider stops, a route
misconfiguration and a changed surface are raised: they stop the run.

Every MCP request to the graph server carries ``X-OKG-Question`` and
``X-OKG-Try`` (issue #3773), so the server's call log can be joined to the
try. The session's system text and tool list must hash to the values pinned
in the setup; a try that would see anything else stops the run.
"""
from __future__ import annotations

import asyncio
import contextlib
import json
import time
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Any

from .errors import ReplayError, ReplayProblem, SurfaceChanged
from .openai_client import ModelCallError, OpenAIResponses, Reply, replay_items
from .serve import Connector, McpSession, Surface, ToolDef
from .setup import sha256_json, sha256_text

QUESTION_HEADER = "X-OKG-Question"
TRY_HEADER = "X-OKG-Try"
#: Start-up retries when the tools cannot be connected (harness DEFAULT_SETUP_RETRIES).
SETUP_RETRIES = 2
FAILURE_LABELS = ("context_overflow", "answer_timeout", "step_limit", "api_error", "output_limit",
                  "empty_answer", "other")
FORCED_FINAL_TURN_LIMIT = "model_turn_limit"
FORCED_FINAL_SAFETY_CAP = "tool_call_safety_cap"
FINAL_NOTE = ("Tool-call limit reached: no more tool calls can be made. Answer now from what you have found so far, "
              "and say what you could not check.")
TURN_FINAL_NOTE = ("Turn limit reached: no more tool calls can be made. Answer now from what you have found so far, "
                   "and say what you could not check.")
NOT_RUN_REPLY = "not run: the tool-call limit of {limit} calls is reached"
#: Fixed runner settings that change what a try does; recorded in the setup.
RUNNER_CONSTANTS = {
    "setup_retries": SETUP_RETRIES,
    "final_note_sha256": sha256_text(FINAL_NOTE),
    "turn_final_note_sha256": sha256_text(TURN_FINAL_NOTE),
    "not_run_reply_sha256": sha256_text(NOT_RUN_REPLY),
}
#: Run problems (spend cap, unknown spend, provider stop, changed surface, corrupt ledger): never a try failure.
_RUN_STOPS = (ReplayError,)
_TOKEN_OK = frozenset(chr(c) for c in range(ord("!"), ord("~") + 1)) - {"#", ","}


class StepLimit(RuntimeError):
    pass


class ForcedFinalFailed(StepLimit):
    pass


class OutputLimit(RuntimeError):
    pass


class EmptyAnswer(RuntimeError):
    pass


class SetupError(RuntimeError):
    pass


class RouteError(RuntimeError):
    """The tool routes are misconfigured; every try would fail the same way, so the run stops."""


def header_safe_id(question_id: str) -> bool:
    """True when the id can travel as X-OKG-Question (one printable-ASCII token, no '#' or ',')."""
    return 0 < len(question_id) <= 200 and all(ch in _TOKEN_OK for ch in question_id)


@dataclass(frozen=True)
class Endpoint:
    name: str
    url: str
    headers: Mapping[str, str]
    tools: tuple[str, ...] | None  # None: every tool the server lists
    instructions_shown: bool
    labels_question: bool  # send X-OKG-Question / X-OKG-Try


@dataclass(frozen=True)
class AnswerSettings:
    max_output_tokens: int
    max_model_turns: int
    tool_call_safety_cap: int
    tool_reply_cap_bytes: int
    tool_timeout_s: float
    answer_timeout_s: float


@dataclass(frozen=True)
class ModelInput:
    system_text: str
    tools: list[dict[str, Any]]
    routes: Mapping[str, tuple[str, str]]  # exposed name -> (endpoint, server tool)

    @property
    def digests(self) -> dict[str, str]:
        return {"system_text": sha256_text(self.system_text), "tool_list": sha256_json(self.tools)}


def tool_spec(tool: ToolDef) -> dict[str, Any]:
    return {"type": "function", "name": tool.name, "description": tool.description,
            "parameters": dict(tool.input_schema) or {"type": "object", "properties": {}}, "strict": False}


def build_model_input(system_prompt: str, endpoints: Sequence[Endpoint],
                      surfaces: Mapping[str, Surface]) -> ModelInput:
    """The exact system text and tool list a try sends, and the tool routes."""
    specs: list[dict[str, Any]] = []
    routes: dict[str, tuple[str, str]] = {}
    parts: list[str] = []
    for ep in endpoints:
        surface = surfaces[ep.name]
        offered = {t.name: t for t in surface.tools}
        names = list(ep.tools) if ep.tools is not None else sorted(offered)
        missing = [n for n in names if n not in offered]
        if missing:
            raise RouteError(f"endpoint {ep.name} does not offer {missing}")
        for name in names:
            if name in routes:
                raise RouteError(f"tool name {name} is offered by two endpoints")
            routes[name] = (ep.name, name)
            specs.append(tool_spec(offered[name]))
        if ep.instructions_shown and surface.instructions:
            parts.append(surface.instructions)
    instructions = "\n\n".join(parts)
    system = system_prompt + ("\n\n" + instructions if instructions else "")
    return ModelInput(system, specs, routes)


def cap_reply(text: str, cap: int) -> tuple[str, int | None]:
    """Cut a tool reply to at most ``cap`` UTF-8 bytes plus a note (answer.py cap_reply)."""
    raw = text.encode("utf-8")
    if cap <= 0 or len(raw) <= cap:
        return text, None
    head = raw[:cap].decode("utf-8", errors="ignore")
    kept = len(head.encode("utf-8"))
    note = (f"\n\n[tool reply cut: showed the first {kept:,} of {len(raw):,} bytes, so the text above is incomplete "
            "and may not be valid JSON. Ask a narrower question (fewer items, a filter, or a smaller page) to see "
            "the rest.]")
    return head + note, len(raw)


def final_answer(reply: Reply, max_output_tokens: int) -> str:
    """The answer of a final reply (no tool calls), or OutputLimit / EmptyAnswer."""
    if reply.status == "incomplete" and reply.incomplete_reason == "max_output_tokens":
        raise OutputLimit(f"the model stopped at the output limit of {max_output_tokens:,} tokens with "
                          f"{len(reply.text.strip()):,} characters of answer text")
    if not reply.text.strip():
        raise EmptyAnswer(f"the final reply had no text and no tool calls (status {reply.status!r})")
    return reply.text


def leaves(exc: BaseException) -> list[BaseException]:
    if isinstance(exc, BaseExceptionGroup):
        return [leaf for sub in exc.exceptions for leaf in leaves(sub)]
    return [exc]


def failure_label(exc: BaseException) -> str:
    exc = leaves(exc)[0]
    if isinstance(exc, StepLimit):
        return "step_limit"
    if isinstance(exc, OutputLimit):
        return "output_limit"
    if isinstance(exc, EmptyAnswer):
        return "empty_answer"
    text = f"{type(exc).__name__} {exc}".lower()
    if "context" in text and ("window" in text or "length" in text or "overflow" in text):
        return "context_overflow"
    if isinstance(exc, ModelCallError):
        return "api_error"
    return "other"


class _Try:
    """State of one try; filled as it goes, so a failure keeps what happened."""

    def __init__(self, client: OpenAIResponses, question_id: str, question: str, try_number: int,
                 settings: AnswerSettings) -> None:
        self.client = client
        self.question_id = question_id
        self.question = question
        self.try_number = try_number
        self.s = settings
        self.calls: list[dict[str, Any]] = []
        self.turns: list[dict[str, Any]] = []
        self.setup_tries = 0
        self.forced_final: str | None = None

    async def model_call(self, model_input: ModelInput, items: list[dict[str, Any]], tool_choice: str,
                         forced: bool = False) -> Reply:
        t0 = time.perf_counter()
        reply = await self.client.respond(
            instructions=model_input.system_text, input_items=items, tools=model_input.tools or None,
            tool_choice=tool_choice if model_input.tools else None, max_output_tokens=self.s.max_output_tokens,
            purpose="answer", question_id=self.question_id, try_number=self.try_number)
        self.turns.append({"ms": round((time.perf_counter() - t0) * 1000), "usd": reply.usd,
                           "status": reply.status, "incomplete_reason": reply.incomplete_reason,
                           "forced_final": forced, **reply.usage})
        return reply

    async def run(self, connector: Connector, endpoints: Sequence[Endpoint], system_prompt: str,
                  pinned: Mapping[str, str]) -> str:
        async with contextlib.AsyncExitStack() as stack:
            sessions: dict[str, McpSession] = {}
            surfaces: dict[str, Surface] = {}
            self.setup_tries += 1
            try:
                for ep in endpoints:
                    headers = dict(ep.headers)
                    if ep.labels_question:
                        headers[QUESTION_HEADER] = self.question_id
                        headers[TRY_HEADER] = str(self.try_number)
                    session = await stack.enter_async_context(connector(ep.url, headers, self.s.answer_timeout_s))
                    sessions[ep.name] = session
                    surfaces[ep.name] = Surface(session.instructions or "", tuple(await session.list_tools()))
            except Exception as exc:  # noqa: BLE001 - re-raised so the caller can retry start-up
                raise SetupError(f"tools not connected: {type(exc).__name__}: {exc}") from exc
            model_input = build_model_input(system_prompt, endpoints, surfaces)
            if model_input.digests != dict(pinned):
                raise SurfaceChanged(ReplayProblem(
                    "model_input_changed",
                    f"try {self.try_number} of {self.question_id} would send a system text or tool list other "
                    "than the setup's"))
            return await self._loop(model_input, sessions)

    async def _loop(self, model_input: ModelInput, sessions: Mapping[str, McpSession]) -> str:
        items: list[dict[str, Any]] = [
            {"role": "user", "content": [{"type": "input_text", "text": self.question}]}]
        turns = 0
        while True:
            reply = await self.model_call(model_input, items, "auto")
            turns += 1
            items.extend(replay_items(reply.output))
            if not reply.calls:
                return final_answer(reply, self.s.max_output_tokens)
            if len(self.calls) + len(reply.calls) > self.s.tool_call_safety_cap:
                why = (f"the model asked for {len(reply.calls)} more tool calls after {len(self.calls)}; "
                       f"the safety cap is {self.s.tool_call_safety_cap}")
                return await self._forced_final(model_input, items, reply.calls, why, FORCED_FINAL_SAFETY_CAP,
                                                FINAL_NOTE, NOT_RUN_REPLY.format(limit=self.s.tool_call_safety_cap))
            for call in reply.calls:
                text = await self._tool_call(model_input, sessions, call)
                items.append({"type": "function_call_output", "call_id": call["call_id"], "output": text})
            if turns >= self.s.max_model_turns - 1:
                why = f"{turns} model calls asked for tools; the limit is {self.s.max_model_turns} model calls"
                return await self._forced_final(model_input, items, [], why, FORCED_FINAL_TURN_LIMIT,
                                                TURN_FINAL_NOTE, "")

    async def _tool_call(self, model_input: ModelInput, sessions: Mapping[str, McpSession],
                         call: Mapping[str, Any]) -> str:
        name = call["name"]
        endpoint, target = model_input.routes.get(name, (None, None))
        timed_out, ms = False, 0
        args: Any = call.get("arguments") or "{}"
        try:
            args = json.loads(call.get("arguments") or "{}")
            if not isinstance(args, dict):
                raise ValueError("arguments are not an object")
        except ValueError:
            text, is_error = "invalid JSON arguments", True
        else:
            t0 = time.perf_counter()
            if endpoint is None:
                text, is_error = f"unknown tool: {name}", True
            else:
                try:
                    text, is_error = await asyncio.wait_for(
                        sessions[endpoint].call_tool(target, args), self.s.tool_timeout_s)
                except TimeoutError:
                    text, is_error, timed_out = f"tool call timed out after {self.s.tool_timeout_s:g} s", True, True
                except Exception as exc:  # noqa: BLE001 - a transport/protocol failure is a recorded result
                    text, is_error = f"tool call failed: {type(exc).__name__}: {exc}", True
            ms = round((time.perf_counter() - t0) * 1000)
        text, cut_from = cap_reply(text, self.s.tool_reply_cap_bytes)
        # `args` and `reply` (issue #4281): the transcript diagnosis reads. They go to the private
        # answers.jsonl only; tries.jsonl keeps numbers and labels.
        self.calls.append({"tool": name, "endpoint": endpoint, "ms": ms, "bytes": len(text.encode("utf-8")),
                           "cut_from_bytes": cut_from, "error": is_error, "timed_out": timed_out,
                           "args": args, "reply": text})
        return text

    async def _forced_final(self, model_input: ModelInput, items: list[dict[str, Any]],
                            pending: Sequence[Mapping[str, Any]], why: str, reason: str, note: str,
                            not_run: str) -> str:
        for call in pending:
            items.append({"type": "function_call_output", "call_id": call["call_id"], "output": not_run})
        items.append({"role": "user", "content": [{"type": "input_text", "text": note}]})
        try:
            reply = await self.model_call(model_input, items, "none", forced=True)
        except _RUN_STOPS:
            raise
        except Exception as exc:  # noqa: BLE001 - the forced call failed: the try keeps step_limit
            raise ForcedFinalFailed(f"{why}; the forced final call failed: {type(exc).__name__}") from exc
        if reply.calls:
            raise ForcedFinalFailed(f"{why}; the forced final call asked for {len(reply.calls)} tool calls again")
        text = final_answer(reply, self.s.max_output_tokens)
        self.forced_final = reason
        return text


async def answer_try(client: OpenAIResponses, connector: Connector, endpoints: Sequence[Endpoint],
                     settings: AnswerSettings, *, system_prompt: str, question_id: str, question: str,
                     try_number: int, pinned: Mapping[str, str]) -> dict[str, Any]:
    """One try. Returns its record (the answer text included: keep it private)."""
    t0 = time.perf_counter()
    state = _Try(client, question_id, question, try_number, settings)
    answer, failure, error = "", None, None
    limit = asyncio.timeout(settings.answer_timeout_s)
    try:
        async with limit:
            while True:
                try:
                    answer = await state.run(connector, endpoints, system_prompt, pinned)
                    break
                except Exception as exc:  # noqa: BLE001 - only a start-up failure is retried
                    if not any(isinstance(x, SetupError) for x in leaves(exc)) \
                            or state.setup_tries > SETUP_RETRIES:
                        raise
    except Exception as exc:  # noqa: BLE001 - every other failure is the try's, labelled
        for leaf in leaves(exc):
            if isinstance(leaf, (*_RUN_STOPS, RouteError)):
                raise leaf from exc
        failure = "answer_timeout" if limit.expired() else failure_label(exc)
        first = leaves(exc)[0]
        error = f"{type(first).__name__}: {str(first)[:300]}"
    turns = state.turns
    return {
        "question_id": question_id,
        "try": try_number,
        "status": "failed" if failure else "answered",
        "failure": failure,
        "error": error,
        "answer": answer if failure is None else "",
        "forced_final": state.forced_final if failure is None else None,
        "model_calls": len(turns),
        "input_tokens": sum(t.get("input_tokens", 0) for t in turns),
        "cached_tokens": sum(t.get("cached_tokens", 0) for t in turns),
        "output_tokens": sum(t.get("output_tokens", 0) for t in turns),
        "reasoning_tokens": sum(t.get("reasoning_tokens", 0) for t in turns),
        "answer_usd": round(sum(t["usd"] for t in turns), 8),
        "tool_calls": len(state.calls),
        "tool_errors": sum(1 for c in state.calls if c["error"]),
        "tool_timeouts": sum(1 for c in state.calls if c["timed_out"]),
        "replies_cut": sum(1 for c in state.calls if c["cut_from_bytes"] is not None),
        "wall_s": round(time.perf_counter() - t0, 3),
        "setup_tries": state.setup_tries,
        "turns": turns,
        "calls": state.calls,
    }
