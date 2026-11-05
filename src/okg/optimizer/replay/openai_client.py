"""OpenAI Responses API client with a ledger-enforced spend cap.

Ported from the golden-bench harness route ``openai-direct-responses``
(okg-deployments@40b9ba3ad2 eval/golden-bench/openai_direct.py and the
Responses item helpers of sub_route.py): one non-streamed POST per model call
to ``/v1/responses`` with ``store: false``, encrypted reasoning carried to the
next turn, function tools with ``strict: false``, an explicit reasoning effort
(never OpenAI's default) and ``service_tier: default``.

Spend: OpenAI's reply carries token usage, not cost, so the cost is usage
times ``PRICES`` (the runner's pinned price table, part of the setup digest).
Every attempt reserves before it is sent and settles after (ledger.py); an
attempt that returns no usable usage settles at its reserve and, when the
reply had none, stops the run (SpendUnknown). A reply served on a tier the
table does not price also stops the run, and so does a reply whose ``model``
is neither the priced model nor its dated snapshot (``<model>-YYYY-MM-DD``).

Errors: 408/429/5xx and transport errors are retried 3 times; a
context-length 400 fails the try (ModelCallError); an out-of-quota 429, a 401
or 403 or any other 4xx stops the run (ProviderStop).

Key: ``OPENAI_API_KEY`` from the environment only. It is sent in the
Authorization header and nowhere else; any ``sk-...`` text in an error is
masked before it is raised.
"""
from __future__ import annotations

import asyncio
import hashlib
import json
import os
import re
from collections.abc import Awaitable, Callable, Mapping, Sequence
from dataclasses import dataclass, field
from typing import Any

import httpx

from .errors import ProviderStop, ReplayProblem, SpendUnknown, refuse
from .ledger import SOURCE_HTTP_ERROR, SOURCE_RESERVE, SOURCE_USAGE, Ledger
from .setup import sha256_json

URL = "https://api.openai.com/v1/responses"
KEY_ENV = "OPENAI_API_KEY"
#: $ per 1M tokens (input, cached input, output); OpenAI Standard tier, read 2026-10-26.
PRICES: Mapping[str, tuple[float, float, float]] = {
    "gpt-6-luna": (0.10, 0.01, 0.50),
}
#: Tokens one request can hold (input plus output), per priced model; developers.openai.com model page, read
#: 2026-10-31 (gpt-6-luna: 1,050,000 context, 128,000 max output). It caps the worst-case input of a call.
CONTEXT_WINDOWS: Mapping[str, int] = {
    "gpt-6-luna": 1_050_000,
}
LONG_CONTEXT_TOKENS = 272_000
#: input, cached, output multipliers above LONG_CONTEXT_TOKENS (estimate; the harness's value).
LONG_CONTEXT_MULT = (2.0, 2.0, 1.5)
SERVICE_TIER = "default"
PRICED_TIERS = ("default", "auto")
#: Every 5xx is retried: a CDN in front of the API also answers 520-524 for a passing upstream fault.
RETRY_STATUSES = frozenset({408, 429, *range(500, 600)})
RETRY_DELAYS_S = (5.0, 10.0, 15.0)
#: Seconds to wait for one reply (the harness's CALL_TIMEOUT_S).
CALL_TIMEOUT_S = 900.0
_SNAPSHOT = re.compile(r"-\d{4}-\d{2}-\d{2}")
_KEYLIKE = re.compile(r"sk-[A-Za-z0-9_*.\-…]+")
_CONTEXT_TEXT = re.compile(
    r"context_length_exceeded|context length|context window|maximum context|string too long|maximum length", re.I)


def price_table_digest() -> str:
    return sha256_json({
        "prices_per_million": {k: list(v) for k, v in PRICES.items()},
        "context_windows": dict(CONTEXT_WINDOWS),
        "long_context_tokens": LONG_CONTEXT_TOKENS,
        "long_context_mult": list(LONG_CONTEXT_MULT),
        "service_tier": SERVICE_TIER,
    })


def check_priced(model: str, pointer: str) -> None:
    if model not in PRICES:
        raise refuse("model_unpriced", f"model {model!r} has no price in the runner's table; "
                     f"priced: {', '.join(sorted(PRICES))}", pointer)
    if model not in CONTEXT_WINDOWS:
        raise refuse("model_context_unknown", f"model {model!r} has no context window in the runner's table, so a "
                     "call's worst-case cost cannot be bounded", pointer)


def cost_of(model: str, input_tokens: int, cached_tokens: int, output_tokens: int) -> tuple[float, bool]:
    p_in, p_cached, p_out = PRICES[model]
    cached = max(0, min(cached_tokens, input_tokens))
    long = input_tokens > LONG_CONTEXT_TOKENS
    if long:
        p_in, p_cached, p_out = (p * k for p, k in zip((p_in, p_cached, p_out), LONG_CONTEXT_MULT))
    usd = ((input_tokens - cached) * p_in + cached * p_cached + output_tokens * p_out) / 1e6
    return round(usd, 8), long


def worst_call_usd(model: str, input_tokens_bound: int, max_output_tokens: int) -> tuple[float, int]:
    """The most one call can cost, and the input it is priced at: the input bound capped at the model's
    context window (none of it cached) plus the full output limit."""
    check_priced(model, "")
    tokens = min(input_tokens_bound, CONTEXT_WINDOWS[model])
    return cost_of(model, tokens, 0, max_output_tokens)[0], tokens


def model_matches(priced: str, served: Any) -> bool:
    """True when a reply's ``model`` is the priced model or its dated snapshot (``<model>-YYYY-MM-DD``)."""
    if not isinstance(served, str):
        return False
    if served == priced:
        return True
    return served.startswith(priced) and _SNAPSHOT.fullmatch(served[len(priced):]) is not None


def mask(text: str) -> str:
    return _KEYLIKE.sub("sk-<masked>", str(text))


def key_from_env(environ: Mapping[str, str] | None = None) -> str:
    key = (os.environ if environ is None else environ).get(KEY_ENV, "").strip()
    if not key:
        raise refuse("openai_key_missing", f"set {KEY_ENV} in the environment (it is never read from the run file)")
    return key


class ModelCallError(RuntimeError):
    """The call failed after retries, or for a reason that fails this try only (for example context length)."""


@dataclass
class Reply:
    """One model turn: the output items, the text, the function calls and the priced usage."""

    output: list[dict[str, Any]]
    text: str
    calls: list[dict[str, Any]]
    status: str | None
    incomplete_reason: str | None
    usage: dict[str, Any] = field(default_factory=dict)
    usd: float = 0.0


def replay_items(items: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    """A turn's output items as the next request's input (sub_route.replay_items)."""
    out: list[dict[str, Any]] = []
    for it in items:
        kind = it.get("type")
        if kind == "reasoning":
            out.append(dict(it))
        elif kind == "message":
            text = "".join(c.get("text") or "" for c in it.get("content") or [] if c.get("type") == "output_text")
            msg: dict[str, Any] = {"type": "message", "role": "assistant", "status": "completed",
                                   "content": [{"type": "output_text", "text": text, "annotations": []}]}
            if it.get("id"):
                msg["id"] = it["id"]
            if it.get("phase") is not None:
                msg["phase"] = it["phase"]
            out.append(msg)
        elif kind == "function_call":
            call = {"type": "function_call", "call_id": it["call_id"], "name": it["name"],
                    "arguments": it.get("arguments") or "{}"}
            if it.get("id"):
                call["id"] = it["id"]
            out.append(call)
    return out


def parse_output(data: Mapping[str, Any]) -> Reply:
    items = list(data.get("output") or [])
    unknown = sorted({str(it.get("type")) for it in items} - {"reasoning", "message", "function_call"})
    if unknown:
        raise ModelCallError(f"the reply holds output items the runner cannot handle: {unknown}")
    text = "".join(
        "".join(c.get("text") or "" for c in it.get("content") or [] if c.get("type") == "output_text")
        for it in items if it.get("type") == "message")
    calls = [{"call_id": it["call_id"], "name": it["name"], "arguments": it.get("arguments") or "{}"}
             for it in items if it.get("type") == "function_call"]
    return Reply(output=items, text=text, calls=calls, status=data.get("status"),
                 incomplete_reason=(data.get("incomplete_details") or {}).get("reason"))


Sleep = Callable[[float], Awaitable[None]]


class OpenAIResponses:
    """``respond()`` posts one Responses request through the ledger."""

    def __init__(self, ledger: Ledger, *, model: str, reasoning_effort: str, key: str,
                 transport: httpx.AsyncBaseTransport | None = None, sleep: Sleep = asyncio.sleep,
                 timeout_s: float = CALL_TIMEOUT_S) -> None:
        check_priced(model, "")
        self.ledger = ledger
        self.model = model
        self.reasoning_effort = reasoning_effort
        self._headers = {"Authorization": f"Bearer {key}", "Content-Type": "application/json"}
        self._http = httpx.AsyncClient(timeout=httpx.Timeout(timeout_s, connect=20.0), transport=transport)
        self._sleep = sleep

    def __repr__(self) -> str:  # never show the headers
        return f"OpenAIResponses(model={self.model!r}, effort={self.reasoning_effort!r})"

    async def aclose(self) -> None:
        await self._http.aclose()

    def body(self, *, instructions: str, input_items: list[dict[str, Any]], tools: list[dict[str, Any]] | None,
             tool_choice: str | None, text_format: dict[str, Any] | None, max_output_tokens: int) -> dict[str, Any]:
        cache_key = "okg-" + hashlib.sha256(
            (instructions + "\0" + json.dumps(input_items[:1], sort_keys=True)).encode("utf-8")).hexdigest()[:40]
        text: dict[str, Any] = {"verbosity": "medium"}
        if text_format is not None:
            text["format"] = text_format
        body: dict[str, Any] = {
            "model": self.model, "instructions": instructions, "input": input_items, "store": False,
            "include": ["reasoning.encrypted_content"], "prompt_cache_key": cache_key, "text": text,
            "max_output_tokens": max_output_tokens, "reasoning": {"effort": self.reasoning_effort},
            "service_tier": SERVICE_TIER,
        }
        if tools:
            body["tools"] = tools
            body["tool_choice"] = tool_choice or "auto"
            body["parallel_tool_calls"] = True
        return body

    async def respond(self, *, instructions: str, input_items: list[dict[str, Any]],
                      tools: list[dict[str, Any]] | None = None, tool_choice: str | None = None,
                      text_format: dict[str, Any] | None = None, max_output_tokens: int, purpose: str,
                      question_id: str, try_number: int | None) -> Reply:
        body = self.body(instructions=instructions, input_items=input_items, tools=tools, tool_choice=tool_choice,
                         text_format=text_format, max_output_tokens=max_output_tokens)
        last_error = "no attempt made"
        for attempt in range(len(RETRY_DELAYS_S) + 1):
            outcome = await self._attempt(body, purpose, question_id, try_number)
            if isinstance(outcome, Reply):
                return outcome
            last_error = outcome
            if attempt < len(RETRY_DELAYS_S):
                await self._sleep(RETRY_DELAYS_S[attempt])
        raise ModelCallError(f"gave up after {len(RETRY_DELAYS_S) + 1} attempts: {last_error}")

    async def _attempt(self, body: dict[str, Any], purpose: str, question_id: str,
                       try_number: int | None) -> Reply | str:
        """One POST. Returns the Reply, or the text of a retryable failure; raises a stop."""
        rid = self.ledger.reserve(purpose=purpose, question_id=question_id, try_number=try_number)
        settled = False

        def settle(usd: float, source: str, usage: Mapping[str, Any] | None) -> None:
            nonlocal settled
            self.ledger.settle(rid, usd=usd, source=source, model=self.model, usage=usage, purpose=purpose,
                               question_id=question_id, try_number=try_number)
            settled = True

        try:
            try:
                response = await self._http.post(URL, headers=self._headers, json=body)
            except httpx.TransportError as exc:
                return mask(f"transport: {type(exc).__name__}: {exc}")
            if response.status_code != 200:
                settle(0.0, SOURCE_HTTP_ERROR, None)
                text = mask(response.text[:600])
                if response.status_code == 400 and _CONTEXT_TEXT.search(text):
                    raise ModelCallError(f"http 400 context length exceeded: {text[:300]}")
                if response.status_code == 429 and "insufficient_quota" in text:
                    raise ProviderStop(ReplayProblem("provider_quota", f"http 429 insufficient_quota: {text[:300]}"))
                if response.status_code not in RETRY_STATUSES:
                    raise ProviderStop(ReplayProblem(
                        "provider_refused", f"http {response.status_code} from the OpenAI API: {text[:300]}"))
                return f"http {response.status_code}: {text[:300]}"
            try:
                data = response.json()
            except ValueError:
                return f"unparseable 200 body ({len(response.content)} bytes)"
            if not isinstance(data, dict):
                return f"200 body is not an object ({type(data).__name__})"
            usage = _usage(data)
            if usage is None:
                raise SpendUnknown(ReplayProblem(
                    "usage_missing", "the OpenAI reply carried no token usage; its cost cannot be computed"))
            usd, long = cost_of(self.model, usage["input_tokens"], usage["cached_tokens"], usage["output_tokens"])
            usage["long_context"] = long
            settle(usd, SOURCE_USAGE, usage)
            tier = data.get("service_tier")
            if tier not in PRICED_TIERS:
                raise SpendUnknown(ReplayProblem(
                    "service_tier_unpriced",
                    f"the reply was served on tier {tier!r}; the price table prices {SERVICE_TIER!r} only"))
            if not model_matches(self.model, data.get("model")):
                served = data.get("model")
                raise ProviderStop(ReplayProblem(
                    "reply_model_mismatch",
                    f"the reply names model {str(served)[:80]!r}; the run prices {self.model!r} (or its dated "
                    "snapshot) only"))
            if data.get("error") or data.get("status") == "failed":
                return mask(f"api error: {str(data.get('error'))[:300]} (status {data.get('status')})")
            reply = parse_output(data)
            reply.usage, reply.usd = usage, usd
            return reply
        finally:
            if not settled:
                # No usable usage: the call may still be billed, so the reserve stays charged.
                self.ledger.settle(rid, usd=self.ledger.reserve_usd, source=SOURCE_RESERVE, model=self.model,
                                   usage=None, purpose=purpose, question_id=question_id, try_number=try_number)


def _usage(data: Mapping[str, Any]) -> dict[str, Any] | None:
    u = data.get("usage")
    if not isinstance(u, Mapping):
        return None
    inp, out = u.get("input_tokens"), u.get("output_tokens")
    if isinstance(inp, bool) or isinstance(out, bool) or not isinstance(inp, int) or not isinstance(out, int) \
            or inp < 0 or out < 0:
        return None
    cached = (u.get("input_tokens_details") or {}).get("cached_tokens") or 0
    reasoning = (u.get("output_tokens_details") or {}).get("reasoning_tokens") or 0
    if not isinstance(cached, int) or not isinstance(reasoning, int):
        return None
    return {"input_tokens": inp, "cached_tokens": cached, "output_tokens": out, "reasoning_tokens": reasoning}
