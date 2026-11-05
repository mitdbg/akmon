"""Fakes for the replay runner tests: a synthetic question set, an in-process MCP
server, and an OpenAI Responses endpoint on httpx.MockTransport.

Nothing here touches the network or names a real deployment.
"""
from __future__ import annotations

import contextlib
import json
from collections.abc import AsyncIterator, Callable, Mapping
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import httpx
import yaml

from okg.optimizer.question_sets import question_set_digest, parse_question_set
from okg.optimizer.replay.serve import ToolDef

GEN = "okg:synthetic:branch:default:gen:20261030T000000000000Z:0123456789ab"
KEY = "sk-test-0123456789abcdefghijklmnop"
TOOLS = ("inspect", "search")


def question_doc() -> dict[str, Any]:
    def q(qid: str, tags: list[str], facts: int, *, held_out: bool = False) -> dict[str, Any]:
        return {
            "id": qid, "question": f"Synthetic question {qid}: which widget colour is configured for the lab?",
            "origin": "operator", "scope_tags": tags, "held_out": held_out,
            "required_facts": [{"id": f"{qid}-f{i}", "text": f"Synthetic fact {i} of {qid} names the colour",
                                "required": True} for i in range(1, facts + 1)],
        }
    return {"schema": "okg.question_set", "version": 1, "id": "synthetic-3",
            "questions": [q("q1", ["alpha"], 2), q("q2", ["alpha", "beta"], 1), q("q3", ["beta"], 3),
                          q("q4", ["beta"], 1, held_out=True)]}


def write_inputs(root: Path, *, tries: int = 2, passes: int = 2, workers: int = 2, run_cap: float = 1.0,
                 total_cap: float = 5.0, reserve: float = 0.01, max_model_turns: int = 4,
                 extra: Mapping[str, Any] | None = None) -> Path:
    """A deployment folder, question set, registry, objectives, prompt and run file under ``root``."""
    dep = root / "deployments" / "synthetic"
    dep.mkdir(parents=True)
    (dep / "deployment.yaml").write_text("name: synthetic\n", encoding="utf-8")
    doc = question_doc()
    (root / "qs.yaml").write_text(yaml.safe_dump(doc), encoding="utf-8")
    digest = question_set_digest(parse_question_set(doc))
    (root / "question_sets.yaml").write_text(yaml.safe_dump({
        "version": 1, "question_sets": {"synthetic-3": {"format": "okg.question_set", "path": "qs.yaml",
                                                         "digest": digest}}}), encoding="utf-8")
    (root / "objectives.yaml").write_text(yaml.safe_dump({
        "version": 1, "hard_constraints": ["provenance", "versioning", "access_control"],
        "noise": {"min_tries": 1},
        "objectives": [{"id": "recall", "measure": "required_fact_recall", "question_set": "synthetic-3"}],
        "costs": [{"id": "usd", "measure": "usd_per_question", "question_set": "synthetic-3"}],
        "specific": [{"id": "recall-floor", "measure": "required_fact_recall", "question_set": "synthetic-3",
                      "floor": 0.5}],
    }), encoding="utf-8")
    (root / "prompt.md").write_text("You answer questions about a synthetic lab.\n", encoding="utf-8")
    (root / "dsn").write_text("postgresql://synthetic@127.0.0.1:1/none\n", encoding="utf-8")
    run = {
        "schema": "okg.replay.run.v1",
        "deployment": "synthetic",
        "deployment_dir": "deployments/synthetic",
        "output_dir": "${REPLAY_TEST_OUT}",
        "question_set": {"registry": "question_sets.yaml", "id": "synthetic-3"},
        "objectives": "objectives.yaml",
        "serve": {"branch": "default", "expect_generation": GEN, "tools": list(TOOLS), "guidance": None,
                  "instructions_shown": True, "expect_surface_digest": None, "dsn_file": "dsn",
                  "env_files": {}, "pass_env": []},
        "extra_endpoints": [],
        "answerer": {"provider": "openai", "model": "gpt-6-luna", "reasoning_effort": "medium",
                     "max_output_tokens": 4000, "max_model_turns": max_model_turns, "tool_call_safety_cap": 8,
                     "tool_reply_cap_bytes": 4096, "tool_timeout_s": 5, "answer_timeout_s": 30,
                     "system_prompt": "prompt.md"},
        "judge": {"provider": "openai", "model": "gpt-6-luna", "reasoning_effort": "xhigh",
                  "max_output_tokens": 4000, "passes": passes, "prompt": "builtin:qa-answer-comparator-v3"},
        "tries": tries,
        "workers": workers,
        "spend": {"ledger": "${REPLAY_TEST_LEDGER}", "run_cap_usd": run_cap, "total_cap_usd": total_cap,
                  "reserve_usd": reserve},
        "budget": {"expected_usd_per_try": 0.001, "check_after_tries": 4, "max_ratio": 1.5},
    }
    for dotted, value in (extra or {}).items():
        target = run
        *parts, last = dotted.split(".")
        for part in parts:
            target = target[part]
        target[last] = value
    path = root / "run.yaml"
    path.write_text(yaml.safe_dump(run, sort_keys=False), encoding="utf-8")
    return path


def env_for(root: Path) -> dict[str, str]:
    return {"REPLAY_TEST_OUT": str(root / "out"), "REPLAY_TEST_LEDGER": str(root / "ledger.jsonl")}


# --- MCP ----------------------------------------------------------------------------------------------------------


@dataclass
class FakeServer:
    instructions: str = "Synthetic graph instructions."
    tools: tuple[str, ...] = TOOLS
    generation: str = GEN
    reply: Callable[[str, dict[str, Any]], tuple[str, bool]] | None = None
    delay_s: float = 0.0
    seen: list[dict[str, Any]] = field(default_factory=list)
    #: What `okg://readside-metadata/<deployment>` serves as `surface_text` (issue #4280); None omits the field.
    surface_text: dict[str, Any] | None = None

    def tool_defs(self) -> list[ToolDef]:
        return [ToolDef(name, f"The {name} tool.", {"type": "object", "properties": {"query": {"type": "string"}}})
                for name in self.tools]


class FakeSession:
    def __init__(self, server: FakeServer, headers: Mapping[str, str]) -> None:
        self.server = server
        self.headers = dict(headers)
        self.instructions = server.instructions

    async def list_tools(self) -> list[ToolDef]:
        self.server.seen.append({"op": "list_tools", "headers": self.headers})
        return self.server.tool_defs()

    async def call_tool(self, name: str, args: dict[str, Any]) -> tuple[str, bool]:
        import asyncio

        self.server.seen.append({"op": "call_tool", "name": name, "args": args, "headers": self.headers})
        if self.server.delay_s:
            await asyncio.sleep(self.server.delay_s)
        if name == "inspect" and args == {"target": {"kind": "graph"}}:
            return f"inspect ok · generation {self.server.generation}", False
        if self.server.reply is not None:
            return self.server.reply(name, args)
        return f"{name} ok · 1 row\n[1] node:synthetic:widget · colour blue", False

    async def read_resource(self, uri: str) -> str:
        import json

        self.server.seen.append({"op": "read_resource", "uri": uri, "headers": self.headers})
        body: dict[str, Any] = {"deployment": uri.rsplit("/", 1)[-1]}
        if self.server.surface_text is not None:
            body["surface_text"] = self.server.surface_text
        return json.dumps(body)


def fake_connector(servers: Mapping[str, FakeServer]):
    @contextlib.asynccontextmanager
    async def connect(url: str, headers: Mapping[str, str], timeout: float) -> AsyncIterator[FakeSession]:
        yield FakeSession(servers[url], headers)
    return connect


def fake_server_factory(url: str = "http://fake/mcp", token: str = "fake-token"):
    @contextlib.asynccontextmanager
    async def factory():
        yield url, {"Authorization": f"Bearer {token}"}
    return factory


# --- OpenAI -------------------------------------------------------------------------------------------------------


def usage(input_tokens: int = 1000, cached: int = 200, output_tokens: int = 50) -> dict[str, Any]:
    return {"input_tokens": input_tokens, "input_tokens_details": {"cached_tokens": cached},
            "output_tokens": output_tokens, "output_tokens_details": {"reasoning_tokens": 10},
            "total_tokens": input_tokens + output_tokens}


def response(output: list[dict[str, Any]], *, status: str = "completed", tier: str = "default",
             incomplete: str | None = None, model: str | None = "gpt-6-luna", **usage_kw: int) -> dict[str, Any]:
    body: dict[str, Any] = {"status": status, "service_tier": tier, "output": output, "usage": usage(**usage_kw)}
    if model is not None:
        body["model"] = model
    if incomplete:
        body["incomplete_details"] = {"reason": incomplete}
    return body


def function_call(name: str, args: Mapping[str, Any], call_id: str) -> dict[str, Any]:
    return {"type": "function_call", "id": f"fc_{call_id}", "call_id": call_id, "name": name,
            "arguments": json.dumps(args)}


def message(text: str) -> dict[str, Any]:
    return {"type": "message", "id": "msg_1", "role": "assistant",
            "content": [{"type": "output_text", "text": text}]}


@dataclass
class FakeOpenAI:
    """Answers: one search call, then an answer. Judge: every atom ``judge_outcome``."""

    answer_text: str = "The configured widget colour in the synthetic lab is blue, per node widget."
    judge_outcome: str = "entailed"
    always_call_tools: bool = False
    answer_status: int = 200  # another status: every answer call fails with it
    judge_status: int = 200  # another status: every judge call fails with it
    transport_errors: int = 0  # this many answer calls fail with a connection error first (a "blip")
    judge_raises: BaseException | None = None  # raised by every judge call (an interrupt or a crash)
    requests: list[dict[str, Any]] = field(default_factory=list)

    def handler(self, request: httpx.Request) -> httpx.Response:
        assert request.headers["Authorization"] == f"Bearer {KEY}"
        body = json.loads(request.content)
        self.requests.append(body)
        if "format" in body["text"]:
            if self.judge_raises is not None:
                raise self.judge_raises
            if self.judge_status != 200:
                return httpx.Response(self.judge_status, text="busy")
            payload = json.loads(body["input"][0]["content"][0]["text"])
            js = [{"atom_id": a["id"], "outcome": self.judge_outcome, "rationale": "Synthetic judge rationale."}
                  for a in payload["gold_atoms"]]
            return httpx.Response(200, json=response([message(json.dumps({"judgments": js}))]))
        if self.transport_errors > 0:
            self.transport_errors -= 1
            raise httpx.ConnectError("synthetic blip", request=request)
        if self.answer_status != 200:
            return httpx.Response(self.answer_status, text="busy")
        outputs = [i for i in body["input"] if i.get("type") == "function_call_output"]
        if body.get("tool_choice") == "none" or (outputs and not self.always_call_tools):
            return httpx.Response(200, json=response([message(self.answer_text)]))
        return httpx.Response(200, json=response([function_call("search", {"query": "widget"},
                                                                f"c{len(outputs)}")]))

    def transport(self) -> httpx.MockTransport:
        return httpx.MockTransport(self.handler)

    @property
    def judge_requests(self) -> int:
        return sum(1 for b in self.requests if "format" in b["text"])

    @property
    def answer_requests(self) -> int:
        return sum(1 for b in self.requests if "format" not in b["text"])
