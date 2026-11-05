"""The replay run file: schema ``okg.replay.run.v1``.

One YAML file states everything a replay needs. Every key is required (an
empty list or mapping is written out), an unknown key is refused, and each
refusal carries a stable code and a JSON pointer.

``${NAME}`` environment references are allowed only in host fields (paths,
URLs, the ledger file): those differ between hosts and never enter the setup
digest. Every field that can change a score (models, efforts, limits, tools,
branch, generation, tries) must be written literally, so the digest sees the
value itself. An unset variable is refused by name, never read as empty.

``budget`` (expected answer cost per try, when to start checking, the
allowed ratio) has no defaults: the run stops with ``budget_ratio_exceeded``
when answer spend per finished try passes ``max_ratio`` x the estimate once
``check_after_tries`` tries have finished.

The provider must be ``openai``: the runner prices each call from token usage
with its own price table and has no other client.
"""
from __future__ import annotations

import math
import os
import re
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml

from .errors import ReplayError, ReplayProblem

RUN_SCHEMA = "okg.replay.run.v1"
PROVIDERS = ("openai",)
#: Reasoning efforts the OpenAI API accepts for the priced models.
EFFORTS = ("none", "low", "medium", "high", "xhigh")
#: The judge prompts the runner carries (judge.py). ``builtin:`` names one.
JUDGE_PROMPTS = ("builtin:qa-answer-comparator-v3",)
MIN_MODEL_TURNS = 2
#: Fields that may hold ``${NAME}``. Pointers use ``*`` for any list index or key.
HOST_FIELDS = frozenset({
    "/deployment_dir", "/output_dir", "/question_set/registry", "/objectives",
    "/serve/dsn_file", "/serve/env_files/*/file",
    "/extra_endpoints/*/url", "/extra_endpoints/*/token_file",
    "/answerer/system_prompt", "/spend/ledger",
})
_DIGEST = re.compile(r"^sha256:[0-9a-f]{64}$")
_NAME = re.compile(r"^[A-Za-z][A-Za-z0-9_-]{0,63}$")
_ENV_NAME = re.compile(r"^[A-Z][A-Z0-9_]{0,127}$")
#: Variables the runner sets for the server itself; neither env_files nor pass_env may name them.
RUNNER_SET_ENV = ("OKG_DSN", "OKG_MCP_RO_DSN", "OKG_MCP_AUTH_TOKEN", "OKG_MCP_TOOL_ALLOWLIST", "OKG_DEPLOYMENTS_DIR")

_TOP = ("schema", "deployment", "deployment_dir", "output_dir", "question_set", "objectives", "serve",
        "extra_endpoints", "answerer", "judge", "tries", "workers", "spend", "budget")
_QS = ("registry", "id")
_SERVE = ("branch", "expect_generation", "tools", "guidance", "instructions_shown",
          "expect_surface_digest", "dsn_file", "env_files", "pass_env")
_ENV_FILE = ("file", "prefix")
_EXTRA = ("name", "url", "token_file", "tools", "instructions_shown")
_ANSWERER = ("provider", "model", "reasoning_effort", "max_output_tokens", "max_model_turns",
             "tool_call_safety_cap", "tool_reply_cap_bytes", "tool_timeout_s", "answer_timeout_s",
             "system_prompt")
_JUDGE = ("provider", "model", "reasoning_effort", "max_output_tokens", "passes", "prompt")
_SPEND = ("ledger", "run_cap_usd", "total_cap_usd", "reserve_usd")
_BUDGET = ("expected_usd_per_try", "check_after_tries", "max_ratio")


@dataclass(frozen=True)
class EnvFile:
    """An environment variable the server gets, read from a file (value never logged)."""

    name: str
    file: Path
    prefix: str


@dataclass(frozen=True)
class ServeConfig:
    branch: str
    expect_generation: str
    tools: tuple[str, ...]
    instructions_shown: bool
    expect_surface_digest: str | None
    dsn_file: Path
    env_files: tuple[EnvFile, ...]
    pass_env: tuple[str, ...]  # variables passed through from the runner's environment (names only recorded)


@dataclass(frozen=True)
class ExtraEndpoint:
    """Another MCP server the answerer may use (for example a live-events service)."""

    name: str
    url: str
    token_file: Path
    tools: tuple[str, ...] | None  # None: every tool the server lists (written ``all``)
    instructions_shown: bool


@dataclass(frozen=True)
class AnswererConfig:
    provider: str
    model: str
    reasoning_effort: str
    max_output_tokens: int
    max_model_turns: int
    tool_call_safety_cap: int
    tool_reply_cap_bytes: int
    tool_timeout_s: float
    answer_timeout_s: float
    system_prompt: Path


@dataclass(frozen=True)
class JudgeConfig:
    provider: str
    model: str
    reasoning_effort: str
    max_output_tokens: int
    passes: int
    prompt: str


@dataclass(frozen=True)
class SpendConfig:
    ledger: Path
    run_cap_usd: float
    total_cap_usd: float
    reserve_usd: float


@dataclass(frozen=True)
class BudgetConfig:
    """The budget stop: answer spend per finished try, checked once ``check_after_tries`` tries have finished,
    must stay at or below ``max_ratio`` x ``expected_usd_per_try``."""

    expected_usd_per_try: float
    check_after_tries: int
    max_ratio: float


@dataclass(frozen=True)
class RunConfig:
    path: Path
    deployment: str
    deployment_dir: Path
    output_dir: Path
    registry: Path
    question_set_id: str
    objectives: Path
    serve: ServeConfig
    extra_endpoints: tuple[ExtraEndpoint, ...]
    answerer: AnswererConfig
    judge: JudgeConfig
    tries: int
    workers: int
    spend: SpendConfig
    budget: BudgetConfig


class _UniqueKeyLoader(yaml.SafeLoader):
    """safe_load that refuses a repeated mapping key instead of keeping the last."""


def _unique_mapping(loader: yaml.SafeLoader, node: yaml.MappingNode, deep: bool = False):
    seen: set[Any] = set()
    for key_node, _ in node.value:
        key = loader.construct_object(key_node, deep=deep)
        if key in seen:
            raise yaml.constructor.ConstructorError(
                None, None, f"duplicate key {key!r}", key_node.start_mark,
            )
        seen.add(key)
    return loader.construct_mapping(node, deep=deep)


_UniqueKeyLoader.add_constructor(yaml.resolver.BaseResolver.DEFAULT_MAPPING_TAG, _unique_mapping)


def load_config(path: str | Path, environ: Mapping[str, str] | None = None) -> RunConfig:
    """Read and validate one run file; raise ReplayError listing every problem."""
    config_path = Path(path)
    try:
        text = config_path.read_text(encoding="utf-8")
    except (OSError, UnicodeDecodeError) as exc:
        raise ReplayError(ReplayProblem("config_unreadable", f"run file cannot be read: {exc}")) from exc
    try:
        raw = yaml.load(text, Loader=_UniqueKeyLoader)  # noqa: S506 - SafeLoader subclass
    except yaml.YAMLError as exc:
        mark = getattr(exc, "problem_mark", None)
        where = f" at line {mark.line + 1}, column {mark.column + 1}" if mark is not None else ""
        raise ReplayError(ReplayProblem("config_yaml_parse", f"run file does not parse{where}")) from exc
    return parse_config(raw, base_dir=config_path.resolve().parent, path=config_path,
                        environ=os.environ if environ is None else environ)


def parse_config(raw: Any, *, base_dir: Path, path: Path, environ: Mapping[str, str]) -> RunConfig:
    v = _Validator(base_dir, environ)
    if not isinstance(raw, Mapping):
        raise ReplayError(ReplayProblem("config_not_mapping", "the run file must be a mapping"))
    v.keys(raw, _TOP, "")
    if raw.get("schema") != RUN_SCHEMA:
        v.add("schema_mismatch", f"schema must be {RUN_SCHEMA!r}", "/schema")
    deployment = v.name(raw.get("deployment"), "/deployment")
    deployment_dir = v.path(raw.get("deployment_dir"), "/deployment_dir", kind="dir")
    output_dir = v.path(raw.get("output_dir"), "/output_dir", kind="any")
    qs = v.mapping(raw.get("question_set"), "/question_set", _QS)
    registry = v.path(qs.get("registry"), "/question_set/registry", kind="file")
    set_id = v.text(qs.get("id"), "/question_set/id")
    objectives = v.path(raw.get("objectives"), "/objectives", kind="file")
    serve = _serve(v, raw.get("serve"))
    extra = _extra_endpoints(v, raw.get("extra_endpoints"))
    answerer = _answerer(v, raw.get("answerer"))
    judge = _judge(v, raw.get("judge"))
    tries = v.integer(raw.get("tries"), "/tries", minimum=1)
    workers = v.integer(raw.get("workers"), "/workers", minimum=1)
    spend = _spend(v, raw.get("spend"))
    budget = _budget(v, raw.get("budget"))
    if deployment_dir != Path() and deployment_dir.is_dir():
        # The deployment tree is hashed into the setup: run output or the ledger inside it would change the
        # digest after every run and make a re-judge refuse.
        for value, pointer in ((output_dir, "/output_dir"), (spend.ledger, "/spend/ledger")):
            if value != Path() and _inside(value, deployment_dir):
                v.add("path_inside_deployment", f"{pointer[1:]} must be outside deployment_dir (its tree is part of "
                      "the setup digest)", pointer)
    if v.problems:
        raise ReplayError(v.problems)
    return RunConfig(
        path=path, deployment=deployment, deployment_dir=deployment_dir, output_dir=output_dir,
        registry=registry, question_set_id=set_id, objectives=objectives, serve=serve,
        extra_endpoints=extra, answerer=answerer, judge=judge, tries=tries, workers=workers, spend=spend,
        budget=budget,
    )


def _serve(v: _Validator, raw: Any) -> ServeConfig:
    m = v.mapping(raw, "/serve", _SERVE)
    tools = v.names(m.get("tools"), "/serve/tools")
    if "guidance" in m and m.get("guidance") is not None:
        v.add("serve_guidance_unsupported",
              "serve.guidance must be null: run file v1 serves the deployment's own instructions unchanged",
              "/serve/guidance")
    expect_digest = m.get("expect_surface_digest")
    if expect_digest is not None and (not isinstance(expect_digest, str) or not _DIGEST.match(expect_digest)):
        v.add("surface_digest_invalid", "expect_surface_digest must be null or sha256:<64 hex>",
              "/serve/expect_surface_digest")
        expect_digest = None
    env_files: list[EnvFile] = []
    raw_env = m.get("env_files")
    if "env_files" in m and not isinstance(raw_env, Mapping):
        v.add("env_files_invalid", "env_files must be a mapping of variable name to {file, prefix} ({} for none)",
              "/serve/env_files")
    elif isinstance(raw_env, Mapping):
        for name, entry in raw_env.items():
            pointer = f"/serve/env_files/{name}"
            if not isinstance(name, str) or not _ENV_NAME.match(name):
                v.add("env_name_invalid", "an env_files key must be an upper-case variable name", pointer)
                continue
            if name in RUNNER_SET_ENV:
                v.add("env_name_reserved", f"{name} is set by the runner itself", pointer)
                continue
            em = v.mapping(entry, pointer, _ENV_FILE)
            file = v.path(em.get("file"), f"{pointer}/file", kind="file", pattern="/serve/env_files/*/file")
            prefix = em.get("prefix")
            if "prefix" in em and not isinstance(prefix, str):
                v.add("env_prefix_invalid", "prefix must be a string ('' for none)", f"{pointer}/prefix")
            env_files.append(EnvFile(name, file, prefix if isinstance(prefix, str) else ""))
    return ServeConfig(
        branch=v.text(m.get("branch"), "/serve/branch"),
        expect_generation=v.text(m.get("expect_generation"), "/serve/expect_generation"),
        tools=tools,
        instructions_shown=v.boolean(m.get("instructions_shown"), "/serve/instructions_shown"),
        expect_surface_digest=expect_digest,
        dsn_file=v.path(m.get("dsn_file"), "/serve/dsn_file", kind="file"),
        env_files=tuple(sorted(env_files, key=lambda e: e.name)),
        pass_env=_pass_env(v, m, {e.name for e in env_files}),
    )


def _pass_env(v: _Validator, m: Mapping[str, Any], file_names: set[str]) -> tuple[str, ...]:
    """serve.pass_env: names of runner variables the server also gets ([] for none); values never recorded."""
    raw = m.get("pass_env")
    if "pass_env" not in m:
        return ()
    if not isinstance(raw, list):
        v.add("pass_env_invalid", "pass_env must be a list of variable names ([] for none)", "/serve/pass_env")
        return ()
    out: list[str] = []
    for index, name in enumerate(raw):
        pointer = f"/serve/pass_env/{index}"
        if not isinstance(name, str) or not _ENV_NAME.match(name):
            v.add("env_name_invalid", "a pass_env entry must be an upper-case variable name", pointer)
        elif name in RUNNER_SET_ENV or name == "OPENAI_API_KEY":
            v.add("env_name_reserved", f"{name} is set by the runner or must never reach the server", pointer)
        elif name in file_names or name in out:
            v.add("env_name_duplicate", f"{name} is named twice (pass_env or env_files)", pointer)
        else:
            out.append(name)
    return tuple(sorted(out))


def _inside(path: Path, root: Path) -> bool:
    try:
        return path.resolve() == root.resolve() or path.resolve().is_relative_to(root.resolve())
    except OSError:
        return False


def _extra_endpoints(v: _Validator, raw: Any) -> tuple[ExtraEndpoint, ...]:
    if not isinstance(raw, list):
        v.add("extra_endpoints_invalid", "extra_endpoints must be a list ([] for none)", "/extra_endpoints")
        return ()
    out: list[ExtraEndpoint] = []
    seen: set[str] = set()
    for index, entry in enumerate(raw):
        pointer = f"/extra_endpoints/{index}"
        m = v.mapping(entry, pointer, _EXTRA)
        name = v.name(m.get("name"), f"{pointer}/name")
        if name in seen or name == "okg":
            v.add("endpoint_name_duplicate", f"endpoint name {name!r} is used twice (okg is the graph server)",
                  f"{pointer}/name")
        seen.add(name)
        raw_tools = m.get("tools")
        tools: tuple[str, ...] | None
        if raw_tools == "all":
            tools = None
        else:
            tools = v.names(raw_tools, f"{pointer}/tools")
        url = v.text(m.get("url"), f"{pointer}/url", pattern="/extra_endpoints/*/url")
        if url and not url.startswith(("http://", "https://")):
            v.add("endpoint_url_invalid", "url must be http:// or https://", f"{pointer}/url")
        out.append(ExtraEndpoint(
            name=name, url=url,
            token_file=v.path(m.get("token_file"), f"{pointer}/token_file", kind="file",
                              pattern="/extra_endpoints/*/token_file"),
            tools=tools,
            instructions_shown=v.boolean(m.get("instructions_shown"), f"{pointer}/instructions_shown"),
        ))
    return tuple(out)


def _answerer(v: _Validator, raw: Any) -> AnswererConfig:
    m = v.mapping(raw, "/answerer", _ANSWERER)
    p = "/answerer"
    turns = v.integer(m.get("max_model_turns"), f"{p}/max_model_turns", minimum=MIN_MODEL_TURNS)
    return AnswererConfig(
        provider=v.provider(m.get("provider"), f"{p}/provider"),
        model=v.text(m.get("model"), f"{p}/model"),
        reasoning_effort=v.effort(m.get("reasoning_effort"), f"{p}/reasoning_effort"),
        max_output_tokens=v.integer(m.get("max_output_tokens"), f"{p}/max_output_tokens", minimum=1),
        max_model_turns=turns,
        tool_call_safety_cap=v.integer(m.get("tool_call_safety_cap"), f"{p}/tool_call_safety_cap", minimum=1),
        tool_reply_cap_bytes=v.integer(m.get("tool_reply_cap_bytes"), f"{p}/tool_reply_cap_bytes", minimum=1),
        tool_timeout_s=v.number(m.get("tool_timeout_s"), f"{p}/tool_timeout_s", positive=True),
        answer_timeout_s=v.number(m.get("answer_timeout_s"), f"{p}/answer_timeout_s", positive=True),
        system_prompt=v.path(m.get("system_prompt"), f"{p}/system_prompt", kind="file"),
    )


def _judge(v: _Validator, raw: Any) -> JudgeConfig:
    m = v.mapping(raw, "/judge", _JUDGE)
    p = "/judge"
    prompt = m.get("prompt")
    if "prompt" in m and prompt not in JUDGE_PROMPTS:
        v.add("judge_prompt_unknown", f"judge.prompt must be one of {', '.join(JUDGE_PROMPTS)}", f"{p}/prompt")
    return JudgeConfig(
        provider=v.provider(m.get("provider"), f"{p}/provider"),
        model=v.text(m.get("model"), f"{p}/model"),
        reasoning_effort=v.effort(m.get("reasoning_effort"), f"{p}/reasoning_effort"),
        max_output_tokens=v.integer(m.get("max_output_tokens"), f"{p}/max_output_tokens", minimum=1),
        passes=v.integer(m.get("passes"), f"{p}/passes", minimum=1),
        prompt=prompt if isinstance(prompt, str) else "",
    )


def _spend(v: _Validator, raw: Any) -> SpendConfig:
    m = v.mapping(raw, "/spend", _SPEND)
    run_cap = v.number(m.get("run_cap_usd"), "/spend/run_cap_usd", positive=True)
    total_cap = v.number(m.get("total_cap_usd"), "/spend/total_cap_usd", positive=True)
    reserve = v.number(m.get("reserve_usd"), "/spend/reserve_usd", positive=True)
    if run_cap and total_cap and run_cap > total_cap:
        v.add("caps_inverted", "run_cap_usd cannot be above total_cap_usd", "/spend/run_cap_usd")
    if reserve and run_cap and reserve > run_cap:
        v.add("reserve_above_cap", "reserve_usd cannot be above run_cap_usd", "/spend/reserve_usd")
    return SpendConfig(
        ledger=v.path(m.get("ledger"), "/spend/ledger", kind="any"),
        run_cap_usd=run_cap, total_cap_usd=total_cap, reserve_usd=reserve,
    )


def _budget(v: _Validator, raw: Any) -> BudgetConfig:
    m = v.mapping(raw, "/budget", _BUDGET)
    ratio = v.number(m.get("max_ratio"), "/budget/max_ratio", positive=True)
    if ratio and ratio < 1.0:
        v.add("budget_ratio_invalid", "max_ratio must be at least 1 (it multiplies the expected cost)",
              "/budget/max_ratio")
    return BudgetConfig(
        expected_usd_per_try=v.number(m.get("expected_usd_per_try"), "/budget/expected_usd_per_try", positive=True),
        check_after_tries=v.integer(m.get("check_after_tries"), "/budget/check_after_tries", minimum=1),
        max_ratio=ratio,
    )


class _Validator:
    def __init__(self, base_dir: Path, environ: Mapping[str, str]) -> None:
        self.base_dir = base_dir
        self.environ = environ
        self.problems: list[ReplayProblem] = []

    def add(self, code: str, message: str, pointer: str) -> None:
        self.problems.append(ReplayProblem(code, message, pointer))

    def keys(self, m: Mapping[str, Any], allowed: tuple[str, ...], pointer: str) -> None:
        for key in m:
            if key not in allowed:
                self.add("unknown_key", f"unknown key {key!r}; allowed: {', '.join(allowed)}", f"{pointer}/{key}")
        for key in allowed:
            if key not in m:
                self.add("key_missing", f"{key!r} is required (no default)", f"{pointer}/{key}")

    def mapping(self, raw: Any, pointer: str, allowed: tuple[str, ...]) -> Mapping[str, Any]:
        if not isinstance(raw, Mapping):
            self.add("block_not_mapping", "must be a mapping", pointer)
            return {}
        self.keys(raw, allowed, pointer)
        return raw

    def _literal(self, value: Any, pointer: str, pattern: str | None = None) -> Any:
        """Expand ``${NAME}`` in a host field; refuse it anywhere else."""
        if not isinstance(value, str) or "$" not in value:
            return value
        where = pattern or pointer
        if where not in HOST_FIELDS:
            self.add("env_not_allowed",
                     "${NAME} is allowed only in host fields (paths, URLs, the ledger); write this value literally "
                     "so the setup digest sees it", pointer)
            return None
        # OKG's one shared ${NAME} expander: defaults and commands are refused.
        from okg.substrate.db import dsn

        result = dsn.expand(value, self.environ)
        if result.unsupported:
            self.add("env_syntax_unsupported",
                     f"placeholder syntax {', '.join(result.unsupported)} is not expanded; only ${{NAME}}",
                     pointer)
            return None
        missing = [*result.unset, *result.empty]
        if missing or result.value is None:
            self.add("env_unset", f"environment variable(s) {', '.join(missing)} unset or empty", pointer)
            return None
        return result.value

    def text(self, value: Any, pointer: str, pattern: str | None = None) -> str:
        value = self._literal(value, pointer, pattern)
        if not isinstance(value, str) or not value.strip() or value != value.strip():
            if value is not None:
                self.add("text_invalid", "must be a non-empty string without surrounding whitespace", pointer)
            elif not any(p.pointer == pointer for p in self.problems):
                self.add("text_invalid", "must be a non-empty string", pointer)
            return ""
        return value

    def name(self, value: Any, pointer: str) -> str:
        text = self.text(value, pointer)
        if text and not _NAME.match(text):
            self.add("name_invalid", "must be a name: a letter, then letters, digits, '-' or '_'", pointer)
        return text

    def names(self, value: Any, pointer: str) -> tuple[str, ...]:
        if not isinstance(value, list) or not value:
            self.add("tools_invalid", "must be a non-empty list of tool names", pointer)
            return ()
        out: list[str] = []
        for index, item in enumerate(value):
            if not isinstance(item, str) or not _NAME.match(item):
                self.add("tools_invalid", f"tool name {item!r} is not a name", f"{pointer}/{index}")
            elif item in out:
                self.add("tools_duplicate", f"tool {item!r} is listed twice", f"{pointer}/{index}")
            else:
                out.append(item)
        return tuple(out)

    def path(self, value: Any, pointer: str, *, kind: str, pattern: str | None = None) -> Path:
        text = self.text(value, pointer, pattern)
        if not text:
            return Path()
        path = Path(text)
        if not path.is_absolute():
            path = (self.base_dir / path).resolve()
        if kind == "file" and not path.is_file():
            self.add("path_missing", f"{pointer.rsplit('/', 1)[-1]} is not a file", pointer)
        elif kind == "dir" and not path.is_dir():
            self.add("path_missing", f"{pointer.rsplit('/', 1)[-1]} is not a directory", pointer)
        return path

    def boolean(self, value: Any, pointer: str) -> bool:
        if not isinstance(value, bool):
            self.add("bool_invalid", "must be true or false", pointer)
            return False
        return value

    def integer(self, value: Any, pointer: str, *, minimum: int) -> int:
        if isinstance(value, bool) or not isinstance(value, int) or value < minimum:
            self.add("int_invalid", f"must be a whole number of at least {minimum}", pointer)
            return 0
        return value

    def number(self, value: Any, pointer: str, *, positive: bool) -> float:
        if isinstance(value, bool) or not isinstance(value, int | float) or not math.isfinite(float(value)) \
                or (positive and float(value) <= 0):
            self.add("number_invalid", "must be a positive finite number", pointer)
            return 0.0
        return float(value)

    def provider(self, value: Any, pointer: str) -> str:
        if value not in PROVIDERS:
            self.add("provider_unsupported", f"provider must be one of {', '.join(PROVIDERS)}; got {value!r}",
                     pointer)
            return ""
        return str(value)

    def effort(self, value: Any, pointer: str) -> str:
        if value not in EFFORTS:
            self.add("effort_invalid", f"reasoning_effort must be one of {', '.join(EFFORTS)}; got {value!r}",
                     pointer)
            return ""
        return str(value)
