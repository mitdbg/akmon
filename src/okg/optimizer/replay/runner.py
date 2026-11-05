"""Prepare, run and judge one replay; the CLI (``__main__``) is a thin layer over this.

``prepare`` loads the run file's inputs, starts (or attaches to) the graph
server, reads and pins what it serves, and builds the setup record. It makes
no model call. ``run_replay`` then answers every (question, try) of the
loop view, re-checks the served surface and generation after the last try,
judges every try once per pass, and writes the run folder:

* ``setup.json``: the setup record and digest;
* ``answers.jsonl`` / ``judgments.jsonl``: private, they hold answer and
  rationale text;
* ``tries.jsonl``: one row per try, numbers and labels only, with each
  pass's score; compare reads this;
* ``run.json``: the summary (status, completeness, failures by label, spend,
  budget check, stop reason). It is written whenever the run folder exists,
  also when answering, the post-run probe or judging fails unexpectedly
  (stop code ``run_crashed``), so ``judge`` can resume;
* ``measurement.json``, ``score.json``, ``acceptance.json``: only for a
  complete run.

Held-out questions are never loaded (``Registry.load`` returns the loop view).

Before any model call the run refuses a reserve below one call's worst case
(``reserve_below_worst_call``): the input bound counts every text a call can
carry as one token per UTF-8 byte (a byte-level BPE token covers at least
one byte), every earlier turn's output at the full output limit, and a fixed
overhead per input item, capped at the model's context window (a request
cannot hold more). ``judge`` on an existing run rebuilds what it can
of the setup from the run file and the okg checkout and refuses any
difference from ``setup.json`` (``run_file_changed``, ``okg_changed``); the
record's tries, passes and deployment come from ``setup.json``.
"""
from __future__ import annotations

import asyncio
import contextlib
import copy
import dataclasses
import json
import os
from collections import Counter
from collections.abc import AsyncIterator, Callable, Mapping, Sequence
from contextlib import AbstractAsyncContextManager
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from okg.optimizer.objectives import Objectives, ObjectivesError, load_objectives, parse_measurement, \
    score_objectives
from okg.optimizer.question_sets import LoopView, Question, QuestionSetError, load_registry

from . import agent as agent_mod
from . import judge as judge_mod
from .config import BudgetConfig, ExtraEndpoint, RunConfig
from .errors import ReplayError, ReplayProblem, refuse
from .ledger import SOURCE_HTTP_ERROR, SOURCE_USAGE, Ledger
from .measure import completeness, measurement_record, replay_measurement
from .openai_client import OpenAIResponses, check_priced, mask, price_table_digest, worst_call_usd
from .serve import INHERITED_ENV, INHERITED_ENV_PREFIXES, Connector, Surface, check_pins, check_server_env, \
    check_unchanged, probe_surface, read_generation, read_surface_text
from .setup import build_setup, diff_setups, okg_identity, sha256_file, sha256_text, tree_digest

OKG_ENDPOINT = "okg"
#: Input-bound slack per input item (role, ids, framing), in tokens.
ITEM_OVERHEAD_TOKENS = 16
#: Bytes the reply-cut note can add to a capped tool reply.
CUT_NOTE_BYTES = 300
#: Stop phases after which judging cannot complete the run.
_FINAL_PHASES = ("setup", "answer", "probe")


@dataclass
class Prepared:
    config: RunConfig
    objectives: Objectives
    view: LoopView
    system_prompt: str
    endpoints: list[agent_mod.Endpoint]
    surfaces: dict[str, Surface]
    generation: str
    model_input: agent_mod.ModelInput
    setup: dict[str, Any]


def load_inputs(config: RunConfig) -> tuple[Objectives, LoopView, str]:
    """Objectives, the loop view of the question set, and the system prompt; each refusal named."""
    try:
        objectives = load_objectives(config.objectives)
    except ObjectivesError as exc:
        raise ReplayError([ReplayProblem(p.code, p.message, "/objectives" + p.pointer)
                           for p in exc.problems]) from exc
    if config.tries < objectives.min_tries:
        raise refuse("tries_below_min", f"tries {config.tries} is below the objectives' min_tries "
                     f"{objectives.min_tries}", "/tries")
    named = {o.question_set for o in (*objectives.objectives, *objectives.costs, *objectives.specific)}
    if config.question_set_id not in named:
        raise refuse("question_set_not_in_objectives",
                     f"no objective names question set {config.question_set_id!r}", "/question_set/id")
    try:
        view = load_registry(config.registry).load(config.question_set_id)
    except QuestionSetError as exc:
        raise ReplayError([ReplayProblem(p.code, p.message, "/question_set" + p.pointer)
                           for p in exc.problems]) from exc
    if len(view) == 0:
        raise refuse("question_set_empty", "the loop view of the question set has no questions", "/question_set")
    unsafe = [q.id for q in view.questions if not agent_mod.header_safe_id(q.id)]
    if unsafe:
        raise refuse("question_id_not_header_safe",
                     f"{len(unsafe)} question id(s) cannot travel as {agent_mod.QUESTION_HEADER} "
                     "(one printable-ASCII token without '#' or ',', at most 200 characters)", "/question_set")
    check_priced(config.answerer.model, "/answerer/model")
    check_priced(config.judge.model, "/judge/model")
    system_prompt = config.answerer.system_prompt.read_text(encoding="utf-8")
    if not system_prompt.strip():
        raise refuse("system_prompt_empty", "the answerer's system prompt file is empty", "/answerer/system_prompt")
    if len(view) * config.tries < config.budget.check_after_tries:
        raise refuse("budget_never_checked",
                     f"{len(view)} questions x {config.tries} tries is fewer than budget.check_after_tries "
                     f"{config.budget.check_after_tries}, so the budget stop could never fire",
                     "/budget/check_after_tries")
    check_reserve("judge", config.judge.model, judge_input_bound(config, view), config.judge.max_output_tokens,
                  config.spend.reserve_usd)
    return objectives, view, system_prompt


def _bytes(text: str) -> int:
    return len(text.encode("utf-8"))


def judge_input_bound(config: RunConfig, view: LoopView) -> int:
    """Tokens one judge call can send: prompt, the largest question payload, an answer at twice the answerer's
    output limit (JSON escaping can add a character per character), item overhead."""
    payload = max(_bytes(judge_mod.judge_input(q, "")) for q in view.questions)
    return (_bytes(judge_mod.COMPARATOR_SYSTEM_PROMPT) + _bytes(json.dumps(judge_mod.TEXT_FORMAT)) + payload
            + 2 * config.answerer.max_output_tokens + 2 * ITEM_OVERHEAD_TOKENS)


def answer_input_bound(config: RunConfig, view: LoopView, model_input: agent_mod.ModelInput) -> int:
    """Tokens one answer call can send: system text and tool list, the largest question, every tool reply at
    its cap (with the cut note), every earlier turn's output at the output limit, the final notes and
    not-run replies (counted for at most ``tool_call_safety_cap`` pending calls), and item overhead."""
    a = config.answerer
    calls = a.tool_call_safety_cap
    notes = (max(_bytes(agent_mod.FINAL_NOTE), _bytes(agent_mod.TURN_FINAL_NOTE))
             + calls * _bytes(agent_mod.NOT_RUN_REPLY.format(limit=calls)))
    items = 2 + a.max_model_turns + 2 * calls + calls
    return (_bytes(model_input.system_text) + _bytes(json.dumps(model_input.tools))
            + max(_bytes(q.question) for q in view.questions)
            + calls * (a.tool_reply_cap_bytes + CUT_NOTE_BYTES)
            + (a.max_model_turns - 1) * a.max_output_tokens
            + notes + items * ITEM_OVERHEAD_TOKENS)


def check_reserve(kind: str, model: str, input_bound: int, max_output_tokens: int, reserve_usd: float) -> None:
    worst, priced_input = worst_call_usd(model, input_bound, max_output_tokens)
    if reserve_usd < worst:
        capped = " (capped at the context window)" if priced_input < input_bound else ""
        raise refuse("reserve_below_worst_call",
                     f"reserve_usd ${reserve_usd:.4f} is below one {kind} call's worst case ${worst:.4f} "
                     f"({priced_input:,} input tokens{capped} + {max_output_tokens:,} output tokens)",
                     "/spend/reserve_usd")


ServerFactory = Callable[[], AbstractAsyncContextManager[tuple[str, dict[str, str]]]]


def extra_endpoints(config: RunConfig) -> list[agent_mod.Endpoint]:
    out = []
    for ep in config.extra_endpoints:
        token = ep.token_file.read_text(encoding="utf-8").strip()
        if not token:
            raise refuse("endpoint_token_empty", f"the token file of endpoint {ep.name} is empty",
                         "/extra_endpoints")
        out.append(agent_mod.Endpoint(ep.name, ep.url, {"Authorization": f"Bearer {token}"}, ep.tools,
                                      ep.instructions_shown, labels_question=False))
    return out


def _extra_config(ep: ExtraEndpoint) -> dict[str, Any]:
    return {"instructions_shown": ep.instructions_shown, "allowed": list(ep.tools) if ep.tools is not None else "all"}


def config_fields(config: RunConfig, view: LoopView, system_prompt: str, okg: Mapping[str, str]) -> dict[str, Any]:
    """The setup fields that follow from the run file, its inputs and the okg checkout alone (no server)."""
    a, j = config.answerer, config.judge
    return {
        "okg": dict(okg),
        "deployment": {"name": config.deployment, "tree": tree_digest(config.deployment_dir),
                       "branch": config.serve.branch, "generation": config.serve.expect_generation},
        "question_set": {"id": view.set_id, "digest": view.digest, "questions": len(view),
                         "held_out": view.held_out_count},
        "answerer": {"provider": a.provider, "model": a.model, "reasoning_effort": a.reasoning_effort,
                     "max_output_tokens": a.max_output_tokens, "max_model_turns": a.max_model_turns,
                     "tool_call_safety_cap": a.tool_call_safety_cap, "tool_reply_cap_bytes": a.tool_reply_cap_bytes,
                     "tool_timeout_s": a.tool_timeout_s, "answer_timeout_s": a.answer_timeout_s,
                     "system_prompt": sha256_text(system_prompt)},
        "judge": {"provider": j.provider, "model": j.model, "reasoning_effort": j.reasoning_effort,
                  "max_output_tokens": j.max_output_tokens, "passes": j.passes, "prompt": j.prompt,
                  "prompt_sha256": judge_mod.PROMPT_SHA256, "schema_sha256": judge_mod.SCHEMA_SHA256,
                  "tries": judge_mod.JUDGE_TRIES},
        "serve_env": {"env_files": [[e.name, e.prefix] for e in config.serve.env_files],
                      "pass_env": list(config.serve.pass_env),
                      "inherited": list(INHERITED_ENV), "inherited_prefixes": list(INHERITED_ENV_PREFIXES)},
        "tries": config.tries,
        "prices": price_table_digest(),
        "runner": dict(agent_mod.RUNNER_CONSTANTS),
    }


async def prepare(config: RunConfig, connector: Connector, url: str, headers: Mapping[str, str], *,
                  okg: Mapping[str, str] | None = None) -> Prepared:
    """Pin the served setup at ``url`` and build the setup record. No model call."""
    objectives, view, system_prompt = load_inputs(config)
    check_server_env(config)
    okg = okg_identity() if okg is None else okg
    surface = await probe_surface(connector, url, headers)
    generation = await read_generation(connector, url, headers)
    check_pins(config.serve, surface, generation)
    surface_text = await read_surface_text(connector, url, headers, config.deployment)
    endpoints = [agent_mod.Endpoint(OKG_ENDPOINT, url, dict(headers), config.serve.tools,
                                    config.serve.instructions_shown, labels_question=True)]
    surfaces = {OKG_ENDPOINT: surface}
    extra_rows: dict[str, dict[str, Any]] = {}
    for ep, cfg in zip(extra_endpoints(config), config.extra_endpoints, strict=True):
        extra_surface = await probe_surface(connector, ep.url, ep.headers)
        endpoints.append(ep)
        surfaces[ep.name] = extra_surface
        extra_rows[ep.name] = {"tools": extra_surface.tools_digest, "tool_names": extra_surface.tool_names,
                               "instructions": extra_surface.instructions_digest, **_extra_config(cfg)}
    try:
        model_input = agent_mod.build_model_input(system_prompt, endpoints, surfaces)
    except agent_mod.RouteError as exc:
        raise refuse("tool_routes_invalid", str(exc), "/extra_endpoints") from exc
    check_reserve("answer", config.answerer.model, answer_input_bound(config, view, model_input),
                  config.answerer.max_output_tokens, config.spend.reserve_usd)
    fields = config_fields(config, view, system_prompt, okg)
    fields["deployment"]["generation"] = generation  # equal to expect_generation (check_pins)
    setup = build_setup(
        okg=fields["okg"], deployment=fields["deployment"],
        served={"surface": surface.digest, "tools": surface.tools_digest,
                "instructions": surface.instructions_digest, "tool_names": surface.tool_names,
                "surface_text": surface_text,
                "instructions_shown": config.serve.instructions_shown},
        model_input=model_input.digests, question_set=fields["question_set"], answerer=fields["answerer"],
        judge=fields["judge"], extra_endpoints=extra_rows, serve_env=fields["serve_env"], tries=fields["tries"],
        prices=fields["prices"], runner=fields["runner"],
    )
    setup["workers"] = config.workers  # recorded for the report; not a score input, so not in the digest
    return Prepared(config, objectives, view, system_prompt, endpoints, surfaces, generation, model_input, setup)


def expected_setup_fields(config: RunConfig, view: LoopView, system_prompt: str, okg: Mapping[str, str],
                          stored: Mapping[str, Any]) -> dict[str, Any]:
    """``stored`` with every field the run file and okg checkout decide replaced by its value now.

    Fields only the server can tell (surface, tool and instruction digests, model input) are kept from
    ``stored``; everything else is rebuilt, so any edit to the run file or checkout shows as a difference."""
    expected = copy.deepcopy(dict(stored))
    expected.update(config_fields(config, view, system_prompt, okg))
    served = dict(expected.get("served") or {})
    served.update({"tool_names": sorted(config.serve.tools), "instructions_shown": config.serve.instructions_shown})
    expected["served"] = served
    old_extra = stored.get("extra_endpoints") or {}
    expected["extra_endpoints"] = {ep.name: {**(old_extra.get(ep.name) or {}), **_extra_config(ep)}
                                   for ep in config.extra_endpoints}
    return json.loads(json.dumps(expected))


def real_server(config: RunConfig, log_path: Path, connector: Connector) -> ServerFactory:
    from .serve import start_server

    @contextlib.asynccontextmanager
    async def factory() -> AsyncIterator[tuple[str, dict[str, str]]]:
        async with start_server(config, log_path, connector) as handle:
            yield handle.url, handle.headers
    return factory


def _append(path: Path, row: Mapping[str, Any]) -> None:
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(row, ensure_ascii=False, sort_keys=True) + "\n")


def _read_jsonl(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]


def _write_text(path: Path, text: str) -> None:
    """Write to a temporary file, then rename: a reader never sees half a file."""
    tmp = path.with_name(path.name + ".tmp")
    tmp.write_text(text, encoding="utf-8")
    os.replace(tmp, path)


def _write_json(path: Path, value: Any) -> None:
    _write_text(path, json.dumps(value, indent=1, sort_keys=True, ensure_ascii=False, default=str) + "\n")


def crashed(phase: str, exc: BaseException) -> ReplayError:
    """A named stop for an unexpected error, so the run folder still gets its run.json."""
    first = agent_mod.leaves(exc)[0]
    return refuse("run_crashed", f"{phase} stopped on an unexpected {type(first).__name__}: {mask(str(first))[:300]}")


@dataclass
class RunOutcome:
    run_dir: Path
    summary: dict[str, Any]
    record: dict[str, Any] | None
    score: dict[str, Any] | None
    stop: ReplayError | None


def ledger_for(config: RunConfig, run_id: str) -> Ledger:
    s = config.spend
    return Ledger(s.ledger, run_id=run_id, run_cap_usd=s.run_cap_usd, total_cap_usd=s.total_cap_usd,
                  reserve_usd=s.reserve_usd)


def new_budget_state(budget: BudgetConfig) -> dict[str, Any]:
    return {"expected_usd_per_try": budget.expected_usd_per_try, "check_after_tries": budget.check_after_tries,
            "max_ratio": budget.max_ratio, "checked": False, "finished_tries": 0, "usd_per_try": None,
            "ratio": None, "reserve_charged_calls": 0, "reserve_charged_usd": 0.0}


#: Settle sources whose cost is known: computed from reported usage, or $0 for a refused request.
_KNOWN_COST_SOURCES = (SOURCE_USAGE, SOURCE_HTTP_ERROR)


def check_budget(state: dict[str, Any], ledger: Ledger, finished: Sequence[tuple[str, int]]) -> None:
    """Once ``check_after_tries`` tries have finished: answer spend per finished try over the estimate must not
    pass ``max_ratio``.

    Only settle rows with a known cost (``usage``, or ``http_error_status`` at $0) count toward the ratio.
    Every other row (a call settled at the full reserve because its cost was unknown: transport error,
    timeout, cancelled call) is left out of the ratio and counted in ``reserve_charged_calls`` /
    ``reserve_charged_usd``; such rows always count toward the run and total caps. ``Ledger.settle`` refuses
    a row with no known source, so a row without one can only come from an edited ledger."""
    state["finished_tries"] = len(finished)
    keys = set(finished)
    known = charged = 0.0
    charged_calls = 0
    for r in ledger.run_rows():
        if r.get("purpose") != "answer" or (r.get("question"), r.get("try")) not in keys:
            continue
        if r.get("source") in _KNOWN_COST_SOURCES:
            known += float(r["usd"])
        else:
            charged += float(r["usd"])
            charged_calls += 1
    state.update(reserve_charged_calls=charged_calls, reserve_charged_usd=round(charged, 8))
    if len(finished) < state["check_after_tries"]:
        return
    per_try = known / len(finished)
    ratio = per_try / state["expected_usd_per_try"]
    state.update(checked=True, usd_per_try=round(per_try, 8), ratio=round(ratio, 6))
    if ratio > state["max_ratio"]:
        raise refuse("budget_ratio_exceeded",
                     f"answer spend per try ${per_try:.6f} (known costs only) over {len(finished)} finished tries "
                     f"is {ratio:.2f}x the estimate ${state['expected_usd_per_try']:.6f}; the limit is "
                     f"{state['max_ratio']:g}x", "/budget")


async def run_replay(config: RunConfig, *, run_id: str, connector: Connector, server: ServerFactory,
                     key: str, command: str, transport: Any = None, okg: Mapping[str, str] | None = None,
                     sleep: Any = None) -> RunOutcome:
    """Answer, re-check, judge and measure one run.

    A spend, setup or budget stop, or an unexpected error once the run folder exists, ends it with ``stop``
    set and run.json written. An interrupt (KeyboardInterrupt, cancellation; ``__main__`` turns SIGTERM into
    one) writes run.json with status stopped and code ``interrupted``, then is raised again."""
    if not agent_mod.header_safe_id(run_id) or "/" in run_id:
        raise refuse("run_id_invalid", "the run id must be one printable-ASCII token without '/', '#' or ','")
    run_dir = config.output_dir / run_id
    if run_dir.exists():
        raise refuse("run_exists", f"run folder {run_id} already exists; pick a new run id")
    ledger = ledger_for(config, run_id)
    ledger.check_start()
    client_kw = {"transport": transport} if transport is not None else {}
    if sleep is not None:
        client_kw["sleep"] = sleep
    budget = new_budget_state(config.budget)
    stop: ReplayError | None = None
    phase: str | None = None
    step = "setup"  # the step under way: an error's error_during and an interrupt's stop_phase
    prepared: Prepared | None = None
    try:
        async with server() as (url, headers):
            prepared = await prepare(config, connector, url, headers, okg=okg)
            run_dir.mkdir(parents=True)
            _write_json(run_dir / "setup.json", prepared.setup)
            (run_dir / "prompts").mkdir()
            (run_dir / "prompts" / "system.txt").write_text(prepared.model_input.system_text, encoding="utf-8")
            _write_json(run_dir / "prompts" / "tools.json", prepared.model_input.tools)
            answerer = OpenAIResponses(ledger, model=config.answerer.model,
                                       reasoning_effort=config.answerer.reasoning_effort, key=key, **client_kw)
            try:
                step = "answer"
                await _answer_all(prepared, answerer, connector, run_dir, budget)
            except ReplayError as exc:
                stop, phase = exc, "answer"
            except Exception as exc:  # noqa: BLE001 - named and recorded in run.json, never a traceback-only stop
                stop, phase = crashed("answering", exc), "answer"
            finally:
                await answerer.aclose()
            if stop is None:
                step = "probe"
                try:
                    after = await probe_surface(connector, url, headers)
                    generation_after = await read_generation(connector, url, headers)
                    check_unchanged(prepared.surfaces[OKG_ENDPOINT], prepared.generation, after, generation_after)
                except ReplayError as exc:
                    stop, phase = exc, "probe"
                except Exception as exc:  # noqa: BLE001 - the served setup after the run is unknown: stop, named
                    stop, phase = crashed("the post-run probe", exc), "probe"
            step = "teardown"  # answers done and, when no stop is set, re-checked: only closing remains
        if stop is None:
            step = "judge"
            stop = await judge_run(config, prepared.view, run_dir, ledger, key=key, client_kw=client_kw)
            phase = "judge" if stop is not None else None
        step = "measure"
    except BaseException as exc:
        if prepared is None or not (run_dir / "setup.json").is_file():
            raise
        if isinstance(exc, Exception):
            # An error outside the guarded steps (writing the prompts, making the client, closing it, stopping
            # the server): a stop already set stays the run's stop and the error is recorded beside it as
            # error_during, labelled with its step. Without a stop, the run stops in that step's phase: after a
            # passed post-run check that is "teardown", which judge may resume and complete; in "setup" or
            # "answer" the answers were never re-checked, so the phase is final.
            error = exc if isinstance(exc, ReplayError) else crashed(f"the {step} step", exc)
            if stop is None:
                stop, phase = error, step
            return finish_run(prepared.objectives, prepared.view, prepared.setup, run_dir, ledger, stop, phase,
                              command, budget, extra={"error_during": _problem_row(error, step)})
        _record_interrupt(exc, prepared.objectives, prepared.view, prepared.setup, run_dir, ledger, step,
                          command, budget, keep=(stop, phase or step) if stop is not None else None)
        raise
    return finish_run(prepared.objectives, prepared.view, prepared.setup, run_dir, ledger, stop, phase, command,
                      budget)


def _problem_row(error: ReplayError, step: str) -> dict[str, str]:
    first = error.problems[0]
    return {"code": first.code, "message": first.message, "step": step}


def _record_interrupt(exc: BaseException, objectives: Objectives, view: LoopView, setup: Mapping[str, Any],
                      run_dir: Path, ledger: Ledger, step: str, command: str, budget: Mapping[str, Any] | None,
                      keep: tuple[ReplayError, str] | None = None) -> None:
    """Write run.json for a run that is ending by an exception (status stopped); the caller raises it again.

    ``keep`` is a stop the run already has (and its phase): it stays the run's stop, and the interrupt is
    recorded beside it as ``last_interrupt``, so an earlier final-phase stop is never replaced."""
    if isinstance(exc, ReplayError):
        interrupt = exc
    elif isinstance(exc, Exception):
        interrupt = crashed(step, exc)
    else:
        interrupt = refuse("interrupted", f"the run was interrupted ({type(exc).__name__}) during {step}")
    stop, phase = keep if keep is not None else (interrupt, step)
    extra = {"last_interrupt": _problem_row(interrupt, step)} if keep is not None else None
    try:
        finish_run(objectives, view, setup, run_dir, ledger, stop, phase, command, budget, extra=extra)
    except Exception as write_exc:  # noqa: BLE001 - the interrupt stays the exception raised; say why no run.json
        exc.add_note(f"run.json was not written: {type(write_exc).__name__}: {write_exc}")


async def _answer_all(prepared: Prepared, client: OpenAIResponses, connector: Connector, run_dir: Path,
                      budget: dict[str, Any]) -> None:
    config = prepared.config
    a = config.answerer
    settings = agent_mod.AnswerSettings(a.max_output_tokens, a.max_model_turns, a.tool_call_safety_cap,
                                        a.tool_reply_cap_bytes, a.tool_timeout_s, a.answer_timeout_s)
    gate = asyncio.Semaphore(config.workers)
    answers = run_dir / "answers.jsonl"
    pinned = prepared.model_input.digests
    finished: list[tuple[str, int]] = []
    stopped: list[ReplayError] = []  # a budget stop starts no further try, even one already past the gate

    async def one(question: Question, try_number: int) -> None:
        async with gate:
            if stopped:
                return
            row = await agent_mod.answer_try(
                client, connector, prepared.endpoints, settings, system_prompt=prepared.system_prompt,
                question_id=question.id, question=question.question, try_number=try_number, pinned=pinned)
        _append(answers, row)
        finished.append((question.id, try_number))
        try:
            check_budget(budget, client.ledger, finished)
        except ReplayError as exc:
            stopped.append(exc)
            raise

    try:
        async with asyncio.TaskGroup() as group:
            for t in range(1, config.tries + 1):
                for q in prepared.view.questions:
                    group.create_task(one(q, t))
    except BaseExceptionGroup as exc:
        stops = [leaf for leaf in agent_mod.leaves(exc) if isinstance(leaf, ReplayError)]
        if stops:
            raise stops[0] from exc
        raise


async def judge_run(config: RunConfig, view: LoopView, run_dir: Path, ledger: Ledger, *, key: str,
                    client_kw: Mapping[str, Any]) -> ReplayError | None:
    """Judge every answered try once per pass (cached verdicts cost nothing).

    A unit whose model call fails is recorded ``judge_failed`` (the run stays incomplete and ``judge``
    retries it); a spend or provider stop is returned; any other error is returned as ``run_crashed``."""
    settings = judge_mod.JudgeSettings(config.judge.model, config.judge.reasoning_effort,
                                       config.judge.max_output_tokens)
    cache = judge_mod.VerdictCache(config.output_dir / "judge-cache")
    client = OpenAIResponses(ledger, model=config.judge.model, reasoning_effort=config.judge.reasoning_effort,
                             key=key, **client_kw)
    done = {(r["question_id"], r["try"], r["pass"]) for r in _read_jsonl(run_dir / "judgments.jsonl")
            if r["status"] == "judged"}
    gate = asyncio.Semaphore(config.workers)

    async def one(row: Mapping[str, Any], pass_number: int) -> None:
        question = view.question(row["question_id"])
        async with gate:
            result = await judge_mod.judge_answer(client, cache, settings, question, row["answer"],
                                                  pass_number=pass_number, try_number=row["try"])
        _append(run_dir / "judgments.jsonl", {"question_id": row["question_id"], "try": row["try"],
                                              "pass": pass_number, **result})

    try:
        async with asyncio.TaskGroup() as group:
            for row in _read_jsonl(run_dir / "answers.jsonl"):
                if row["status"] != "answered":
                    continue
                for p in range(1, config.judge.passes + 1):
                    if (row["question_id"], row["try"], p) not in done:
                        group.create_task(one(row, p))
    except BaseExceptionGroup as exc:
        stops = [leaf for leaf in agent_mod.leaves(exc) if isinstance(leaf, ReplayError)]
        return stops[0] if stops else crashed("judging", exc)
    except Exception as exc:  # noqa: BLE001 - e.g. an unreadable answers.jsonl: named, run.json still written
        return crashed("judging", exc)
    finally:
        await client.aclose()
    return None


def try_rows(passes: int, view: LoopView, run_dir: Path, ledger: Ledger) -> list[dict[str, Any]]:
    """tries.jsonl rows: per try numbers and labels, each pass's score; no text."""
    answers = {(r["question_id"], r["try"]): r for r in _read_jsonl(run_dir / "answers.jsonl")}
    judged: dict[tuple[str, int, int], dict[str, Any]] = {}
    for r in _read_jsonl(run_dir / "judgments.jsonl"):
        k = (r["question_id"], r["try"], r["pass"])
        if r["status"] == "judged" or k not in judged:
            judged[k] = r
    answer_usd: dict[tuple[str, int], float] = {}
    for r in ledger.run_rows():
        if r.get("purpose") == "answer":
            k = (r["question"], r["try"])
            answer_usd[k] = answer_usd.get(k, 0.0) + float(r["usd"])
    rows = []
    for (qid, t), a in sorted(answers.items()):
        question = view.question(qid)
        units = []
        for p in range(1, passes + 1):
            if a["status"] != "answered":
                units.append({"pass": p, "status": "answer_failed", **_unit(judge_mod.score(question, None))})
                continue
            j = judged.get((qid, t, p))
            if j is None:
                continue
            if j["status"] != "judged":
                units.append({"pass": p, "status": "judge_failed"})
                continue
            units.append({"pass": p, "status": "judged", "cached": j["cached"],
                          **_unit(judge_mod.score(question, j["judgments"]))})
        rows.append({k: a[k] for k in ("question_id", "try", "status", "failure", "forced_final", "model_calls",
                                       "input_tokens", "cached_tokens", "output_tokens", "reasoning_tokens",
                                       "answer_usd", "tool_calls", "tool_errors", "tool_timeouts", "replies_cut",
                                       "wall_s", "setup_tries")}
                    | {"ledger_answer_usd": round(answer_usd.get((qid, t), 0.0), 8), "passes": units})
    return rows


def _unit(score: Mapping[str, Any]) -> dict[str, Any]:
    return {"recall": score["recall"], "contradicted": score["contradicted"], "correct": score["correct"],
            "required_facts": score["required_facts"], "required_entailed": score["required_entailed"],
            "contradicted_facts": score["contradicted_facts"]}


_MEASURE_FILES = ("measurement.json", "score.json", "acceptance.json")


def finish_run(objectives: Objectives, view: LoopView, setup: Mapping[str, Any], run_dir: Path, ledger: Ledger,
               stop: ReplayError | None, stop_phase: str | None, command: str,
               budget: Mapping[str, Any] | None, *, extra: Mapping[str, Any] | None = None) -> RunOutcome:
    """tries.jsonl, run.json and, for a complete run, the measurement files. Tries, passes, deployment and
    generation come from the setup record, never from the run file.

    run.json is written in ``finally``, so a failure or an interrupt while measuring still leaves it. The
    measurement files are replaced only once all three are built (each written to a temporary file, then
    renamed); a run that ends without a record removes older ones, so no stale record outlives it."""
    fields = setup["fields"]
    tries, passes = int(fields["tries"]), int(fields["judge"]["passes"])
    deployment, generation = fields["deployment"]["name"], fields["deployment"]["generation"]
    rows = try_rows(passes, view, run_dir, ledger)
    _write_text(run_dir / "tries.jsonl", "".join(json.dumps(r, sort_keys=True) + "\n" for r in rows))
    ids = [q.id for q in view.questions]
    state = completeness(rows, ids, tries, passes)
    record: dict[str, Any] | None = None
    score: dict[str, Any] | None = None
    acceptance: dict[str, Any] | None = None
    measured = False
    try:
        if stop is None and state["complete"]:
            try:
                record = measurement_record(rows, view, deployment=deployment, generation_id=generation,
                                            run_id=run_dir.name, setup_digest=setup["digest"], tries=tries,
                                            passes=passes)
                parsed = parse_measurement(record)
                score = score_objectives(objectives, parsed, deployment=deployment).as_dict()
                acceptance = _acceptance_dict(replay_measurement(
                    objectives, parsed, runner_digest=fields["okg"]["runner_tree"], command=command,
                    artifact_refs=("measurement.json", "setup.json", "tries.jsonl"), measured_at=datetime.now(UTC)))
            except ReplayError as exc:
                stop, stop_phase = exc, "measure"
            except ObjectivesError as exc:
                stop, stop_phase = ReplayError([ReplayProblem(p.code, p.message, p.pointer)
                                                for p in exc.problems]), "measure"
            except Exception as exc:  # noqa: BLE001 - named in run.json
                stop, stop_phase = crashed("measuring", exc), "measure"
        measured = True
    finally:
        if not measured and stop is None:
            stop, stop_phase = refuse("interrupted", "the run was interrupted while it was measured"), "measure"
        if stop is not None:
            record = score = acceptance = None
        if record is not None and score is not None and acceptance is not None:
            _write_json(run_dir / "measurement.json", record)
            _write_json(run_dir / "score.json", score)
            _write_json(run_dir / "acceptance.json", acceptance)
        else:
            for name in _MEASURE_FILES:
                (run_dir / name).unlink(missing_ok=True)
        summary = _summary(run_dir, view, setup, rows, state, ledger, stop, stop_phase, budget,
                           complete=record is not None)
        summary.update({"last_interrupt": None, "error_during": None, **(extra or {})})
        _write_json(run_dir / "run.json", summary)
    return RunOutcome(run_dir, summary, record, score, stop)


def _summary(run_dir: Path, view: LoopView, setup: Mapping[str, Any], rows: Sequence[Mapping[str, Any]],
             state: Mapping[str, Any], ledger: Ledger, stop: ReplayError | None, stop_phase: str | None,
             budget: Mapping[str, Any] | None, *, complete: bool) -> dict[str, Any]:
    fields = setup["fields"]
    judgments = _read_jsonl(run_dir / "judgments.jsonl")
    spent = ledger.run_rows()
    status = "stopped" if stop is not None else ("complete" if complete else "incomplete")
    return {
        "run_id": run_dir.name, "status": status, "setup_digest": setup["digest"],
        "generation_id": fields["deployment"]["generation"], "deployment": fields["deployment"]["name"],
        "question_set_id": view.set_id, "question_set_digest": view.digest,
        "question_ids": [q.id for q in view.questions], "held_out": view.held_out_count,
        "tries": int(fields["tries"]), "passes": int(fields["judge"]["passes"]), "completeness": dict(state),
        "workers": setup.get("workers"),
        "failed_tries": sum(1 for r in rows if r["status"] != "answered"),
        "failures_by_label": dict(sorted(Counter(r["failure"] for r in rows if r["status"] != "answered").items())),
        "forced_final_tries": sum(1 for r in rows if r["forced_final"]),
        "judge_calls_made": sum(r.get("model_calls", 0) for r in judgments),
        "judge_calls_cached": sum(1 for r in judgments if r.get("cached")),
        "spend": {"run_usd": round(sum(float(r["usd"]) for r in spent), 8),
                  "answer_usd": round(sum(float(r["usd"]) for r in spent if r["purpose"] == "answer"), 8),
                  "judge_usd": round(sum(float(r["usd"]) for r in spent if r["purpose"].startswith("judge")), 8),
                  "total_usd": ledger.totals()["total_usd"]},
        "budget": None if budget is None else dict(budget),
        "stop": None if stop is None else [p.as_dict() for p in stop.problems],
        "stop_phase": stop_phase,
    }


def private_texts(view: LoopView, run_dirs: Sequence[Path] = ()) -> list[str]:
    """Every private text: questions, facts, reference answers, and each run's answers and rationales."""
    out: list[str] = []
    for q in view.questions:
        out.append(q.question)
        out.extend(f.text for f in q.required_facts)
        if q.reference_answer:
            out.append(q.reference_answer)
    for run_dir in run_dirs:
        out.extend(r.get("answer") or "" for r in _read_jsonl(run_dir / "answers.jsonl"))
        for r in _read_jsonl(run_dir / "judgments.jsonl"):
            out.extend(j.get("rationale") or "" for j in r.get("judgments") or ())
    return [t for t in out if t]


def file_digest(path: Path) -> str:
    return sha256_file(path)


def _read_json(path: Path, code: str) -> dict[str, Any]:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError) as exc:
        raise refuse(code, f"{path.name} of the run cannot be read ({type(exc).__name__})") from exc


def judge_existing(config: RunConfig, run_dir: Path, *, key: str, command: str, transport: Any = None,
                   okg: Mapping[str, str] | None = None) -> RunOutcome:
    """Re-judge a stopped or incomplete run (cached verdicts are free), then re-measure it.

    The run file and the okg checkout must still give the run's setup: every field they decide is rebuilt
    and compared with setup.json (``okg`` None reads the checkout's identity)."""
    from .setup import check_setup

    setup = _read_json(run_dir / "setup.json", "run_setup_unreadable")
    check_setup(setup)
    summary = _read_json(run_dir / "run.json", "run_summary_unreadable")
    if summary.get("stop") and any(p["code"] in ("surface_changed", "generation_changed", "model_input_changed")
                                   for p in summary["stop"]):
        raise refuse("run_not_judgeable", "the run stopped because the served setup changed; its answers do not "
                     "belong to one setup")
    objectives, view, system_prompt = load_inputs(config)
    okg = okg_identity() if okg is None else okg
    expected = expected_setup_fields(config, view, system_prompt, okg, setup["fields"])
    differing = diff_setups(setup["fields"], expected)
    if differing:
        problems = []
        moved = [f for f in differing if f.startswith("okg.")]
        rest = [f for f in differing if not f.startswith("okg.")]
        if moved:
            problems.append(ReplayProblem("okg_changed", "the okg checkout (commit or runner code) differs from the "
                                          f"run's: {', '.join(moved)}; re-judge from the run's commit"))
        if rest:
            problems.append(ReplayProblem("run_file_changed", "the run file or its inputs give another setup than "
                                          f"the run's in: {', '.join(rest[:12])}"))
        raise ReplayError(problems)
    ledger = ledger_for(config, run_dir.name)
    client_kw = {"transport": transport} if transport is not None else {}
    # Answering (or the post-run check) stopped: judging what was answered never completes the run, and no
    # later write may replace that stop (an interrupt or crash while judging is recorded beside it).
    final: tuple[ReplayError, str] | None = None
    if summary.get("stop") and summary.get("stop_phase") in _FINAL_PHASES:
        final = ReplayError([ReplayProblem(**p) for p in summary["stop"]]), summary["stop_phase"]
    try:
        stop = asyncio.run(judge_run(config, view, run_dir, ledger, key=key, client_kw=client_kw))
    except BaseException as exc:
        _record_interrupt(exc, objectives, view, setup, run_dir, ledger, "judge", command, summary.get("budget"),
                          keep=final)
        raise
    phase = "judge" if stop is not None else None
    extra = None
    if final is not None:
        if stop is not None:
            extra = {"last_interrupt": _problem_row(stop, "judge")}
        stop, phase = final
    return finish_run(objectives, view, setup, run_dir, ledger, stop, phase, command, summary.get("budget"),
                      extra=extra)


def describe(outcome: RunOutcome) -> dict[str, Any]:
    s = outcome.summary
    out = {"run_id": s["run_id"], "status": s["status"], "setup_digest": s["setup_digest"],
           "generation_id": s["generation_id"], "complete": s["completeness"]["complete"],
           "failures_by_label": s["failures_by_label"], "spend": s["spend"],
           "measures": None if outcome.record is None else outcome.record["measures"],
           "stop": s["stop"], "stop_phase": s["stop_phase"]}
    for name in ("error_during", "last_interrupt"):  # printed only when set
        if s.get(name):
            out[name] = s[name]
    return out


__all__: Sequence[str] = ("Prepared", "RunOutcome", "prepare", "run_replay", "judge_existing", "load_inputs",
                          "real_server", "private_texts", "describe", "file_digest")


def _acceptance_dict(m: Any) -> dict[str, Any]:
    """A RewriteReplayMeasurement as JSON-ready values (its mappings are read-only proxies)."""
    out: dict[str, Any] = {}
    for f in dataclasses.fields(m):
        value = getattr(m, f.name)
        if isinstance(value, Mapping):
            value = dict(value)
        elif isinstance(value, tuple):
            value = list(value)
        elif isinstance(value, datetime):
            value = value.isoformat()
        elif hasattr(value, "value"):
            value = value.value
        out[f.name] = value
    return out
