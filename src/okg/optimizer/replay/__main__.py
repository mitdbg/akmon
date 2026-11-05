"""``python -m okg.optimizer.replay dry-run | run | judge | compare | setup-diff``.

* ``dry-run --config RUN.yaml [--out setup.json]``: start the server, pin what
  it serves, print the setup digest and its field table. No model call, $0.
* ``run --config RUN.yaml --run-id ID``: answer, judge, measure. Needs
  ``OPENAI_API_KEY`` in the environment.
* ``judge --config RUN.yaml --run-id ID``: judge an existing run again
  (cached verdicts are free), then measure it.
* ``compare RUN_A RUN_B --out REPORT.json [--change FIELD] [--config RUN.yaml
  --evidence-dir DIR]``: the noise report.
* ``setup-diff A.json B.json``: the setup fields that differ.

Exit codes: 0 done, 2 refused or stopped, 3 spend cap or budget reached, 4 run
incomplete, 130 interrupted (Ctrl-C or SIGTERM; run.json says code interrupted).
Output names digests, ids, counts and measures only; never question, answer or
key text.
"""
from __future__ import annotations

import argparse
import asyncio
import json
import shlex
import signal
import sys
import time
from collections.abc import Sequence
from pathlib import Path

from .config import load_config
from .errors import EXIT_CAP, EXIT_INCOMPLETE, EXIT_INTERRUPTED, EXIT_OK, EXIT_REFUSED, CapReached, ReplayError, \
    ReplayProblem
from .evidence import report_evidence, run_evidence, setup_evidence, write_evidence
from .openai_client import key_from_env
from .serve import mcp_connect
from .setup import check_setup, diff_setups, field_table

_CAP_CODES = ("run_cap_reached", "total_cap_reached", "budget_ratio_exceeded")


def _parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="python -m okg.optimizer.replay", description=__doc__.split("\n\n")[0])
    sub = p.add_subparsers(dest="command", required=True)
    d = sub.add_parser("dry-run", help="pin the served setup and print its digest; no model call")
    d.add_argument("--config", required=True)
    d.add_argument("--out", help="write the setup record here")
    d.add_argument("--json", action="store_true")
    for name in ("run", "judge"):
        r = sub.add_parser(name)
        r.add_argument("--config", required=True)
        r.add_argument("--run-id", required=True)
        r.add_argument("--evidence-dir", help="write commit-safe setup and run evidence here")
    c = sub.add_parser("compare", help="noise report of run B against run A")
    c.add_argument("run_a")
    c.add_argument("run_b")
    c.add_argument("--out", required=True)
    c.add_argument("--change", help="the one setup field allowed to differ")
    c.add_argument("--evidence-dir", help="write a commit-safe copy of the report here (needs --config)")
    c.add_argument("--config", help="the run file whose question set holds the runs' private text")
    s = sub.add_parser("setup-diff", help="setup fields that differ between two setup records")
    s.add_argument("a")
    s.add_argument("b")
    return p


def _err(exc: ReplayError) -> int:
    for problem in exc.problems:
        print(f"refused: {problem.code}: {problem.message}" + (f" ({problem.pointer})" if problem.pointer else ""),
              file=sys.stderr)
    return EXIT_CAP if isinstance(exc, CapReached) or any(c in _CAP_CODES for c in exc.codes) else EXIT_REFUSED


async def _dry_run(args: argparse.Namespace) -> int:
    from .runner import prepare, real_server

    config = load_config(args.config)
    log = config.output_dir / "logs" / f"dry-run-{time.strftime('%Y%m%dT%H%M%SZ', time.gmtime())}.log"
    async with real_server(config, log, mcp_connect)() as (url, headers):
        prepared = await prepare(config, mcp_connect, url, headers)
    setup = prepared.setup
    if args.out:
        Path(args.out).write_text(json.dumps(setup, indent=1, sort_keys=True) + "\n", encoding="utf-8")
    if args.json:
        print(json.dumps({"digest": setup["digest"], "fields": dict(field_table(setup["fields"])),
                          "workers": setup.get("workers")}, indent=1))
    else:
        print(f"setup digest {setup['digest']}")
        for name, value in field_table(setup["fields"]):
            print(f"  {name} = {value}")
        print(f"workers = {setup.get('workers')} (not in the digest)")
    return EXIT_OK


def _outcome_exit(outcome) -> int:
    from .runner import describe

    print(json.dumps(describe(outcome), indent=1, sort_keys=True))
    if outcome.stop is not None:
        return _err(outcome.stop)
    if outcome.record is None:
        return EXIT_INCOMPLETE
    return EXIT_OK


def _write_run_evidence(args: argparse.Namespace, config, outcome) -> None:
    from .runner import private_texts
    from okg.optimizer.question_sets import load_registry

    if not args.evidence_dir:
        return
    view = load_registry(config.registry).load(config.question_set_id)
    private = private_texts(view, [outcome.run_dir])
    out = Path(args.evidence_dir)
    setup = json.loads((outcome.run_dir / "setup.json").read_text(encoding="utf-8"))
    write_evidence(setup_evidence(setup), out / f"{outcome.run_dir.name}-setup.json", private)
    write_evidence(run_evidence(outcome.summary, outcome.record, outcome.score),
                   out / f"{outcome.run_dir.name}-run.json", private)


def _run(args: argparse.Namespace) -> int:
    from .runner import real_server, run_replay

    config = load_config(args.config)
    key = key_from_env()
    log = config.output_dir / "logs" / f"{args.run_id}-server.log"
    outcome = asyncio.run(run_replay(config, run_id=args.run_id, connector=mcp_connect,
                                     server=real_server(config, log, mcp_connect), key=key,
                                     command=_command()))
    _write_run_evidence(args, config, outcome)
    return _outcome_exit(outcome)


def _judge(args: argparse.Namespace) -> int:
    from .runner import judge_existing

    config = load_config(args.config)
    outcome = judge_existing(config, config.output_dir / args.run_id, key=key_from_env(), command=_command())
    _write_run_evidence(args, config, outcome)
    return _outcome_exit(outcome)


def _compare(args: argparse.Namespace) -> int:
    from .compare import compare_runs, load_run
    from okg.optimizer.question_sets import load_registry

    from .runner import private_texts

    if args.evidence_dir and not args.config:
        raise ReplayError(ReplayProblem(
            "compare_evidence_needs_config",
            "--evidence-dir needs --config: the question set's text is private and must be checked against"))
    a, b = load_run(Path(args.run_a)), load_run(Path(args.run_b))
    report = compare_runs(a, b, change=args.change)
    Path(args.out).write_text(json.dumps(report, indent=1, sort_keys=True) + "\n", encoding="utf-8")
    if args.evidence_dir:
        config = load_config(args.config)
        view = load_registry(config.registry).load(config.question_set_id)
        for run in (a, b):
            if run.setup["fields"]["question_set"]["digest"] != view.digest:
                raise ReplayError(ReplayProblem(
                    "question_set_changed",
                    f"run {run.run_id} answered another question set than the one --config names"))
        private = private_texts(view, [Path(args.run_a), Path(args.run_b)])
        write_evidence(report_evidence(report), Path(args.evidence_dir) / f"{Path(args.out).stem}.json", private)
    print(json.dumps({"within_floor": report["within_floor"],
                      "measures": {k: {"difference": v["difference"], "range": v["range"]}
                                   for k, v in report["measures"].items()}}, indent=1))
    print(report["determinism"])
    return EXIT_OK


def _setup_diff(args: argparse.Namespace) -> int:
    a = json.loads(Path(args.a).read_text(encoding="utf-8"))
    b = json.loads(Path(args.b).read_text(encoding="utf-8"))
    check_setup(a)
    check_setup(b)
    differing = diff_setups(a["fields"], b["fields"])
    print(json.dumps({"same": not differing, "digests": [a["digest"], b["digest"]],
                      "differing_fields": differing}, indent=1))
    return EXIT_OK


def _command() -> str:
    return "python -m okg.optimizer.replay " + shlex.join(sys.argv[1:])


def _term_as_interrupt(_signum: int, _frame: object) -> None:
    """SIGTERM is handled as Ctrl-C: asyncio cancels the run, which writes run.json (code interrupted)."""
    signal.raise_signal(signal.SIGINT)


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    previous = signal.signal(signal.SIGTERM, _term_as_interrupt) if args.command in ("run", "judge") else None
    try:
        if args.command == "dry-run":
            return asyncio.run(_dry_run(args))
        if args.command == "run":
            return _run(args)
        if args.command == "judge":
            return _judge(args)
        if args.command == "compare":
            return _compare(args)
        return _setup_diff(args)
    except ReplayError as exc:
        return _err(exc)
    except (KeyboardInterrupt, asyncio.CancelledError):
        print("stopped: interrupted (run.json records it when the run folder exists)", file=sys.stderr)
        return EXIT_INTERRUPTED
    finally:
        if previous is not None:
            signal.signal(signal.SIGTERM, previous)


if __name__ == "__main__":
    sys.exit(main())
