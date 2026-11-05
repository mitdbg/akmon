"""``python -m okg.optimizer.diagnose``: waste, label and agree (issue #4281).

* ``waste``: rule-based wasted work per try, no model call. Writes one record
  per try (private: it quotes replies) and prints the totals (numbers only).
* ``label``: one model call per miss, through the replay runner's ledger and
  caps. Needs ``OPENAI_API_KEY`` in the environment. Writes diagnosis records
  (private) and prints counts per class and stage.
* ``agree``: Cohen's kappa between two label files on the same misses.

Inputs are a replay run folder plus its run file (the question set gives the
facts), or a harness ``records.jsonl`` plus a misses file.
"""
from __future__ import annotations

import argparse
import asyncio
import json
import sys
from collections import Counter
from pathlib import Path
from typing import Any

from . import agreement, labeler, traces, waste


def _questions(path: str | None) -> dict[str, str] | None:
    """A JSON object {question id: question text}, for harness records (they do not hold it)."""
    if not path:
        return None
    value = json.loads(Path(path).read_text(encoding="utf-8"))
    if not isinstance(value, dict) or not all(isinstance(v, str) for v in value.values()):
        raise SystemExit("--questions must be a JSON object of question id to text")
    return value


def _load(args: argparse.Namespace) -> tuple[list[traces.TryTrace], list[traces.Miss]]:
    if args.replay_run:
        from okg.optimizer.replay.config import load_config
        from okg.optimizer.replay.runner import load_inputs

        config = load_config(Path(args.run_file))
        _objectives, view, _prompt = load_inputs(config)
        facts = {q.id: {f.id: f.text for f in q.required_facts if f.required} for q in view.questions}
        questions = {q.id: q.question for q in view.questions}
        return traces.load_replay_run(Path(args.replay_run), facts, questions, passes=config.judge.passes)
    if args.records and args.misses:
        try:
            return traces.load_harness_records(Path(args.records), Path(args.misses), run_id=args.run_id,
                                               all_tries=getattr(args, "all_tries", False),
                                               questions=_questions(args.questions))
        except ValueError as exc:
            raise SystemExit(str(exc)) from exc
    raise SystemExit("give --replay-run DIR --run-file FILE, or --records FILE --misses FILE")


def _write_jsonl(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("".join(json.dumps(r, sort_keys=True, ensure_ascii=False) + "\n" for r in rows),
                    encoding="utf-8")


def cmd_waste(args: argparse.Namespace) -> int:
    tries, _misses = _load(args)
    settings = waste.WasteSettings(search_tools=tuple(t.strip() for t in args.search_tools.split(",") if t.strip()))
    records = [waste.waste_of_try(t, settings) for t in tries]
    if args.out:
        _write_jsonl(Path(args.out), records)
    print(json.dumps(waste.summarize(records), sort_keys=True))
    return 0


def _fields(text: str) -> list[str]:
    return [k.strip() for k in text.split(",") if k.strip()]


def cmd_agree(args: argparse.Namespace) -> int:
    """Each side keyed by its own fields, so census rows (rid,qid,attempt,atom)
    meet label rows (rid,question_id,try,fact_id)."""

    def read(paths: list[str], keys: list[str], label: str) -> dict[tuple[Any, ...], str]:
        rows: list[dict[str, Any]] = []
        for p in paths:
            rows.extend(json.loads(line) for line in Path(p).read_text(encoding="utf-8").splitlines() if line.strip())
        missing = sorted({k for r in rows for k in keys if k not in r})
        if missing:
            raise SystemExit(f"rows in {paths[0]} lack key field(s) {missing}")
        return agreement.labels_from_rows(rows, keys, label)

    a_keys = _fields(args.key)
    b_keys = _fields(args.b_key) if args.b_key else a_keys
    if len(a_keys) != len(b_keys):
        raise SystemExit("--key and --b-key must name the same number of fields")
    a = read(args.a, a_keys, args.label)
    b = read(args.b, b_keys, args.b_label or args.label)
    print(json.dumps(agreement.cohen_kappa(a, b), sort_keys=True))
    return 0


def cmd_label(args: argparse.Namespace) -> int:
    from okg.optimizer.replay.ledger import Ledger
    from okg.optimizer.replay.openai_client import OpenAIResponses, key_from_env

    tries, misses = _load(args)
    if args.limit is not None:
        misses = misses[: args.limit]
    key = key_from_env()
    ledger = Ledger(Path(args.ledger), run_id=args.run_id_ledger, run_cap_usd=args.run_cap_usd,
                    total_cap_usd=args.total_cap_usd, reserve_usd=args.reserve_usd)
    settings = labeler.LabelSettings()
    out = Path(args.out)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text("", encoding="utf-8")
    tag = {"rid": args.run_id} if args.run_id else {}

    def write(row: dict[str, Any]) -> None:
        with out.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps({**row, **tag}, sort_keys=True, ensure_ascii=False) + "\n")

    async def run() -> list[dict[str, Any]]:
        client = OpenAIResponses(ledger, model=args.model, reasoning_effort=args.effort, key=key)
        try:
            return await labeler.label_misses(client, labeler.LabelCache(Path(args.cache)), traces.index(tries),
                                              misses, settings, on_row=write)
        finally:
            await client.aclose()

    rows = asyncio.run(run())
    labelled = [r for r in rows if r["status"] == "labelled"]
    print(json.dumps({"misses": len(rows), "labelled": len(labelled),
                      "by_status": dict(Counter(r["status"] for r in rows)),
                      "by_class": dict(sorted(Counter(r["cls"] for r in labelled).items())),
                      "by_stage": dict(sorted(Counter(r["stage"] for r in labelled).items())),
                      "quote_verified": sum(1 for r in labelled if r.get("quote_verified"))}, sort_keys=True))
    return 0 if len(labelled) == len(rows) else 1


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="python -m okg.optimizer.diagnose")
    sub = parser.add_subparsers(dest="command", required=True)

    def inputs(p: argparse.ArgumentParser) -> None:
        p.add_argument("--replay-run")
        p.add_argument("--run-file")
        p.add_argument("--records")
        p.add_argument("--misses")
        p.add_argument("--run-id", help="keep misses rows whose rid is this run id")
        p.add_argument("--questions", help="harness records: JSON object {question id: text}")

    w = sub.add_parser("waste", help="rule-based wasted work per try (no model call)")
    inputs(w)
    w.add_argument("--out", help="per-try records (private: they quote replies)")
    w.add_argument("--search-tools", default="search", help="comma-separated tool names that count as searches")
    w.add_argument("--all-tries", action="store_true", help="harness records: every try, not only tries with a miss")
    w.set_defaults(func=cmd_waste)

    a = sub.add_parser("agree", help="Cohen's kappa between two label files")
    a.add_argument("--a", nargs="+", required=True)
    a.add_argument("--b", nargs="+", required=True)
    a.add_argument("--key", required=True, help="comma-separated key fields, e.g. rid,qid,attempt,atom")
    a.add_argument("--b-key", help="key fields of the --b rows, in the same order (default: --key)")
    a.add_argument("--label", default="cls")
    a.add_argument("--b-label")
    a.set_defaults(func=cmd_agree)

    lab = sub.add_parser("label", help="label each miss with a model call (paid; capped by the ledger)")
    inputs(lab)
    lab.add_argument("--out", required=True)
    lab.add_argument("--cache", required=True)
    lab.add_argument("--ledger", required=True)
    lab.add_argument("--run-id-ledger", required=True, help="the ledger's run id for this labelling")
    lab.add_argument("--run-cap-usd", type=float, required=True)
    lab.add_argument("--total-cap-usd", type=float, required=True)
    lab.add_argument("--reserve-usd", type=float, required=True)
    lab.add_argument("--model", default="gpt-6-luna")
    lab.add_argument("--effort", default="high")
    lab.add_argument("--limit", type=int, help="label only the first N misses (checkpoints)")
    lab.set_defaults(func=cmd_label)

    args = parser.parse_args(argv)
    return int(args.func(args))


if __name__ == "__main__":
    sys.exit(main())
