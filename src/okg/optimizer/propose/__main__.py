"""``python -m okg.optimizer.propose``: surface and run (issue #4282).

* ``surface --run-file F --out S.json``: start the run file's server once
  and save its instructions and tools ($0, no model call).
* ``run``: build the context from diagnosis files, run the model loop
  (paid, capped by the ledger), and publish the accepted change on a new
  experiment branch; writes ``proposal.json``. Needs ``OPENAI_API_KEY``.
"""
from __future__ import annotations

import argparse
import asyncio
import json
import sys
from pathlib import Path
from typing import Any

from . import driver, loop


def cmd_surface(args: argparse.Namespace) -> int:
    surface = asyncio.run(driver.read_surface(Path(args.run_file), Path(args.out).parent))
    Path(args.out).write_text(json.dumps(surface, indent=1, sort_keys=True), encoding="utf-8")
    print(json.dumps({"tools": [t["name"] for t in surface["tools"]], "instructions_chars": len(surface["instructions"])}))
    return 0


def _answer_key(path: str | None) -> list[str]:
    """Texts the change must not repeat: a JSON list of strings (required facts, reference answers)."""
    if not path:
        return []
    value = json.loads(Path(path).read_text(encoding="utf-8"))
    if not isinstance(value, list) or not all(isinstance(v, str) for v in value):
        raise SystemExit("--answer-key must be a JSON list of strings")
    return value


def cmd_run(args: argparse.Namespace) -> int:
    from okg.optimizer.replay.ledger import Ledger
    from okg.optimizer.replay.openai_client import OpenAIResponses, key_from_env

    root = Path(args.deployment_dir)
    surface = json.loads(Path(args.surface).read_text(encoding="utf-8")) if args.surface else None
    context = driver.build_context(root, labels=Path(args.labels) if args.labels else None,
                                   waste=Path(args.waste) if args.waste else None, surface=surface,
                                   objectives=Path(args.objectives) if args.objectives else None)
    answer_key = _answer_key(args.answer_key)
    env = driver.server_env({"OKG_DSN": args.dsn})
    kinds = tuple(args.kinds.split(","))
    # What the graph holds: the proposer must link records that exist (#4282 trial 2 linked a
    # schema-only type and added nothing).
    context.summary += "\n\n" + driver.graph_facts(args.dsn, env)
    link_check = driver.link_check_for(args.dsn, env) if "link" in kinds else None
    key = key_from_env()
    ledger = Ledger(Path(args.ledger), run_id=args.run_id_ledger, run_cap_usd=args.run_cap_usd,
                    total_cap_usd=args.total_cap_usd, reserve_usd=args.reserve_usd)
    out = Path(args.out)
    out.parent.mkdir(parents=True, exist_ok=True)
    turns_log = out.with_suffix(".turns.jsonl")

    def on_turn(row: dict[str, Any]) -> None:
        with turns_log.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(row) + "\n")

    async def run() -> dict[str, Any]:
        client = OpenAIResponses(ledger, model=args.model, reasoning_effort=args.effort, key=key)
        try:
            return await loop.propose(client, context, root, args.branch, answer_key=answer_key,
                                      link_check=link_check, kinds=kinds, on_turn=on_turn)
        finally:
            await client.aclose()

    publish: dict[str, Any] | None = None
    try:
        result = asyncio.run(run())
    except Exception as exc:  # noqa: BLE001 - a record is written whatever stopped the loop
        result = {"status": "loop_error", "error": f"{type(exc).__name__}: {str(exc)[:300]}"}
    if result["status"] == "proposed" and not args.no_publish:
        try:
            publish = driver.publish_trial(root, result["proposal"], deployment=args.deployment, dsn=args.dsn,
                                           env=env, answer_key=answer_key, surface=surface)
        except Exception as exc:  # noqa: BLE001 - a guard refused before any change (dirty, locked, worker)
            publish = {"outcome": "refused_before_trial", "error": f"{type(exc).__name__}: {str(exc)[:300]}"}
    record = driver.proposal_record(result, publish)
    out.write_text(json.dumps(record, indent=1, sort_keys=True, default=str), encoding="utf-8")
    print(json.dumps({"status": record["status"], "kind": (record["proposal"] or {}).get("kind"),
                      "outcome": (publish or {}).get("outcome"), "generation_id": (publish or {}).get("generation_id"),
                      "catalog_restored_verified": (publish or {}).get("catalog_restored_verified"),
                      "turns": record["model"]["turns"], "usd": record["model"]["usd"]}, sort_keys=True))
    ok = (publish or {}).get("outcome") == "published" and bool((publish or {}).get("restored_tree"))
    if record["proposal"] and record["proposal"]["kind"] == "link":
        ok = ok and bool((publish or {}).get("catalog_restored_verified"))
    return 0 if ok else 1


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="python -m okg.optimizer.propose")
    sub = parser.add_subparsers(dest="command", required=True)
    s = sub.add_parser("surface", help="save the served instructions and tools ($0)")
    s.add_argument("--run-file", required=True)
    s.add_argument("--out", required=True)
    s.set_defaults(func=cmd_surface)
    r = sub.add_parser("run", help="propose one change and publish it on an experiment branch (paid)")
    r.add_argument("--deployment-dir", required=True)
    r.add_argument("--deployment", required=True)
    r.add_argument("--dsn", required=True, help="DSN without password (PGPASSWORD from the environment)")
    r.add_argument("--branch", required=True)
    r.add_argument("--kinds", default="text,link")
    r.add_argument("--labels")
    r.add_argument("--waste")
    r.add_argument("--surface")
    r.add_argument("--answer-key")
    r.add_argument("--objectives", help="the deployment's objectives file, shown to the proposer")
    r.add_argument("--out", required=True)
    r.add_argument("--ledger", required=True)
    r.add_argument("--run-id-ledger", required=True)
    r.add_argument("--run-cap-usd", type=float, required=True)
    r.add_argument("--total-cap-usd", type=float, required=True)
    r.add_argument("--reserve-usd", type=float, required=True)
    r.add_argument("--model", default="gpt-6-luna")
    r.add_argument("--effort", default="high")
    r.add_argument("--no-publish", action="store_true", help="propose only; write no deployment file")
    r.set_defaults(func=cmd_run)
    args = parser.parse_args(argv)
    return int(args.func(args))


if __name__ == "__main__":
    sys.exit(main())
