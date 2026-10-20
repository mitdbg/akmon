#!/usr/bin/env python3
"""Claim: admission rejects a bad write with a typed verdict.

Against the published reviewer deployment, submit (through the real
`okg admission submit` CLI) a review payload with one edge the deployment's
catalog forbids: `references` from one record to another, where the
bootstrapped narrowings allow `references` only between a record and a
document. Accepting it (`okg admission decide --decision accept`) must end
with the envelope `rejected`, a machine-readable verdict code
`narrowing_violation`, and no node or edge fact written by the producer.

Usage:
    uv run python scripts/akmon/claim_admission_rejection.py \
        --state <workdir>/reviewer-state.json --dsn <postgres-url>
"""

from __future__ import annotations

import argparse
import json
import sys
import uuid
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from _common import (  # noqa: E402
    run_main,
    DSN_ENV,
    deployment_environment,
    fail,
    load_state,
    note,
    passed,
    require_keyless,
    resolve_dsn,
    run_okg,
)


EXPECTED_VERDICT = "narrowing_violation"


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--state", required=True, type=Path)
    parser.add_argument("--dsn", help=f"PostgreSQL URL (or set {DSN_ENV})")
    parser.add_argument(
        "--edge-type",
        default="references",
        help="edge type to submit (default: references, which the catalog "
        "forbids between two records)",
    )
    parser.add_argument(
        "--dst",
        help="edge target node id (default: record rec-2 of the deployment)",
    )
    args = parser.parse_args(argv)

    require_keyless("admission-rejection")
    dsn = resolve_dsn(args.dsn)
    state = load_state(args.state)
    name = state["deployment_name"]
    env = deployment_environment(state, dsn)
    producer_id = f"agent:akmon-reviewer-{uuid.uuid4().hex[:8]}"
    src = f"{name}:records:record:rec-1"
    dst = args.dst or f"{name}:records:record:rec-2"
    payload = {"edges": [{"src": src, "dst": dst, "edge_type": args.edge_type}]}
    note(f"producer: {producer_id}")
    note(f"payload: {json.dumps(payload, sort_keys=True)}")

    submitted = run_okg("submit", [
        "admission", "submit", "--deployment", name, "--dsn", dsn,
        "--producer-id", producer_id, "--producer-kind", "agent",
        "--intent", "review_proposal",
        "--idempotency-key", f"akmon-reviewer-claim-{uuid.uuid4().hex}",
        "--payload-json", json.dumps(payload), "--json",
    ], env=env)
    output = submitted.get("output") or {}
    admission_id = output.get("admission_id")
    if not admission_id:
        fail("submit", "no admission id returned", output=submitted)
    note(
        f"submitted admission_id={admission_id} "
        f"path={submitted.get('workflow_status')} status={output.get('status')}"
    )

    decided = run_okg("decide", [
        "admission", "decide", "--deployment", name, "--dsn", dsn,
        str(admission_id), "--decision", "accept", "--actor", "reviewer",
        "--json",
    ], env=env)
    decision = decided.get("output") or {}
    note(f"decide accept -> status={decision.get('status')}")

    shown = run_okg("show", [
        "admission", "show", "--deployment", name, "--dsn", dsn,
        str(admission_id), "--json",
    ], env=env)
    admission = shown.get("admission") or {}
    verdict = admission.get("verdict") or {}
    events = shown.get("events") or []
    rejected_reasons = [e.get("reason") for e in events if e.get("event_type") == "rejected"]
    note(f"envelope status={admission.get('status')} verdict={json.dumps(verdict, sort_keys=True)}")
    note(f"rejected event reasons={rejected_reasons}")

    import psycopg

    with psycopg.connect(dsn, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute("SELECT count(*) FROM okg.edge_facts WHERE source = %s", (producer_id,))
        edge_facts = int(cur.fetchone()[0])
        cur.execute("SELECT count(*) FROM okg.node_facts WHERE source = %s", (producer_id,))
        node_facts = int(cur.fetchone()[0])
    facts = edge_facts + node_facts

    if admission.get("status") != "rejected":
        fail("admission-rejection", "envelope was not rejected",
             status=admission.get("status"), verdict=verdict)
    if verdict.get("code") != EXPECTED_VERDICT:
        fail("admission-rejection", f"verdict code is not {EXPECTED_VERDICT}",
             verdict=verdict)
    if EXPECTED_VERDICT not in rejected_reasons:
        fail("admission-rejection", "no rejected event carries the verdict reason",
             reasons=rejected_reasons)
    if facts != 0:
        fail("admission-rejection", "the rejected write left facts behind",
             edge_facts=edge_facts, node_facts=node_facts)

    passed(
        "CLAIM admission-rejection",
        status="rejected",
        verdict=verdict["code"],
        facts=facts,
    )
    return 0


if __name__ == "__main__":
    run_main("admission-rejection", main)
