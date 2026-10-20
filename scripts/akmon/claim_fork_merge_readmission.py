#!/usr/bin/env python3
"""Claim: a branch fork merges back only after its changes are re-checked.

Against the published reviewer deployment:

1. Fork `default` (`okg branch create --role experiment`) and write one valid
   record node on the fork. `merge_back` stages the fork on a fresh child of
   `default`, runs a gate publish of that candidate, then lands it on
   `default` under a named human decision; the landing publish runs the
   deployment's consistency gate over `default`. The node must be readable
   on `default` in a new published generation.
2. Fork again and write an edge the catalog forbids (`references` between
   two records). The merge must be refused with a recorded reason naming the
   narrowing violation, and `default` must keep its node set, its head
   generation and its edges.

   What catches it today: the staging gate. Its gate publish reads the
   staged candidate, the consistency check names the narrowing violation,
   and merge_back refuses before anything lands on `default`, so the PASS
   line reports `violation=caught_at_staging_gate`. If a block ever arrives
   from the landing publish instead (merge_back then rolls `default` back),
   the script still checks that `default` is unchanged and reports
   `violation=caught_at_landing_and_rolled_back`.

There is no CLI for merge-back yet, so this script calls the public
`okg.substrate.merge_back.merge_back` function directly.

Usage:
    uv run python scripts/akmon/claim_fork_merge_readmission.py \
        --state <workdir>/reviewer-state.json --dsn <postgres-url>
"""

from __future__ import annotations

import argparse
import re
import sys
import uuid
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from _common import (  # noqa: E402
    run_main,
    DSN_ENV,
    activate_deployment,
    deployment_environment,
    fail,
    load_state,
    note,
    passed,
    require_keyless,
    resolve_dsn,
    run_okg,
)


DECIDER_NAME = "reviewer@example.invalid"


def _node_ids(store) -> set[str]:
    return {
        row["node_id"]
        for row in store.checkout("default").query("SELECT node_id FROM okg.graph_nodes")
    }


def _edge_count(store, src: str, dst: str, edge_type: str) -> int:
    rows = store.checkout("default").query(
        "SELECT count(*) AS n FROM okg.graph_edges "
        "WHERE src = %(src)s AND dst = %(dst)s AND edge_type = %(edge_type)s",
        {"src": src, "dst": dst, "edge_type": edge_type},
    )
    return int(rows[0]["n"])


def _summarize_block(reason: object) -> tuple[str, str, bool]:
    """(stage, gate reason, narrowing violation named) from a blocked reason.

    merge_back reports the gate's full evaluation as the blocked reason; the
    claim needs three facts from it, so print those and keep the rest out of
    the transcript. A block at landing arrives as a string prefixed with its
    stage (`land_gate_blocked_rolled_back:{...}`); a block by the staging
    gate arrives as the gate's own evaluation mapping, with no prefix.
    """
    if isinstance(reason, str):
        stage = reason.split(":", 1)[0]
    else:
        stage = "staging_gate"
        reason = str(reason)
    gate = re.search(r"'reason': '([a-z_]+)'", reason)
    violation = re.search(r"'narrowing_violation_edge_ids': \[\d", reason) is not None
    return stage, (gate.group(1) if gate else ""), violation


def _head(store, name: str) -> str:
    generation = store.latest_generation(deployment_name=name)
    if generation is None:
        fail("read", "deployment has no published generation", deployment=name)
    return str(generation.checkpoint_id)


def _fork(name: str, dsn: str, env: dict[str, str], label: str) -> str:
    branch = f"experiment:akmon-reviewer-{label}-{uuid.uuid4().hex[:8]}"
    run_okg(f"fork-{label}", [
        "branch", "create", branch, "--role", "experiment",
        "--deployment", name, "--dsn", dsn, "--json",
    ], env=env)
    return branch


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--state", required=True, type=Path)
    parser.add_argument("--dsn", help=f"PostgreSQL URL (or set {DSN_ENV})")
    parser.add_argument(
        "--bad-edge-type",
        default="references",
        help="edge type written on the second fork (default: references, "
        "which the catalog forbids between two records)",
    )
    parser.add_argument(
        "--bad-edge-dst",
        help="target node of the second fork's edge (default: record rec-2)",
    )
    args = parser.parse_args(argv)

    require_keyless("fork-merge-readmission")
    dsn = resolve_dsn(args.dsn)
    state = load_state(args.state)
    name = state["deployment_name"]
    env = deployment_environment(state, dsn)
    activate_deployment(state, dsn)

    import psycopg

    from okg.substrate.graph_store import ChronosGraphStore
    from okg.substrate.library.sources.base import EdgeFact, NodeFact
    from okg.substrate.merge_back import merge_back
    from okg.substrate.merge_guard import MergeDecision

    decider = MergeDecision(decided_by=DECIDER_NAME, decision="accept", fast_track=False)
    record_subtype = f"{name.replace('-', '_')}_record"
    rec1 = f"{name}:records:record:rec-1"
    rec2 = args.bad_edge_dst or f"{name}:records:record:rec-2"

    with psycopg.connect(dsn, autocommit=True) as conn:
        store = ChronosGraphStore.from_connection(conn)
        base_generation = _head(store, name)
        before = _node_ids(store)
        note(f"default head={base_generation} nodes={len(before)}")

        # --- Fork 1: a valid node, merged back through the gate.
        good_fork = _fork(name, dsn, env, "valid")
        new_node = f"{name}:records:record:reviewer-{uuid.uuid4().hex[:8]}"
        store.apply_facts(
            [NodeFact(
                new_node,
                record_subtype,
                {"promote_intent": "durable", "title": "Reviewer fork record"},
                {"id": new_node},
                {"v": 1},
            )],
            branch=good_fork, source="akmon-reviewer.fork", strict_endpoints=True,
        )
        note(f"fork {good_fork}: wrote node {new_node} ({record_subtype})")
        merged = merge_back(
            store, good_fork, dsn=dsn, base_generation_id=base_generation,
            decider=decider, deployment_name=name,
        )
        note(
            f"merge_back({good_fork}) -> status={merged.status} "
            f"generation={merged.generation_id} blocked_reason={merged.blocked_reason}"
        )
        after_merge = _node_ids(store)
        merged_head = _head(store, name)
        if merged.status != "merged":
            fail("fork-merge-readmission", "valid fork did not merge",
                 status=merged.status, blocked_reason=merged.blocked_reason)
        if new_node not in after_merge:
            fail("fork-merge-readmission", "merged node is not readable on default",
                 node=new_node)
        if after_merge - before != {new_node}:
            fail("fork-merge-readmission", "default changed by more than the merged node",
                 added=sorted(after_merge - before), removed=sorted(before - after_merge))
        if merged_head == base_generation:
            fail("fork-merge-readmission", "merge published no new default generation",
                 head=merged_head)
        note(f"default head={merged_head} nodes={len(after_merge)} (+1: {new_node})")

        # --- Fork 2: a narrowing-violating edge, blocked at the gate.
        bad_fork = _fork(name, dsn, env, "violating")
        store.apply_facts(
            [EdgeFact(
                rec1, rec2, args.bad_edge_type,
                attrs={"promote_intent": "durable"},
                source_record_id={"id": f"reviewer-edge-{uuid.uuid4().hex[:8]}"},
                source_revision={"v": 1},
            )],
            branch=bad_fork, source="akmon-reviewer.fork", strict_endpoints=True,
        )
        note(f"fork {bad_fork}: wrote edge {rec1} -{args.bad_edge_type}-> {rec2}")
        blocked = merge_back(
            store, bad_fork, dsn=dsn, base_generation_id=merged_head,
            decider=decider, deployment_name=name,
        )
        stage, gate_reason, narrowing_named = _summarize_block(blocked.blocked_reason or "")
        note(
            f"merge_back({bad_fork}) -> status={blocked.status} stage={stage} "
            f"gate_reason={gate_reason} narrowing_violation_named={narrowing_named}"
        )
        after_block = _node_ids(store)
        blocked_head = _head(store, name)
        illegal_edges = _edge_count(store, rec1, rec2, args.bad_edge_type)
        if blocked.status != "blocked" or not blocked.blocked_reason:
            fail("fork-merge-readmission", "violating fork was not blocked with a reason",
                 status=blocked.status, blocked_reason=blocked.blocked_reason)
        if not narrowing_named:
            fail("fork-merge-readmission",
                 "the block does not name the narrowing violation",
                 stage=stage, gate_reason=gate_reason)
        if after_block != after_merge or blocked_head != merged_head:
            fail("fork-merge-readmission", "blocked merge changed default",
                 head_before=merged_head, head_after=blocked_head)
        if illegal_edges:
            fail("fork-merge-readmission", "forbidden edge reached default",
                 edges=illegal_edges)
        note(f"default head={blocked_head} unchanged, forbidden edges on default=0")

    if stage.startswith("land_"):
        caught = "caught_at_landing_and_rolled_back"
        note(
            "the violation was caught at landing and rolled back, not by the "
            "staging gate (issue #2865)"
        )
    else:
        caught = f"caught_at_{stage}"
    passed(
        "CLAIM fork-merge-readmission",
        merged=merged_head,
        violation=caught,
    )
    return 0


if __name__ == "__main__":
    run_main("fork-merge-readmission", main)
