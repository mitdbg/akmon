"""The leakage audit — validity lives or dies here (protocol §Leakage).

Per-instance, machine-checkable, fail-closed. Three assertions against a
PUBLISHED generation (the graph the okg arm will actually read):

(a) zero nodes whose semantic timestamp postdates the cutoff — checked over
    every timestamp-bearing attr the git/github sources emit
    (created_at, authored_at, committed_at, updated_at, closed_at,
    merged_at, last_modified_at). ``observed_at`` is ingest time, not
    source time, and is deliberately NOT the audited column. Facts from the
    ``agent_sessions`` source are EXEMPT by protocol: the graph for task k
    may contain artifacts from tasks 1..k-1 (sessions, attempted patches,
    outcomes) — that accumulation is the thing being measured, and it
    necessarily postdates the repo cutoff. Everything else (repo history,
    issues, PRs) stays bound by the cutoff.
(b) zero facts referencing the gold PR number (issue/pull_request nodes).
(c) the corpus checkout's ref surface is still the cutoff: exactly one
    branch ref parked at base_commit (re-verified, not trusted).

Reads go through ``ChronosGraphStore.checkout_generation`` — the substrate's
supported Python read path for chronos-native storage (bare SQL over
``okg.v_nodes`` is refused by ``okg.require_graph_checkout``); the audit
reads exactly the pinned generation the okg arm will read.

Any violation raises LeakageViolation with the offending rows.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

from .checkouts import verify_ref_surface
from .errors import LeakageViolation

TIMESTAMP_ATTRS = (
    "created_at",
    "authored_at",
    "committed_at",
    "updated_at",
    "closed_at",
    "merged_at",
    "last_modified_at",
)

_MAX_REPORTED_ROWS = 20
# The agent-sessions emitter stamps its facts source="agent_sessions".
SESSION_SOURCE_EXEMPT = "agent_sessions"


def audit(
    *,
    dsn: str,
    generation_id: str,
    cutoff: str,
    gold_pr_number: int | None,
    checkout: Path,
    base_commit: str,
) -> dict[str, Any]:
    """Run all three assertions. Returns the audit report on pass; raises
    LeakageViolation on any hit."""
    verify_ref_surface(checkout, base_commit)

    from okg.substrate.graph_store import ChronosGraphStore

    store = ChronosGraphStore.connect(dsn, ensure_schema=False)
    try:
        graph = store.checkout_generation(generation_id)
        violations: list[dict[str, Any]] = []
        violations.extend(_post_cutoff_nodes(graph, cutoff))
        if gold_pr_number is not None:
            violations.extend(_gold_pr_references(graph, gold_pr_number))
        node_count = _node_count(graph)
    finally:
        store.close()
    if node_count == 0:
        raise LeakageViolation(
            f"generation {generation_id} has zero nodes — an empty corpus is a "
            "failed build, not a clean one"
        )
    if violations:
        shown = violations[:_MAX_REPORTED_ROWS]
        raise LeakageViolation(
            f"leakage audit FAILED for generation {generation_id} "
            f"(cutoff {cutoff}, gold PR {gold_pr_number}): "
            f"{len(violations)} violating fact(s); first {len(shown)}: {shown}"
        )
    return {
        "generation_id": generation_id,
        "cutoff": cutoff,
        "gold_pr_number": gold_pr_number,
        "node_count": node_count,
        "post_cutoff_nodes": 0,
        "gold_pr_references": 0,
    }


def _post_cutoff_nodes(graph: Any, cutoff: str) -> list[dict[str, Any]]:
    """Nodes in the pinned generation with any semantic timestamp >= cutoff.

    Timestamps are ISO-8601 UTC strings from the sources, so lexicographic
    comparison is temporal comparison."""
    clauses = " OR ".join(
        f"(attrs ? '{attr}' AND (attrs->>'{attr}') >= %(cutoff)s)"
        for attr in TIMESTAMP_ATTRS
    )
    rows = graph.query(
        "SELECT node_id, subtype, attrs FROM okg.graph_nodes "
        f"WHERE ({clauses}) "
        "AND source IS DISTINCT FROM %(exempt)s",
        {"cutoff": cutoff, "exempt": SESSION_SOURCE_EXEMPT},
    )
    out = []
    for row in rows:
        attrs = row["attrs"] or {}
        stamps = {
            a: attrs.get(a)
            for a in TIMESTAMP_ATTRS
            if attrs.get(a) and str(attrs[a]) >= cutoff
        }
        out.append(
            {"kind": "post_cutoff", "node_id": row["node_id"],
             "subtype": row["subtype"], "timestamps": stamps}
        )
    return out


def _gold_pr_references(graph: Any, gold_pr_number: int) -> list[dict[str, Any]]:
    """Nodes carrying the gold PR's number.

    `forge_comment` is in scope with its own `parent_number` attr: the
    comments source carries the gold PR's whole thread, and a comment is a
    first-class retrieval candidate the benchmark would score. Keying only
    on `subtype IN ('issue','pull_request')` — the shape before PACT
    okg-corpus-repo-history — would have let every gold-thread comment
    through an audit that reports PASS.
    """
    rows = graph.query(
        "SELECT node_id, subtype FROM okg.graph_nodes "
        "WHERE subtype IN ('issue', 'pull_request', 'forge_comment') "
        "AND ((attrs->>'number') = %(num_s)s "
        "  OR (attrs->>'issue_number') = %(num_s)s "
        "  OR (attrs->>'parent_number') = %(num_s)s)",
        {"num_s": str(gold_pr_number)},
    )
    return [
        {"kind": "gold_pr_reference", "node_id": row["node_id"],
         "subtype": row["subtype"], "number": gold_pr_number}
        for row in rows
    ]


def _node_count(graph: Any) -> int:
    rows = graph.query("SELECT count(*) AS n FROM okg.graph_nodes")
    return int(rows[0]["n"])
