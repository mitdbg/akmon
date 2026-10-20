"""MCP tools for source registry administration
(`agent-led-mcp-onboarding` Phase 4).

Surfaces agent-callable verbs that drive the deployment's
single setup path over `source_registry.yaml`:

  - `list_sources(deployment)` — enumerate sources + status.
  - `add_source(deployment, short_name, params, *, dry_run=False)`
    — low-level recovery hook for one source. The normal operator
    setup path is `propose_setup` → `commit_setup`; credentialed
    connector sources return `needs_connection` rather than
    per-source credential instructions.
  - `remove_source(deployment, short_name)` — same shape as
    `okg remove <source>`.
  - `source_status(deployment, short_name)` — inspect one source:
    connection/config state, last ingest watermark, node + edge counts.

These tools return state-machine envelopes:

  {status: "<verb-specific>", ...verb fields..., agent_hint?: str}
"""
from __future__ import annotations

from pathlib import Path
from typing import Any, Mapping, Optional

import psycopg
from okg.substrate import fastyaml

# SOURCE_CATALOG + family rank + the three frozensets were extracted
# to `mcp.source_catalog` by `extract-mcp-source-catalog` (2026-05).
# Re-export them as attributes on this module so any
# `source_admin.SOURCE_CATALOG` access in tests / external code
# keeps working.
from okg.substrate.mcp.source_catalog import (  # noqa: F401, E402
    SOURCE_CATALOG,
    _CREDENTIAL_SOURCE_IDS,
    _DEEP_INDEXING_SOURCE_IDS,
    _LOCAL_PROJECT_SOURCE_IDS,
    _SOURCE_FAMILY_RANK,
    _load_catalog_from_wizards,
)


__all__ = [
    "SOURCE_CATALOG",
    "_CREDENTIAL_SOURCE_IDS",
    "_DEEP_INDEXING_SOURCE_IDS",
    "_LOCAL_PROJECT_SOURCE_IDS",
    "_SOURCE_FAMILY_RANK",
    "ADMISSION_ADVERSE_STATUSES",
    "ADMISSION_REFUSAL_STATUSES",
    "ADMISSION_UNDECIDED_STATUSES",
    "_load_catalog_from_wizards",
    "connector_ingest_batch",
    "connector_route_status",
    "_credentialed_connector_row",
    "discover_sources",
    "propose_setup",
    "add_source",
    "list_sources",
    "remove_source",
    "source_status",
]


def _repo_root_from_cwd() -> Path:
    """Locate the substrate repo root from CWD (where the MCP
    server runs). Mirrors `install._find_repo_root` semantics."""
    cur = Path.cwd().resolve()
    for parent in (cur, *cur.parents):
        if (parent / "pyproject.toml").is_file() and (
            parent / "deployments"
        ).is_dir():
            return parent
    return cur


def _deployment_dir(deployment: str, *, repo_root: Optional[Path] = None) -> Path:
    from okg.substrate.deployments import (
        DeploymentResolutionError,
        find_deployment_dir,
    )
    try:
        resolved = find_deployment_dir(deployment)
    except DeploymentResolutionError as e:
        raise FileNotFoundError(str(e)) from e
    if resolved is not None:
        return resolved
    return (repo_root or _repo_root_from_cwd()) / "deployments" / deployment


def _load_registry_yaml(deployment: str) -> tuple[Path, dict]:
    dep_dir = _deployment_dir(deployment)
    path = dep_dir / "source_registry.yaml"
    if not path.is_file():
        raise FileNotFoundError(
            f"source_registry.yaml not found for deployment "
            f"{deployment!r} at {path}"
        )
    raw = fastyaml.safe_load(path.read_text()) or {}
    return path, raw


def _watermark_for(
    conn: psycopg.Connection, ownership_id: str,
) -> dict:
    """Read last-ingest fields for one source. The watermark table
    is keyed by full `ownership_id` (e.g. `dogfood3.agent_sessions`),
    NOT by the bare source_id from the YAML. Returns {} if no
    watermark row exists yet — that's the normal state for a
    source that hasn't run."""
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT last_reconcile_at, last_applied_rev "
                "FROM okg.source_watermarks WHERE source = %s",
                (ownership_id,),
            )
            row = cur.fetchone()
    except psycopg.errors.UndefinedTable:
        return {}
    if not row:
        return {}
    last_reconcile, last_applied = row
    out: dict[str, Any] = {}
    if last_reconcile is not None:
        out["last_ingest_at"] = last_reconcile.isoformat()
    if last_applied is not None:
        out["last_applied_rev"] = last_applied
    return out


def _counts_for(
    conn: psycopg.Connection, ownership_id: str,
) -> dict:
    """Row counts per source. `node_facts.source` + `edge_facts.source`
    are top-level columns keyed by the full ownership_id
    (e.g. `dogfood3.agent_sessions`). Returns count_available=False rather
    than raising when the legacy fact log is absent or intentionally hidden
    from the caller.
    """
    out = {"node_count": 0, "edge_count": 0, "count_available": True}

    def _read_counts() -> None:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT COUNT(*) FROM okg.node_facts WHERE source = %s",
                (ownership_id,),
            )
            row = cur.fetchone()
            if row:
                out["node_count"] = int(row[0] or 0)
            cur.execute(
                "SELECT COUNT(*) FROM okg.edge_facts WHERE source = %s",
                (ownership_id,),
            )
            row = cur.fetchone()
            if row:
                out["edge_count"] = int(row[0] or 0)

    try:
        transaction = getattr(conn, "transaction", None)
        if callable(transaction):
            with transaction():
                _read_counts()
        else:
            _read_counts()
    except (
        psycopg.errors.InsufficientPrivilege,
        psycopg.errors.UndefinedTable,
        psycopg.errors.UndefinedColumn,
    ):
        out["count_available"] = False
    return out


def _last_run_counts(
    conn: psycopg.Connection, deployment: str, ownership_id: str,
) -> Optional[dict]:
    """Authoritative per-run append counts from the SourceRunner's
    persisted `sync.last_run` payload in `okg.source_progress`
    (schema `okg.source_runner.last_run.v1`).

    This is the only count surface the runner writes on every
    write path — including Chronos-native deployments, where the
    legacy `node_facts` / `edge_facts` logs that `_counts_for`
    reads are empty by design (the source of the "ingested 0
    rows while 84 papers landed" miscount,
    `fix-source-ingest-count-attribution`).

    Returns None when the row is absent or unparseable. Callers
    MUST treat None as "accounting unavailable" and fall back to
    an envelope without counts — never as zero.
    """
    import json

    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT value FROM okg.source_progress "
                "WHERE deployment = %s AND source_name = %s "
                "AND key = 'sync.last_run'",
                (deployment, ownership_id),
            )
            row = cur.fetchone()
    except psycopg.errors.UndefinedTable:
        return None
    if not row or not row[0]:
        return None
    try:
        payload = json.loads(row[0])
    except (ValueError, TypeError):
        return None
    if not isinstance(payload, dict) or "run_id" not in payload:
        return None
    try:
        return {
            "run_id": str(payload["run_id"]),
            "nodes_appended": int(payload.get("nodes_appended") or 0),
            "edges_appended": int(payload.get("edges_appended") or 0),
        }
    except (ValueError, TypeError):
        return None


def _latest_source_progress(
    conn: psycopg.Connection, deployment: str, ownership_id: str,
) -> Optional[dict]:
    """Newest source-progress row proving runtime observation.

    `enabled_by_default` is setup/preflight state, not live source-sync
    health. Dogfood sources can be scheduler-managed while their setup default
    remains false, so source-progress is the runtime authority for whether the
    source has actually participated in sync.
    """
    import json

    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT key, value, updated_at FROM okg.source_progress "
                "WHERE deployment = %s AND source_name = %s "
                "AND (key = 'sync.last_run' "
                "     OR key LIKE 'sync.stage_timings.run.%%') "
                "ORDER BY updated_at DESC LIMIT 1",
                (deployment, ownership_id),
            )
            row = cur.fetchone()
    except psycopg.errors.UndefinedTable:
        return None
    if not row:
        return None
    key, raw_value, updated_at = row
    payload: dict[str, Any] = {}
    if raw_value:
        try:
            parsed = json.loads(raw_value)
        except (TypeError, ValueError):
            parsed = {}
        if isinstance(parsed, dict):
            payload = parsed
    return {
        "key": key,
        "updated_at": updated_at.isoformat() if updated_at else None,
        "run_id": payload.get("run_id"),
        "status": payload.get("status"),
        "run_mode": payload.get("run_mode"),
    }


def _admission_reason(verdict: Any) -> tuple[Optional[str], Optional[str]]:
    """Pull (reason code, message) out of an admission verdict payload.

    Producer-signature refusals carry a top-level `code` / `message`
    (`producer_authority._violation`). Nomos denials instead nest
    `{"nomos": {"reason_code": ...}}`, and some paths carry a `violations`
    list. All three are read here; reporting a refusal whose reason is
    `None` would hand the operator "refused, reason unknown".
    """
    import json

    if isinstance(verdict, (str, bytes)):
        try:
            verdict = json.loads(verdict)
        except (TypeError, ValueError):
            return None, None
    if not isinstance(verdict, Mapping):
        return None, None
    code = verdict.get("code") or verdict.get("reason_code")
    message = verdict.get("message")
    if code is None:
        violations = verdict.get("violations")
        if isinstance(violations, list) and violations:
            first = violations[0]
            if isinstance(first, Mapping):
                code = first.get("code") or first.get("reason_code")
                message = message or first.get("message")
    if code is None:
        nomos = verdict.get("nomos")
        if isinstance(nomos, Mapping):
            code = nomos.get("reason_code") or nomos.get("code")
            message = message or nomos.get("message")
    if code is None:
        # A blocked generation stamps the block reason, not a refusal code.
        blocked = verdict.get("blocked_reason")
        if blocked:
            code = blocked
    return (
        str(code) if code is not None else None,
        str(message) if message is not None else None,
    )


def _last_admission_outcome(
    conn: psycopg.Connection,
    deployment: str,
    ownership_id: str,
    producer_id: Optional[str] = None,
    producer_kind: str = "source",
) -> dict:
    """Latest admission outcome for one source, from `okg.admission_envelopes`.

    A refused admission writes ONLY here. It writes no source-run row, no
    watermark, and no `node_facts` / `edge_facts` rows, so a source whose
    every admission is refused looks identical to a healthy idle source on
    every other surface. That false negative is what let
    `okg-workspace.latex_project` contribute nothing for its whole enrolment
    without an operator-visible signal (issue #1323).

    Envelopes match on `producer_id`, OR on `source` when the row's
    `producer_kind` is the source's own. Both arms are needed and both are
    narrow:

      - A registry entry may declare `admission_policy.producer_id`
        independently of its `ownership_id` (nine sources in one
        production registry do), and ingest stamps `producer_id` from that policy, so
        matching on `ownership_id` alone returns nothing for them.
      - `source` alone is worse than useless: `entity_merge`,
        `source_retirement` and `source_lineage_repair` all stamp
        `source = <the affected source>` while owning `producer_kind:
        "system"`. Without the kind guard, a failed merge batch would become
        the source's newest "refusal", and the operator would be told the
        source wrote no rows -- exactly the misdirection this change removes.

    The returned `producer_id` is the one the ROW carries, never the one asked
    for, so the report cannot attribute a verdict to the wrong producer.

    Returns one of:
      - `{"authority": "unavailable", "reason": ...}` when the envelope table
        cannot be read. Callers MUST NOT read this as "no refusals":
        an unreadable surface is not evidence of health.
      - `{"authority": "admission_envelopes", "envelope_count": 0, ...}` when
        the source has never been admitted.
      - `{"authority": "admission_envelopes", "status": ..., "reason_code":
        ..., ...}` for the newest envelope, with lifetime counts.
    """
    row: Any = None
    match_producer = producer_id or ownership_id

    def _read_envelope() -> None:
        nonlocal row
        with conn.cursor() as cur:
            cur.execute(
                "SELECT status, created_at, verdict, producer_id, "
                "count(*) OVER () AS envelope_count, "
                "count(*) FILTER (WHERE status = ANY(%s)) OVER () "
                "  AS adverse_count "
                "FROM okg.admission_envelopes "
                "WHERE deployment_name = %s "
                "  AND (producer_id = %s "
                "       OR (source = %s AND producer_kind = %s)) "
                # Newest DECIDED envelope: an envelope coalesced on its
                # idempotency key is re-transitioned in place, so ordering on
                # created_at alone can hide a fresh verdict behind an older
                # row's creation time.
                "ORDER BY GREATEST(created_at, updated_at) DESC LIMIT 1",
                (
                    list(ADMISSION_ADVERSE_STATUSES),
                    deployment,
                    match_producer,
                    ownership_id,
                    producer_kind,
                ),
            )
            row = cur.fetchone()

    try:
        # Run inside a savepoint, as `_counts_for` does. Without it an
        # UndefinedTable / InsufficientPrivilege here aborts the caller's
        # transaction, and every later read in `list_sources` fails with
        # InFailedSqlTransaction -- one unreadable surface would blank the
        # whole census. Verified against Postgres.
        transaction = getattr(conn, "transaction", None)
        if callable(transaction):
            with transaction():
                _read_envelope()
        else:
            _read_envelope()
    except (
        psycopg.errors.InsufficientPrivilege,
        psycopg.errors.UndefinedTable,
        psycopg.errors.UndefinedColumn,
    ) as exc:
        return {
            "authority": "unavailable",
            "reason": type(exc).__name__,
            "producer_id": None,
            "status": None,
            "envelope_count": None,
            "adverse_count": None,
        }
    if not row:
        return {
            "authority": "admission_envelopes",
            "producer_id": None,
            "status": None,
            "envelope_count": 0,
            "adverse_count": 0,
            "created_at": None,
            "reason_code": None,
            "reason_message": None,
        }
    (
        status, created_at, verdict, row_producer_id,
        envelope_count, adverse_count,
    ) = row[:6]
    reason_code, reason_message = _admission_reason(verdict)
    return {
        "authority": "admission_envelopes",
        # The row's own producer, so a verdict is never attributed to a
        # producer that did not receive it.
        "producer_id": (
            str(row_producer_id) if row_producer_id is not None else None
        ),
        "status": str(status) if status is not None else None,
        "envelope_count": int(envelope_count or 0),
        "adverse_count": int(adverse_count or 0),
        "created_at": (
            created_at.isoformat()
            if hasattr(created_at, "isoformat")
            else created_at
        ),
        "reason_code": reason_code,
        "reason_message": reason_message,
    }


def _preflight_status(
    source_id: str, entry: dict,
) -> str:
    """Approximate per-source status without instantiating the
    adapter. Connector-gated sources report connection readiness
    through the setup planner.

    Post-MCP-audit cleanup: docstring previously claimed 5 states
    (`active`, `awaiting_config`, `needs_connection`, `disabled`,
    `failed`); the code only returns 3 and never inspected
    `entry["params"]` despite the prior comment claiming so. The
    `deployment` argument was also unused. Aligning the
    docstring to actual behavior; trimming the dead arg.

    Returns one of: `active`, `needs_connection`, `disabled`.
    """
    if not entry.get("enabled_by_default", False):
        return "disabled"
    short = _short_name_for_source_id(source_id)
    if short in {"gmail", "google-cal", "slack"}:
        return "needs_connection"
    return "active"


def _short_name_for_source_id(source_id: str) -> Optional[str]:
    for short, (sid, _) in SOURCE_CATALOG.items():
        if sid == source_id:
            return short
    return None


# `_source_family`, `_source_priority_key` moved to
# `mcp/setup_planning.py` (their only callers are there).


# In-process LRU for plans, keyed by `plan_id`. TTL 30 minutes.
# Cross-cutting between propose_setup (in setup_planning.py)
# and commit_setup (below): the former stashes the plan, the
# latter loads it for execution.
_PLAN_TTL_SECONDS = 30 * 60
_PLANS: dict[str, tuple[float, dict]] = {}


def _stash_plan(plan: dict) -> str:
    import time as _t
    import uuid as _uuid
    plan_id = str(_uuid.uuid4())
    _PLANS[plan_id] = (_t.monotonic() + _PLAN_TTL_SECONDS, plan)
    # Bound the LRU loosely — drop any expired entries on insert. Iterate a
    # snapshot: tool calls run in worker threads (issue #3493), and another
    # call's insert would otherwise change the dict mid-iteration.
    expired = [
        k for k, (deadline, _) in list(_PLANS.items())
        if deadline < _t.monotonic()
    ]
    for k in expired:
        _PLANS.pop(k, None)
    return plan_id


def _load_plan(plan_id: str) -> Optional[dict]:
    import time as _t
    item = _PLANS.get(plan_id)
    if item is None:
        return None
    deadline, plan = item
    if deadline < _t.monotonic():
        _PLANS.pop(plan_id, None)
        return None
    return plan


def _format_count_summary(counts: dict) -> str:
    """One-line ≤80-char descriptor of a source's row counts.

    Generic shape — `"<n> nodes / <m> edges"`. Cross-cutting
    formatter used by both setup_planning's propose_setup
    summaries and commit_setup's per-source summaries.
    """
    n = int(counts.get("node_count", 0) or 0)
    m = int(counts.get("edge_count", 0) or 0)
    return f"{n} nodes / {m} edges"


def _truncate_summary(s: str, max_len: int = 80) -> str:
    """Truncate to `max_len` chars total; trailing `...` if
    truncation occurred."""
    if len(s) <= max_len:
        return s
    if max_len <= 3:
        return s[:max_len]
    return s[: max_len - 3] + "..."


# ----------------------------------------------------------------
# Tool 1: list_sources
# ----------------------------------------------------------------


# `list_sources`, `add_source`, `remove_source`,
# `source_status`, `_apply_source_with_ingest`,
# `_scoped_ingest` moved to `mcp/source_lifecycle.py` by
# `extract-mcp-source-lifecycle`. Re-exported below.


def _format_row_count(nodes: int, edges: int) -> str:
    """Compact per-source row-count descriptor for use inside
    a summary's `"<names with row counts>"` clause.

    - `nodes > 0` AND `edges == 0`: `"<n> nodes"` (paper-like)
    - `nodes == 0` AND `edges > 0`: `"<n> edges"`
    - `nodes > 0` AND `edges > 0`: `"<n>n/<m>e"` (tight)
    - both zero: `"0 rows"` (empty)
    """
    n = int(nodes or 0)
    m = int(edges or 0)
    if n > 0 and m == 0:
        return f"{n} nodes"
    if n == 0 and m > 0:
        return f"{m} edges"
    if n > 0 and m > 0:
        return f"{n}n/{m}e"
    return "0 rows"


def _build_graph_delta(
    newly_wired: list[dict], max_listed: int = 3,
) -> dict:
    """Per `trim-setup-to-graph-reality`: roll up the
    newly-wired entries' row counts into a single envelope
    field so the agent can answer "did anything land?"
    without a follow-up `list_sources` call.
    """
    del max_listed  # reserved; not used yet but documents shape
    total_n = sum(int(r.get("nodes_added") or 0) for r in newly_wired)
    total_e = sum(int(r.get("edges_added") or 0) for r in newly_wired)

    with_rows: list[dict] = []
    empty: list[dict] = []
    for r in newly_wired:
        n = int(r.get("nodes_added") or 0)
        e = int(r.get("edges_added") or 0)
        if (
            r.get("status") == "ingest_complete_no_rows"
            or (n == 0 and e == 0)
        ):
            empty.append({
                "short": r["short"],
                "source_id": r["source_id"],
                "status": r.get("status"),
            })
        else:
            with_rows.append({
                "short": r["short"],
                "source_id": r["source_id"],
                "nodes_added": n,
                "edges_added": e,
            })
    return {
        "total_nodes_added": total_n,
        "total_edges_added": total_e,
        "newly_wired_with_rows": with_rows,
        "newly_wired_empty": empty,
    }


def _format_commit_summary(
    newly_wired: list[dict],
    reconfirmed: list[dict],
    failed: list[dict],
    other: list[dict],
    graph_delta: Optional[dict] = None,
    max_len: int = 80,
) -> str:
    """Build the operator-facing top-level summary for
    `commit_setup`. Format depends on outcome:

    - All zero / all reconfirmed:
        `"Nothing changed: <N> already configured."`
    - newly_wired_with_rows > 0, no empty, no failed:
        `"Wired <N> new: <names with row counts>."`
    - newly_wired_with_rows > 0, some empty, no failed:
        `"Wired <N1>: <names with rows>. <N2> empty: <names>."`
    - all newly_wired empty (no rows), no failed:
        `"Wired <N> new but empty: <names>. Re-confirmed: <M>."`
    - failed > 0:
        `"Newly wired: N. Re-confirmed: M. Failed: K."`

    Length budget enforced; names truncated with `(+X)`.
    """
    n_new = len(newly_wired)
    n_re = len(reconfirmed)
    n_fail = len(failed)
    n_other = len(other)

    # Use the rollup if the caller built one; otherwise
    # synthesize a minimal shape so legacy call sites still
    # work.
    gd = graph_delta or _build_graph_delta(newly_wired)
    with_rows = gd.get("newly_wired_with_rows") or []
    empties = gd.get("newly_wired_empty") or []

    if n_new == 0 and n_fail == 0 and (n_re > 0 or n_other == 0):
        if n_re == 0:
            return _truncate_summary("Nothing to wire.", max_len)
        return _truncate_summary(
            f"Nothing changed: {n_re} already configured.", max_len,
        )

    if n_fail == 0 and n_new > 0:
        # All-empty: every newly-wired entry produced zero rows.
        if not with_rows and empties:
            shorts = [r["short"] for r in empties]
            listed = shorts[:3]
            extra = len(empties) - len(listed)
            name_part = ", ".join(listed)
            if extra > 0:
                name_part = f"{name_part} (+{extra})"
            s = f"Wired {n_new} new but empty: {name_part}."
            if n_re:
                s += f" Re-confirmed: {n_re}."
            return _truncate_summary(s, max_len)

        # Some rows + some empty: name both buckets.
        if with_rows and empties:
            row_bits = [
                f"{r['short']} "
                f"({_format_row_count(r['nodes_added'], r['edges_added'])})"
                for r in with_rows[:3]
            ]
            row_extra = len(with_rows) - len(row_bits)
            row_part = ", ".join(row_bits)
            if row_extra > 0:
                row_part = f"{row_part} (+{row_extra})"
            empty_shorts = [r["short"] for r in empties[:3]]
            empty_extra = len(empties) - len(empty_shorts)
            empty_part = ", ".join(empty_shorts)
            if empty_extra > 0:
                empty_part = f"{empty_part} (+{empty_extra})"
            s = (
                f"Wired {len(with_rows)}: {row_part}. "
                f"{len(empties)} empty: {empty_part}."
            )
            return _truncate_summary(s, max_len)

        # All rows (no empties): name the wired sources with
        # their row counts.
        row_bits = [
            f"{r['short']} "
            f"({_format_row_count(r['nodes_added'], r['edges_added'])})"
            for r in with_rows[:3]
        ]
        extra = len(with_rows) - len(row_bits)
        name_part = ", ".join(row_bits)
        if extra > 0:
            name_part = f"{name_part} (+{extra})"
        s = f"Wired {n_new} new: {name_part}."
        if n_re:
            s += f" Re-confirmed: {n_re}."
        return _truncate_summary(s, max_len)

    # n_fail > 0 — name the bucket counts; the agent_hint /
    # per-entry summaries carry which short_name failed and
    # why.
    s = (
        f"Newly wired: {n_new}. Re-confirmed: {n_re}. "
        f"Failed: {n_fail}."
    )
    return _truncate_summary(s, max_len)


def commit_setup(
    conn: psycopg.Connection,
    plan_id: str,
) -> dict:
    """Execute a previously-proposed plan: call `add_source`
    for each `would_wire` entry, aggregate results.

    The agent's typical loop: propose_setup → relay summary →
    operator approves → commit_setup. Operator's only typed
    inputs are intent ("set me up") and approval ("yes").
    """
    plan = _load_plan(plan_id)
    if plan is None:
        return {
            "status": "plan_not_found",
            "plan_id": plan_id,
            "summary": (
                "Plan expired (30 min TTL) or MCP server restarted."
            ),
            "agent_hint": (
                "Re-run `propose_setup` to generate a fresh plan, "
                "then call `commit_setup` with the new id."
            ),
        }
    deployment = plan["deployment"]

    # Snapshot pre-call enabled state so we can categorize
    # each result as `newly_wired` (was off, now on) vs
    # `reconfirmed` (was already on). Per
    # `tighten-setup-feedback-loop`: the operator needs to
    # know whether commit_setup actually did anything new
    # vs just re-affirmed existing config.
    pre_enabled: dict[str, bool] = {}
    try:
        _path, raw = _load_registry_yaml(deployment)
        sources_block = raw.get("sources") or {}
        for source_id, entry in sources_block.items():
            short = (
                _short_name_for_source_id(source_id) or source_id
            )
            pre_enabled[short] = bool(
                entry.get("enabled_by_default", False),
            )
    except FileNotFoundError:
        pre_enabled = {}

    results: list[dict] = []
    newly_wired: list[dict] = []
    reconfirmed: list[dict] = []
    failed: list[dict] = []
    other: list[dict] = []

    # Look up add_source via this module's namespace so tests
    # that monkeypatch `okg.substrate.mcp.source_admin.add_source`
    # hit the patched value. The actual `add_source` body lives
    # in `mcp/source_lifecycle.py` (per Wave D-19) and is
    # re-exported below; the module-attribute lookup wires
    # through to whichever value the test set.
    import sys as _sys
    _self = _sys.modules[__name__]
    for entry in plan["would_wire"]:
        short = entry["short"]
        params = entry["params"]
        envelope = _self.add_source(
            conn, deployment=deployment, short_name=short,
            params=params,
        )
        status = envelope.get("status")
        was_enabled = pre_enabled.get(short, False)
        result_row = {
            "short": short,
            "source_id": entry["source_id"],
            "status": status,
            "summary": envelope.get("summary"),
            "nodes_added": envelope.get("nodes_added"),
            "edges_added": envelope.get("edges_added"),
        }
        results.append(result_row)

        if status == "ok_already_configured":
            reconfirmed.append(result_row)
        elif status in ("ingest_complete", "ingest_complete_no_rows"):
            if was_enabled:
                # Edge case: pre-enabled source whose registry
                # entry didn't carry matching params, so
                # `add_source` flowed past `ok_already_configured`
                # into a full ingest. Treat as `reconfirmed`
                # (operator's view: "you already had this").
                reconfirmed.append(result_row)
            else:
                newly_wired.append(result_row)
        elif status in (
            "activation_blocked",
            "ingest_failed",
            "rollback_failed",
        ):
            failed.append(result_row)
        else:
            # needs_connection, awaiting_params, unknown_source,
            # deployment_not_found, dry_run_* — carried but
            # not in the three named buckets.
            other.append(result_row)

    graph_delta = _build_graph_delta(newly_wired)
    summary = _format_commit_summary(
        newly_wired, reconfirmed, failed, other, graph_delta,
    )
    return {
        "status": (
            "activation_blocked"
            if any(
                row.get("status") == "activation_blocked"
                for row in failed
            )
            else "ok"
        ),
        "plan_id": plan_id,
        "deployment": deployment,
        "results": results,
        "newly_wired": newly_wired,
        "reconfirmed": reconfirmed,
        "failed": failed,
        "other": other,
        "graph_delta": graph_delta,
        "summary": summary,
    }


# ----------------------------------------------------------------
# Connector route status + agent-mediated import
# ----------------------------------------------------------------


# `connector_route_status`, `connector_ingest_batch`, and 5
# private helpers (_stable_hash, _normalize_connector_record,
# _missing_connector_allowlists, _connector_fact_rows,
# _copy_connector_rows) moved to `mcp/connector_ingest.py` by
# `extract-mcp-connector-ingest`. Re-export the public tools
# so server.py's `_source_admin.connector_*` thunks keep
# working without a server-build change.
from okg.substrate.mcp.connector_ingest import (  # noqa: F401, E402
    connector_ingest_batch,
    connector_route_status,
)


# Re-export setup_planning's MCP tools + the one helper
# that connector_ingest defer-imports.
from okg.substrate.mcp.setup_planning import (  # noqa: F401, E402
    _credentialed_connector_row,
    discover_sources,
    propose_setup,
)


# Re-export source_lifecycle's MCP tools so server.py
# wrappers + commit_setup's direct add_source call keep
# resolving via this module's attribute namespace.
from okg.substrate.mcp.source_lifecycle import (  # noqa: F401, E402
    ADMISSION_ADVERSE_STATUSES,
    ADMISSION_REFUSAL_STATUSES,
    ADMISSION_UNDECIDED_STATUSES,
    add_source,
    list_sources,
    remove_source,
    source_status,
)
