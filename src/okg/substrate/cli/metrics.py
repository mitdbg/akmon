"""CLI handlers for `okg metrics`.

Extracted from ``cli/main.py`` by ``extract-cli-metrics`` (2026-05).
Subcommands: qa-snapshot, default metrics dump. cli.main helpers
loaded lazily via the shared ``_load_main_helpers()`` pattern
(per Wave D-1), with the Wave-D-12 None-guard so test
monkeypatches survive the first invocation.
"""
from __future__ import annotations

import argparse
import os
import sys
from typing import Any


# Module-level helper bindings, populated by _load_main_helpers().
_deployment_embeddings_enabled: Any = None
_emit_error: Any = None
_emit_ok: Any = None
_resolve_dsn: Any = None
SubstrateError: Any = None


def _load_main_helpers() -> None:
    """Lazily import ``cli.main`` helpers + ``SubstrateError``."""
    global _deployment_embeddings_enabled
    global _emit_error, _emit_ok
    global _resolve_dsn, SubstrateError
    from okg.substrate.cli.main import (
        _deployment_embeddings_enabled as _dee,
        _emit_error as _e,
        _emit_ok as _o,
        _resolve_dsn as _r,
    )
    from okg.substrate.errors import SubstrateError as _se
    # None-guard pattern: only populate slots still unset, so
    # test monkeypatches survive first invocation.
    if _deployment_embeddings_enabled is None:
        _deployment_embeddings_enabled = _dee
    if _emit_error is None:
        _emit_error = _e
    if _emit_ok is None:
        _emit_ok = _o
    if _resolve_dsn is None:
        _resolve_dsn = _r
    if SubstrateError is None:
        SubstrateError = _se


_SINCE_UNITS = {"s": 1, "m": 60, "h": 3600, "d": 86400}


def _parse_since(s: str) -> int:
    """Parse a duration like '1h', '24h', '7d' into seconds.

    Raises ``SubstrateError`` on invalid input.
    """
    _load_main_helpers()
    s = (s or "").strip().lower()
    if not s or s[-1] not in _SINCE_UNITS or not s[:-1].isdigit():
        raise SubstrateError(
            f"invalid --since value {s!r}; expected like '1h', "
            f"'24h', '7d'",
            fix_hint="--since 1h | 24h | 7d",
        )
    return int(s[:-1]) * _SINCE_UNITS[s[-1]]


def _cmd_metrics_qa_snapshot(
    args: argparse.Namespace, *, as_json: bool
) -> int:
    """`okg metrics --qa-snapshot --deployment <name> [--generation N]`
    — emit one ``QAGateMetrics`` JSON object."""
    _load_main_helpers()
    deployment = getattr(args, "deployment", None) or os.environ.get(
        "OKG_DEPLOYMENT"
    )
    if not deployment:
        return _emit_error(
            SubstrateError(
                "okg metrics --qa-snapshot requires --deployment "
                "(or OKG_DEPLOYMENT) — every metric is per-deployment "
                "scoped",
                fix_hint="okg metrics --qa-snapshot --deployment "
                         "<name> --json",
            ),
            as_json=as_json,
        )

    try:
        dsn = _resolve_dsn(args)
    except SubstrateError as err:
        return _emit_error(err, as_json=as_json)

    from okg.substrate.qa.metrics import snapshot

    try:
        ignored_job_kinds: set[str] = set()
        from okg.substrate.deployments import find_deployment_dir
        dep_dir = find_deployment_dir(deployment)
        if dep_dir is not None and not _deployment_embeddings_enabled(dep_dir):
            ignored_job_kinds.add("embed")
        metrics = snapshot(
            dsn=dsn,
            deployment=deployment,
            generation_id=getattr(args, "generation", None),
            ignored_job_kinds=ignored_job_kinds,
        )
    except RuntimeError as exc:
        return _emit_error(
            SubstrateError(
                f"qa-snapshot failed: {exc}",
                fix_hint=(
                    "verify deployment has a published generation "
                    "(`okg status --deployment <name>`); confirm "
                    "DSN is reachable"
                ),
            ),
            as_json=as_json,
        )
    except Exception as exc:  # noqa: BLE001 — surface unexpected
        return _emit_error(
            SubstrateError(
                f"qa-snapshot failed: {exc}",
                fix_hint="check DSN connectivity and migration state",
            ),
            as_json=as_json,
        )

    payload = {
        "subcmd": "metrics",
        "mode": "qa-snapshot",
        **metrics.to_dict(),
    }
    if as_json:
        _emit_ok(payload, as_json=True)
    else:
        _emit_ok(payload, as_json=False)
    return 0


def _cmd_metrics_disk(args: argparse.Namespace, *, as_json: bool) -> int:
    """`okg metrics --disk [--deep] [--top N] [--deployment <name>]` —
    disk-usage report: total size, top relations (heap/index), dead-tuple %,
    TimescaleDB compressed-chunk ratio, unused-index footprint, and marginal
    bytes-per-generation (PACT ``bound-okg-workspace-disk-growth``, task 1).

    Cheap catalog reads by default; ``--deep`` runs pgstattuple_approx (scans,
    slow) for ground-truth dead-tuple percentages.
    """
    _load_main_helpers()
    from okg.substrate.db.management import disk_usage_report

    try:
        dsn = _resolve_dsn(args)
    except SubstrateError as err:
        return _emit_error(err, as_json=as_json)

    deployment = getattr(args, "deployment", None) or os.environ.get(
        "OKG_DEPLOYMENT",
    )
    try:
        payload = disk_usage_report(
            dsn,
            deployment_name=deployment,
            top_limit=int(getattr(args, "top", 20) or 20),
            deep=bool(getattr(args, "deep", False)),
        )
    except Exception as exc:
        return _emit_error(
            SubstrateError(
                f"okg metrics --disk failed: {exc}",
                fix_hint="ensure the DSN is reachable and migrated (okg "
                         "migrate); --deep needs the pgstattuple extension "
                         "(superuser to install it once)",
            ),
            as_json=as_json,
        )

    from okg.substrate.cli._output import payload_verdict_exit

    out, code = payload_verdict_exit(
        {"subcmd": "metrics", "mode": "disk", **payload}, source="metrics_disk",
    )
    _emit_ok(out, as_json=as_json)
    return code


def _cmd_metrics_consistency(
    args: argparse.Namespace, *, as_json: bool
) -> int:
    """`okg metrics --consistency --deployment <name> [--generation N]` —
    the per-update closed-taxonomy fallback-cause distribution from
    ``okg.work_ledger`` (PACT ``consistency-maintenance-accounting``,
    req.accounting.fallback-taxonomy). Surfaces the aggregate cause histogram,
    the per-(generation, mode) distribution, and any LOUD instrumentation gaps."""
    _load_main_helpers()
    from okg.substrate.db.connection import connect as db_connect

    deployment = getattr(args, "deployment", None) or os.environ.get(
        "OKG_DEPLOYMENT"
    )
    if not deployment:
        return _emit_error(
            SubstrateError(
                "okg metrics --consistency requires --deployment "
                "(or OKG_DEPLOYMENT) — the work ledger is per-deployment scoped",
                fix_hint="okg metrics --consistency --deployment <name> --json",
            ),
            as_json=as_json,
        )
    try:
        dsn = _resolve_dsn(args)
    except SubstrateError as err:
        return _emit_error(err, as_json=as_json)

    try:
        from okg.substrate.telemetry.work_ledger import work_ledger_distribution

        with db_connect(dsn) as conn:
            payload = work_ledger_distribution(
                conn, deployment,
                generation_id=getattr(args, "generation", None),
            )
    except Exception as exc:  # noqa: BLE001 — surface unexpected
        return _emit_error(
            SubstrateError(
                f"okg metrics --consistency failed: {exc}",
                fix_hint="check that schema is migrated (okg migrate); ensure "
                         "DSN is reachable and the work ledger has rows "
                         "(run the equivalence-series harness)",
            ),
            as_json=as_json,
        )

    if as_json:
        _emit_ok({"subcmd": "metrics", "mode": "consistency", **payload},
                 as_json=True)
        return 0

    from okg.substrate.telemetry.fallback_taxonomy import (
        FALLBACK_CAUSE_DESCRIPTIONS,
    )

    aggregate = payload.get("aggregate_cause_distribution") or {}
    gaps = payload.get("gaps") or []
    sys.stdout.write(
        f"work-ledger fallback-cause distribution — deployment {deployment!r}"
        + (f" generation {payload['generation_id']}"
           if payload.get("generation_id") else "")
        + f"  ({payload.get('row_count', 0)} ledger rows)\n"
    )
    if not aggregate:
        sys.stdout.write(
            "  no classified causes recorded yet "
            "(run the equivalence-series harness to populate the ledger)\n"
        )
    else:
        width = max((len(c) for c in aggregate), default=8)
        for cause, count in sorted(
            aggregate.items(), key=lambda kv: (-kv[1], kv[0])
        ):
            desc = FALLBACK_CAUSE_DESCRIPTIONS.get(cause, "")
            sys.stdout.write(
                f"  {cause.ljust(width)}  {str(count).rjust(6)}   {desc}\n"
            )
    if gaps:
        sys.stdout.write(
            f"  LOUD GAPS ({len(gaps)} uninstrumented category rows):\n"
        )
        for gap in gaps:
            sys.stdout.write(
                f"    gen {gap.get('generation_id')} "
                f"[{gap.get('mode')}] {gap.get('category')}: "
                f"{gap.get('gap_reason')}\n"
            )
    return 0


def _cmd_metrics(args: argparse.Namespace, *, as_json: bool) -> int:
    """`okg metrics --llm [--since 24h] [--deployment <name>]` —
    roll-up of ``okg.llm_calls`` (per
    ``add-substrate-llm-call-tracking``).

    `okg metrics --qa-snapshot --deployment <name> [--generation N]`
    — emit one ``QAGateMetrics`` JSON object for the deployment's
    latest published generation (per the ``deployment-qa``
    capability, proposal ``qa-stabilize-main``).
    """
    _load_main_helpers()
    from okg.substrate.db.connection import connect as db_connect

    qa_snapshot = bool(getattr(args, "qa_snapshot", False))
    llm = bool(getattr(args, "llm", False))
    disk = bool(getattr(args, "disk", False))
    consistency = bool(getattr(args, "consistency", False))

    if sum((qa_snapshot, llm, disk, consistency)) > 1:
        return _emit_error(
            SubstrateError(
                "okg metrics mode flags are mutually exclusive",
                fix_hint=(
                    "run --llm OR --consistency OR --qa-snapshot OR --disk as "
                    "separate invocations (source-sync metrics moved to "
                    "`okg sync health`)"
                ),
            ),
            as_json=as_json,
        )

    if disk:
        return _cmd_metrics_disk(args, as_json=as_json)

    if qa_snapshot:
        return _cmd_metrics_qa_snapshot(args, as_json=as_json)

    if consistency:
        return _cmd_metrics_consistency(args, as_json=as_json)

    if not llm:
        return _emit_error(
            SubstrateError(
                "okg metrics requires a mode flag "
                "(--llm, --consistency, --qa-snapshot, or --disk)",
                fix_hint=(
                    "okg metrics --llm [--since 24h]  OR  "
                    "okg metrics --consistency --deployment <name>  OR  "
                    "okg metrics --qa-snapshot --deployment <name>  OR  "
                    "okg metrics --disk --deployment <name>  "
                    "(source-sync metrics moved to `okg sync health`)"
                ),
            ),
            as_json=as_json,
        )

    try:
        seconds = _parse_since(getattr(args, "since", "24h") or "24h")
    except SubstrateError as err:
        return _emit_error(err, as_json=as_json)

    try:
        dsn = _resolve_dsn(args)
    except SubstrateError as err:
        return _emit_error(err, as_json=as_json)

    deployment = getattr(args, "deployment", None) or os.environ.get(
        "OKG_DEPLOYMENT",
    )

    # One shared read (issue #2573 slice 2, closes #1739): an unpriced call is
    # counted, never summed as $0, and a group with one reads "cost unknown".
    from okg.substrate.telemetry.llm_usage import (
        LlmUsageFilters,
        llm_usage_summary,
    )

    filters = LlmUsageFilters(deployment=deployment, since_seconds=seconds)
    try:
        with db_connect(dsn) as conn:
            with conn.cursor() as cur:
                groups = llm_usage_summary(
                    cur,
                    filters=filters,
                    group_by=("deployment_name", "caller", "model"),
                )
                (total,) = llm_usage_summary(cur, filters=filters)
    except Exception as exc:
        return _emit_error(
            SubstrateError(
                f"okg metrics --llm failed: {exc}",
                fix_hint="check that schema is migrated "
                         "(okg migrate); ensure DSN is reachable",
            ),
            as_json=as_json,
        )

    # Highest priced spend first; among equals, the group with more unpriced
    # calls first, so an all-unpriced group does not sink to the bottom as $0.
    groups.sort(key=lambda g: (
        -g.priced_cost_usd, -g.unpriced_calls,
        g.group["caller"], g.group["model"],
    ))
    rollup = []
    for usage in groups:
        row = usage.as_payload()
        row["deployment"] = row.pop("deployment_name") or None
        rollup.append(row)
    totals = total.as_payload()
    human = [(usage, _llm_cells(usage)) for usage in groups]

    if as_json:
        _emit_ok({
            "subcmd": "metrics",
            "mode": "llm",
            "since_seconds": seconds,
            "deployment": deployment,
            "rows": rollup,
            "totals": totals,
        }, as_json=True)
        return 0

    if not rollup:
        sys.stdout.write(
            "no LLM calls in the requested window "
            f"(since {getattr(args, 'since', '24h')}"
            + (f", deployment={deployment}" if deployment else "")
            + ")\n"
        )
        return 0

    total_cells = _llm_cells(total)
    cost_w = max(12, *(len(cells[2]) for _usage, cells in human), len(total_cells[2]))
    caller_w = max(8, max(len(r["caller"]) for r in rollup))
    model_w = max(6, max(len(r["model"]) for r in rollup))
    header = (
        f"{'caller'.ljust(caller_w)}  "
        f"{'model'.ljust(model_w)}  "
        f"{'calls'.rjust(7)}  "
        f"{'prompt_tok'.rjust(12)}  "
        f"{'compl_tok'.rjust(12)}  "
        f"{'cost_usd'.rjust(cost_w)}"
    )
    sys.stdout.write(header + "\n")
    sys.stdout.write("-" * len(header) + "\n")
    for usage, (prompt_cell, completion_cell, cost_cell) in human:
        sys.stdout.write(
            f"{usage.group['caller'].ljust(caller_w)}  "
            f"{usage.group['model'].ljust(model_w)}  "
            f"{str(usage.calls).rjust(7)}  "
            f"{prompt_cell.rjust(12)}  "
            f"{completion_cell.rjust(12)}  "
            f"{cost_cell.rjust(cost_w)}\n"
        )
    sys.stdout.write("-" * len(header) + "\n")
    sys.stdout.write(
        f"{'TOTAL'.ljust(caller_w)}  "
        f"{''.ljust(model_w)}  "
        f"{str(total.calls).rjust(7)}  "
        f"{total_cells[0].rjust(12)}  "
        f"{total_cells[1].rjust(12)}  "
        f"{total_cells[2].rjust(cost_w)}\n"
    )
    return 0


def _llm_cells(usage: Any) -> tuple[str, str, str]:
    """Prompt tokens, completion tokens and cost as a person reads them.

    A token count with a missing row reads ``unknown (<n> missing)``; a cost
    with an unpriced call reads ``cost unknown: N of M unpriced (priced $X)``
    (decision D3 of #2573), never a complete-looking dollar figure.
    """
    from okg.substrate.telemetry.llm_usage import format_cost_usd

    def tokens(measure: str) -> str:
        missing = int(usage.missing_tokens.get(measure, 0))
        if missing:
            return f"unknown ({missing} missing)"
        return f"{int(usage.recorded_tokens.get(measure, 0)):,}"

    return (
        tokens("prompt_tokens"),
        tokens("completion_tokens"),
        format_cost_usd(usage.calls, usage.unpriced_calls, usage.priced_cost_usd),
    )
