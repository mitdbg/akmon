from __future__ import annotations

import argparse
from collections.abc import Callable
from datetime import datetime, timezone
import json
import os
from typing import Any

from okg.substrate.cli._options import add_deployment_arg, add_dsn_arg


_emit_error: Any = None
_emit_ok: Any = None
_resolve_dsn: Any = None
SubstrateError: Any = None

JsonFlagAdder = Callable[[argparse.ArgumentParser], None]


def register_subparsers(
    sub: argparse._SubParsersAction,
    *,
    add_json_flag: JsonFlagAdder,
) -> None:
    p_tel = sub.add_parser(
        "telemetry",
        help="Unified AKMON telemetry metrics, reports, and benchmarks",
    )
    _add_common_args(p_tel, add_json_flag=add_json_flag)
    _add_apply_arg(p_tel)
    p_tel_sub = p_tel.add_subparsers(dest="telemetry_subcmd", required=True)

    p_tel_metrics = p_tel_sub.add_parser(
        "metrics",
        help="show unified telemetry metric catalog",
    )
    _add_common_args(
        p_tel_metrics,
        add_json_flag=add_json_flag,
        inherit_parent=True,
    )
    p_tel_metrics.add_argument(
        "--since",
        default="24h",
        help="time window for metric rollups (e.g. '1h', '24h', '7d')",
    )

    p_tel_contract = p_tel_sub.add_parser(
        "contract",
        help="show unified telemetry contract reports",
    )
    _add_common_args(
        p_tel_contract,
        add_json_flag=add_json_flag,
        inherit_parent=True,
    )
    p_tel_contract_sub = p_tel_contract.add_subparsers(
        dest="contract_subcmd",
        required=True,
    )
    p_tel_contract_report = p_tel_contract_sub.add_parser(
        "report",
        help="report the durable telemetry schema and collector contract",
    )
    _add_common_args(
        p_tel_contract_report,
        add_json_flag=add_json_flag,
        inherit_parent=True,
    )

    p_tel_bench = p_tel_sub.add_parser(
        "bench",
        help="run unified telemetry benchmarks",
    )
    _add_common_args(
        p_tel_bench,
        add_json_flag=add_json_flag,
        inherit_parent=True,
    )
    _add_apply_arg(p_tel_bench, inherit_parent=True)
    p_tel_bench_sub = p_tel_bench.add_subparsers(
        dest="bench_subcmd",
        required=True,
    )
    p_tel_bench_overhead = p_tel_bench_sub.add_parser(
        "overhead",
        help="measure always-on telemetry write overhead",
    )
    _add_common_args(
        p_tel_bench_overhead,
        add_json_flag=add_json_flag,
        inherit_parent=True,
    )
    _add_apply_arg(p_tel_bench_overhead, inherit_parent=True)
    p_tel_bench_overhead.add_argument(
        "--write-budget-ms",
        type=float,
        default=5.0,
        help="maximum allowed telemetry write latency per scenario",
    )

    p_tel_profile = p_tel_sub.add_parser(
        "profile",
        help="build or report a profile bundle for a telemetry trace",
    )
    _add_common_args(
        p_tel_profile,
        add_json_flag=add_json_flag,
        inherit_parent=True,
    )
    _add_apply_arg(p_tel_profile, inherit_parent=True)
    p_tel_profile.add_argument(
        "profile_subcmd",
        nargs="?",
        choices=["report"],
        help="use 'report' to show the latest bundle report",
    )
    p_tel_profile.add_argument("--trace-id", required=True)
    p_tel_profile.add_argument(
        "--deep",
        action="store_true",
        help="mark the bundle as deep-profile requested",
    )

    p_tel_trace = p_tel_sub.add_parser(
        "trace",
        help="list or inspect unified telemetry traces",
    )
    _add_common_args(
        p_tel_trace,
        add_json_flag=add_json_flag,
        inherit_parent=True,
    )
    p_tel_trace_sub = p_tel_trace.add_subparsers(
        dest="trace_subcmd",
        required=True,
    )
    p_tel_trace_list = p_tel_trace_sub.add_parser(
        "list",
        help="list recent telemetry traces",
    )
    _add_common_args(
        p_tel_trace_list,
        add_json_flag=add_json_flag,
        inherit_parent=True,
    )
    p_tel_trace_list.add_argument("--slow", action="store_true")
    p_tel_trace_list.add_argument("--limit", type=int, default=20)
    p_tel_trace_show = p_tel_trace_sub.add_parser(
        "show",
        help="inspect one telemetry trace",
    )
    _add_common_args(
        p_tel_trace_show,
        add_json_flag=add_json_flag,
        inherit_parent=True,
    )
    _add_apply_arg(p_tel_trace_show, inherit_parent=True)
    p_tel_trace_show.add_argument("trace_id")
    p_tel_trace_show.add_argument(
        "--include-profile",
        action="store_true",
        help=(
            "include the profile report if one has been built; "
            "build one with `okg telemetry profile`"
        ),
    )

    p_tel_coverage = p_tel_sub.add_parser(
        "coverage",
        help="report telemetry parent/child coverage and unattributed gaps",
    )
    _add_common_args(
        p_tel_coverage,
        add_json_flag=add_json_flag,
        inherit_parent=True,
    )
    _add_apply_arg(p_tel_coverage, inherit_parent=True)
    p_tel_coverage.add_argument(
        "--since",
        default="24h",
        help="fallback time window when start/end are omitted",
    )
    p_tel_coverage.add_argument("--limit", type=int)
    p_tel_coverage.add_argument("--start-at")
    p_tel_coverage.add_argument("--end-at")
    p_tel_coverage.add_argument(
        "--scope",
        choices=["system", "trace", "workflow", "generation", "source_run", "component"],
        default="system",
    )
    p_tel_coverage.add_argument("--trace-id")
    p_tel_coverage.add_argument("--workflow-id")
    p_tel_coverage.add_argument("--generation-id")
    p_tel_coverage.add_argument("--source-name")
    p_tel_coverage.add_argument("--run-id")
    p_tel_coverage.add_argument("--component")
    p_tel_coverage.add_argument(
        "--record-metrics",
        action="store_true",
        help="persist compact coverage summary facts to okg.telemetry_metrics",
    )
    p_tel_coverage.add_argument(
        "--unknown-gap-budget-ms",
        type=float,
        default=0.0,
        help="allowed unknown unattributed gap time before budget warning/failure",
    )
    p_tel_coverage.add_argument(
        "--instrumentation-debt-budget-ms",
        type=float,
        default=0.0,
        help="allowed instrumentation-debt gap time before budget warning/failure",
    )
    p_tel_coverage.add_argument(
        "--unknown-gap-budget-count",
        type=int,
        default=0,
        help="allowed unknown gap count before budget warning/failure",
    )
    p_tel_coverage.add_argument(
        "--budget-action",
        choices=["warn", "fail"],
        default="warn",
        help="whether coverage budget violations warn in output or fail the command",
    )

    p_tel_report = p_tel_sub.add_parser(
        "report",
        help="operator report for one telemetry trace",
    )
    _add_common_args(
        p_tel_report,
        add_json_flag=add_json_flag,
        inherit_parent=True,
    )
    _add_apply_arg(p_tel_report, inherit_parent=True)
    p_tel_report.add_argument("--trace-id", required=True)


def _add_common_args(
    parser: argparse.ArgumentParser,
    *,
    add_json_flag: JsonFlagAdder,
    inherit_parent: bool = False,
) -> None:
    default = argparse.SUPPRESS if inherit_parent else None
    if inherit_parent:
        try:
            add_json_flag(parser, default=argparse.SUPPRESS)
        except TypeError:
            add_json_flag(parser)
    else:
        add_json_flag(parser)
    add_deployment_arg(parser, default=default)
    add_dsn_arg(parser, default=default)


def _add_apply_arg(
    parser: argparse.ArgumentParser,
    *,
    inherit_parent: bool = False,
) -> None:
    parser.add_argument(
        "--apply",
        action="store_true",
        default=argparse.SUPPRESS if inherit_parent else False,
        help="Required for telemetry commands that write benchmark, profile, report, or coverage artifacts",
    )


def _load_main_helpers() -> None:
    global _emit_error, _emit_ok, _resolve_dsn, SubstrateError
    from okg.substrate.cli.main import (
        _emit_error as _e,
        _emit_ok as _o,
        _resolve_dsn as _r,
    )
    from okg.substrate.errors import SubstrateError as _se

    if _emit_error is None:
        _emit_error = _e
    if _emit_ok is None:
        _emit_ok = _o
    if _resolve_dsn is None:
        _resolve_dsn = _r
    if SubstrateError is None:
        SubstrateError = _se


def _deployment(args: argparse.Namespace) -> str | None:
    return getattr(args, "deployment", None) or os.environ.get("OKG_DEPLOYMENT")


def _parse_since_seconds(raw: str | None) -> int:
    from okg.substrate.cli.metrics import _parse_since

    return _parse_since(raw or "24h")


def _parse_datetime(raw: str | None) -> datetime | None:
    if not raw:
        return None
    value = raw.strip()
    if value.endswith("Z"):
        value = f"{value[:-1]}+00:00"
    parsed = datetime.fromisoformat(value)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _telemetry_requires_apply(args: argparse.Namespace) -> bool:
    """Which telemetry commands write, and so need the operator's authority.

    Neither `trace show --include-profile` nor `report` is one of them any more.
    Both used to be, because the profile report built a bundle when none
    existed, and building writes four files under the profile root and inserts
    artifact rows. Gating them made a read demand authority for a write it was
    not making -- the refusal said `mutation_requested: false` in the same
    payload that told the operator to authorize a write.

    Reading and building are now separate: showing a trace and reporting on one
    say when no profile has been built, and `okg telemetry profile` is the
    single command that builds one.
    """
    subcmd = getattr(args, "telemetry_subcmd", None)
    return (
        subcmd in {"bench", "profile"}
        or (
            subcmd == "coverage"
            and bool(getattr(args, "record_metrics", False))
        )
    )


def _telemetry_write_command(args: argparse.Namespace) -> str:
    subcmd = getattr(args, "telemetry_subcmd", None)
    if subcmd == "bench":
        return "telemetry bench overhead"
    if subcmd == "profile":
        suffix = " report" if getattr(args, "profile_subcmd", None) == "report" else ""
        return f"telemetry profile{suffix}"
    return f"telemetry {subcmd}"


def _cmd_telemetry(args: argparse.Namespace, *, as_json: bool) -> int:
    if _telemetry_requires_apply(args):
        from okg.substrate.cli.apply_boundary import missing_apply_error

        command = _telemetry_write_command(args)
        if err := missing_apply_error(
            args,
            command=command,
            fix_command=f"okg {command} --apply ...",
        ):
            _load_main_helpers()
            return _emit_error(err, as_json=as_json)
    _load_main_helpers()
    from okg.substrate.db.connection import connect as db_connect
    from okg.substrate.telemetry.catalog import (
        telemetry_contract_report,
        telemetry_metrics_catalog,
        telemetry_overhead_benchmark,
    )
    from okg.substrate.telemetry.profile import (
        build_profile_bundle,
        profile_report,
    )
    from okg.substrate.telemetry.operator import (
        operator_report,
        record_telemetry_coverage_metrics,
        telemetry_coverage,
        trace_list,
        trace_show,
    )

    deployment = _deployment(args)
    if not deployment:
        return _emit_error(
            SubstrateError(
                "okg telemetry requires --deployment or OKG_DEPLOYMENT",
                fix_hint="okg telemetry metrics --deployment <name> --json",
            ),
            as_json=as_json,
        )
    try:
        dsn = _resolve_dsn(args)
    except SubstrateError as err:
        return _emit_error(err, as_json=as_json)

    subcmd = getattr(args, "telemetry_subcmd", None)
    try:
        if subcmd == "metrics":
            since_seconds = _parse_since_seconds(getattr(args, "since", "24h"))
            with db_connect(dsn) as conn:
                payload = telemetry_metrics_catalog(
                    conn,
                    deployment=deployment,
                    since_seconds=since_seconds,
                )
        elif subcmd == "contract":
            if getattr(args, "contract_subcmd", None) != "report":
                raise SubstrateError(
                    "okg telemetry contract requires subcommand report",
                    fix_hint="okg telemetry contract report --deployment <name>",
                )
            payload = {
                "deployment": deployment,
                **telemetry_contract_report(),
            }
        elif subcmd == "bench":
            if getattr(args, "bench_subcmd", None) != "overhead":
                raise SubstrateError(
                    "okg telemetry bench requires subcommand overhead",
                    fix_hint="okg telemetry bench overhead --deployment <name>",
                )
            payload = telemetry_overhead_benchmark(
                dsn,
                deployment=deployment,
                write_budget_ms=float(getattr(args, "write_budget_ms", 100.0)),
            )
        elif subcmd == "profile":
            trace_id = getattr(args, "trace_id", None)
            if not trace_id:
                raise SubstrateError(
                    "okg telemetry profile requires --trace-id",
                    fix_hint="okg telemetry profile --deployment <name> "
                             "--trace-id <id> --json",
                )
            with db_connect(dsn, autocommit=True) as conn:
                if getattr(args, "profile_subcmd", None) == "report":
                    payload = profile_report(
                        conn,
                        deployment=deployment,
                        trace_id=trace_id,
                    )
                else:
                    payload = build_profile_bundle(
                        conn,
                        deployment=deployment,
                        trace_id=trace_id,
                        deep=bool(getattr(args, "deep", False)),
                    )
        elif subcmd == "trace":
            with db_connect(dsn) as conn:
                trace_subcmd = getattr(args, "trace_subcmd", None)
                if trace_subcmd == "list":
                    payload = trace_list(
                        conn,
                        deployment=deployment,
                        slow=bool(getattr(args, "slow", False)),
                        limit=int(getattr(args, "limit", 20)),
                    )
                elif trace_subcmd == "show":
                    payload = trace_show(
                        conn,
                        deployment=deployment,
                        trace_id=getattr(args, "trace_id"),
                        include_profile=bool(
                            getattr(args, "include_profile", False)
                        ),
                    )
                else:
                    raise SubstrateError(
                        "okg telemetry trace requires list or show",
                        fix_hint="okg telemetry trace list|show --deployment <name>",
                    )
        elif subcmd == "coverage":
            since_seconds = _parse_since_seconds(getattr(args, "since", "24h"))
            with db_connect(dsn) as conn:
                payload = telemetry_coverage(
                    conn,
                    deployment=deployment,
                    since_seconds=since_seconds,
                    limit=getattr(args, "limit", None),
                    start_at=_parse_datetime(getattr(args, "start_at", None)),
                    end_at=_parse_datetime(getattr(args, "end_at", None)),
                    scope=getattr(args, "scope", "system"),
                    trace_id=getattr(args, "trace_id", None),
                    workflow_id=getattr(args, "workflow_id", None),
                    generation_id=getattr(args, "generation_id", None),
                    source_name=getattr(args, "source_name", None),
                    run_id=getattr(args, "run_id", None),
                    component=getattr(args, "component", None),
                    unknown_gap_budget_ms=float(
                        getattr(args, "unknown_gap_budget_ms", 0.0)
                    ),
                    instrumentation_debt_budget_ms=float(
                        getattr(args, "instrumentation_debt_budget_ms", 0.0)
                    ),
                    unknown_gap_budget_count=int(
                        getattr(args, "unknown_gap_budget_count", 0)
                    ),
                    budget_action=getattr(args, "budget_action", "warn"),
                )
                if bool(getattr(args, "record_metrics", False)):
                    record_telemetry_coverage_metrics(conn, payload)
        elif subcmd == "report":
            trace_id = getattr(args, "trace_id", None)
            if not trace_id:
                raise SubstrateError(
                    "okg telemetry report requires --trace-id",
                    fix_hint="okg telemetry report --deployment <name> "
                             "--trace-id <id> --json",
                )
            with db_connect(dsn) as conn:
                payload = operator_report(
                    conn,
                    deployment=deployment,
                    trace_id=trace_id,
                )
        else:
            raise SubstrateError(
                "unknown okg telemetry subcommand",
                fix_hint=(
                    "okg telemetry metrics|contract report|bench overhead|"
                    "profile|trace|coverage|report"
                ),
            )
    except SubstrateError as err:
        return _emit_error(err, as_json=as_json)
    except Exception as exc:  # noqa: BLE001 - normalize command failures.
        return _emit_error(
            SubstrateError(
                f"okg telemetry {subcmd or ''} failed: {exc}",
                fix_hint="check DSN connectivity and migration state",
            ),
            as_json=as_json,
        )

    exit_code = 1 if (
        subcmd == "coverage"
        and (payload.get("budget") or {}).get("status") == "fail"
    ) else 0
    if as_json:
        _emit_ok(payload, as_json=True)
        return exit_code
    print(json.dumps(payload, indent=2, sort_keys=True))
    return exit_code


__all__ = ["_cmd_telemetry", "register_subparsers"]
