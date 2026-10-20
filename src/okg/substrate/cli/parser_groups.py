"""Grouped argparse registration helpers for substrate CLI families."""

from __future__ import annotations

import argparse
from collections.abc import Callable

from okg.substrate.cli._options import (
    add_deployment_arg,
    add_dsn_arg,
    install_actionable_errors,
)


AddJsonFlag = Callable[[argparse.ArgumentParser], None]
AddOwnerFlags = Callable[[argparse.ArgumentParser], None]


def register_db_cleanup_parsers(
    db_subparsers: argparse._SubParsersAction,
    *,
    add_json_flag: AddJsonFlag,
) -> None:
    """Register destructive database cleanup routes."""
    p_db_remove_job_runtime = db_subparsers.add_parser(
        "remove-job-runtime-subsystem",
        help="Remove the obsolete OKG job-runtime schema after safety checks",
    )
    add_deployment_arg(p_db_remove_job_runtime)
    add_dsn_arg(p_db_remove_job_runtime)
    p_db_remove_job_runtime.add_argument("--confirm", action="store_true")
    p_db_remove_job_runtime.add_argument("--apply", action="store_true")
    p_db_remove_job_runtime.add_argument(
        "--accept-retired-ledger-rows",
        action="store_true",
        help=(
            "Proceed even though retired job_* rows are still marked "
            "queued/running/retryable. The subsystem has no worker left to "
            "drain them, so without this the cleanup can never be satisfied. "
            "Requires --retired-rows-dump-ref; the destroyed row counts and "
            "that reference are recorded in the migration ledger."
        ),
    )
    p_db_remove_job_runtime.add_argument(
        "--retired-rows-dump-ref",
        help=(
            "Path or URI of the pre-drop dump of the retired job_* tables. "
            "Required with --accept-retired-ledger-rows so the destroyed rows "
            "stay recoverable."
        ),
    )
    add_json_flag(p_db_remove_job_runtime)


def register_read_admin_parsers(
    subparsers: argparse._SubParsersAction,
    *,
    add_json_flag: AddJsonFlag,
    add_owner_flags: AddOwnerFlags,
) -> None:
    """Register read/admin command families already backed by modular handlers."""
    register_generation_parser(subparsers, add_json_flag=add_json_flag)
    register_branch_parser(subparsers, add_json_flag=add_json_flag)
    register_admission_parser(subparsers, add_json_flag=add_json_flag)
    register_consistency_rerun_parser(subparsers, add_json_flag=add_json_flag)
    register_bench_parser(subparsers, add_json_flag=add_json_flag)
    register_consistency_scope_report_parser(
        subparsers, add_json_flag=add_json_flag
    )
    register_view_parser(subparsers, add_json_flag=add_json_flag)
    register_catalog_parser(
        subparsers,
        add_json_flag=add_json_flag,
        add_owner_flags=add_owner_flags,
    )
    register_alias_parser(subparsers, add_json_flag=add_json_flag)
    register_remediate_parser(subparsers, add_json_flag=add_json_flag)
    register_snapshot_load_parser(subparsers, add_json_flag=add_json_flag)
    register_external_action_parser(subparsers, add_json_flag=add_json_flag)


def register_external_action_parser(
    subparsers: argparse._SubParsersAction,
    *,
    add_json_flag: AddJsonFlag,
) -> None:
    """Register the standalone external-action HTTP client and server."""

    parser = subparsers.add_parser(
        "external-action",
        help="Review external effects without executing them in OKG",
    )
    add_json_flag(parser)
    parser.add_argument("--api-url")
    parser.add_argument(
        "--authorization-env",
        default="OKG_EXTERNAL_ACTION_AUTHORIZATION",
    )
    parser.add_argument("--dpop-env", default="OKG_EXTERNAL_ACTION_DPOP")
    parser.add_argument("--client-cert")
    parser.add_argument("--client-key")
    parser.add_argument("--ca-bundle")
    parser.add_argument("--timeout-seconds", type=float, default=30.0)
    commands = parser.add_subparsers(dest="external_action_subcmd", required=True)

    serve = commands.add_parser("serve", help="Run the standalone review API")
    add_json_flag(serve)
    add_dsn_arg(serve)
    serve.add_argument("--action-contracts", required=True)
    serve.add_argument("--review-config", required=True)
    serve.add_argument("--epoch-lease-chain", required=True)
    serve.add_argument("--host", default="127.0.0.1")
    serve.add_argument("--port", type=int, default=8788)
    serve.add_argument("--tls-cert")
    serve.add_argument("--tls-key")
    serve.add_argument("--tls-ca")

    def read_command(name: str, *, proposal: bool = False) -> argparse.ArgumentParser:
        item = commands.add_parser(name)
        add_json_flag(item)
        item.add_argument("--deployment", required=True)
        if proposal:
            item.add_argument("proposal_id")
        return item

    listing = read_command("list")
    listing.add_argument(
        "--state",
        choices=["pending", "edited", "approved", "rejected", "expired", "cancelled"],
    )
    listing.add_argument("--limit", type=int, default=100)
    read_command("show", proposal=True)
    read_command("timeline", proposal=True)
    recovery = read_command("recovery")
    recovery.add_argument("effect_instance_key")

    def mutation(
        name: str,
        *,
        proposal: bool = True,
        execution: bool = False,
        restore: bool = False,
        method_body: bool = True,
    ) -> argparse.ArgumentParser:
        item = commands.add_parser(name)
        add_json_flag(item)
        item.add_argument("--deployment", required=True)
        if proposal:
            item.add_argument("proposal_id")
        if execution:
            item.add_argument("execution_id")
        if restore:
            item.add_argument("restore_id")
        item.add_argument("--idempotency-key", required=True)
        if method_body:
            item.add_argument("--body-file", required=True)
        return item

    mutation("submit", proposal=False)
    for name in ("edit", "decide", "cancel", "claim"):
        item = mutation(name)
        item.add_argument("--if-match", required=True)
    attest = mutation("attest-owner-unavailable", execution=True)
    attest.add_argument("--if-match", required=True)
    outcome = mutation("outcome", execution=True)
    outcome.add_argument("--if-match", required=True)
    mutation("restore-create", proposal=False)
    batch = mutation("restore-batch", proposal=False, restore=True)
    batch.add_argument("batch_index", type=int)
    mutation("restore-finalize", proposal=False, restore=True)


def register_generation_parser(
    subparsers: argparse._SubParsersAction,
    *,
    add_json_flag: AddJsonFlag,
) -> None:
    p_gen = subparsers.add_parser(
        "generation",
        help="Inspect Chronos generation checkpoints "
        "(used by generation-blocked / publisher-lock-held runbooks)",
    )
    add_json_flag(p_gen)
    install_actionable_errors(
        p_gen,
        "okg generation describe <generation-ref> --deployment <deployment> --json",
    )
    p_gen_sub = p_gen.add_subparsers(dest="subcmd", required=True)
    p_gen_describe = p_gen_sub.add_parser(
        "describe",
        help="Print one generation row + graph_nodes / graph_edges "
        "counts at that generation",
    )
    add_json_flag(p_gen_describe)

    p_gen_describe.add_argument(
        "generation_id",
        type=_generation_id_token,
        help="generation_id to inspect: a Chronos checkpoint id from "
        "`okg generation list` (e.g. okg:...:gen:...), a target "
        "generation id, or a legacy integer id",
    )
    p_gen_describe.add_argument(
        "--deployment",
        help="deployment name (used by the DSN resolver if --dsn is not given)",
    )
    p_gen_describe.add_argument(
        "--branch",
        help=(
            "branch that published the generation (default: any branch). "
            "Pass the `branch` column from `okg generation list` to scope "
            "the lookup to one branch."
        ),
    )
    add_dsn_arg(p_gen_describe)

    p_gen_list = p_gen_sub.add_parser(
        "list",
        help="List recent published generations from okg.v_generations",
    )
    add_json_flag(p_gen_list)
    p_gen_list.add_argument(
        "--deployment",
        help="filter to this deployment (default: all deployments)",
    )
    p_gen_list.add_argument(
        "--branch",
        help="filter to this branch (default: all branches)",
    )
    p_gen_list.add_argument(
        "--limit",
        type=int,
        default=20,
        help="number of rows to return (default: 20)",
    )
    add_dsn_arg(p_gen_list)

    p_gen_compact = p_gen_sub.add_parser(
        "compact",
        help="Plan or apply deployment Chronos interval-history retention",
    )
    add_json_flag(p_gen_compact)
    p_gen_compact.add_argument("--deployment", required=True)
    p_gen_compact.add_argument("--branch", default="default")
    p_gen_compact.add_argument(
        "--confirm",
        metavar="PLAN_ID",
        help="exact persisted plan id; valid only together with --apply",
    )
    p_gen_compact.add_argument(
        "--apply",
        action="store_true",
        help="apply the plan named by --confirm (dry-run is the default)",
    )
    p_gen_compact.add_argument("--batch-size", type=int, default=5000)
    p_gen_compact.add_argument(
        "--offline-reclaim",
        action="store_true",
        help="after logical apply, physically reclaim storage in a stopped-worker window",
    )
    p_gen_compact.add_argument(
        "--workers-stopped",
        action="store_true",
        help="explicit assertion required by --offline-reclaim",
    )
    p_gen_compact.add_argument(
        "--progress",
        action="store_true",
        help="stream dry-run planner phase progress to stderr in real time "
             "(apply reports its progress in the plan row, not the log)",
    )
    add_dsn_arg(p_gen_compact)


def _generation_id_token(value: str) -> int | str:
    # Chronos-native generation ids are STRING checkpoint ids (what
    # `okg generation list` emits, e.g. "okg:...:gen:..."). Accept those
    # as well as legacy integer ids; `store.get_generation` takes int|str.
    try:
        return int(value)
    except ValueError:
        return value


def register_branch_parser(
    subparsers: argparse._SubParsersAction,
    *,
    add_json_flag: AddJsonFlag,
) -> None:
    p_branch = subparsers.add_parser(
        "branch",
        help="Inspect substrate branches (okg.branches)",
    )
    add_json_flag(p_branch)
    p_branch_sub = p_branch.add_subparsers(dest="subcmd", required=True)
    p_branch_list = p_branch_sub.add_parser(
        "list",
        help="List branches for a deployment with role/status/latest gen",
    )
    add_json_flag(p_branch_list)
    p_branch_list.add_argument(
        "--deployment",
        required=True,
        help="deployment name (also used by the DSN resolver)",
    )
    add_dsn_arg(p_branch_list)
    p_branch_show = p_branch_sub.add_parser(
        "show",
        help="Show one branch's metadata plus a generation-count summary",
    )
    add_json_flag(p_branch_show)
    p_branch_show.add_argument(
        "name",
        help="branch name (e.g. 'default', 'quarantine:fixture-test')",
    )
    p_branch_show.add_argument(
        "--deployment",
        required=True,
        help="deployment name (also used by the DSN resolver)",
    )
    add_dsn_arg(p_branch_show)

    p_branch_create = p_branch_sub.add_parser(
        "create",
        help="Create an experiment or quarantine branch (default is "
        "auto-materialized)",
    )
    add_json_flag(p_branch_create)
    p_branch_create.add_argument(
        "name",
        help="branch name (e.g. 'experiment:rename-pass-1', "
        "'quarantine:risky-import')",
    )
    p_branch_create.add_argument(
        "--role",
        required=True,
        choices=["experiment", "quarantine"],
        help="branch role",
    )
    p_branch_create.add_argument(
        "--base-branch",
        default="default",
        help="base branch name to lineage from (default: 'default')",
    )
    p_branch_create.add_argument(
        "--deployment",
        required=True,
        help="deployment name (also used by the DSN resolver)",
    )
    add_dsn_arg(p_branch_create)

    p_branch_abandon = p_branch_sub.add_parser(
        "abandon",
        help=(
            "Tombstone an experiment or quarantine branch (metadata only; "
            "the branch row survives — use `okg branch delete` to remove it)"
        ),
    )
    add_json_flag(p_branch_abandon)
    p_branch_abandon.add_argument(
        "name",
        help=(
            "branch name to abandon (refuses the default branch). This only "
            "stamps status='abandoned'; it does not free the branch's "
            "interval space"
        ),
    )
    p_branch_abandon.add_argument(
        "--deployment",
        required=True,
        help="deployment name (also used by the DSN resolver)",
    )
    add_dsn_arg(p_branch_abandon)

    p_branch_delete = p_branch_sub.add_parser(
        "delete",
        help=(
            "Remove a Chronos branch row outright, freeing its interval "
            "space (refuses 'main' and the default branch)"
        ),
    )
    add_json_flag(p_branch_delete)
    p_branch_delete.add_argument(
        "name",
        help=(
            "branch name to delete (refuses 'main', the default branch, and "
            "any branch owned by another deployment)"
        ),
    )
    p_branch_delete.add_argument(
        "--deployment",
        required=True,
        help="deployment name (also used by the DSN resolver)",
    )
    p_branch_delete.add_argument(
        "--cascade",
        action="store_true",
        help=(
            "also delete the branch's descendants. Without it a branch that "
            "still has children is refused rather than silently taking the "
            "whole subtree with it."
        ),
    )
    p_branch_delete.add_argument(
        "--confirm",
        action="store_true",
        help=(
            "actually delete the branch. Without --confirm the verb "
            "dry-runs: it reports what it would delete (including "
            "checkpoints that would be stranded) and mutates nothing."
        ),
    )
    add_dsn_arg(p_branch_delete)

    p_branch_compact = p_branch_sub.add_parser(
        "compact",
        help="Rebase a Chronos branch from its latest published generation",
    )
    add_json_flag(p_branch_compact)
    p_branch_compact.add_argument(
        "name",
        help=(
            "branch name to compact (refuses the default branch — "
            "compaction rewrites branch lineage and is refused on the "
            "deployment's primary branch)"
        ),
    )
    p_branch_compact.add_argument(
        "--deployment",
        required=True,
        help="deployment name (also used by the DSN resolver)",
    )
    p_branch_compact.add_argument(
        "--generation-id",
        help="published generation/checkpoint to compact from "
        "(default: latest published generation on the branch)",
    )
    p_branch_compact.add_argument(
        "--confirm",
        action="store_true",
        help=(
            "actually rebase the branch. Without --confirm the verb "
            "dry-runs: it reports what it would compact and mutates "
            "nothing (convention point (d): destructive verbs default "
            "to dry-run)."
        ),
    )
    add_dsn_arg(p_branch_compact)

    p_branch_rollover = p_branch_sub.add_parser(
        "rollover",
        help=(
            "Restore a leaf branch onto fresh interval space while preserving "
            "all generation checkpoints"
        ),
    )
    add_json_flag(p_branch_rollover)
    p_branch_rollover.add_argument(
        "name",
        help=(
            "deployment-owned leaf branch to recover (including the default "
            "branch)"
        ),
    )
    p_branch_rollover.add_argument(
        "--deployment",
        required=True,
        help="deployment name (also used by the DSN resolver)",
    )
    p_branch_rollover.add_argument(
        "--generation-id",
        help=(
            "published generation/checkpoint to restore from "
            "(default: latest published generation on the branch)"
        ),
    )
    p_branch_rollover.add_argument(
        "--confirm",
        action="store_true",
        help=(
            "actually replace the working branch head. Without --confirm the "
            "verb reports the intended rollover and mutates nothing."
        ),
    )
    add_dsn_arg(p_branch_rollover)


def register_admission_parser(
    subparsers: argparse._SubParsersAction,
    *,
    add_json_flag: AddJsonFlag,
) -> None:
    p_adm = subparsers.add_parser(
        "admission",
        help="Inspect admission envelopes and lifecycle evidence",
    )
    add_json_flag(p_adm)
    p_adm_sub = p_adm.add_subparsers(dest="subcmd", required=True)
    p_adm_list = p_adm_sub.add_parser(
        "list",
        help="List recent admission envelopes",
    )
    add_json_flag(p_adm_list)
    p_adm_list.add_argument(
        "--deployment",
        help="deployment name (used as both DSN source and filter)",
    )
    add_dsn_arg(p_adm_list)
    p_adm_list.add_argument(
        "--status",
        choices=[
            "pending",
            "accepted",
            "rejected",
            "published",
            "blocked",
            "failed",
            "quarantined",
        ],
    )
    p_adm_mode = p_adm_list.add_mutually_exclusive_group()
    p_adm_mode.add_argument(
        "--mode",
        choices=["direct", "fast_track", "review", "quarantine"],
    )
    p_adm_mode.add_argument(
        "--held",
        action="store_true",
        help=(
            "only envelopes in a held admission mode (review or "
            "quarantine): the runs that wait for `okg admission decide`"
        ),
    )
    p_adm_list.add_argument("--source")
    p_adm_list.add_argument("--limit", type=int, default=20)
    p_adm_show = p_adm_sub.add_parser(
        "show",
        help="Show one admission envelope with events and record refs",
    )
    add_json_flag(p_adm_show)
    p_adm_show.add_argument("admission_id")
    p_adm_show.add_argument(
        "--deployment",
        help="deployment name (used as both DSN source and filter)",
    )
    add_dsn_arg(p_adm_show)
    p_adm_submit = p_adm_sub.add_parser(
        "submit",
        help="Submit a review-mode node/edge payload through DBOS",
    )
    add_json_flag(p_adm_submit)
    add_deployment_arg(p_adm_submit, required=True)
    add_dsn_arg(p_adm_submit)
    p_adm_submit.add_argument("--payload-json")
    p_adm_submit.add_argument("--payload-file")
    p_adm_submit.add_argument("--producer-id", required=True)
    p_adm_submit.add_argument(
        "--producer-kind",
        choices=["human", "agent", "workflow", "system", "source"],
        default="agent",
    )
    p_adm_submit.add_argument("--intent", default="review_proposal")
    p_adm_submit.add_argument("--idempotency-key")
    p_adm_submit.add_argument("--authority-json")
    p_adm_submit.add_argument("--check-summary-json")
    p_adm_submit.add_argument("--max-wait-seconds", type=float, default=600.0)
    p_adm_submit.add_argument("--poll-interval-seconds", type=float, default=2.0)
    p_adm_decide = p_adm_sub.add_parser(
        "decide",
        help="Accept, reject, or quarantine a review-mode admission",
    )
    add_json_flag(p_adm_decide)
    p_adm_decide.add_argument("admission_id", nargs="?")
    add_deployment_arg(p_adm_decide, required=True)
    add_dsn_arg(p_adm_decide)
    p_adm_decide.add_argument(
        "--decision",
        choices=[
            "accept",
            "accepted",
            "approve",
            "approved",
            "reject",
            "rejected",
            "quarantine",
            "quarantined",
        ],
    )
    p_adm_decide.add_argument(
        "--action",
        choices=["promote"],
        help="Open a lens-promotion admission envelope. Use with "
        "--request-id <uuid>.",
    )
    p_adm_decide.add_argument("--request-id")
    p_adm_decide.add_argument(
        "--target-branch",
        default=None,
        help="With --action promote: pin the resulting envelope to a "
        "specific branch (e.g. `quarantine:<reason>`). NULL means the "
        "deployment's default branch.",
    )
    p_adm_decide.add_argument("--actor", default="operator")
    p_adm_decide.add_argument("--reason")
    p_adm_decide.add_argument("--verdict-json")
    p_adm_decide.add_argument("--max-wait-seconds", type=float, default=600.0)
    p_adm_decide.add_argument("--poll-interval-seconds", type=float, default=2.0)


def register_bench_parser(
    subparsers: argparse._SubParsersAction,
    *,
    add_json_flag: AddJsonFlag,
) -> None:
    """`okg bench <verb>` — the shipped benchmark harness (okg.bench)."""
    p_bench = subparsers.add_parser(
        "bench",
        help="run a benchmark from the shipped okg.bench harness",
    )
    bench_sub = p_bench.add_subparsers(dest="bench_cmd", required=True)
    p_inject = bench_sub.add_parser(
        "inject",
        help="run the synthetic inconsistency injection benchmark "
        "against a deployment's corpus and print the rendered scorecard",
    )
    add_json_flag(p_inject)
    p_inject.add_argument(
        "--deployment-dir",
        required=True,
        help="deployment directory carrying corpus/ and operators.yaml "
        "(e.g. deployments/consistency-injection-fixture)",
    )
    p_inject.add_argument(
        "--corpus",
        help="corpus directory override (default: <deployment-dir>/corpus)",
    )
    p_inject.add_argument(
        "--out",
        required=True,
        help="artifact directory for the sealed answer key + scorecards "
        "(must not exist)",
    )
    p_inject.add_argument(
        "--work-dir",
        help="working directory the deployment ingests from "
        "(default: <out>-working)",
    )
    p_inject.add_argument(
        "--seed", type=int, default=101, help="injection seed (default 101)"
    )
    add_dsn_arg(p_inject)

    p_synth = bench_sub.add_parser(
        "synth",
        help="run the KG-first synthetic-corpus benchmark: generate a "
        "corpus from the frozen Synthea world, corrupt, render, publish, "
        "detect, and print the scorecard",
    )
    add_json_flag(p_synth)
    p_synth.add_argument(
        "--deployment-dir",
        required=True,
        help="deployment directory. v1 supports the synthetic-corpus-smoke "
        "deployment only (the runner publishes under that deployment name); "
        "point at a repo checkout's deployments/synthetic-corpus-smoke",
    )
    p_synth.add_argument(
        "--out",
        required=True,
        help="artifact directory for the rendered corpora, sealed manifest, "
        "and scorecard",
    )
    p_synth.add_argument(
        "--preset",
        default="smoke",
        help="config preset name (smoke/default/easy/hard; default smoke)",
    )
    p_synth.add_argument(
        "--config",
        help="path to a synth config YAML (overrides --preset)",
    )
    p_synth.add_argument(
        "--world-dir",
        help="frozen Synthea world directory (default: the packaged smoke world)",
    )
    add_dsn_arg(p_synth)


def register_consistency_rerun_parser(
    subparsers: argparse._SubParsersAction,
    *,
    add_json_flag: AddJsonFlag,
) -> None:
    p_consistency_rerun_flat = subparsers.add_parser(
        "consistency-rerun",
        help="Run deployment post-publish consistency checks for "
        "an existing published generation",
    )
    add_json_flag(p_consistency_rerun_flat)
    p_consistency_rerun_flat.add_argument(
        "--deployment",
        help="deployment name owning the post-publish checks",
    )
    p_consistency_rerun_flat.add_argument(
        "--generation",
        help="published generation id; omitted means latest published",
    )
    p_consistency_rerun_flat.add_argument(
        "--scope-subject",
        action="append",
        default=[],
        help=(
            "limit checks to one subject node id; may be repeated or "
            "comma-separated"
        ),
    )
    add_dsn_arg(p_consistency_rerun_flat)


def register_consistency_scope_report_parser(
    subparsers: argparse._SubParsersAction,
    *,
    add_json_flag: AddJsonFlag,
) -> None:
    p_scope_report = subparsers.add_parser(
        "consistency-scope-report",
        help="Report per-check IR scope decisions (scoped vs full runs, "
        "scope modes, and full-fallback reasons) across recent runs -- "
        "the locality / scope-decision observability surface",
        description=(
            "The locality / scope-decision observability surface for IR "
            "consistency checks. For each check in the deployment it reports, "
            "across the last --lookback runs: how many ran scoped vs. a full "
            "universe sweep, the scope-mode distribution, the fallback-reason "
            "distribution over the FULL runs (WHY each went full), and the "
            "last-N raw scope decisions for drill-down -- so 'which checks "
            "never run incrementally, and why' is a measurement, not "
            "archaeology. Any future scope-analyzer extension (teaching the "
            "analyzer to scope GROUP BY / LIMIT / node-joins) MUST be "
            "justified by this measurement: extend the analyzer against a "
            "real full-fallback the surface shows, never against a guess."
        ),
    )
    add_json_flag(p_scope_report)
    p_scope_report.add_argument(
        "--deployment",
        help="deployment whose consistency-check scope decisions to report",
    )
    p_scope_report.add_argument(
        "--check-id",
        dest="check_id",
        help="limit the report to a single check id (default: all checks "
        "in the deployment)",
    )
    p_scope_report.add_argument(
        "--lookback",
        type=int,
        default=20,
        help="number of most-recent runs to summarize per check (default 20)",
    )
    add_dsn_arg(p_scope_report)


def register_view_parser(
    subparsers: argparse._SubParsersAction,
    *,
    add_json_flag: AddJsonFlag,
) -> None:
    p_view = subparsers.add_parser(
        "view",
        help="Read canonical native view artifacts",
    )
    add_json_flag(p_view)
    p_view_sub = p_view.add_subparsers(dest="subcmd", required=True)
    p_view_list = p_view_sub.add_parser(
        "list",
        help="List completed native view manifests for a deployment",
    )
    add_json_flag(p_view_list)
    p_view_list.add_argument(
        "--deployment",
        help="deployment name owning the view definitions",
    )
    p_view_list.add_argument(
        "--generation",
        type=int,
        help="exact generation id to list; omitted means latest per view",
    )
    add_dsn_arg(p_view_list)

    p_view_show = p_view_sub.add_parser(
        "show",
        help="Print a typed output slot from a completed view manifest",
    )
    add_json_flag(p_view_show)
    p_view_show.add_argument(
        "--deployment",
        help="deployment name owning the view definition",
    )
    p_view_show.add_argument("--view", required=True, help="logical view id")
    p_view_show.add_argument(
        "--slot",
        default="markdown",
        help="output slot name (default: markdown)",
    )
    p_view_show.add_argument(
        "--generation",
        type=int,
        help="exact generation id to read; omitted means latest published",
    )
    p_view_show.add_argument(
        "--definition-id",
        help="disambiguate when one logical view id has multiple definitions",
    )
    p_view_show.add_argument(
        "--audience",
        help="Nomos audience identifier (or OKG_READ_AUDIENCE)",
    )
    p_view_show.add_argument(
        "--tenant",
        help="Nomos tenant identifier (or OKG_READ_TENANT)",
    )
    add_dsn_arg(p_view_show)

    p_view_inspect = p_view_sub.add_parser(
        "inspect",
        help="Inspect a view slot manifest and backing storage",
    )
    add_json_flag(p_view_inspect)
    p_view_inspect.add_argument(
        "--deployment",
        help="deployment name owning the view definition",
    )
    p_view_inspect.add_argument(
        "--view",
        required=True,
        help="logical view id",
    )
    p_view_inspect.add_argument(
        "--slot",
        default="relation",
        help="output slot name (default: relation)",
    )
    p_view_inspect.add_argument(
        "--generation",
        type=int,
        help="exact generation id to inspect; omitted means latest published",
    )
    p_view_inspect.add_argument(
        "--definition-id",
        help="disambiguate when one logical view id has multiple definitions",
    )
    p_view_inspect.add_argument(
        "--preview-limit",
        type=int,
        default=0,
        help="include up to N relation rows in the inspection output",
    )
    add_dsn_arg(p_view_inspect)


def register_catalog_parser(
    subparsers: argparse._SubParsersAction,
    *,
    add_json_flag: AddJsonFlag,
    add_owner_flags: AddOwnerFlags,
) -> None:
    p_cat = subparsers.add_parser(
        "catalog",
        help="Compose / load deployment catalog (modules + bridges)",
    )
    add_json_flag(p_cat)
    p_cat_sub = p_cat.add_subparsers(dest="subcmd", required=True)
    p_cat_load = p_cat_sub.add_parser(
        "load",
        help="Compose subtypes + narrowings + aliases + identifier "
        "patterns from a deployment's modules + bridges",
    )
    add_json_flag(p_cat_load)
    add_owner_flags(p_cat_load)
    p_cat_load.add_argument(
        "--deployment",
        required=True,
        help=(
            "Deployment directory or active sealed environment id"
        ),
    )
    _add_writer_scope_flags(p_cat_load)
    p_cat_load.add_argument(
        "--apply",
        action="store_true",
        help="Apply the composed catalog under a fresh catalog_versions row "
        "(otherwise dry-run + summary)",
    )
    p_cat_load.add_argument(
        "--dsn",
        help="Postgres DSN for --apply; defaults to $OKG_DSN",
    )

    p_cat_hist = p_cat_sub.add_parser(
        "history",
        help="List recent catalog versions from okg.catalog_versions",
    )
    add_json_flag(p_cat_hist)
    p_cat_hist.add_argument(
        "--limit",
        type=int,
        default=20,
        help="number of rows to return (default: 20)",
    )
    add_dsn_arg(p_cat_hist)

    p_cat_owner = p_cat_sub.add_parser(
        "ownership",
        help="Diagnose or claim writable catalog ownership",
    )
    add_json_flag(p_cat_owner)
    p_cat_owner_sub = p_cat_owner.add_subparsers(
        dest="ownership_cmd",
        required=True,
    )
    p_cat_owner_diag = p_cat_owner_sub.add_parser(
        "diagnose",
        help="Report catalog fingerprints, provenance, and owner state",
    )
    add_json_flag(p_cat_owner_diag)
    p_cat_owner_diag.add_argument(
        "--deployment",
        required=True,
        help="Deployment directory, deployment name, or environment id",
    )
    add_dsn_arg(p_cat_owner_diag)
    p_cat_owner_diag.add_argument(
        "--limit",
        type=int,
        default=12,
        help="recent applied catalog versions to inspect",
    )
    _add_writer_scope_flags(p_cat_owner_diag)
    p_cat_owner_claim = p_cat_owner_sub.add_parser(
        "claim",
        help="Claim this DB for the current deployment/worktree",
    )
    add_json_flag(p_cat_owner_claim)
    p_cat_owner_claim.add_argument(
        "--deployment",
        required=True,
        help="Deployment directory, deployment name, or environment id",
    )
    add_dsn_arg(p_cat_owner_claim)
    p_cat_owner_claim.add_argument(
        "--force",
        action="store_true",
        help="Claim ownership despite mixed or legacy catalog history",
    )
    _add_writer_scope_flags(p_cat_owner_claim)
    p_cat_owner_claim.add_argument(
        "--allow-scope-change",
        action="store_true",
        help=(
            "Deliberately re-scope the writer (environment <-> "
            "deployment_root). Refused by default, and NOT implied by "
            "--force: recording the wrong scope makes every publish fail "
            "catalog_ownership_foreign_writer while the claim reports "
            "claimed: true"
        ),
    )

    p_cat_restamp = p_cat_sub.add_parser(
        "repair-restamp-ownership",
        help="Return graph nodes an earlier catalog restamp took over "
        "to the source that wrote them (read-only without --apply)",
        description=(
            "Return graph nodes an earlier catalog restamp took over under "
            "_catalog:restamp to the source that wrote them, read from the "
            "row version the restamp replaced. Without --apply nothing is "
            "written, no branch lock is taken and no DDL runs. A node whose "
            "source the branch history cannot name is left alone and the "
            "command exits 1; with --apply the nodes it could resolve are "
            "still written. A node the source stopped emitting while the "
            "restamp owned it stays published until that source alone runs "
            "`okg ingest --deployment <name> --include <source> "
            "--no-predecessors --inline --reset-cursor` (add "
            "--allow-full-retraction <source> if its current read emits no "
            "records); the output lists those nodes and commands, and marks "
            "a source with no stored record set for inspection instead."
        ),
    )
    add_json_flag(p_cat_restamp)
    p_cat_restamp.add_argument(
        "--deployment",
        required=True,
        help="deployment name recorded on the repair's admission record",
    )
    add_dsn_arg(p_cat_restamp)
    p_cat_restamp.add_argument(
        "--branch",
        default="default",
        help="graph branch to repair (default: default)",
    )
    p_cat_restamp.add_argument(
        "--apply",
        action="store_true",
        help="write the repair; otherwise only report what it would do",
    )


def _add_writer_scope_flags(parser: argparse.ArgumentParser) -> None:
    """Expose the catalog writer authority consumed by every write path."""
    parser.add_argument(
        "--writer-scope",
        choices=("deployment_root", "environment"),
        help=(
            "assert the resolved writer scope; otherwise a directory or "
            "deployment name is deployment_root and an environment id is "
            "environment"
        ),
    )
    parser.add_argument(
        "--environment-id",
        help="explicit environment identity when --writer-scope=environment",
    )


def register_alias_parser(
    subparsers: argparse._SubParsersAction,
    *,
    add_json_flag: AddJsonFlag,
) -> None:
    p_alias = subparsers.add_parser(
        "alias",
        help="Manage the alias dictionary (manual operator path "
        "for entity_mention resolution)",
    )
    add_json_flag(p_alias)
    p_alias_sub = p_alias.add_subparsers(dest="subcmd", required=True)

    p_alias_seed = p_alias_sub.add_parser(
        "seed-from-subtype",
        help="Bulk-import aliases from a subtype's attribute",
    )
    add_json_flag(p_alias_seed)
    p_alias_seed.add_argument("--subtype", required=True)
    p_alias_seed.add_argument("--field", required=True)
    p_alias_seed.add_argument("--scope", default=None)
    p_alias_seed.add_argument(
        "--canonical-template",
        default=None,
        help="Format string with {field_name} placeholders; defaults to the node_id",
    )
    p_alias_seed.add_argument("--apply", action="store_true")
    add_dsn_arg(p_alias_seed)

    p_alias_list = p_alias_sub.add_parser("list")
    add_json_flag(p_alias_list)
    p_alias_list.add_argument("--scope", default=None)
    p_alias_list.add_argument("--limit", type=int, default=200)
    add_dsn_arg(p_alias_list)

    p_alias_collisions = p_alias_sub.add_parser(
        "collisions",
        help="List refused ambiguous aliases at the active catalog version",
    )
    add_json_flag(p_alias_collisions)
    p_alias_collisions.add_argument("--limit", type=int, default=100)
    add_dsn_arg(p_alias_collisions)

    p_alias_add = p_alias_sub.add_parser("add")
    add_json_flag(p_alias_add)
    p_alias_add.add_argument("alias")
    p_alias_add.add_argument("canonical")
    p_alias_add.add_argument("--scope", default=None)
    add_dsn_arg(p_alias_add)

    p_alias_remove = p_alias_sub.add_parser("remove")
    add_json_flag(p_alias_remove)
    p_alias_remove.add_argument("alias")
    add_dsn_arg(p_alias_remove)


def register_remediate_parser(
    subparsers: argparse._SubParsersAction,
    *,
    add_json_flag: AddJsonFlag,
) -> None:
    """`okg remediate <subcmd>` — bounded remediation of pre-fix
    projection-identity damage (fix-projection-alias-identity, ruling 4). Every
    subcommand is DRY-RUN by default and mutates only under --apply."""
    p_rem = subparsers.add_parser(
        "remediate",
        help="Bounded remediation of stranded link-cache / collapsed aliases / "
        "multi-target edges (dry-run by default)",
    )
    add_json_flag(p_rem)
    p_rem_sub = p_rem.add_subparsers(dest="subcmd", required=True)

    p_strand = p_rem_sub.add_parser(
        "stranded-link-cache",
        help="Invalidate link_cache for the extractor's source subtypes so the "
        "next projection re-extracts strands that left no retry record",
    )
    add_json_flag(p_strand)
    p_strand.add_argument(
        "--subtype",
        action="append",
        required=True,
        dest="subtypes",
        help="Extractor source subtype (repeatable)",
    )
    p_strand.add_argument("--limit", type=int, required=True)
    p_strand.add_argument("--cursor", default=None)
    p_strand.add_argument("--deployment-name", default="default")
    p_strand.add_argument("--branch", default="default")
    p_strand.add_argument("--apply", action="store_true")
    add_dsn_arg(p_strand)

    p_endpoints = p_rem_sub.add_parser(
        "link-endpoints",
        help="Queue the records whose misses predate a route's on_miss, so "
        "the next publish writes their not-loaded link endpoints",
    )
    add_json_flag(p_endpoints)
    p_endpoints.add_argument(
        "--entity-type",
        action="append",
        required=True,
        dest="entity_types",
        help="Entity type of a route with on_miss (repeatable)",
    )
    p_endpoints.add_argument("--limit", type=int, required=True)
    p_endpoints.add_argument("--cursor", default=None)
    p_endpoints.add_argument(
        "--deployment-name",
        required=True,
        help="Deployment whose retry rows to page (no default: a wrong name "
        "would quietly find nothing)",
    )
    p_endpoints.add_argument("--branch", default="default")
    p_endpoints.add_argument("--apply", action="store_true")
    add_dsn_arg(p_endpoints)

    p_collapsed = p_rem_sub.add_parser(
        "collapsed-aliases",
        help="Detect collapsed-alias collisions and (under --apply) quarantine "
        "the ambiguous alias rows + requeue affected src nodes",
    )
    add_json_flag(p_collapsed)
    p_collapsed.add_argument("--subtype", required=True)
    p_collapsed.add_argument("--field", required=True)
    p_collapsed.add_argument("--entity-type", default=None, dest="entity_type")
    p_collapsed.add_argument("--limit", type=int, required=True)
    p_collapsed.add_argument("--cursor", default=None)
    p_collapsed.add_argument("--run-id", default=None)
    p_collapsed.add_argument("--deployment-name", default="default")
    p_collapsed.add_argument("--branch", default="default")
    p_collapsed.add_argument("--apply", action="store_true")
    add_dsn_arg(p_collapsed)

    p_multi = p_rem_sub.add_parser(
        "multi-target-report",
        help="Enumerate every offending (src, edge_type) group with all targets "
        "and provenance; read-only, never auto-picks a survivor",
    )
    add_json_flag(p_multi)
    p_multi.add_argument(
        "--catalog-version-id", type=int, default=None,
        dest="catalog_version_id",
    )
    p_multi.add_argument("--limit", type=int, default=100)
    p_multi.add_argument("--cursor", default=None)
    add_dsn_arg(p_multi)

    p_merge = p_rem_sub.add_parser(
        "entity-merge",
        help="Plan or apply one checkpoint-pinned duplicate-node merge",
    )
    add_json_flag(p_merge)
    p_merge.add_argument("--survivor", required=True)
    p_merge.add_argument("--retire", required=True)
    p_merge.add_argument("--deployment-name", default="default")
    p_merge.add_argument("--branch", default="default")
    p_merge.add_argument("--expected-plan-digest", default=None)
    p_merge.add_argument(
        "--accept-survivor-attrs",
        action="store_true",
        help=(
            "Take the survivor's value for every conflicting attribute that "
            "has no catalog declaration and no --merge-attr override"
        ),
    )
    p_merge.add_argument(
        "--merge-attr",
        action="append",
        default=[],
        metavar="NAME=DISPOSITION",
        help=(
            "How to merge one conflicting attribute: union (keep every entry "
            "from both lists), keep_survivor, or keep_retiree. Repeatable. "
            "Outranks the catalog declaration and --accept-survivor-attrs"
        ),
    )
    p_merge.add_argument("--actor", default="operator")
    p_merge.add_argument("--apply", action="store_true")
    add_dsn_arg(p_merge)

    p_exposure = p_rem_sub.add_parser(
        "derived-node-exposure",
        help=(
            "Report derived-node support exposure from an immutable published "
            "generation; read-only"
        ),
    )
    add_json_flag(p_exposure)
    p_exposure.add_argument(
        "--deployment-name",
        default="default",
        help="Deployment whose published graph is inspected",
    )
    p_exposure.add_argument(
        "--branch",
        default="default",
        help="Branch whose published graph is inspected",
    )
    p_exposure.add_argument(
        "--generation",
        default=None,
        help="Published checkpoint to inspect (default: latest on the branch)",
    )
    p_exposure.add_argument(
        "--example-limit",
        type=int,
        default=20,
        help="Maximum node ids retained per classification (default: 20)",
    )
    add_dsn_arg(p_exposure)

    p_retire_source = p_rem_sub.add_parser(
        "retire-source",
        help=(
            "Plan or apply a checkpoint-pinned source retirement; dry-run "
            "unless --apply is explicitly supplied"
        ),
    )
    add_json_flag(p_retire_source)
    p_retire_source.add_argument(
        "--source",
        required=True,
        help="Registry source name to retire",
    )
    p_retire_source.add_argument(
        "--deployment-name",
        default="default",
        help="Deployment whose source registry and graph are targeted",
    )
    p_retire_source.add_argument(
        "--branch",
        default="default",
        help="Branch to inspect and, under --apply, retire",
    )
    p_retire_source.add_argument(
        "--expected-plan-digest",
        default=None,
        help="Require this reviewed plan digest before applying",
    )
    p_retire_source.add_argument(
        "--confirm-source",
        default=None,
        help=(
            "Source confirmed for an apply: its registry name or the "
            "ownership id the plan shows"
        ),
    )
    p_retire_source.add_argument(
        "--actor",
        default="operator",
        help="Operator identity recorded in the retirement receipt",
    )
    p_retire_source.add_argument(
        "--apply",
        action="store_true",
        help="Apply the reviewed plan; omitted means dry-run",
    )
    add_dsn_arg(p_retire_source)


def register_snapshot_load_parser(
    subparsers: argparse._SubParsersAction,
    *,
    add_json_flag: AddJsonFlag,
) -> None:
    p_snap = subparsers.add_parser(
        "snapshot-load",
        help="Load a JSON snapshot as the deployment's first "
        "published generation (renamed from `okg bootstrap`)",
    )
    add_json_flag(p_snap)
    p_snap.add_argument(
        "--snapshot",
        required=True,
        help="Path to a JSON snapshot with `nodes` and `edges` arrays",
    )
    p_snap.add_argument(
        "--deployment-name",
        dest="deployment_name",
        default=None,
        help="Deployment name to record on the published generation "
        "(default: 'default')",
    )
    p_snap.add_argument(
        "--deployment",
        default=None,
        help="Deployment name (alias of --deployment-name)",
    )
    p_snap.add_argument(
        "--dsn",
        default=None,
        help="Postgres DSN; overrides the manifest's postgres.dsn",
    )
    p_snap.add_argument(
        "--apply",
        action="store_true",
        help="Actually load the snapshot (otherwise dry-run)",
    )
