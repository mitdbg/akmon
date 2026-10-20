"""argparse acceptance tests for every CLI subcommand.

These tests cover the dispatcher + arg-parsing layer in
`okg.substrate.cli.main` without exercising any subcommand's
deeper semantics (each `_cmd_*` function has its own test
elsewhere). The purpose is to lift `cli/main.py` line coverage by
forcing argparse to walk every parser branch.
"""
from __future__ import annotations

import pytest

from okg.substrate.cli.main import _build_parser


# ---------- Top-level help / unknown command ----------


def test_top_level_help_succeeds() -> None:
    """`okg --help` succeeds; argparse prints help and exits 0."""
    parser = _build_parser()
    with pytest.raises(SystemExit) as exc:
        parser.parse_args(["--help"])
    assert exc.value.code == 0


def test_unknown_command_exits_non_zero() -> None:
    parser = _build_parser()
    with pytest.raises(SystemExit) as exc:
        parser.parse_args(["not-a-command"])
    assert exc.value.code != 0


def test_deployment_up_accepts_ci_readiness_profile() -> None:
    args = _build_parser().parse_args([
        "deployment",
        "up",
        "okg-ci",
        "--checks",
        "ci",
        "--confirm-destructive-migrations",
    ])
    assert args.readiness_profile == "ci"
    assert args.confirm_destructive_migrations is True


# ---------- migrate ----------


def test_migrate_argparse_minimal() -> None:
    args = _build_parser().parse_args(["migrate"])
    assert args.command == "migrate"


def test_migrate_argparse_with_apply_and_dsn() -> None:
    args = _build_parser().parse_args([
        "migrate", "--apply", "--dsn", "postgres://x",
    ])
    assert args.apply is True
    assert args.dsn == "postgres://x"


def test_migrate_argparse_views_only() -> None:
    args = _build_parser().parse_args(["migrate", "--views-only"])
    assert args.command == "migrate"
    assert args.views_only is True


# ---------- doctor ----------


def test_doctor_argparse_minimal() -> None:
    args = _build_parser().parse_args(["doctor"])
    assert args.command == "doctor"


def test_doctor_argparse_check_modes() -> None:
    for check in (
        "extraction-wrap-invariant",
        "extraction",
        "extractors",
        "integrity",
        "acceptance-criteria",
        "external-live-authority",
        "database",
        "source-auth",
        "sources",
        "deployment-owned-check",
    ):
        args = _build_parser().parse_args(["doctor", "--check", check])
        assert args.check == check


# ---------- db ----------


def test_db_list_argparse() -> None:
    args = _build_parser().parse_args(["db", "list"])
    assert args.command == "db"
    assert args.db_subcmd == "list"


def test_db_list_cluster_argparse() -> None:
    args = _build_parser().parse_args([
        "db", "list", "--cluster", "--cluster-dsn", "postgres://x",
    ])
    assert args.cluster is True
    assert args.cluster_dsn == "postgres://x"


def test_db_status_argparse() -> None:
    args = _build_parser().parse_args([
        "db", "status", "--deployment", "okg-workspace", "--brief",
    ])
    assert args.db_subcmd == "status"
    assert args.deployment == "okg-workspace"
    assert args.brief is True


def test_db_gc_argparse_requires_dry_run() -> None:
    args = _build_parser().parse_args([
        "db", "gc", "--dry-run", "--cluster-dsn", "postgres://x",
    ])
    assert args.db_subcmd == "gc"
    assert args.dry_run is True


def test_db_clone_argparse() -> None:
    args = _build_parser().parse_args([
        "db",
        "clone",
        "--source-dsn",
        "postgres://source/source",
        "--target-dsn",
        "postgres://target/target",
        "--drain-timeout-seconds",
        "12.5",
        "--apply",
    ])
    assert args.db_subcmd == "clone"
    assert args.source_dsn == "postgres://source/source"
    assert args.target_dsn == "postgres://target/target"
    assert args.drain_timeout_seconds == 12.5
    assert args.apply is True


def test_db_dbos_gc_argparse() -> None:
    args = _build_parser().parse_args([
        "db", "dbos-gc",
        "--dsn", "postgres://x",
        "--stale-after-seconds", "86400",
        "--status", "PENDING",
        "--queue-name", "okg-okg-workspace",
        "--confirm",
        "--apply",
    ])
    assert args.db_subcmd == "dbos-gc"
    assert args.dsn == "postgres://x"
    assert args.stale_after_seconds == 86400
    assert args.status == ["PENDING"]
    assert args.queue_name == "okg-okg-workspace"
    assert args.confirm is True
    assert args.apply is True


def test_db_remove_job_runtime_subsystem_argparse() -> None:
    args = _build_parser().parse_args([
        "db", "remove-job-runtime-subsystem",
        "--dsn", "postgres://x",
        "--confirm",
        "--apply",
    ])
    assert args.db_subcmd == "remove-job-runtime-subsystem"
    assert args.dsn == "postgres://x"
    assert args.confirm is True
    assert args.apply is True


def test_deployment_bug_report_argparse() -> None:
    args = _build_parser().parse_args([
        "deployment",
        "bug-report",
        "--deployment",
        "demo",
        "--dsn",
        "postgres://x",
        "--repo",
        "owner/repo",
        "--deployment-repo",
        "deployments/demo",
        "--title",
        "Bug",
        "--summary",
        "Summary",
        "--label",
        "bug",
        "--label",
        "deployment",
        "--apply",
    ])
    assert args.command == "deployment"
    assert args.subcmd == "bug-report"
    assert args.deployment == "demo"
    assert args.dsn == "postgres://x"
    assert args.repo == "owner/repo"
    assert args.deployment_repo == "deployments/demo"
    assert args.title == "Bug"
    assert args.summary == "Summary"
    assert args.label == ["bug", "deployment"]
    assert args.apply is True


def test_deployment_bug_report_preview_file_argparse() -> None:
    args = _build_parser().parse_args([
        "deployment",
        "bug-report",
        "--preview-json-file",
        "preview.json",
        "--apply",
    ])
    assert args.command == "deployment"
    assert args.subcmd == "bug-report"
    assert args.preview_json_file == "preview.json"
    assert args.apply is True


def test_deployment_bug_report_issue_repo_alias_argparse() -> None:
    args = _build_parser().parse_args([
        "deployment",
        "bug-report",
        "--deployment",
        "demo",
        "--issue-repo",
        "owner/repo",
    ])
    assert args.repo == "owner/repo"


def test_deployment_new_argparse() -> None:
    args = _build_parser().parse_args([
        "deployment",
        "new",
        "demo",
        "--deployments-root",
        "deployments",
        "--force",
        "--yes",
        "--json",
    ])
    assert args.command == "deployment"
    assert args.subcmd == "new"
    assert args.name == "demo"
    assert args.deployments_root == "deployments"
    assert args.force is True
    assert args.yes is True


def test_deployment_env_doctor_argparse_preserves_existing_env_command() -> None:
    env_command = _build_parser().parse_args([
        "deployment",
        "env",
        "demo",
        "--write-local",
        "--env-name",
        "OKG_DSN",
        "--dsn",
        "postgres://x",
    ])
    assert env_command.subcmd == "env"
    assert env_command.deployment == "demo"
    assert env_command.write_local is True

    doctor = _build_parser().parse_args([
        "deployment",
        "env",
        "doctor",
        "demo",
        "--for-command",
        "runtime worker",
        "--show-values",
        "--json",
    ])
    assert doctor.subcmd == "env"
    assert doctor.deployment == "doctor"
    assert doctor.for_command == "runtime worker"
    assert doctor.show_values is True


def test_deployment_plan_lint_profile_argparse() -> None:
    plan = _build_parser().parse_args([
        "deployment",
        "plan",
        "demo",
        "--deployments-root",
        "deployments",
        "--no-check-imports",
        "--json",
    ])
    assert plan.subcmd == "plan"
    assert plan.deployment == "demo"
    assert plan.no_check_imports is True

    lint = _build_parser().parse_args([
        "deployment",
        "lint",
        "demo",
        "--deployments-root",
        "deployments",
        "--live",
    ])
    assert lint.subcmd == "lint"
    assert lint.deployment == "demo"
    assert lint.live is True

    profile = _build_parser().parse_args([
        "deployment",
        "profile",
        "explain",
        "workspace-graph",
        "--json",
    ])
    assert profile.subcmd == "profile"
    assert profile.profile_subcmd == "explain"
    assert profile.profile == "workspace-graph"


def test_deployment_ready_explain_argparse() -> None:
    args = _build_parser().parse_args([
        "deployment",
        "ready",
        "demo",
        "--explain",
    ])
    assert args.subcmd == "ready"
    assert args.deployment == "demo"
    assert args.explain is True


def test_deployment_fork_gc_isolated_argparse() -> None:
    fork = _build_parser().parse_args([
        "deployment",
        "fork-worktree",
        "--deployment",
        "demo",
        "--worktree-path",
        "/tmp/worktree",
    ])
    assert fork.subcmd == "fork-worktree"
    assert fork.deployment == "demo"
    assert fork.worktree_path == "/tmp/worktree"

    gc = _build_parser().parse_args([
        "deployment",
        "gc-forks",
        "--deployment",
        "demo",
        "--apply",
    ])
    assert gc.subcmd == "gc-forks"
    assert gc.deployment == "demo"
    assert gc.apply is True

    isolated = _build_parser().parse_args([
        "deployment",
        "isolated",
        "list",
        "demo",
    ])
    assert isolated.subcmd == "isolated"
    assert isolated.iso_action == "list"
    assert isolated.deployment == "demo"

    isolated_gc = _build_parser().parse_args([
        "deployment",
        "isolated",
        "gc",
        "demo",
        "--yes",
    ])
    assert isolated_gc.subcmd == "isolated"
    assert isolated_gc.iso_action == "gc"
    assert isolated_gc.deployment == "demo"
    assert isolated_gc.yes is True


def test_db_drop_argparse_requires_explicit_name() -> None:
    args = _build_parser().parse_args([
        "db", "drop", "--name", "tmp_okg_test",
        "--cluster-dsn", "postgres://x", "--confirm", "--apply",
    ])
    assert args.db_subcmd == "drop"
    assert args.name == "tmp_okg_test"
    assert args.confirm is True
    assert args.apply is True


def test_db_migrations_argparse() -> None:
    args = _build_parser().parse_args([
        "db", "migrations", "--dsn", "postgres://x",
    ])
    assert args.db_subcmd == "migrations"
    assert args.dsn == "postgres://x"


# ---------- run ----------


def test_run_argparse_requires_once_flag_in_practice() -> None:
    """`run` accepts arg parse without --once; the dispatcher
    enforces --once at runtime per `cli/main.py:946`."""
    args = _build_parser().parse_args([
        "run", "--once", "--deployment", "af_pubs",
    ])
    assert args.once is True
    assert args.deployment == "af_pubs"


def test_run_argparse_include_llm_invariants_flag() -> None:
    args = _build_parser().parse_args([
        "run", "--once", "--deployment", "x",
        "--include-llm-invariants",
    ])
    assert args.include_llm_invariants is True


def test_run_argparse_llm_invariant_filter() -> None:
    args = _build_parser().parse_args([
        "run", "--once", "--deployment", "x",
        "--include-llm-invariants",
        "--llm-invariant", "body_contradicts",
    ])
    assert args.llm_invariant == "body_contradicts"


# ---------- ingest ----------


def test_ingest_argparse_requires_deployment() -> None:
    parser = _build_parser()
    with pytest.raises(SystemExit):
        parser.parse_args(["ingest"])  # missing --deployment


def test_ingest_argparse_full_flag_set() -> None:
    args = _build_parser().parse_args([
        "ingest", "--deployment", "x",
        "--include", "src1,src2",
        "--exclude", "src3",
        "--no-publish",
        "--reset-cursor",
        "--progress",
    ])
    assert args.deployment == "x"
    assert args.include == "src1,src2"
    assert args.exclude == "src3"
    assert args.no_publish is True
    assert args.reset_cursor is True
    assert args.progress is True
    assert args.allow_full_retraction == []


def test_ingest_argparse_allow_full_retraction_repeats() -> None:
    args = _build_parser().parse_args([
        "ingest", "--deployment", "x", "--inline",
        "--allow-full-retraction", "external_repos",
        "--allow-full-retraction", "pact",
    ])
    assert args.allow_full_retraction == ["external_repos", "pact"]


# ---------- metrics ----------


def test_metrics_llm_argparse() -> None:
    args = _build_parser().parse_args([
        "metrics", "--llm", "--since", "1h", "--deployment", "x",
    ])
    assert args.llm is True
    assert args.since == "1h"


def test_metrics_qa_snapshot_argparse() -> None:
    args = _build_parser().parse_args([
        "metrics", "--qa-snapshot", "--deployment", "x",
    ])
    assert args.qa_snapshot is True


def test_metrics_qa_snapshot_with_generation_arg() -> None:
    # Chronos-native generation ids are checkpoint-id STRINGS; the flag
    # accepts both those and compat integer target ids, so argparse keeps
    # the raw token and the resolver decides.
    args = _build_parser().parse_args([
        "metrics", "--qa-snapshot",
        "--deployment", "x", "--generation", "42",
    ])
    assert args.generation == "42"


# ---------- status ----------


def test_status_argparse_minimal() -> None:
    args = _build_parser().parse_args(["status"])
    assert args.command == "status"


def test_status_argparse_watch_with_interval() -> None:
    args = _build_parser().parse_args([
        "status", "--watch", "--interval", "10",
    ])
    assert args.watch is True
    assert args.interval == 10


# ---------- schema ----------


def test_schema_show_argparse_requires_file() -> None:
    # `schema` grew show/quality subcommands; `schema show` requires a file.
    parser = _build_parser()
    with pytest.raises(SystemExit):
        parser.parse_args(["schema", "show"])


def test_schema_show_argparse_with_file() -> None:
    args = _build_parser().parse_args(["schema", "show", "deployment.yaml"])
    assert args.file == "deployment.yaml"


def test_schema_bare_file_rewrites_to_show() -> None:
    # Back-compat: `okg [--json] schema <file>` is rewritten to `schema show`.
    from okg.substrate.cli.main import _maybe_rewrite_schema_show

    assert _maybe_rewrite_schema_show(["schema", "deployment.yaml"]) == [
        "schema", "show", "deployment.yaml",
    ]
    assert _maybe_rewrite_schema_show(["--json", "schema", "deployment.yaml"]) == [
        "--json", "schema", "show", "deployment.yaml",
    ]


def test_schema_quality_argparse() -> None:
    args = _build_parser().parse_args(["schema", "quality", "--current"])
    assert args.schema_cmd == "quality"
    assert args.current is True


# ---------- deleted legacy routes ----------


def test_worker_argparse_is_removed() -> None:
    parser = _build_parser()
    with pytest.raises(SystemExit) as exc:
        parser.parse_args([
        "worker", "run",
        "--deployment", "okg-workspace",
        "--kinds", "publish,embed",
        "--lease-seconds", "60",
        ])
    assert exc.value.code != 0


@pytest.mark.parametrize("subcmd", ["install", "list", "uninstall"])
def test_cron_argparse_is_removed(subcmd: str) -> None:
    parser = _build_parser()
    with pytest.raises(SystemExit) as exc:
        parser.parse_args(["cron", subcmd, "--deployment", "okg-workspace"])
    assert exc.value.code != 0


# ---------- mcp-serve ----------


def test_mcp_serve_argparse() -> None:
    args = _build_parser().parse_args([
        "mcp-serve", "--deployment", "x", "--embedder", "hash",
    ])
    assert args.command == "mcp-serve"
    assert args.embedder == "hash"


# ---------- init ----------


def test_init_argparse_accepts_no_args() -> None:
    """Per Layer 2A (`add-okg-init-and-module-scaffold-cli`):
    `okg init` now accepts either `--profile <name>` (preferred)
    or `--template <name>` (legacy). Argparse no longer rejects
    the bare invocation; the runtime command surfaces the
    "must supply one of" error via the structured error envelope.
    """
    args = _build_parser().parse_args(["init"])
    assert args.command == "init"
    assert getattr(args, "template", None) is None
    assert getattr(args, "profile_name", None) is None


def test_init_argparse_with_profile_flag() -> None:
    """Layer 2A: `--profile <name>` is the new preferred mode."""
    args = _build_parser().parse_args([
        "init", "--profile", "academic-researcher",
        "--deployment-name", "personal",
    ])
    assert args.profile_name == "academic-researcher"
    assert args.deployment_name == "personal"


def test_init_argparse_with_template_and_target() -> None:
    args = _build_parser().parse_args([
        "init", "--template", "knowledge_work", "--target", "/tmp/x",
    ])
    assert args.template == "knowledge_work"
    assert args.target == "/tmp/x"


def test_init_argparse_accepts_okg_workspace_flags() -> None:
    args = _build_parser().parse_args([
        "init",
        "--template",
        "okg-workspace",
        "--target",
        "/tmp/x",
        "--repo-root",
        "/tmp/repo",
        "--openspec-root",
        "/tmp/repo/specs",
        "--claude-projects-root",
        "/tmp/claude/projects",
        "--deployment-name",
        "test-lp",
        "--since",
        "30d",
        "--notes-root",
        "/tmp/notes",
    ])
    assert args.template == "okg-workspace"
    assert args.repo_root == "/tmp/repo"
    assert args.openspec_root == "/tmp/repo/specs"
    assert args.claude_projects_root == "/tmp/claude/projects"
    assert args.deployment_name == "test-lp"
    assert args.since == "30d"
    assert args.notes_root == "/tmp/notes"


# ---------- library ----------


def test_library_list_argparse() -> None:
    args = _build_parser().parse_args(["library", "list"])
    assert args.subcmd == "list"


def test_library_list_with_kind_filter() -> None:
    args = _build_parser().parse_args([
        "library", "list", "--kind", "invariants",
    ])
    assert args.kind == "invariants"


def test_library_show_requires_ref() -> None:
    parser = _build_parser()
    with pytest.raises(SystemExit):
        parser.parse_args(["library", "show"])


def test_library_show_argparse() -> None:
    args = _build_parser().parse_args([
        "library", "show", "okg.modules.openspec",
    ])
    assert args.subcmd == "show"
    assert args.ref == "okg.modules.openspec"


# ---------- trace ----------


def test_trace_node_argparse() -> None:
    args = _build_parser().parse_args([
        "trace", "node", "publication:abc",
    ])
    assert args.subcmd == "node"
    assert args.node_id == "publication:abc"


# ---------- generation ----------


def test_generation_describe_argparse() -> None:
    args = _build_parser().parse_args([
        "generation", "describe", "42", "--deployment", "x",
    ])
    assert args.subcmd == "describe"
    assert args.generation_id == 42


# ---------- search ----------


def test_search_accepts_leading_and_trailing_json() -> None:
    leading = _build_parser().parse_args([
        "--json", "search", "--deployment", "x", "--query", "alpha",
    ])
    trailing = _build_parser().parse_args([
        "search", "--deployment", "x", "--query", "alpha", "--json",
    ])
    assert leading.command == "search"
    assert leading.json is True
    assert trailing.command == "search"
    assert trailing.json is True


def _run_search_until_dsn(monkeypatch, capsys, argv: list[str]):
    """Run `okg search` through `main` and stop at the DSN lookup.

    The stub records the query the command resolved and then refuses, so no
    database is touched. Returns (exit code, resolved query or None, stderr).
    """
    import json as _json

    from okg.substrate.cli import _read_side
    from okg.substrate.cli.main import main
    from okg.substrate.errors import SubstrateError

    seen: dict[str, str] = {}

    def _stop(args, *, surface):
        seen["query"] = args.query
        raise SubstrateError("stub: stop before any database")

    monkeypatch.setattr(_read_side, "resolve_read_dsn", _stop)
    code = main(argv)
    err = capsys.readouterr().err.strip().splitlines()
    payload = _json.loads(err[-1]) if err else {}
    return code, seen.get("query"), payload


def test_search_takes_the_query_as_a_positional_argument(
    monkeypatch, capsys,
) -> None:
    code, query, _ = _run_search_until_dsn(
        monkeypatch, capsys,
        ["search", "--deployment", "x", "alpha beta", "--json"],
    )
    assert code == 1  # the stub's refusal, reached after the query resolved
    assert query == "alpha beta"


def test_search_keeps_query_option_as_an_alias(monkeypatch, capsys) -> None:
    code, query, _ = _run_search_until_dsn(
        monkeypatch, capsys,
        ["search", "--deployment", "x", "--query", "alpha", "--json"],
    )
    assert code == 1
    assert query == "alpha"


def test_search_accepts_the_same_query_given_both_ways(
    monkeypatch, capsys,
) -> None:
    _, query, _ = _run_search_until_dsn(
        monkeypatch, capsys,
        ["search", "--deployment", "x", "alpha", "--query", "alpha", "--json"],
    )
    assert query == "alpha"


def test_search_refuses_two_different_queries(monkeypatch, capsys) -> None:
    code, query, payload = _run_search_until_dsn(
        monkeypatch, capsys,
        ["search", "--deployment", "x", "alpha", "--query", "beta", "--json"],
    )
    assert code == 1
    assert query is None  # refused before any lookup
    assert payload["error"]["code"] == "search_query_conflict"
    assert payload["error"]["data"] == {"positional": "alpha", "option": "beta"}


def test_search_refuses_a_missing_query(monkeypatch, capsys) -> None:
    code, query, payload = _run_search_until_dsn(
        monkeypatch, capsys, ["search", "--deployment", "x", "--json"],
    )
    assert code == 1
    assert query is None
    assert payload["error"]["code"] == "search_query_missing"


# ---------- admission ----------


def test_admission_list_argparse() -> None:
    args = _build_parser().parse_args([
        "admission", "list",
        "--deployment", "x",
        "--status", "accepted",
        "--mode", "fast_track",
        "--source", "source-a",
        "--limit", "5",
    ])
    assert args.command == "admission"
    assert args.subcmd == "list"
    assert args.status == "accepted"
    assert args.mode == "fast_track"
    assert args.source == "source-a"
    assert args.limit == 5


def test_admission_list_held_argparse() -> None:
    """Issue #1752: `--held` selects every held mode and excludes `--mode`."""
    args = _build_parser().parse_args([
        "admission", "list", "--status", "pending", "--held",
    ])
    assert args.held is True
    assert args.mode is None
    assert _build_parser().parse_args(["admission", "list"]).held is False
    with pytest.raises(SystemExit):
        _build_parser().parse_args([
            "admission", "list", "--held", "--mode", "review",
        ])


def test_admission_show_argparse() -> None:
    args = _build_parser().parse_args([
        "admission", "show",
        "550e8400-e29b-41d4-a716-446655440000",
        "--deployment", "x",
    ])
    assert args.command == "admission"
    assert args.subcmd == "show"
    assert args.admission_id == "550e8400-e29b-41d4-a716-446655440000"


def test_admission_submit_argparse() -> None:
    args = _build_parser().parse_args([
        "admission", "submit",
        "--deployment", "x",
        "--producer-id", "agent:test",
        "--payload-json", '{"node":{"node_id":"n","subtype":"doc"}}',
        "--idempotency-key", "same",
        "--max-wait-seconds", "12",
    ])
    assert args.command == "admission"
    assert args.subcmd == "submit"
    assert args.producer_id == "agent:test"
    assert args.idempotency_key == "same"
    assert args.max_wait_seconds == 12


def test_admission_decide_argparse() -> None:
    args = _build_parser().parse_args([
        "admission", "decide",
        "550e8400-e29b-41d4-a716-446655440000",
        "--deployment", "x",
        "--decision", "accept",
        "--actor", "reviewer",
        "--reason", "looks-good",
    ])
    assert args.command == "admission"
    assert args.subcmd == "decide"
    assert args.decision == "accept"
    assert args.actor == "reviewer"
    assert args.reason == "looks-good"


# ---------- agent ----------


def test_agent_context_subcommand_is_retired() -> None:
    with pytest.raises(SystemExit):
        _build_parser().parse_args([
            "agent", "context",
            "--deployment", "okg-workspace",
            "--change", "add-agent-okg-usage-contract",
        ])


# ---------- catalog ----------


def test_catalog_load_argparse() -> None:
    args = _build_parser().parse_args([
        "catalog", "load", "--deployment", "/path/to/dep",
    ])
    assert args.subcmd == "load"


def test_catalog_load_with_apply() -> None:
    args = _build_parser().parse_args([
        "catalog", "load", "--deployment", "x", "--apply",
    ])
    assert args.apply is True


# ---------- alias ----------


def test_alias_seed_argparse() -> None:
    """The seed subcmd is `seed-from-subtype`."""
    args = _build_parser().parse_args([
        "alias", "seed-from-subtype",
        "--subtype", "publication",
        "--field", "title",
    ])
    assert args.subcmd == "seed-from-subtype"
    assert args.subtype == "publication"
    assert args.field == "title"


def test_alias_list_argparse() -> None:
    args = _build_parser().parse_args(["alias", "list"])
    assert args.subcmd == "list"


# ---------- views ----------


def test_view_read_surface_argparse() -> None:
    parser = _build_parser()
    args = parser.parse_args([
        "view", "show",
        "--deployment", "test",
        "--view", "test.overview",
    ])
    assert args.command == "view"
    assert args.subcmd == "show"
    assert args.deployment == "test"
    assert args.view == "test.overview"


def test_view_refresh_argparse_rejected() -> None:
    parser = _build_parser()
    with pytest.raises(SystemExit) as exc:
        parser.parse_args(["view", "refresh"])
    assert exc.value.code == 2




# ---------- watch ----------
# Deleted per `harden-runtime-supervisor` §D. The embedded Watcher
# inside `okg runtime worker` is now the only watch path; argparse
# rejects `okg watch` outright.


def test_watch_argparse_rejected_after_hard_cutover() -> None:
    parser = _build_parser()
    with pytest.raises(SystemExit):
        parser.parse_args(["watch", "--deployment", "x"])
