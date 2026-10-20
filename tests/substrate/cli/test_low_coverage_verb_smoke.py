"""Behavioral smoke tests for low-coverage CLI verbs.

Per `audit-test-and-document-cli-surface` Phase 2.3: every verb
marked `real-low-coverage` in the audit doc gets a behavioral
smoke. The cheap, uniform pattern: invoke without `$OKG_DSN`
and assert the handler exits non-zero (most DSN-resolving verbs
short-circuit there); for verbs that don't need a DSN, exercise
the read-side path and assert exit 0.

These complement [test_cli_smoke.py](test_cli_smoke.py)'s argparse
+ dispatch smoke. Together they cover:

  - argparse parses the verb (`test_cli_smoke.py`)
  - handler is wired in the dispatch table (`test_cli_smoke.py`)
  - handler runs and fails predictably without state (this file)
  - handler runs and succeeds for the read-only verbs (this file)

A verb landing here should have its manifest entry updated to
point at this test file (`tests/substrate/cli/CLI_SMOKE_INDEX.yaml`).
"""
from __future__ import annotations

import pytest

from okg.substrate.cli.main import main


def _rc(argv: list[str]) -> int:
    """Run `main(argv)`; surface argparse's SystemExit as an rc.

    main() lets argparse's `sys.exit(2)` propagate. This helper
    normalizes both code paths so a test can write `rc != 0`
    regardless of whether the failure was in argparse or the
    handler.
    """
    try:
        return main(argv)
    except SystemExit as e:
        return e.code if isinstance(e.code, int) else 1


# Verbs whose handler short-circuits at DSN resolution. For each,
# argv satisfies argparse (positional + required flags) but the
# handler reaches the DSN check first and emits an error envelope.
_NO_DSN_NONZERO: list[tuple[str, list[str]]] = [
    # admission
    ("admission_list", ["admission", "list"]),
    ("admission_show", ["admission", "show", "fake-id"]),
    ("admission_decide", ["admission", "decide", "fake-id", "--deployment", "x"]),
    # alias
    ("alias_list", ["alias", "list"]),
    ("alias_add", ["alias", "add", "surface", "canonical"]),
    ("alias_remove", ["alias", "remove", "alias-id"]),
    # branch
    ("branch_list", ["branch", "list", "--deployment", "x"]),
    ("branch_show", ["branch", "show", "--deployment", "x", "default"]),
    ("branch_compact", ["branch", "compact", "--deployment", "x", "default"]),
    # consistency-rerun
    ("consistency_rerun", ["consistency-rerun", "--deployment", "x"]),
    # cron
    ("cron_list", ["cron", "list", "--deployment", "x"]),
    # db
    ("db_status", ["db", "status", "--deployment", "x"]),
    ("db_doctor", ["db", "doctor", "--deployment", "x"]),
    ("db_migrations", ["db", "migrations", "--deployment", "x"]),
    ("db_prune_effective_intervals",
     ["db", "prune-effective-intervals", "--deployment", "x"]),
    ("db_backfill_effective_interval_raw_attrs",
     ["db", "backfill-effective-interval-raw-attrs", "--deployment", "x"]),
    ("db_cold_history", ["db", "cold-history", "--deployment", "x"]),
    # lease
    ("lease_ls", ["lease", "ls"]),
    ("lease_audit", ["lease", "audit", "--deployment", "x"]),
    # library
    ("library_show_unknown", ["library", "show", "okg.modules.bogus_xyz"]),
    # perf (and its deprecated old name `profile`, decision 310)
    ("perf_show", ["perf", "show", "fake-id"]),
    ("profile_show", ["profile", "show", "fake-id"]),
    # runtime
    ("runtime_drain", ["runtime", "drain", "--deployment", "x"]),
    ("runtime_reset", ["runtime", "reset", "--deployment", "x", "--confirm"]),
    ("runtime_smoke", ["runtime", "smoke", "--deployment", "x"]),
    ("runtime_list_schedules", ["runtime", "list-schedules", "--deployment", "x"]),
    ("runtime_measure_recovery", ["runtime", "measure-recovery", "--deployment", "x"]),
    ("runtime_fence_status", ["runtime", "fence-status", "--deployment", "x"]),
    ("runtime_quiesce",
     ["runtime", "quiesce", "--deployment", "x",
      "--owner", "smoke", "--candidate-revision", "rev",
      "--lifecycle-plan-digest", "plan",
      "--lifecycle-contract-digest", "contract"]),
    # chat
    ("chat_sso_check", ["chat", "sso-check", "--deployment", "x"]),
    # sync
    ("sync_explain", ["sync", "explain", "--deployment", "x"]),
    ("sync_health", ["sync", "health", "--deployment", "x"]),
    ("sync_poll", ["sync", "poll", "--deployment", "x"]),
]


# Verbs whose handler does not need a DSN to produce a useful
# envelope (filesystem-only or `--help`-style read). These should
# succeed (rc == 0) under a clean env.
_NO_DSN_SUCCEEDS: list[tuple[str, list[str]]] = [
    ("library_list", ["library", "list"]),
    ("db_list", ["db", "list"]),
    ("perf_list", ["perf", "list"]),
    ("profile_list", ["profile", "list"]),
    ("schema_deployment", ["schema", "deployment.yaml"]),
]


@pytest.fixture
def _no_dsn(monkeypatch: pytest.MonkeyPatch) -> None:
    """Remove every env var that could feed a DSN to the handler."""
    for var in (
        "OKG_DSN",
        "OKG_DEPLOYMENT",
        "OKG_CLUSTER_DSN",
        "PGHOST",
        "PGPORT",
        "PGDATABASE",
        "PGUSER",
        "PGPASSWORD",
    ):
        monkeypatch.delenv(var, raising=False)


@pytest.mark.parametrize(
    "argv",
    [argv for _, argv in _NO_DSN_NONZERO],
    ids=[name for name, _ in _NO_DSN_NONZERO],
)
def test_verb_fails_without_dsn(_no_dsn, argv: list[str]) -> None:
    """Without DSN, the verb returns non-zero (most via error
    envelope; some via argparse's required-arg path)."""
    rc = _rc(["--json", *argv])
    assert rc != 0, (
        f"`okg {' '.join(argv)}` returned 0 without a DSN — "
        f"expected non-zero error envelope"
    )


@pytest.mark.parametrize(
    "argv",
    [argv for _, argv in _NO_DSN_SUCCEEDS],
    ids=[name for name, _ in _NO_DSN_SUCCEEDS],
)
def test_verb_succeeds_without_dsn(_no_dsn, argv: list[str]) -> None:
    """Filesystem-only / pure read verbs succeed without a DSN."""
    rc = _rc(["--json", *argv])
    assert rc == 0, (
        f"`okg {' '.join(argv)}` returned {rc} for a verb that "
        f"should not require a DSN"
    )


def test_runtime_get_event_requires_workflow_id(_no_dsn) -> None:
    """`runtime get-event` errors when --workflow-id / --key absent."""
    rc = _rc(["--json", "runtime", "get-event"])
    assert rc != 0


def test_runtime_trigger_schedule_requires_name(_no_dsn) -> None:
    """`runtime trigger-schedule` errors without --name."""
    rc = _rc(["--json", "runtime", "trigger-schedule", "--deployment", "x"])
    assert rc != 0


def test_runtime_pause_schedule_requires_name(_no_dsn) -> None:
    rc = _rc(["--json", "runtime", "pause-schedule", "--deployment", "x"])
    assert rc != 0


def test_runtime_resume_schedule_requires_name(_no_dsn) -> None:
    rc = _rc(["--json", "runtime", "resume-schedule", "--deployment", "x"])
    assert rc != 0


def test_runtime_escalation_requires_subverb(_no_dsn) -> None:
    """`runtime escalation` exits non-zero without a sub-action."""
    rc = _rc(["--json", "runtime", "escalation"])
    assert rc != 0


def test_db_drop_dry_runs_without_apply(_no_dsn, capsys) -> None:
    """`db drop` dry-runs without --apply (destructive verb, #694).

    Convention point (d): the verb defaults to a dry-run that reports what it
    would drop and mutates nothing. --confirm alone no longer executes.
    """
    import json

    rc = _rc([
        "--json", "db", "drop",
        "--name", "tmp_okg_test",
        "--cluster-dsn", "postgres://nope/x",
    ])
    assert rc == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["dry_run"] is True
    assert payload["applied"] is False


def test_db_dbos_gc_refuses_without_confirm(_no_dsn) -> None:
    """`db dbos-gc` refuses to cancel without --confirm."""
    rc = _rc([
        "--json", "db", "dbos-gc",
        "--deployment", "x", "--apply",
    ])
    assert rc != 0


def test_db_prune_source_blobs_refuses_without_confirm(_no_dsn) -> None:
    """`db prune-source-blobs` refuses to delete with --apply but no --confirm."""
    rc = _rc([
        "--json", "db", "prune-source-blobs",
        "--dsn", "postgres://nope/x", "--apply",
    ])
    assert rc != 0


def test_db_prune_blobs_refuses_without_confirm(_no_dsn) -> None:
    """`db prune-blobs` refuses to delete with --apply but no --confirm."""
    rc = _rc([
        "--json", "db", "prune-blobs", "--kind", "image",
        "--dsn", "postgres://nope/x", "--apply",
    ])
    assert rc != 0


def test_db_remove_job_runtime_subsystem_refuses_without_confirm(_no_dsn) -> None:
    """`db remove-job-runtime-subsystem` refuses without --confirm."""
    rc = _rc([
        "--json", "db", "remove-job-runtime-subsystem",
        "--deployment", "x", "--apply",
    ])
    assert rc != 0


def test_agent_context_subcommand_is_retired(_no_dsn) -> None:
    """`agent context` is retired; graph context uses bootstrap + MCP/OKG."""
    rc = _rc(["--json", "agent", "context", "--deployment", "x"])
    assert rc != 0


def test_admission_submit_requires_payload(_no_dsn) -> None:
    """`admission submit` errors without a real --payload-file or --payload-json."""
    rc = _rc([
        "--json", "admission", "submit",
        "--deployment", "x", "--mode", "review",
        "--producer-id", "test:fixture",
        "--payload-file", "/nonexistent/path/payload.json",
    ])
    assert rc != 0


def test_sync_webhook_requires_payload(_no_dsn) -> None:
    """`sync webhook` errors without a payload file."""
    rc = _rc([
        "--json", "sync", "webhook",
        "--deployment", "x",
        "--source", "test-fixture",
        "--payload-file", "/nonexistent/webhook.json",
    ])
    assert rc != 0
