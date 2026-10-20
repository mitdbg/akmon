"""Phase A schema tests for `okg.branches` and the branch_id auto-fill
trigger on `okg.graph_generations`.

These tests exercise the database-side invariants the
`add-akmon-branch-and-quarantine` Phase A schema is responsible for:

- `okg.branches` exists with the expected columns / check constraints.
- The trigger auto-fills `branch_id` for any insert that omits it,
  materializing a `default` branch row for the deployment lazily.
- Inserting two different branches with the same `(deployment, name)`
  fails the unique constraint.

Publisher integration and the per-branch pending-uniqueness swap land
in the follow-on slice; tests for those live with that slice.

Each test that mutates branches uses a uuid-suffixed deployment name
so test runs do not contaminate each other (clean_runtime does not
truncate okg.branches — that table is part of the durable substrate
catalog, not per-run runtime state).
"""
from __future__ import annotations

import uuid

import psycopg
import pytest

from tests.pytest.catalog_fixture_builders import bind_test_catalog_writer


pytestmark = [pytest.mark.postgres, pytest.mark.serial_db]


def _fresh_deployment_name(prefix: str = "branch-schema-test") -> str:
    return f"{prefix}-{uuid.uuid4().hex[:12]}"


@pytest.mark.postgres
def test_branches_table_shape(clean_runtime: str) -> None:
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT column_name
                  FROM information_schema.columns
                 WHERE table_schema = 'okg'
                   AND table_name = 'branches'
                 ORDER BY ordinal_position
                """
            )
            columns = [row[0] for row in cur.fetchall()]

    assert columns == [
        "branch_id",
        "deployment_name",
        "name",
        "role",
        "base_branch_id",
        "base_generation_id",
        "status",
        "policy_clauses",
        "created_at",
        "closed_at",
        "metadata",
    ]


@pytest.mark.postgres
def test_branch_role_and_status_checks(clean_runtime: str) -> None:
    deployment = _fresh_deployment_name()
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        with conn.cursor() as cur:
            with pytest.raises(psycopg.errors.CheckViolation):
                cur.execute(
                    """
                    INSERT INTO okg.branches
                      (deployment_name, name, role, status)
                    VALUES (%s, 'b', 'bogus', 'active')
                    """,
                    (deployment,),
                )
        with conn.cursor() as cur:
            with pytest.raises(psycopg.errors.CheckViolation):
                cur.execute(
                    """
                    INSERT INTO okg.branches
                      (deployment_name, name, role, status)
                    VALUES (%s, 'b', 'experiment', 'bogus')
                    """,
                    (deployment,),
                )


@pytest.mark.postgres
def test_branch_deployment_name_unique(clean_runtime: str) -> None:
    deployment = _fresh_deployment_name()
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO okg.branches
                  (deployment_name, name, role, status)
                VALUES (%s, 'feature-x', 'experiment', 'active')
                """,
                (deployment,),
            )
        with conn.cursor() as cur:
            with pytest.raises(psycopg.errors.UniqueViolation):
                cur.execute(
                    """
                    INSERT INTO okg.branches
                      (deployment_name, name, role, status)
                    VALUES (%s, 'feature-x', 'experiment', 'active')
                    """,
                    (deployment,),
                )


@pytest.mark.postgres
def test_graph_generations_branch_id_auto_fills(clean_runtime: str) -> None:
    """Inserting a graph_generations row without branch_id triggers
    the auto-fill: a `default` branch is materialized for the
    deployment, and the inserted row's branch_id is set to it.
    """
    deployment = _fresh_deployment_name("trigger-test")
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT max(catalog_version_id)
                  FROM okg.catalog_versions
                 WHERE status = 'applied'
                """
            )
            cv = int(cur.fetchone()[0])
            cur.execute(
                """
                INSERT INTO okg.graph_generations
                  (deployment_name, status, catalog_version_id,
                   correlation_id)
                VALUES (%s, 'pending', %s, 'test-correlation')
                RETURNING branch_id
                """,
                (deployment, cv),
            )
            branch_id = cur.fetchone()[0]
            cur.execute(
                """
                SELECT name, role, status
                  FROM okg.branches
                 WHERE branch_id = %s
                """,
                (branch_id,),
            )
            row = cur.fetchone()

    assert branch_id is not None
    assert row == ("default", "default", "active")


@pytest.mark.postgres
def test_existing_rows_backfilled_to_default_branch(
    clean_runtime: str,
) -> None:
    """After migration, every existing graph_generations row should
    have a non-null branch_id matching the deployment's `default`
    branch. The bootstrap row (generation_id=0, deployment=bootstrap)
    is the canonical pre-existing fixture.
    """
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT gg.branch_id, b.name, b.role, b.status
                  FROM okg.graph_generations gg
                  JOIN okg.branches b ON b.branch_id = gg.branch_id
                 WHERE gg.generation_id = 0
                """
            )
            row = cur.fetchone()

    assert row is not None
    branch_id, name, role, status = row
    assert branch_id is not None
    assert name == "default"
    assert role == "default"
    assert status == "active"


@pytest.mark.postgres
def test_pending_uniqueness_is_per_branch(clean_runtime: str) -> None:
    """Two pending generations on the same (deployment, branch) raise
    UniqueViolation; pending generations on different branches of the
    same deployment do not collide.
    """
    deployment = _fresh_deployment_name("pending-uniqueness")
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT max(catalog_version_id) FROM okg.catalog_versions "
                "WHERE status = 'applied'"
            )
            cv = int(cur.fetchone()[0])
            # Materialize default + a second branch.
            cur.execute(
                """
                INSERT INTO okg.branches
                  (deployment_name, name, role, status)
                VALUES (%s, 'default', 'default', 'active')
                RETURNING branch_id
                """,
                (deployment,),
            )
            default_branch_id = cur.fetchone()[0]
            cur.execute(
                """
                INSERT INTO okg.branches
                  (deployment_name, name, role, status)
                VALUES (%s, 'experiment:test', 'experiment', 'active')
                RETURNING branch_id
                """,
                (deployment,),
            )
            second_branch_id = cur.fetchone()[0]

            # Same branch: second pending must fail.
            cur.execute(
                """
                INSERT INTO okg.graph_generations
                  (deployment_name, branch_id, status,
                   catalog_version_id, correlation_id)
                VALUES (%s, %s, 'pending', %s, 'corr-a')
                """,
                (deployment, default_branch_id, cv),
            )
        with conn.cursor() as cur:
            with pytest.raises(psycopg.errors.UniqueViolation):
                cur.execute(
                    """
                    INSERT INTO okg.graph_generations
                      (deployment_name, branch_id, status,
                       catalog_version_id, correlation_id)
                    VALUES (%s, %s, 'pending', %s, 'corr-b')
                    """,
                    (deployment, default_branch_id, cv),
                )
        with conn.cursor() as cur:
            # Different branch: pending insert must succeed.
            cur.execute(
                """
                INSERT INTO okg.graph_generations
                  (deployment_name, branch_id, status,
                   catalog_version_id, correlation_id)
                VALUES (%s, %s, 'pending', %s, 'corr-c')
                """,
                (deployment, second_branch_id, cv),
            )


@pytest.mark.postgres
def test_doctor_branches_passes_on_clean_deployment(
    clean_runtime: str,
) -> None:
    """Smoke test: the doctor check returns ok=true for a deployment
    whose generations all carry branch_id and have exactly one default
    branch.
    """
    from argparse import Namespace
    from okg.substrate.cli.doctor import _doctor_branches

    deployment = _fresh_deployment_name("doctor-clean")
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT max(catalog_version_id) FROM okg.catalog_versions "
                "WHERE status = 'applied'"
            )
            cv = int(cur.fetchone()[0])
            cur.execute(
                """
                INSERT INTO okg.graph_generations
                  (deployment_name, status, catalog_version_id,
                   correlation_id)
                VALUES (%s, 'pending', %s, 'corr-doctor-clean')
                """,
                (deployment, cv),
            )

    args = Namespace(deployment=deployment, dsn=clean_runtime)
    exit_code = _doctor_branches(args, as_json=True)
    assert exit_code == 0


@pytest.mark.postgres
def test_doctor_branches_fails_on_duplicate_default(
    clean_runtime: str,
) -> None:
    """Doctor check fails when the deployment somehow ends up with
    two default branches (defense against schema drift)."""
    from argparse import Namespace
    from okg.substrate.cli.doctor import _doctor_branches

    deployment = _fresh_deployment_name("doctor-duplicate")
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        with conn.cursor() as cur:
            # Insert two role=default branches manually (the unique
            # constraint is on (deployment_name, name) so we use two
            # different names).
            cur.execute(
                """
                INSERT INTO okg.branches
                  (deployment_name, name, role, status)
                VALUES (%s, 'default', 'default', 'active')
                """,
                (deployment,),
            )
            cur.execute(
                """
                INSERT INTO okg.branches
                  (deployment_name, name, role, status)
                VALUES (%s, 'default-shadow', 'default', 'active')
                """,
                (deployment,),
            )

    args = Namespace(deployment=deployment, dsn=clean_runtime)
    exit_code = _doctor_branches(args, as_json=True)
    assert exit_code == 1


@pytest.mark.postgres
def test_doctor_branches_warns_on_empty_quarantine_branch_after_grace(
    clean_runtime: str,
) -> None:
    """A quarantine branch with no routed envelopes that's older
    than the grace period (default 24h) emits a
    `quarantine_branch_empty` warning. cleanup-akmon-umbrella-risk-
    debts task 2.
    """
    from argparse import Namespace
    from okg.substrate.cli import doctor as cli_doctor
    from okg.substrate.cli.doctor import _doctor_branches

    deployment = _fresh_deployment_name("doctor-empty-quarantine")
    branch_name = "quarantine:typoed-reason"
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO okg.branches "
                "(deployment_name, name, role, status) "
                "VALUES (%s, 'default', 'default', 'active') "
                "ON CONFLICT DO NOTHING",
                (deployment,),
            )
            cur.execute(
                "INSERT INTO okg.branches "
                "(deployment_name, name, role, status, created_at) "
                "VALUES (%s, %s, 'quarantine', 'active', "
                "now() - interval '36 hours')",
                (deployment, branch_name),
            )

    # `_doctor_branches` calls the `_emit_ok` bound on the `cli.doctor`
    # module (populated lazily by `_load_main_helpers()`), so force the
    # helpers to load, then patch the binding the handler actually calls.
    cli_doctor._load_main_helpers()
    captured: dict = {}
    original_emit = cli_doctor._emit_ok
    cli_doctor._emit_ok = lambda payload, **kw: captured.update(payload)
    try:
        args = Namespace(
            deployment=deployment,
            dsn=clean_runtime,
            quarantine_empty_grace_hours=None,
        )
        rc = _doctor_branches(args, as_json=True)
    finally:
        cli_doctor._emit_ok = original_emit

    assert rc == 0  # warn-only; overall ok stays true
    warning_codes = [w["code"] for w in captured.get("warnings", [])]
    assert "quarantine_branch_empty" in warning_codes


@pytest.mark.postgres
def test_doctor_branches_does_not_warn_on_recent_empty_quarantine_branch(
    clean_runtime: str,
) -> None:
    """A quarantine branch within the grace period does NOT emit a
    `quarantine_branch_empty` warning even if it has zero routed
    envelopes."""
    from argparse import Namespace
    from okg.substrate.cli import doctor as cli_doctor
    from okg.substrate.cli.doctor import _doctor_branches

    deployment = _fresh_deployment_name("doctor-recent-quarantine")
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO okg.branches "
                "(deployment_name, name, role, status) "
                "VALUES (%s, 'default', 'default', 'active') "
                "ON CONFLICT DO NOTHING",
                (deployment,),
            )
            cur.execute(
                "INSERT INTO okg.branches "
                "(deployment_name, name, role, status, created_at) "
                "VALUES (%s, 'quarantine:fresh', 'quarantine', "
                "'active', now() - interval '1 hour')",
                (deployment,),
            )

    cli_doctor._load_main_helpers()
    captured: dict = {}
    original_emit = cli_doctor._emit_ok
    cli_doctor._emit_ok = lambda payload, **kw: captured.update(payload)
    try:
        args = Namespace(
            deployment=deployment,
            dsn=clean_runtime,
            quarantine_empty_grace_hours=None,
        )
        _doctor_branches(args, as_json=True)
    finally:
        cli_doctor._emit_ok = original_emit

    warning_codes = [w["code"] for w in captured.get("warnings", [])]
    assert "quarantine_branch_empty" not in warning_codes


@pytest.mark.postgres
def test_doctor_branches_does_not_warn_on_quarantine_branch_with_envelopes(
    clean_runtime: str,
) -> None:
    """A quarantine branch with at least one routed envelope does
    NOT emit a `quarantine_branch_empty` warning, regardless of
    branch age."""
    from argparse import Namespace
    from okg.substrate.cli import doctor as cli_doctor
    from okg.substrate.cli.doctor import _doctor_branches

    deployment = _fresh_deployment_name("doctor-quarantine-with-env")
    branch_name = "quarantine:has-envelope"
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO okg.branches "
                "(deployment_name, name, role, status) "
                "VALUES (%s, 'default', 'default', 'active') "
                "ON CONFLICT DO NOTHING",
                (deployment,),
            )
            cur.execute(
                "INSERT INTO okg.branches "
                "(deployment_name, name, role, status, created_at) "
                "VALUES (%s, %s, 'quarantine', 'active', "
                "now() - interval '7 days')",
                (deployment, branch_name),
            )
            cur.execute(
                """
                INSERT INTO okg.admission_envelopes
                  (deployment_name, producer_id, producer_kind,
                   intent, admission_mode, status, idempotency_key,
                   target_branch_name)
                VALUES
                  (%s, '_lens:test', 'lens',
                   'promote_lens_overlay', 'review', 'pending',
                   'test-idemp', %s)
                """,
                (deployment, branch_name),
            )

    cli_doctor._load_main_helpers()
    captured: dict = {}
    original_emit = cli_doctor._emit_ok
    cli_doctor._emit_ok = lambda payload, **kw: captured.update(payload)
    try:
        args = Namespace(
            deployment=deployment,
            dsn=clean_runtime,
            quarantine_empty_grace_hours=None,
        )
        _doctor_branches(args, as_json=True)
    finally:
        cli_doctor._emit_ok = original_emit

    warning_codes = [w["code"] for w in captured.get("warnings", [])]
    # The branch is past the stale-after-days threshold too, but
    # the specific test is about empty-vs-routed; we just need to
    # confirm `quarantine_branch_empty` does not fire.
    assert "quarantine_branch_empty" not in warning_codes


@pytest.mark.postgres
def test_branch_list_returns_default_after_publish(
    clean_runtime: str,
) -> None:
    """Smoke test: okg branch list returns the deployment's default
    branch after a generation is inserted."""
    from argparse import Namespace
    from okg.substrate.cli.main import _cmd_branch

    deployment = _fresh_deployment_name("cli-list")
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT max(catalog_version_id) FROM okg.catalog_versions "
                "WHERE status = 'applied'"
            )
            cv = int(cur.fetchone()[0])
            # Use status='pending' so the session-level safety check
            # never sees a stray published row from a previous run.
            cur.execute(
                """
                INSERT INTO okg.graph_generations
                  (deployment_name, status, catalog_version_id,
                   correlation_id)
                VALUES (%s, 'pending', %s, 'corr-cli-list')
                """,
                (deployment, cv),
            )

    args = Namespace(
        deployment=deployment,
        dsn=clean_runtime,
        subcmd="list",
    )
    exit_code = _cmd_branch(args, as_json=True)
    assert exit_code == 0


@pytest.mark.postgres
def test_branch_create_inserts_active_experiment(
    clean_runtime: str,
) -> None:
    """`okg branch create <name> --role experiment` creates an active
    Chronos branch with the deployment's default branch as base."""
    from argparse import Namespace
    from okg.substrate.cli.main import _cmd_branch
    from okg.substrate.graph_store import ChronosGraphStore

    deployment = _fresh_deployment_name("create")
    args = Namespace(
        deployment=deployment,
        dsn=clean_runtime,
        subcmd="create",
        name="experiment:fixture-test",
        role="experiment",
        base_branch="default",
    )
    exit_code = _cmd_branch(args, as_json=True)
    assert exit_code == 0

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        store = ChronosGraphStore.from_connection(conn)
        info = store.context.get_branch("experiment:fixture-test")

    assert info.metadata["role"] == "experiment"
    assert info.metadata["status"] == "active"
    assert info.metadata["deployment_name"] == deployment
    assert info.metadata["base_branch"] == "default"


@pytest.mark.postgres
def test_branch_compact_rebases_latest_published_generation(
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from argparse import Namespace
    from okg.substrate.cli.main import _cmd_branch
    from okg.substrate.graph_store import ChronosGraphStore
    from okg.substrate.library.sources.base import NodeFact

    deployment = _fresh_deployment_name("compact")
    branch = "experiment:compact-cli"
    monkeypatch.setenv("CHRONOS_INTERVAL_CONTINUATION_PERCENT", "5")
    store = ChronosGraphStore.connect(clean_runtime)
    try:
        store.create_branch(branch, from_branch="default")
        store.apply_facts(
            [
                NodeFact(
                    "n:compact",
                    "claim",
                    {"text": "compacted through cli"},
                    {},
                    {},
                ),
            ],
            branch=branch,
        )
        # A private branch starts with a deliberately small interval. Four
        # generations are enough to prove latest-generation selection without
        # exhausting that assignment-local interval before compaction runs.
        for index in range(4):
            store.checkpoint_generation(
                f"{deployment}-pre-compact-{index}",
                deployment_name=deployment,
                catalog_version_id=7,
                branch=branch,
            )
    finally:
        store.close()

    monkeypatch.setattr(
        "okg.substrate.graph_store.deployment_interval_continuation_percent",
        lambda name: 95 if name == deployment else None,
    )
    args = Namespace(
        deployment=deployment,
        dsn=clean_runtime,
        subcmd="compact",
        name=branch,
        generation_id=None,
        confirm=True,
    )
    assert _cmd_branch(args, as_json=True) == 0

    store = ChronosGraphStore.connect(
        clean_runtime,
        ensure_schema=False,
        interval_continuation_percent=95,
    )
    try:
        latest = store.latest_generation(
            deployment_name=deployment,
            branch=branch,
            status="published",
        )
        assert latest is not None
        metadata = store.get_generation(latest.checkpoint_id).metadata
        assert metadata["publisher"] == "chronos_compaction"
        compacted = store.checkout_generation(latest.checkpoint_id)
        assert compacted.query(
            "SELECT attrs FROM okg.graph_nodes WHERE node_id = 'n:compact'"
        ) == [{"attrs": {"text": "compacted through cli"}}]
    finally:
        store.close()


@pytest.mark.postgres
def test_branch_abandon_flips_status(clean_runtime: str) -> None:
    """`okg branch abandon <name>` sets status='abandoned' and
    closed_at=now() in Chronos metadata; refuses the default
    branch."""
    from argparse import Namespace
    from okg.substrate.cli.main import _cmd_branch
    from okg.substrate.graph_store import ChronosGraphStore

    deployment = _fresh_deployment_name("abandon")
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        store = ChronosGraphStore.from_connection(conn)
        store.ensure_branch(
            "default",
            from_branch="main",
            metadata={
                "kind": "okg_branch",
                "deployment_name": deployment,
                "branch_name": "default",
                "role": "default",
                "status": "active",
            },
        )
        store.create_branch(
            "experiment:fixture-test",
            from_branch="default",
            metadata={
                "kind": "okg_branch",
                "deployment_name": deployment,
                "branch_name": "experiment:fixture-test",
                "role": "experiment",
                "status": "active",
                "base_branch": "default",
            },
        )

    # Refuse to abandon default.
    args = Namespace(
        deployment=deployment,
        dsn=clean_runtime,
        subcmd="abandon",
        name="default",
    )
    assert _cmd_branch(args, as_json=True) == 1

    # Abandon experiment.
    args = Namespace(
        deployment=deployment,
        dsn=clean_runtime,
        subcmd="abandon",
        name="experiment:fixture-test",
    )
    assert _cmd_branch(args, as_json=True) == 0

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        store = ChronosGraphStore.from_connection(conn)
        info = store.context.get_branch("experiment:fixture-test")

    assert info.metadata["status"] == "abandoned"
    assert info.metadata.get("closed_at") is not None


def test_branch_abandon_leaves_the_chronos_row_and_delete_removes_it(
    clean_runtime: str,
) -> None:
    """`abandon` tombstones metadata; only `delete` removes the branch (#1306).

    Before the fix there was no `delete` verb at all, so an orphaned
    `okg deployment fork-worktree` branch could never be removed: `abandon`
    reported success while the row survived in
    `_chronos_branch_interval_branches`, holding the parent's `child_count`
    above zero and blocking history compaction forever.
    """
    from argparse import Namespace
    from okg.substrate.cli.main import _cmd_branch
    from okg.substrate.graph_store import ChronosGraphStore

    deployment = _fresh_deployment_name("delete")
    fork = "experiment:delete-fixture"

    def _branch_row(conn: psycopg.Connection, branch_id: str):
        return conn.execute(
            "SELECT parent_branch_id, child_count "
            "FROM _chronos_branch_interval_branches WHERE branch_id = %s",
            (branch_id,),
        ).fetchone()

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        store = ChronosGraphStore.from_connection(conn)
        store.ensure_branch(
            "default",
            from_branch="main",
            metadata={
                "kind": "okg_branch",
                "deployment_name": deployment,
                "branch_name": "default",
                "role": "default",
                "status": "active",
            },
        )
        default_children_before = int(_branch_row(conn, "default")[1])
        store.create_branch(
            fork,
            from_branch="default",
            metadata={
                "kind": "okg_branch",
                "deployment_name": deployment,
                "branch_name": fork,
                "role": "experiment",
                "status": "active",
                "base_branch": "default",
                "created_by": "okg deployment fork-worktree",
            },
        )
        assert int(_branch_row(conn, "default")[1]) == default_children_before + 1

    base = dict(deployment=deployment, dsn=clean_runtime, cascade=False)

    # `main` and the deployment's active lineage are never deletable.
    for protected in ("main", "default"):
        args = Namespace(
            **base, subcmd="delete", name=protected, confirm=True
        )
        assert _cmd_branch(args, as_json=True) == 1

    # abandon: metadata only. The row -- and the parent's child_count -- stay.
    args = Namespace(deployment=deployment, dsn=clean_runtime,
                     subcmd="abandon", name=fork)
    assert _cmd_branch(args, as_json=True) == 0
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        row = _branch_row(conn, fork)
        assert row is not None, "abandon must not remove the branch row"
        assert row[0] == "default"
        assert int(_branch_row(conn, "default")[1]) == default_children_before + 1

    # delete without --confirm: dry-run, still no mutation.
    args = Namespace(**base, subcmd="delete", name=fork, confirm=False)
    assert _cmd_branch(args, as_json=True) == 0
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        assert _branch_row(conn, fork) is not None

    # delete --confirm: row gone, parent's child_count decremented.
    args = Namespace(**base, subcmd="delete", name=fork, confirm=True)
    assert _cmd_branch(args, as_json=True) == 0
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        assert _branch_row(conn, fork) is None
        assert int(_branch_row(conn, "default")[1]) == default_children_before

    # ...and a second delete is an honest not-found, not a silent success.
    args = Namespace(**base, subcmd="delete", name=fork, confirm=True)
    assert _cmd_branch(args, as_json=True) == 1


def test_branch_delete_refuses_another_deployments_branch(
    clean_runtime: str,
) -> None:
    """One database holds every deployment's branches; a mistyped
    `--deployment` must not delete someone else's branch."""
    from argparse import Namespace
    from okg.substrate.cli.main import _cmd_branch
    from okg.substrate.graph_store import ChronosGraphStore

    owner = _fresh_deployment_name("delete-owner")
    other = _fresh_deployment_name("delete-other")
    fork = "experiment:cross-deployment-fixture"

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        store = ChronosGraphStore.from_connection(conn)
        store.ensure_branch(
            "default",
            from_branch="main",
            metadata={
                "kind": "okg_branch",
                "deployment_name": owner,
                "branch_name": "default",
                "role": "default",
                "status": "active",
            },
        )
        store.create_branch(
            fork,
            from_branch="default",
            metadata={
                "kind": "okg_branch",
                "deployment_name": owner,
                "branch_name": fork,
                "role": "experiment",
                "status": "active",
            },
        )

    args = Namespace(
        deployment=other,
        dsn=clean_runtime,
        subcmd="delete",
        name=fork,
        cascade=False,
        confirm=True,
    )
    assert _cmd_branch(args, as_json=True) == 1
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        assert conn.execute(
            "SELECT 1 FROM _chronos_branch_interval_branches WHERE branch_id = %s",
            (fork,),
        ).fetchone() is not None


def test_branch_delete_refuses_a_branch_with_children_unless_cascade(
    clean_runtime: str,
) -> None:
    """`delete_branch` cascades into descendants. Taking a subtree down must be
    something the operator asked for, not a side effect."""
    from argparse import Namespace
    from okg.substrate.cli.main import _cmd_branch
    from okg.substrate.graph_store import ChronosGraphStore

    deployment = _fresh_deployment_name("delete-cascade")
    parent = "experiment:cascade-parent"
    child = "experiment:cascade-child"

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        store = ChronosGraphStore.from_connection(conn)
        store.ensure_branch(
            "default",
            from_branch="main",
            metadata={
                "kind": "okg_branch",
                "deployment_name": deployment,
                "branch_name": "default",
                "role": "default",
                "status": "active",
            },
        )
        for name, base_branch in ((parent, "default"), (child, parent)):
            store.create_branch(
                name,
                from_branch=base_branch,
                metadata={
                    "kind": "okg_branch",
                    "deployment_name": deployment,
                    "branch_name": name,
                    "role": "experiment",
                    "status": "active",
                    "base_branch": base_branch,
                },
            )

    args = Namespace(
        deployment=deployment, dsn=clean_runtime, subcmd="delete",
        name=parent, cascade=False, confirm=True,
    )
    assert _cmd_branch(args, as_json=True) == 1
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        assert conn.execute(
            "SELECT count(*) FROM _chronos_branch_interval_branches "
            "WHERE branch_id = ANY(%s)",
            ([parent, child],),
        ).fetchone()[0] == 2

    args = Namespace(
        deployment=deployment, dsn=clean_runtime, subcmd="delete",
        name=parent, cascade=True, confirm=True,
    )
    assert _cmd_branch(args, as_json=True) == 0
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        assert conn.execute(
            "SELECT count(*) FROM _chronos_branch_interval_branches "
            "WHERE branch_id = ANY(%s)",
            ([parent, child],),
        ).fetchone()[0] == 0


@pytest.mark.postgres
def test_publisher_branch_name_writes_resolved_branch_id(
    clean_runtime: str,
) -> None:
    """Publisher(dsn, deployment, branch_name='experiment:foo')
    reserves a generation with branch_id resolved to the named
    experiment branch (not the default)."""
    from okg.substrate.publisher.publisher import Publisher

    deployment = _fresh_deployment_name("pub-branch")
    # Create the experiment branch ahead of the publisher run.
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO okg.branches
                  (deployment_name, name, role, status)
                VALUES (%s, 'default', 'default', 'active')
                RETURNING branch_id
                """,
                (deployment,),
            )
            default_id = int(cur.fetchone()[0])
            cur.execute(
                """
                INSERT INTO okg.branches
                  (deployment_name, name, role, status, base_branch_id)
                VALUES (%s, 'experiment:foo', 'experiment', 'active', %s)
                RETURNING branch_id
                """,
                (deployment, default_id),
            )
            experiment_id = int(cur.fetchone()[0])

    publisher = Publisher(
        clean_runtime,
        deployment,
        branch_name="experiment:foo",
        catalog_identity=bind_test_catalog_writer(clean_runtime, deployment),
    )
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        # Take the deployment-wide lease ourselves to mirror what
        # Publisher.cycle() does; the inner reserve_generation
        # expects to run inside a transaction it controls.
        conn.autocommit = False
        try:
            reserved = publisher.reserve(conn)
            generation_id = reserved.generation_id
        finally:
            conn.commit()

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT branch_id FROM okg.graph_generations "
                " WHERE generation_id = %s",
                (generation_id,),
            )
            row = cur.fetchone()

    assert row is not None
    assert int(row[0]) == experiment_id


# Chronos cutover: the live-table branch-layering tests were removed.
# `test_nodes_live_branch_id_backfilled_from_generation` exercised the
# BEFORE-INSERT trigger that filled `nodes_live.branch_id` from
# `valid_from_gen`; `test_layered_node_view_overrides_default_with_pinned_branch`
# and `test_layered_edge_view_falls_back_to_default` exercised the
# `okg.v_branch_layered_nodes` / `okg.v_branch_layered_edges` views that
# layered branch overlays over `okg.nodes_live` / `okg.edges_live`. The
# cutover dropped those tables (and with them the per-row branch_id /
# valid_from_gen window machinery the layered views and trigger depended
# on), so these tests no longer have a feature to assert against. Branch
# isolation is now a Chronos-native concern, covered by
# `test_chronos_graph_store.py`.


@pytest.mark.postgres
def test_open_session_resolves_branch_id_and_applies_pin(
    clean_runtime: str,
) -> None:
    """open_session(branch_name='experiment:...') stores the
    Chronos branch id on the Session, and apply_branch_pin sets
    the branch GUC to that id for compatibility with callers that
    still inspect it.
    """
    from okg.substrate.mcp.session import (
        apply_branch_pin,
        open_session,
    )
    from okg.substrate.graph_store import ChronosGraphStore

    deployment = _fresh_deployment_name("session-branch")
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT max(catalog_version_id) FROM okg.catalog_versions "
                "WHERE status = 'applied'"
            )
            cv = int(cur.fetchone()[0])
        store = ChronosGraphStore.from_connection(conn)
        store.ensure_branch(
            "default",
            from_branch="main",
            metadata={
                "kind": "okg_branch",
                "deployment_name": deployment,
                "branch_name": "default",
                "role": "default",
                "status": "active",
            },
        )
        store.create_branch(
            "experiment:s",
            from_branch="default",
            metadata={
                "kind": "okg_branch",
                "deployment_name": deployment,
                "branch_name": "experiment:s",
                "role": "experiment",
                "status": "active",
                "base_branch": "default",
            },
        )
        store.checkpoint_generation(
            None,
            branch="experiment:s",
            deployment_name=deployment,
            catalog_version_id=cv,
            status="published",
        )

        session = open_session(
            conn,
            deployment_name=deployment,
            branch_name="experiment:s",
        )

        assert session.branch_name == "experiment:s"
        assert session.branch_id == "experiment:s"

        apply_branch_pin(conn, session)
        with conn.cursor() as cur:
            cur.execute(
                "SELECT current_setting('okg.pinned_branch_id', true)"
            )
            pinned = cur.fetchone()[0]
        assert pinned == "experiment:s"


@pytest.mark.postgres
def test_open_session_rejects_unknown_non_default_branch(
    clean_runtime: str,
) -> None:
    """A non-default branch that doesn't exist for the deployment
    rejects with SubstrateError pointing at `okg branch create`."""
    from okg.substrate.errors import SubstrateError
    from okg.substrate.mcp.session import open_session

    deployment = _fresh_deployment_name("session-unknown")
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT max(catalog_version_id) FROM okg.catalog_versions "
                "WHERE status = 'applied'"
            )
            cv = int(cur.fetchone()[0])
            cur.execute(
                """
                INSERT INTO okg.graph_generations
                  (deployment_name, status, catalog_version_id,
                   published_at, correlation_id)
                VALUES (%s, 'published', %s, now(),
                        'corr-session-unknown')
                """,
                (deployment, cv),
            )

        with pytest.raises(SubstrateError):
            open_session(
                conn,
                deployment_name=deployment,
                branch_name="experiment:missing",
            )


@pytest.mark.postgres
def test_mcp_get_node_returns_branch_override(clean_runtime: str) -> None:
    """End-to-end check: an MCP session pinned to an experiment
    branch reads the override row via `get_node`, not the default
    branch row. Exercises Chronos branch checkout semantics.
    """
    from okg.substrate.mcp.session import (
        apply_branch_pin,
        open_session,
    )
    from okg.substrate.mcp.tools import inspect_node
    from okg.substrate.graph_store import ChronosGraphStore
    from okg.substrate.library.sources.base import NodeFact

    deployment = _fresh_deployment_name("mcp-get-node-branch")
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT max(catalog_version_id) FROM okg.catalog_versions "
                "WHERE status = 'applied'"
            )
            cv = int(cur.fetchone()[0])
        store = ChronosGraphStore.from_connection(conn)
        node_id = f"doc:test:{deployment}:override"
        store.apply_facts([
            NodeFact(node_id, "document", {"label": "on-default"}, {}, {}),
        ])
        default_gen = store.checkpoint_generation(
            None,
            branch="default",
            deployment_name=deployment,
            catalog_version_id=cv,
            status="published",
        )
        store.create_branch(
            "experiment:o",
            from_branch="default",
            metadata={
                "kind": "okg_branch",
                "deployment_name": deployment,
                "branch_name": "experiment:o",
                "role": "experiment",
                "status": "active",
                "base_branch": "default",
                "base_generation_id": default_gen,
            },
        )
        store.apply_facts([
            NodeFact(node_id, "document", {"label": "on-experiment"}, {}, {}),
        ], branch="experiment:o")
        experiment_gen = store.checkpoint_generation(
            None,
            branch="experiment:o",
            deployment_name=deployment,
            catalog_version_id=cv,
            status="published",
        )

        # Default-branch session: get_node returns the default row.
        default_session = open_session(
            conn,
            deployment_name=deployment,
            requested_generation=default_gen,
        )
        apply_branch_pin(conn, default_session)
        default_node = inspect_node(
            conn,
            default_session,
            node_id,
        )
        assert default_node is not None
        assert default_node["attrs"]["label"] == "on-default"

        # Experiment-branch session: get_node returns the override.
        experiment_session = open_session(
            conn,
            deployment_name=deployment,
            requested_generation=experiment_gen,
            branch_name="experiment:o",
        )
        apply_branch_pin(conn, experiment_session)
        experiment_node = inspect_node(
            conn,
            experiment_session,
            node_id,
        )
        assert experiment_node is not None
        assert experiment_node["attrs"]["label"] == "on-experiment"


@pytest.mark.postgres
def test_publisher_unknown_branch_rejects(clean_runtime: str) -> None:
    """Publisher with an unknown non-default branch_name raises
    BranchNotRegistered pointing the operator at `okg branch create`."""
    from okg.substrate.errors import BranchNotRegistered
    from okg.substrate.publisher.publisher import Publisher

    deployment = _fresh_deployment_name("pub-unknown")
    # Materialize the default branch so the schema isn't entirely
    # empty for this deployment.
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO okg.branches
                  (deployment_name, name, role, status)
                VALUES (%s, 'default', 'default', 'active')
                """,
                (deployment,),
            )

    publisher = Publisher(
        clean_runtime,
        deployment,
        branch_name="experiment:missing",
        catalog_identity=bind_test_catalog_writer(clean_runtime, deployment),
    )
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        conn.autocommit = False
        try:
            with pytest.raises(BranchNotRegistered):
                publisher.reserve(conn)
        finally:
            conn.rollback()
