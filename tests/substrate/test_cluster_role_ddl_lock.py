"""ALTER ROLE from parallel test workers must queue, not collide.

A role is one ``pg_authid`` row shared by every database on the server, and
``ALTER ROLE`` rewrites it without locking it first. Two sessions altering the
same role at once therefore do not wait for each other: one fails with
``tuple concurrently updated``. xdist workers each own a database on ONE
server, so per-test fixtures that altered ``okg_mcp`` in every worker failed
test setup intermittently in ``substrate-db-linux`` (Tests runs 35893854631
and 35817734765). ``alter_role_under_cluster_lock`` serializes every such
statement behind one advisory lock in the maintenance database.

The first cases drive the helper from several threads, each on its own
connection, against the test server; later sections cover the product
writers and the Kubernetes chart's start-up step. Without the lock the stress cases fail
with ``tuple concurrently updated`` and the blocking case sees the ALTER run
while the lock is held.
"""

from __future__ import annotations

import os
import subprocess
import sys
import textwrap
import threading
import time
import uuid
from pathlib import Path

import psycopg
import psycopg.sql
import pytest

from okg.substrate.db.role_lock import lock_roles_for_alter
from okg.substrate.runtime.bootstrap import reconcile_mcp_login_role
from tests.substrate.conftest import (
    _CLUSTER_RUNTIME_ROLE_LOCK_KEY,
    alter_role_under_cluster_lock,
    enable_okg_mcp_login,
)

pytestmark = pytest.mark.postgres

_WORKERS = 4
_ITERATIONS = 20


def _maintenance_dsn(dsn: str) -> str:
    info = psycopg.conninfo.conninfo_to_dict(dsn)
    info["dbname"] = "postgres"
    return psycopg.conninfo.make_conninfo(**info)


def _run_in_parallel(work) -> list[str]:
    """Run ``work()`` _ITERATIONS times in each of _WORKERS threads at once."""
    start = threading.Barrier(_WORKERS)
    errors: list[str] = []
    lock = threading.Lock()

    def worker() -> None:
        start.wait()
        for _ in range(_ITERATIONS):
            try:
                work()
            except psycopg.Error as exc:
                with lock:
                    errors.append(f"{type(exc).__name__}: {exc}")

    threads = [threading.Thread(target=worker) for _ in range(_WORKERS)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=300)
    assert not any(thread.is_alive() for thread in threads), "a worker hung"
    return errors


@pytest.fixture
def probe_role(okg_migrated: str):
    """A throwaway role, so toggling LOGIN never touches a role other tests use."""
    role = f"okg_alter_role_probe_{uuid.uuid4().hex[:12]}"
    with psycopg.connect(_maintenance_dsn(okg_migrated), autocommit=True) as conn:
        conn.execute(
            psycopg.sql.SQL("CREATE ROLE {} NOLOGIN").format(
                psycopg.sql.Identifier(role)
            )
        )
    try:
        yield role
    finally:
        with psycopg.connect(
            _maintenance_dsn(okg_migrated), autocommit=True
        ) as conn:
            conn.execute(
                psycopg.sql.SQL("DROP ROLE IF EXISTS {}").format(
                    psycopg.sql.Identifier(role)
                )
            )


def test_alter_role_waits_while_another_session_holds_the_cluster_lock(
    okg_migrated: str, probe_role: str
) -> None:
    """The helper must queue behind the lock, not run beside it."""
    finished = threading.Event()
    failures: list[BaseException] = []

    def alter() -> None:
        try:
            alter_role_under_cluster_lock(
                okg_migrated,
                psycopg.sql.SQL("ALTER ROLE {} LOGIN").format(
                    psycopg.sql.Identifier(probe_role)
                ),
                lock_role=probe_role,
            )
        except BaseException as exc:  # surfaced by the assertion below
            failures.append(exc)
        finally:
            finished.set()

    with psycopg.connect(
        _maintenance_dsn(okg_migrated), autocommit=True
    ) as holder:
        holder.execute(
            "SELECT pg_advisory_lock(%s, %s)", _CLUSTER_RUNTIME_ROLE_LOCK_KEY
        )
        try:
            thread = threading.Thread(target=alter)
            thread.start()
            time.sleep(1.0)
            assert not finished.is_set(), (
                "ALTER ROLE ran while another session held the cluster role "
                "lock; parallel workers would collide on pg_authid"
            )
            can_login = holder.execute(
                "SELECT rolcanlogin FROM pg_roles WHERE rolname = %s",
                (probe_role,),
            ).fetchone()
            assert can_login == (False,)
        finally:
            holder.execute(
                "SELECT pg_advisory_unlock(%s, %s)", _CLUSTER_RUNTIME_ROLE_LOCK_KEY
            )
    thread.join(timeout=60)
    assert finished.is_set() and not failures, failures
    with psycopg.connect(_maintenance_dsn(okg_migrated)) as conn:
        row = conn.execute(
            "SELECT rolcanlogin FROM pg_roles WHERE rolname = %s", (probe_role,)
        ).fetchone()
    assert row == (True,)


def test_parallel_login_toggles_on_one_role_never_collide(
    okg_migrated: str, probe_role: str
) -> None:
    """Four workers flip one role LOGIN/NOLOGIN 20 times each: no failures."""
    statements = [
        psycopg.sql.SQL("ALTER ROLE {} LOGIN PASSWORD {}").format(
            psycopg.sql.Identifier(probe_role), psycopg.sql.Literal("probe")
        ),
        psycopg.sql.SQL("ALTER ROLE {} NOLOGIN").format(
            psycopg.sql.Identifier(probe_role)
        ),
    ]
    counter = iter(range(10**6))

    def work() -> None:
        alter_role_under_cluster_lock(
            okg_migrated, statements[next(counter) % 2], lock_role=probe_role
        )

    errors = _run_in_parallel(work)
    assert errors == [], f"{len(errors)} failures: {sorted(set(errors))}"


def test_parallel_okg_mcp_login_setup_never_collides(okg_migrated: str) -> None:
    """The fixture's own path, from four workers at once, 20 times each.

    Only ever sets LOGIN, so other tests reading as ``okg_mcp`` are unaffected.
    """
    errors = _run_in_parallel(lambda: enable_okg_mcp_login(okg_migrated))
    assert errors == [], f"{len(errors)} failures: {sorted(set(errors))}"
    with psycopg.connect(okg_migrated) as conn:
        row = conn.execute(
            "SELECT rolcanlogin FROM pg_roles WHERE rolname = 'okg_mcp'"
        ).fetchone()
    assert row == (True,)


# ---------------------------------------------------------------------------
# Issue #1985: the product writers take the role's CLUSTER-WIDE lock.
#
# The advisory lock above is keyed per database, so it never reached product
# code: ``okg runtime bootstrap`` reconciles the MCP role from inside the
# deployment database, and any other database's session altering the same
# role collided with it. ``lock_roles_for_alter`` holds PostgreSQL's own lock
# on the role, which every database on the server shares.
# ---------------------------------------------------------------------------


def _probe_role_dsn(dsn: str, role: str, password: str) -> str:
    return psycopg.conninfo.make_conninfo(dsn, user=role, password=password)


def _can_login(dsn: str, role: str) -> bool:
    with psycopg.connect(dsn) as conn:
        row = conn.execute(
            "SELECT rolcanlogin FROM pg_roles WHERE rolname = %s", (role,)
        ).fetchone()
    return bool(row[0])


def test_mcp_login_reconcile_waits_for_the_role_lock_held_in_another_database(
    okg_migrated: str, probe_role: str
) -> None:
    """``reconcile_mcp_login_role`` queues behind a holder in ANOTHER database.

    The holder takes the role lock from the maintenance database, the way
    the test helper and a second deployment's bootstrap would; the reconcile
    runs inside the worker database. Before #1985 the reconcile's ALTER did
    not wait, so it ran beside the holder, which is the collision.
    """
    dbname = psycopg.conninfo.conninfo_to_dict(okg_migrated)["dbname"]
    grant = psycopg.sql.SQL("GRANT CONNECT ON DATABASE {} TO {}").format(
        psycopg.sql.Identifier(dbname), psycopg.sql.Identifier(probe_role)
    )
    revoke = psycopg.sql.SQL("REVOKE CONNECT ON DATABASE {} FROM {}").format(
        psycopg.sql.Identifier(dbname), psycopg.sql.Identifier(probe_role)
    )
    role_dsn = _probe_role_dsn(okg_migrated, probe_role, "reconcile-1985")
    finished = threading.Event()
    results: list[dict] = []
    failures: list[BaseException] = []

    def reconcile() -> None:
        try:
            with psycopg.connect(okg_migrated, autocommit=True) as conn:
                results.append(reconcile_mcp_login_role(conn, mcp_dsn=role_dsn))
        except BaseException as exc:  # surfaced by the assertion below
            failures.append(exc)
        finally:
            finished.set()

    with psycopg.connect(okg_migrated, autocommit=True) as admin:
        admin.execute(grant)
    try:
        with psycopg.connect(_maintenance_dsn(okg_migrated)) as holder:
            lock_roles_for_alter(holder, [probe_role])
            thread = threading.Thread(target=reconcile)
            thread.start()
            time.sleep(1.0)
            assert not finished.is_set(), (
                "reconcile_mcp_login_role altered the role while another "
                "database's session held its lock; the two ALTERs collide on "
                f"pg_authid (#1985). result={results} failures={failures}"
            )
            assert _can_login(okg_migrated, probe_role) is False
            holder.commit()
        thread.join(timeout=60)
        assert finished.is_set() and not failures, failures
        assert results[0]["reconciled"] is True, results
        assert _can_login(okg_migrated, probe_role) is True
    finally:
        with psycopg.connect(okg_migrated, autocommit=True) as admin:
            admin.execute(revoke)


def test_locked_alters_from_two_databases_never_collide(
    okg_migrated: str, probe_role: str
) -> None:
    """Workers split across two databases alter one role 20 times each."""
    targets = [okg_migrated, _maintenance_dsn(okg_migrated)]
    counter = iter(range(10**6))
    lock = threading.Lock()

    def work() -> None:
        with lock:
            n = next(counter)
        with psycopg.connect(targets[n % 2]) as conn:
            lock_roles_for_alter(conn, [probe_role])
            conn.execute(
                psycopg.sql.SQL("ALTER ROLE {} LOGIN PASSWORD {}").format(
                    psycopg.sql.Identifier(probe_role),
                    psycopg.sql.Literal(f"probe-{n}"),
                )
            )

    errors = _run_in_parallel(work)
    assert errors == [], f"{len(errors)} failures: {sorted(set(errors))}"


def test_role_lock_keeps_the_existing_comment(
    okg_migrated: str, probe_role: str
) -> None:
    """The lock rewrites the comment with its own value: nothing visible changes."""
    maintenance = _maintenance_dsn(okg_migrated)
    with psycopg.connect(maintenance, autocommit=True) as conn:
        conn.execute(
            psycopg.sql.SQL("COMMENT ON ROLE {} IS 'operator note'").format(
                psycopg.sql.Identifier(probe_role)
            )
        )
    with psycopg.connect(maintenance) as conn:
        lock_roles_for_alter(conn, [probe_role])
    with psycopg.connect(maintenance) as conn:
        row = conn.execute(
            "SELECT shobj_description(oid, 'pg_authid') FROM pg_roles "
            "WHERE rolname = %s",
            (probe_role,),
        ).fetchone()
    assert row == ("operator note",)


def test_role_lock_refuses_autocommit_without_a_transaction(
    okg_migrated: str, probe_role: str
) -> None:
    """Outside a transaction the lock would end with its own statement."""
    with psycopg.connect(okg_migrated, autocommit=True) as conn:
        with pytest.raises(RuntimeError, match="needs an open transaction"):
            lock_roles_for_alter(conn, [probe_role])
        with conn.transaction():
            lock_roles_for_alter(conn, [probe_role])


def test_role_lock_refuses_a_missing_role(okg_migrated: str) -> None:
    with psycopg.connect(okg_migrated) as conn:
        with pytest.raises(LookupError, match="does not exist"):
            lock_roles_for_alter(conn, [f"okg_absent_{uuid.uuid4().hex[:12]}"])



def _conn_limits(dsn: str, roles: list[str]) -> dict[str, int]:
    with psycopg.connect(dsn) as conn:
        rows = conn.execute(
            "SELECT rolname, rolconnlimit FROM pg_roles WHERE rolname = ANY(%s)",
            (roles,),
        ).fetchall()
    return dict(rows)


def _wait_until_blocked_on_a_lock(dsn: str, application_name: str) -> bool:
    """Poll until the named backend waits on a heavyweight lock."""
    deadline = time.monotonic() + 30.0
    with psycopg.connect(dsn, autocommit=True) as conn:
        while time.monotonic() < deadline:
            row = conn.execute(
                "SELECT wait_event_type FROM pg_stat_activity "
                "WHERE application_name = %s",
                (application_name,),
            ).fetchone()
            if row is not None and row[0] == "Lock":
                return True
            time.sleep(0.05)
    return False


def test_role_split_waits_for_the_role_lock_held_in_another_database(
    clean_runtime: str,
) -> None:
    """The runtime role split queues behind a holder in ANOTHER database.

    ``role_split.sql`` sets the connection limit of ``okg_substrate`` and
    ``okg_dbos``, which every deployment on the server shares. Before this
    fix those ALTERs took no lock, so a second deployment's bootstrap (here:
    a holder in the maintenance database) could collide with them (#1985).
    """
    from okg.substrate.runtime.bootstrap import apply_role_split

    deployment = "lock1985"
    roles = ["okg_dbos", f"okg_deployment_{deployment}", "okg_substrate"]
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        conn.execute("CREATE SCHEMA IF NOT EXISTS dbos")
        # First run creates the roles; the second one is the one under test.
        apply_role_split(conn, deployment=deployment, dbos_schema="dbos")
        with conn.transaction():
            lock_roles_for_alter(conn, roles)
            for role in roles:
                conn.execute(
                    psycopg.sql.SQL("ALTER ROLE {} CONNECTION LIMIT -1").format(
                        psycopg.sql.Identifier(role)
                    )
                )

    application_name = f"role-split-1985-{uuid.uuid4().hex[:8]}"
    info = psycopg.conninfo.conninfo_to_dict(clean_runtime)
    info["application_name"] = application_name
    split_dsn = psycopg.conninfo.make_conninfo(**info)
    finished = threading.Event()
    failures: list[BaseException] = []

    def role_split() -> None:
        try:
            with psycopg.connect(split_dsn, autocommit=True) as conn:
                apply_role_split(conn, deployment=deployment, dbos_schema="dbos")
        except BaseException as exc:  # surfaced by the assertion below
            failures.append(exc)
        finally:
            finished.set()

    with psycopg.connect(_maintenance_dsn(clean_runtime)) as holder:
        lock_roles_for_alter(holder, ["okg_substrate"])
        thread = threading.Thread(target=role_split)
        thread.start()
        blocked = _wait_until_blocked_on_a_lock(clean_runtime, application_name)
        assert blocked and not finished.is_set(), (
            "apply_role_split did not wait for the okg_substrate lock another "
            "database's session held; its ALTER ROLE can collide on pg_authid "
            f"(#1985). finished={finished.is_set()} failures={failures}"
        )
        assert _conn_limits(clean_runtime, roles)["okg_substrate"] == -1
        holder.commit()
    thread.join(timeout=60)
    assert finished.is_set() and not failures, failures
    assert _conn_limits(clean_runtime, roles) == {
        "okg_dbos": 6,
        f"okg_deployment_{deployment}": 4,
        "okg_substrate": 12,
    }


# ---------------------------------------------------------------------------
# The Kubernetes chart's ``mcp-role`` init container gives ``okg_mcp`` its
# login and password at every pod start. It was the last product statement
# that altered the role without the role lock (issue #2554).
# ---------------------------------------------------------------------------

_RUNTIME_STATEFULSET = (
    Path(__file__).resolve().parents[2]
    / "ops/kubernetes/okg/templates/runtime-statefulset.yaml"
)


def _mcp_role_init_script() -> str:
    """The Python the ``mcp-role`` init container runs, read from the chart."""
    template = _RUNTIME_STATEFULSET.read_text(encoding="utf-8")
    container = template.split("- name: mcp-role\n", 1)[1]
    block = container.split("            - |\n", 1)[1].split("          env:\n", 1)[0]
    return textwrap.dedent(block)


def test_kubernetes_mcp_role_step_waits_for_the_role_lock(
    okg_migrated: str, probe_role: str
) -> None:
    """The chart's own script, run for real against a throwaway role.

    Only the role name is swapped, so the shared ``okg_mcp`` keeps its local
    password. A holder in the maintenance database takes the role lock; the
    script, connected to the worker database, must wait for it and then set
    LOGIN and the password it was given.
    """
    script = _mcp_role_init_script()
    # Checked in two parts: this file holds no literal ALTER of the shared
    # role, which test_shared_role_alter_is_locked.py refuses in any test.
    assert "ALTER ROLE" in script and "okg_mcp" in script, script
    script = script.replace("okg_mcp", probe_role)
    password = f"init-{uuid.uuid4().hex}"
    application_name = f"mcp-role-2554-{uuid.uuid4().hex[:8]}"
    info = psycopg.conninfo.conninfo_to_dict(okg_migrated)
    info["application_name"] = application_name
    env = dict(
        os.environ,
        OKG_DSN=psycopg.conninfo.make_conninfo(**info),
        MCP_ROLE_PASSWORD=password,
    )

    with psycopg.connect(_maintenance_dsn(okg_migrated)) as holder:
        lock_roles_for_alter(holder, [probe_role])
        process = subprocess.Popen(
            [sys.executable, "-c", script],
            env=env,
            stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            # its own session, away from the pytest-xdist worker group (#703)
            start_new_session=True,
        )
        try:
            blocked = _wait_until_blocked_on_a_lock(okg_migrated, application_name)
            assert blocked and process.poll() is None, (
                "the mcp-role init script did not wait for the role lock another "
                "database's session held; its ALTER ROLE can collide on pg_authid "
                f"(#2554). returncode={process.poll()}"
            )
            assert _can_login(okg_migrated, probe_role) is False
            holder.commit()
            _stdout, stderr = process.communicate(timeout=60)
        finally:
            if process.poll() is None:
                process.kill()
                process.communicate()
    assert process.returncode == 0, stderr
    assert _can_login(okg_migrated, probe_role) is True
    grant = psycopg.sql.SQL("GRANT CONNECT ON DATABASE {} TO {}").format(
        psycopg.sql.Identifier(info["dbname"]), psycopg.sql.Identifier(probe_role)
    )
    revoke = psycopg.sql.SQL("REVOKE CONNECT ON DATABASE {} FROM {}").format(
        psycopg.sql.Identifier(info["dbname"]), psycopg.sql.Identifier(probe_role)
    )
    with psycopg.connect(okg_migrated, autocommit=True) as admin:
        admin.execute(grant)
    try:
        with psycopg.connect(
            _probe_role_dsn(okg_migrated, probe_role, password)
        ) as conn:
            assert conn.execute("SELECT current_user").fetchone() == (probe_role,)
    finally:
        with psycopg.connect(okg_migrated, autocommit=True) as admin:
            admin.execute(revoke)
