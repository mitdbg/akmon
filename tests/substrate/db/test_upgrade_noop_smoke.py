"""No-op upgrade smoke: pass only under the validated no-work contract.

Covers req.migrate-idempotent.noop-smoke of ``make-migrate-idempotent``
(#669 regression items 4-5), design ruling 4. An unchanged-source smoke
publish emits exactly ``status=skipped, publish_skipped=true,
reason=no_source_work`` (ingest/service.py:391-396). ``assert_publish_proof``
accepts that shape ONLY when the smoke phase opts in via
``allow_no_source_work=True``, and only when every requested source is OK and
the DLQ / runtime-queue checks still pass. Every other shape — a source
failure, a skip with a different reason, a missing status, or dead-letter work
— still fails.
"""
from __future__ import annotations

import sys
import threading
from pathlib import Path
from uuid import uuid4

import psycopg
import pytest
from psycopg import sql
from psycopg.conninfo import conninfo_to_dict, make_conninfo

import okg.substrate.publish_proof as pp
from okg.substrate.db.migrate import migrate
from okg.substrate.publish_proof import assert_publish_proof

_DSN = "postgres://unused-in-these-hermetic-tests"

_NOMOS_FK_CONSTRAINTS = (
    "federation_connection_requests_nomos_decision_id_fkey",
    "federation_peer_trust_pins_nomos_decision_id_fkey",
    "federation_route_events_nomos_decision_id_fkey",
    "poros_incorporation_events_nomos_decision_id_fkey",
    "nomos_external_claims_nomos_decision_id_fkey",
)
_EXECUTABLE_CHECKSUM_KEY = "executable_schema_checksum"


def _no_op_report(source_status: str = "OK") -> dict:
    """An ingest report whose commit publish skipped with no_source_work."""
    return {
        "ordered": ["canary"],
        "results": [{"name": "canary", "status": source_status}],
        "publishes": [
            {
                "label": "commit",
                "status": "skipped",
                "publish_skipped": True,
                "reason": "no_source_work",
            }
        ],
    }


@pytest.fixture(autouse=True)
def _healthy_queue(monkeypatch: pytest.MonkeyPatch) -> None:
    """Default: a healthy, empty queue so the no-op path can pass."""
    monkeypatch.setattr(pp, "dlq_count", lambda dsn: (0, {}))


def test_validated_no_source_work_passes_when_opted_in() -> None:
    verdict = assert_publish_proof(
        _no_op_report(),
        dsn=_DSN,
        allow_updated_rows=True,
        allow_no_source_work=True,
    )
    assert verdict["ok"] is True
    assert verdict.get("no_source_work") is True


def test_no_source_work_rejected_without_opt_in() -> None:
    # Bootstrap-verify (strict) must NOT accept the skip: the triple carries a
    # skipped status, which is not "published".
    verdict = assert_publish_proof(
        _no_op_report(),
        dsn=_DSN,
        allow_updated_rows=True,
    )
    assert verdict["ok"] is False


def test_source_failure_still_fails_even_when_opted_in() -> None:
    verdict = assert_publish_proof(
        _no_op_report(source_status="ERROR"),
        dsn=_DSN,
        allow_updated_rows=True,
        allow_no_source_work=True,
    )
    assert verdict["ok"] is False


def test_skip_with_other_reason_still_fails() -> None:
    report = _no_op_report()
    report["publishes"][0]["reason"] = "upstream_source_failed"
    verdict = assert_publish_proof(
        report,
        dsn=_DSN,
        allow_updated_rows=True,
        allow_no_source_work=True,
    )
    assert verdict["ok"] is False


def test_missing_status_still_fails() -> None:
    report = {
        "ordered": ["canary"],
        "results": [{"name": "canary", "status": "OK"}],
        "publishes": [],
    }
    verdict = assert_publish_proof(
        report,
        dsn=_DSN,
        allow_updated_rows=True,
        allow_no_source_work=True,
    )
    assert verdict["ok"] is False


def test_missing_requested_source_result_still_fails() -> None:
    report = _no_op_report()
    report["ordered"] = ["canary", "missing"]
    verdict = assert_publish_proof(
        report,
        dsn=_DSN,
        allow_updated_rows=True,
        allow_no_source_work=True,
    )
    assert verdict["ok"] is False


def test_duplicate_source_result_still_fails() -> None:
    report = _no_op_report()
    report["results"].append({"name": "canary", "status": "OK"})
    verdict = assert_publish_proof(
        report,
        dsn=_DSN,
        allow_updated_rows=True,
        allow_no_source_work=True,
    )
    assert verdict["ok"] is False


def test_no_op_over_poisoned_queue_fails(monkeypatch: pytest.MonkeyPatch) -> None:
    # DLQ / runtime-queue checks still run on the skip path: a no-op smoke over
    # dead-lettered work must fail.
    monkeypatch.setattr(pp, "dlq_count", lambda dsn: (3, {}))
    verdict = assert_publish_proof(
        _no_op_report(),
        dsn=_DSN,
        allow_updated_rows=True,
        allow_no_source_work=True,
    )
    assert verdict["ok"] is False


def test_no_op_over_unhealthy_runtime_fails(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(pp, "dlq_count", lambda dsn: (0, {"runtime_degraded": 1}))
    verdict = assert_publish_proof(
        _no_op_report(),
        dsn=_DSN,
        allow_updated_rows=True,
        allow_no_source_work=True,
    )
    assert verdict["ok"] is False


# --------------------------------------------------------------------------
# Bounded Nomos orphan-validation (#669 regression item 7), design ruling 3.
# --------------------------------------------------------------------------


def _nomos_fk_oids(dsn: str) -> dict[str, int]:
    with psycopg.connect(dsn) as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT conname, oid FROM pg_constraint WHERE conname = ANY(%s)",
            (list(_NOMOS_FK_CONSTRAINTS),),
        )
        return {row[0]: int(row[1]) for row in cur.fetchall()}


@pytest.mark.postgres
def test_nomos_fks_are_not_dropped_and_readded_on_replay(okg_dsn: str) -> None:
    """A converged run does NOT drop + re-validate the composite FKs.

    Shape assertion (design ruling 3): a dropped-and-re-added constraint gets a
    NEW pg_constraint.oid, and a re-add forces the pathological full-table FK
    re-validation. We force the schema body to REPLAY (so the FK block runs)
    and assert every composite FK keeps its oid — proving the per-child
    existence guard skipped the drop / orphan anti-join / re-add. Pre-fix the
    block dropped and re-added all five FKs unconditionally, minting new oids.
    """
    dbname = f"okg_nomosfk_{uuid4().hex[:12]}"
    dsn = _create_disposable_database(okg_dsn, dbname)
    try:
        migrate(dsn)
        before = _nomos_fk_oids(dsn)
        assert set(before) == set(_NOMOS_FK_CONSTRAINTS), (
            "expected all five composite nomos FKs installed"
        )

        # Force the schema body to replay by invalidating the recorded
        # executable-schema checksum, so the guarded FK block runs again.
        with psycopg.connect(dsn, autocommit=True) as conn, conn.cursor() as cur:
            cur.execute(
                """
                UPDATE okg.schema_migrations
                   SET result_metadata =
                       jsonb_set(result_metadata, %s, '"force-replay"')
                 WHERE migration_id = '0000_schema_snapshot'
                """,
                ([_EXECUTABLE_CHECKSUM_KEY],),
            )

        migrate(dsn)
        after = _nomos_fk_oids(dsn)
        assert after == before, (
            "composite nomos FKs were dropped and re-added on a converged "
            "replay (oids changed) — the orphan anti-join re-ran"
        )
    finally:
        _drop_disposable_database(okg_dsn, dbname)


# --------------------------------------------------------------------------
# Parallel deployment-source import race (#669 regression item 6), ruling 5.
# --------------------------------------------------------------------------


def test_preimport_serializes_and_caches_source_modules(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Pre-import populates sys.modules so threaded imports don't first-import.

    Repeated parallel imports of a shared-parent source module do not race: the
    main-thread ``preimport_source_modules`` holds the process-wide lock and
    seeds the module cache, so worker-thread ``import_module`` calls are cache
    hits. Without the fix, N threads first-import the same fresh module through
    a mutating sys.path/sys.modules window.
    """
    from okg.substrate.deployments import (
        deployment_module_import_lock,
        preimport_source_modules,
    )

    # The import target MUST be a throwaway package written for this test,
    # never a real (stdlib or first-party) one. The property under test needs a
    # genuine first-import, which means the target must be absent from
    # `sys.modules` -- and evicting a module other code already imported leaves
    # the process holding two copies of it. Aliases bound by the first copy
    # (e.g. `json.JSONDecodeError`, bound in `json/__init__` from the first
    # `json.decoder`) then stop matching the classes the second copy raises, so
    # `except` clauses elsewhere silently stop catching for the rest of the run.
    #
    # It is a package with a child, not a lone module: the contended window in
    # the original race is the shared PARENT's `__init__` execution, so a
    # top-level target would not reproduce the shape under test.
    package = f"okg_preimport_probe_{uuid4().hex}"
    target = f"{package}.child"
    # Each module body appends a line on execution, so "how many times was this
    # imported for real" is directly observable rather than inferred.
    exec_log = tmp_path / "exec_log"
    body = (
        "from pathlib import Path\n"
        f"Path({str(exec_log)!r}).open('a').write('x')\n"
    )
    (tmp_path / package).mkdir()
    (tmp_path / package / "__init__.py").write_text(body, encoding="utf-8")
    (tmp_path / package / "child.py").write_text(body, encoding="utf-8")
    monkeypatch.syspath_prepend(str(tmp_path))

    # The lock is reentrant and process-wide.
    lock = deployment_module_import_lock()
    assert isinstance(lock, type(threading.RLock()))

    try:
        preimport_source_modules([target, "", target])
        assert target in sys.modules, "pre-import did not seed the module cache"
        seeded = sys.modules[target]
        # Parent + child, executed once each, despite `target` being requested
        # twice: the main-thread pre-import is what serialises them.
        assert exec_log.read_text() == "xx"

        # Now a burst of worker threads importing concurrently are all cache
        # hits: none first-imports, so none can race. Assert no import raised,
        # no module body ran again, and every worker got the SAME object --
        # without the seeding, N threads would first-import through a mutating
        # sys.path/sys.modules window.
        import importlib

        errors: list[Exception] = []
        observed: list[object] = []

        def _worker() -> None:
            try:
                observed.append(importlib.import_module(target))
            except Exception as exc:
                errors.append(exc)

        threads = [threading.Thread(target=_worker) for _ in range(16)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        assert errors == []
        assert len(observed) == 16
        assert all(module is seeded for module in observed)
        assert exec_log.read_text() == "xx", "a worker re-executed a module body"
    finally:
        for name in list(sys.modules):
            if name == package or name.startswith(f"{package}."):
                sys.modules.pop(name, None)


# --------------------------------------------------------------------------
# Disposable-DB helpers.
# --------------------------------------------------------------------------


def _create_disposable_database(base_dsn: str, dbname: str) -> str:
    info = conninfo_to_dict(base_dsn)
    info["dbname"] = "postgres"
    maintenance_dsn = make_conninfo(**info)
    with psycopg.connect(
        maintenance_dsn, autocommit=True, connect_timeout=5,
    ) as conn, conn.cursor() as cur:
        cur.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(dbname)))
    info["dbname"] = dbname
    dsn = make_conninfo(**info)
    with psycopg.connect(dsn, autocommit=True, connect_timeout=5) as conn, \
            conn.cursor() as cur:
        for extension in (
            "btree_gist",
            "timescaledb",
            "vector",
            "pg_textsearch",
            "pg_trgm",
            "fuzzystrmatch",
        ):
            cur.execute(
                sql.SQL("CREATE EXTENSION IF NOT EXISTS {}").format(
                    sql.Identifier(extension)
                )
            )
    return dsn


def _drop_disposable_database(base_dsn: str, dbname: str) -> None:
    info = conninfo_to_dict(base_dsn)
    info["dbname"] = "postgres"
    maintenance_dsn = make_conninfo(**info)
    with psycopg.connect(
        maintenance_dsn, autocommit=True, connect_timeout=5,
    ) as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT pg_terminate_backend(pid)
              FROM pg_stat_activity
             WHERE datname = %s
               AND pid <> pg_backend_pid()
            """,
            (dbname,),
        )
        cur.execute(
            sql.SQL("DROP DATABASE IF EXISTS {}").format(sql.Identifier(dbname))
        )
