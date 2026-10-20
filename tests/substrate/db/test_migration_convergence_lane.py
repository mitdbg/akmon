"""The migration-convergence lane: restored pinned dump + migrate == fresh.

Covers req.schema-migration-hardening.convergence-gate:

  1. The pinned dump is a real pin — commit-stamped metadata, and it
     carries the runtime-ensured relations (deployment-apply pair,
     attribute tables, Chronos graph tables) a real deployment of the
     pinned commit carried, because it was captured from
     fresh-install-plus-runtime-ensures, not from a fixture-built shape.
  2. Restore the pinned dump, run migrate(), run the same runtime
     ensures: pg-schema-diff AND a normalized schema-dump comparison
     against a fresh install are both empty. An UNAVAILABLE diff tool
     is a FAILURE in CI (absent tooling is not an empty diff).
  3. Non-vacuity: a deliberately divergent migration injected into the
     restored side's migrate() turns both engines red.

The logic under test is scripts/check_migration_convergence.py — the
same module operators and the dump-refresh procedure use — loaded here
via the scripts-as-modules pattern (they are not a package).
"""
from __future__ import annotations

import hashlib
import importlib.util
import json
import os
import re
import shutil
import subprocess
import sys
from pathlib import Path
from uuid import uuid4

import pytest

from okg.substrate.db import management
from okg.substrate.db.management import KnownMigration
from okg.substrate.db.connection import connect as db_connect

REPO_ROOT = Path(__file__).resolve().parents[3]
SCRIPT_PATH = REPO_ROOT / "scripts" / "check_migration_convergence.py"
DIVERGENCE_FIXTURE = (
    Path(__file__).resolve().parent
    / "fixtures"
    / "convergence"
    / "0098_convergence_divergence_probe.sql"
)
DIVERGENCE_ID = "0098_convergence_divergence_probe"


def _load_convergence_module():
    spec = importlib.util.spec_from_file_location(
        "check_migration_convergence", SCRIPT_PATH
    )
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules["check_migration_convergence"] = module
    spec.loader.exec_module(module)
    return module


conv = _load_convergence_module()


def _require_tooling() -> None:
    """psql + pg_dump + pg-schema-diff. Missing in CI = FAIL, loudly:
    a lane that silently skips its own diff proves nothing. On a
    developer machine a skip is allowed (and says what to install)."""
    missing = [
        name
        for name, present in (
            ("psql", shutil.which("psql") is not None),
            ("pg_dump", shutil.which("pg_dump") is not None),
            (
                "pg-schema-diff",
                conv._pg_schema_diff_binary() is not None,
            ),
        )
        if not present
    ]
    if not missing:
        return
    message = (
        f"convergence tooling missing: {missing} "
        "(install postgresql client tools and pg-schema-diff, or set "
        "OKG_PG_SCHEMA_DIFF)"
    )
    if os.environ.get("OKG_CI_LANE"):
        pytest.fail(message + " — REQUIRED in CI, never skipped")
    pytest.skip(message)


# --------------------------------------------------------------------------
# 1. The pin itself (hermetic).
# --------------------------------------------------------------------------


def test_pinned_dump_is_commit_stamped_and_carries_runtime_ensures() -> None:
    assert conv.PINNED_DUMP_PATH.is_file(), (
        "pinned dump missing; run scripts/refresh_convergence_dump.py"
    )
    meta = json.loads(conv.PINNED_DUMP_META_PATH.read_text(encoding="utf-8"))
    assert re.fullmatch(r"[0-9a-f]{40}", meta["captured_commit"]), (
        "the pin must name the commit it was captured from"
    )
    assert meta["generator"] == "scripts/refresh_convergence_dump.py"
    assert "repin_policy" in meta

    dump_text = conv.PINNED_DUMP_PATH.read_text(encoding="utf-8")
    # Captured from fresh-install-plus-runtime-ensures: the relations
    # that are NOT in schema.sql yet must be present, or the lane would
    # prove convergence for a shape no real deployment has.
    for relation in (
        "CREATE TABLE okg.deployment_apply_runs",
        "CREATE TABLE okg.deployment_worker_ack",
        "CREATE TABLE okg.attribute_effective_view",
        "CREATE TABLE okg.attribute_effective_view_intervals",
        "CREATE TABLE okg.graph_nodes",
        "CREATE TABLE okg.graph_edges",
        "CREATE TABLE okg.schema_migration_phases",
    ):
        assert relation in dump_text, f"pinned dump lacks {relation!r}"
    # Privileges are part of the converged shape. Migration reapplies the
    # shared registry, while the pinned dump must still carry grants outside
    # that registry so the lane can expose unintended authorization drift.
    assert "GRANT SELECT ON " in dump_text
    # pg_dump >= 18 sandbox wrappers are stripped: psql 17 cannot parse
    # them and their token is fresh randomness on every re-pin.
    assert "\\restrict" not in dump_text


def test_failing_diff_tool_is_not_an_empty_diff(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A present-but-crashing pg-schema-diff must read as FAILED, never
    as zero drift statements (adversarial-review finding: nonzero exit
    with empty stdout previously classified as an empty diff)."""
    false_binary = shutil.which("false")
    assert false_binary, "expected a `false` binary on PATH"
    monkeypatch.setattr(
        conv, "_pg_schema_diff_binary", lambda: false_binary
    )
    result = conv.pg_schema_diff("dbname=a", "dbname=b")
    assert result["status"] == "failed"
    assert result["returncode"] != 0


def _stub_pg_dump(
    monkeypatch: pytest.MonkeyPatch, stdout_by_dsn: dict[str, str]
) -> None:
    """Drive the real _normalized_schema_dump over canned pg_dump output.

    The convergence lane's role sensitivity (issue #1999) lives in the
    normalization, not in the database, so it is provable without one.
    """
    monkeypatch.setattr(
        conv.shutil, "which", lambda name: f"/usr/bin/{name}"
    )

    def fake_run(argv, **kwargs):
        dsn = argv[argv.index("--dbname") + 1]
        return subprocess.CompletedProcess(
            argv, 0, stdout_by_dsn[dsn], ""
        )

    monkeypatch.setattr(conv.subprocess, "run", fake_run)


_DUMP_PREAMBLE = (
    "--\n-- PostgreSQL database dump\n--\n"
    "SET statement_timeout = 0;\n"
    "CREATE SCHEMA okg;\n"
    "GRANT SELECT ON TABLE okg.node TO okg_mcp;\n"
)


def _default_privileges_entry(
    role: str = "okg",
    schema: str | None = "okg",
    body: str = "GRANT ALL ON FUNCTIONS TO okg_mcp",
) -> str:
    """One `ALTER DEFAULT PRIVILEGES` statement as pg_dump emits it.

    `schema=None` is the database-global form, which carries no
    `IN SCHEMA` clause. `role` is interpolated verbatim so a test can
    pass a quoted identifier.
    """
    scope = "" if schema is None else f"IN SCHEMA {schema} "
    return f"ALTER DEFAULT PRIVILEGES FOR ROLE {role} {scope}{body};"


# Pairs that MUST normalize equal. Built, not written out: adjacent
# string literals inside a list or dict literal are a CodeQL
# implicit-concatenation finding.
_EQUIVALENT_PAIRS = {
    # Issue #1999's own shape: the restored side carries the role that
    # captured the pin AND the role that migrated it; a fresh install
    # carries only the migrating role.
    "cross-role-fold": (
        [
            _default_privileges_entry(role="okg"),
            _default_privileges_entry(role="postgres"),
        ],
        [_default_privileges_entry(role="okg")],
        1,
    ),
    # Dedup is by design and it is unconditional: ANY set of grantors on
    # one grant collapses to a single entry, including application roles.
    # `okg_substrate` here is deliberate -- that grantor is the known
    # default-ACL drift class described in
    # src/okg/substrate/runtime/sql/role_split.sql (the drift-recovery
    # block around lines 505-523), and this engine can no longer see it.
    # The PACT `why` records that trade; nothing else in the lane covers
    # it.
    "dedup-collapses-every-grantor-by-design": (
        [
            _default_privileges_entry(role="okg"),
            _default_privileges_entry(role="postgres"),
            _default_privileges_entry(role="okg_substrate"),
        ],
        [_default_privileges_entry(role="okg")],
        1,
    ),
    # pg_dump quotes an identifier that needs it; the fold must consume
    # the whole quoted name rather than stopping at the space.
    "quoted-role-with-a-space": (
        [_default_privileges_entry(role='"okg admin"')],
        [_default_privileges_entry(role="okg")],
        1,
    ),
    # The database-global form carries no IN SCHEMA clause.
    "global-form-without-in-schema": (
        [_default_privileges_entry(role="postgres", schema=None)],
        [_default_privileges_entry(role="okg", schema=None)],
        1,
    ),
}


def _default_privilege_lines(normalized: list[str]) -> list[str]:
    return [
        line
        for line in normalized
        if line.startswith("ALTER DEFAULT PRIVILEGES ")
    ]


@pytest.mark.parametrize(
    ("restored_entries", "fresh_entries", "expected_entries"),
    list(_EQUIVALENT_PAIRS.values()),
    ids=list(_EQUIVALENT_PAIRS),
)
def test_default_privileges_grantor_is_normalized(
    monkeypatch: pytest.MonkeyPatch,
    restored_entries: list[str],
    fresh_entries: list[str],
    expected_entries: int,
) -> None:
    """Two dumps differing ONLY in which role issued the default
    privileges normalize equal (issue #1999).

    `ALTER DEFAULT PRIVILEGES` without an explicit `FOR ROLE` is
    recorded against the issuing role and pg_dump names it, so the
    pinned dump carries its capturing role forever. Without folding, the
    lane reads that login-role difference as migration divergence.
    """
    restored = _DUMP_PREAMBLE + "".join(
        entry + "\n" for entry in restored_entries
    )
    fresh = _DUMP_PREAMBLE + "".join(
        entry + "\n" for entry in fresh_entries
    )
    _stub_pg_dump(
        monkeypatch, {"dbname=restored": restored, "dbname=fresh": fresh}
    )
    result = conv.schema_dump_diff("dbname=restored", "dbname=fresh")
    assert result["line_count"] == 0, result["diff"]

    normalized = _default_privilege_lines(
        conv._normalized_schema_dump("dbname=restored")
    )
    assert len(normalized) == expected_entries, normalized
    for line in normalized:
        # The grantor is gone; the grant itself is still compared.
        assert conv._GRANTOR_PLACEHOLDER in line, line
        assert line.endswith("GRANT ALL ON FUNCTIONS TO okg_mcp;"), line
    for role in ("okg", "postgres", "okg_substrate", '"okg admin"'):
        assert not any(
            f"FOR ROLE {role} " in line for line in normalized
        ), normalized


# Built rather than written out: adjacent string literals inside a list
# or dict literal are a CodeQL implicit-concatenation finding.
_DIVERGENT_ENTRIES = {
    "grantee": _default_privileges_entry(
        body="GRANT ALL ON FUNCTIONS TO okg_reader"
    ),
    "privilege-set": _default_privileges_entry(
        body="GRANT EXECUTE ON FUNCTIONS TO okg_mcp"
    ),
    "object-class": _default_privileges_entry(
        body="GRANT ALL ON TABLES TO okg_mcp"
    ),
    "schema": _default_privileges_entry(schema="public"),
    "revoke-not-grant": _default_privileges_entry(
        body="REVOKE ALL ON FUNCTIONS FROM okg_mcp"
    ),
}


@pytest.mark.parametrize(
    "divergent_entry",
    list(_DIVERGENT_ENTRIES.values()),
    ids=list(_DIVERGENT_ENTRIES),
)
def test_default_privileges_differences_still_diff(
    monkeypatch: pytest.MonkeyPatch, divergent_entry: str
) -> None:
    """Folding the grantor must not fold the grant. Anything else about
    a default-privileges entry still has to turn the textual engine red,
    or authorization drift stops being visible."""
    baseline_entry = _default_privileges_entry(role="postgres")
    assert divergent_entry != baseline_entry
    _stub_pg_dump(
        monkeypatch,
        {
            "dbname=restored": _DUMP_PREAMBLE + baseline_entry + "\n",
            "dbname=fresh": _DUMP_PREAMBLE + divergent_entry + "\n",
        },
    )
    result = conv.schema_dump_diff("dbname=restored", "dbname=fresh")
    assert result["line_count"] > 0, (
        "a divergent default-privileges entry normalized away: "
        f"{divergent_entry}"
    )


# --------------------------------------------------------------------------
# 2. The gate: restored + migrated == fresh, by both engines.
# --------------------------------------------------------------------------


@pytest.mark.postgres
def test_restored_pinned_dump_migrates_to_fresh_shape(okg_dsn: str) -> None:
    _require_tooling()
    result = conv.check_convergence(
        okg_dsn, run_suffix=uuid4().hex[:12]
    )
    structural = result["pg_schema_diff"]
    assert structural["status"] == "diffed", (
        "pg-schema-diff did not run; an unavailable tool is a failure, "
        f"not an empty diff: {structural}"
    )
    assert structural["statement_count"] == 0, (
        "restored+migrated diverges from a fresh install:\n"
        + structural["plan"][:4000]
    )
    textual = result["schema_dump_diff"]
    assert textual["line_count"] == 0, (
        "normalized schema dumps diverge:\n" + textual["diff"][:4000]
    )
    assert result["converged"] is True


# --------------------------------------------------------------------------
# 3. Non-vacuity: a divergent migration turns the lane red.
# --------------------------------------------------------------------------


def _patch_in_divergence(monkeypatch: pytest.MonkeyPatch) -> None:
    """Inject the divergence probe through the ordered-migration seams,
    so the divergence enters via migrate() itself — the path the lane
    guards — not via out-of-band DDL."""
    probe_sql = DIVERGENCE_FIXTURE.read_text(encoding="utf-8")
    checksum = hashlib.sha256(probe_sql.encode("utf-8")).hexdigest()
    probe = KnownMigration(
        migration_id=DIVERGENCE_ID,
        description=f"ordered migration {DIVERGENCE_ID}",
        checksum=checksum,
        destructive=False,
    )
    real_known = management.current_known_migrations
    real_manifest = management.load_migrations_manifest()
    real_loader = management._ordered_migration_forward_sql

    def known_with_probe(**kwargs):
        return [*real_known(**kwargs), probe]

    patched_manifest = {
        key: dict(value) for key, value in real_manifest.items()
    }
    patched_manifest[DIVERGENCE_ID] = {
        "migration_id": DIVERGENCE_ID,
        "checksum_sha256": checksum,
        "destructive": False,
    }

    def loader_with_probe(migration_id: str) -> str:
        if migration_id == DIVERGENCE_ID:
            return probe_sql
        return real_loader(migration_id)

    monkeypatch.setattr(
        management, "current_known_migrations", known_with_probe
    )
    monkeypatch.setattr(
        management, "load_migrations_manifest", lambda: patched_manifest
    )
    monkeypatch.setattr(
        management, "_ordered_migration_forward_sql", loader_with_probe
    )


@pytest.mark.postgres
def test_divergent_migration_turns_the_lane_red(
    okg_dsn: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _require_tooling()
    from okg.substrate.db.migrate import migrate

    suffix = uuid4().hex[:12]
    fresh_db = f"okg_test_conv_mfresh_{suffix}"
    restored_db = f"okg_test_conv_mdiv_{suffix}"
    try:
        # Fresh side FIRST, before the divergence patch is installed.
        fresh_dsn = conv.build_fresh_install(okg_dsn, fresh_db)

        restored_dsn = conv.restore_pinned_dump(okg_dsn, restored_db)
        _patch_in_divergence(monkeypatch)
        migrate(restored_dsn)
        conv.runtime_ensures(restored_dsn)

        # The probe actually landed through the ordered runner.
        with db_connect(restored_dsn) as conn, conn.cursor() as cur:
            cur.execute(
                "SELECT to_regclass('okg.convergence_divergence_probe')"
            )
            assert cur.fetchone()[0] is not None
            cur.execute(
                "SELECT count(*) FROM okg.schema_migrations "
                "WHERE migration_id = %s",
                (DIVERGENCE_ID,),
            )
            assert cur.fetchone()[0] == 1

        structural = conv.pg_schema_diff(restored_dsn, fresh_dsn)
        assert structural["status"] == "diffed"
        assert structural["statement_count"] > 0, (
            "pg-schema-diff failed to detect the planted divergence — "
            "the lane would be vacuous"
        )
        textual = conv.schema_dump_diff(restored_dsn, fresh_dsn)
        assert textual["line_count"] > 0, (
            "schema-dump diff failed to detect the planted divergence"
        )
        assert "convergence_divergence_probe" in textual["diff"]
    finally:
        conv.drop_database(okg_dsn, fresh_db)
        conv.drop_database(okg_dsn, restored_db)
