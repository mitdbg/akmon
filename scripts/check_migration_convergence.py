#!/usr/bin/env python3
"""Migration-convergence check: restored pinned dump + migrate == fresh install.

The convergence guarantee external deployments rely on
(`schema-migration-hardening` / req.convergence-gate): restoring the
pinned earlier dump and running ``migrate()`` MUST produce the same
schema as a fresh install at the current commit. This module is the one
source of truth for the CI lane
(tests/substrate/db/test_migration_convergence_lane.py), the dump
refresh procedure (scripts/refresh_convergence_dump.py), and ad-hoc
operator runs (``python scripts/check_migration_convergence.py``).

Both comparison sides are constructed IDENTICALLY, deliberately:

  fresh side:    CREATE DATABASE -> migrate() -> runtime ensures
  restored side: CREATE DATABASE -> restore pinned dump -> migrate()
                 -> runtime ensures

The runtime ensures (deployment-apply pair, attribute tables; the
Chronos graph tables are ensured by migrate() itself) run on BOTH sides
because those relations are not yet in schema.sql — that is the
single-source-ddl child's scope. Running them identically on both sides
means this lane proves MIGRATION convergence (restored+migrated ==
fresh) without conflating it with standalone-installability of the
snapshot.

The diff engine is pg-schema-diff (db-to-db, plan validation disabled —
the posture proven by the PR #407 trial and reused from
scripts/schema_diff_advisory.py). A missing binary is
``status: unavailable`` — the CALLER must treat that as failure in CI
(never a silent pass), and the lane test does. A normalized
``pg_dump --schema-only`` textual comparison runs as a second engine:
pg-schema-diff does not model every object class (views, functions),
and the dump diff catches drift there.
"""
from __future__ import annotations

import argparse
import difflib
import json
import os
import re
import shutil
import subprocess
import sys
from pathlib import Path
from typing import Any

import psycopg
from psycopg import sql
from psycopg.conninfo import conninfo_to_dict, make_conninfo

REPO_ROOT = Path(__file__).resolve().parents[1]
PINNED_DUMP_PATH = (
    REPO_ROOT
    / "tests"
    / "substrate"
    / "db"
    / "fixtures"
    / "convergence"
    / "pinned_convergence_dump.sql"
)
PINNED_DUMP_META_PATH = PINNED_DUMP_PATH.with_suffix(".meta.json")

REQUIRED_EXTENSIONS = (
    "btree_gist",
    "timescaledb",
    "vector",
    "pg_textsearch",
    "pg_trgm",
    "fuzzystrmatch",
)

# Roles the schema's GRANT statements target. Roles are cluster-level
# and never part of a pg_dump, so BOTH sides pre-create them NOLOGIN
# before migrate/restore. This is deliberate shape-pinning, not just
# restore plumbing: several migrations (0014/0015) grant to a role only
# IF it exists, so a cluster where okg_substrate happens to pre-exist
# produces a different fresh-install shape than one where it does not.
# The lane compares shapes under ONE explicit role environment — the
# full set — everywhere (pin capture, fresh side, restored side).
GRANT_TARGET_ROLES = (
    "app_ro",
    "app_rw",
    "catalog_admin",
    "okg_mcp",
    "okg_substrate",
    "projection_remediator",
)


def ensure_grant_target_roles(dsn: str) -> None:
    with psycopg.connect(
        dsn, autocommit=True, connect_timeout=10,
    ) as conn, conn.cursor() as cur:
        for role in GRANT_TARGET_ROLES:
            cur.execute(
                sql.SQL(
                    """
                    DO $okg_conv_role$
                    BEGIN
                      IF NOT EXISTS (
                        SELECT 1 FROM pg_roles WHERE rolname = {rolname}
                      ) THEN
                        CREATE ROLE {role} NOLOGIN;
                      END IF;
                    END
                    $okg_conv_role$
                    """
                ).format(
                    rolname=sql.Literal(role), role=sql.Identifier(role)
                )
            )


def maintenance_dsn(base_dsn: str) -> str:
    info = conninfo_to_dict(base_dsn)
    info["dbname"] = "postgres"
    return make_conninfo(**info)


def dsn_for(base_dsn: str, dbname: str) -> str:
    info = conninfo_to_dict(base_dsn)
    info["dbname"] = dbname
    return make_conninfo(**info)


def create_database(base_dsn: str, dbname: str, *, extensions: bool) -> str:
    """Both lane sides start from template0, and that is load bearing.

    A plain ``CREATE DATABASE`` clones ``template1``, and ``ops/pg/initdb``
    now creates the substrate extensions there (issue #1972). On the restored
    side that hands the database a CLUSTER-DEFAULT TimescaleDB before the
    dump is replayed, so ``CREATE EXTENSION timescaledb VERSION <pin>``
    no-ops at the wrong version and the dump's own
    ``_timescaledb_catalog.hypertable`` COPY collides with the installed
    catalog:

        ERROR:  new row for relation "hypertable" violates check constraint
                "hypertable_num_dimensions_check"

    ``template0`` is never touched by initdb. The fresh side uses it too, so
    the comparison depends on what this function installs rather than on what
    the operator's ``template1`` happens to hold.
    """

    with psycopg.connect(
        maintenance_dsn(base_dsn), autocommit=True, connect_timeout=10,
    ) as conn, conn.cursor() as cur:
        cur.execute(
            sql.SQL("CREATE DATABASE {} TEMPLATE template0").format(
                sql.Identifier(dbname)
            )
        )
    dsn = dsn_for(base_dsn, dbname)
    if extensions:
        with psycopg.connect(
            dsn, autocommit=True, connect_timeout=10,
        ) as conn, conn.cursor() as cur:
            for extension in REQUIRED_EXTENSIONS:
                cur.execute(
                    sql.SQL("CREATE EXTENSION IF NOT EXISTS {}").format(
                        sql.Identifier(extension)
                    )
                )
    return dsn


def drop_database(base_dsn: str, dbname: str) -> None:
    with psycopg.connect(
        maintenance_dsn(base_dsn), autocommit=True, connect_timeout=10,
    ) as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT pg_terminate_backend(pid)
              FROM pg_stat_activity
             WHERE datname = %s AND pid <> pg_backend_pid()
            """,
            (dbname,),
        )
        cur.execute(
            sql.SQL("DROP DATABASE IF EXISTS {}").format(
                sql.Identifier(dbname)
            )
        )


def runtime_ensures(dsn: str) -> None:
    """The runtime-created relations both sides receive identically."""
    from okg.substrate.attributes import ensure_attribute_schema
    from okg.substrate.deployment_apply import ensure_deployment_apply_schema

    with psycopg.connect(dsn) as conn:
        ensure_deployment_apply_schema(conn)
        ensure_attribute_schema(conn)
        conn.commit()


def build_fresh_install(base_dsn: str, dbname: str) -> str:
    """CREATE DATABASE -> roles -> migrate() -> runtime ensures."""
    from okg.substrate.db.migrate import migrate

    dsn = create_database(base_dsn, dbname, extensions=True)
    ensure_grant_target_roles(dsn)
    migrate(dsn)
    runtime_ensures(dsn)
    return dsn


def restore_pinned_dump(
    base_dsn: str,
    dbname: str,
    *,
    dump_path: Path = PINNED_DUMP_PATH,
) -> str:
    """CREATE DATABASE -> psql-restore the pinned plain-format dump.

    TimescaleDB restores need the ``timescaledb.restoring`` bracket
    (``timescaledb_pre_restore`` / ``timescaledb_post_restore``): the
    pre/post calls set a database-level GUC, so the psql restore runs in
    its own session between them — the same bracket the deployment
    backup/restore path uses. The extension is created at the PIN'S
    version for the restore (the catalog COPY data is version-exact),
    then upgraded to the cluster default via ``ALTER EXTENSION
    timescaledb UPDATE`` — the same path a real external deployment
    takes restoring an old dump onto an upgraded cluster, so a CI image
    bump never forces a re-pin.
    """
    if not dump_path.is_file():
        raise FileNotFoundError(
            f"pinned convergence dump not found: {dump_path}; run "
            "scripts/refresh_convergence_dump.py to (re)pin it"
        )
    psql = shutil.which("psql")
    if psql is None:
        raise RuntimeError("psql not found on PATH; required for restore")
    pinned_ts = _pinned_timescale_version(base_dsn, dump_path)
    dsn = create_database(base_dsn, dbname, extensions=False)
    ensure_grant_target_roles(dsn)
    with psycopg.connect(
        dsn, autocommit=True, connect_timeout=10,
    ) as conn, conn.cursor() as cur:
        cur.execute(
            sql.SQL(
                "CREATE EXTENSION IF NOT EXISTS timescaledb VERSION {}"
            ).format(sql.Literal(pinned_ts))
        )
        cur.execute("SELECT timescaledb_pre_restore()")
    try:
        completed = subprocess.run(
            [
                psql,
                "--no-psqlrc",
                "--set", "ON_ERROR_STOP=1",
                "--quiet",
                "--dbname", dsn,
                "--file", str(dump_path),
            ],
            capture_output=True,
            text=True,
            check=False,
        )
        if completed.returncode != 0:
            raise RuntimeError(
                "psql restore of the pinned dump failed "
                f"(rc={completed.returncode}): {completed.stderr[-4000:]}"
            )
    finally:
        with psycopg.connect(
            dsn, autocommit=True, connect_timeout=10,
        ) as conn, conn.cursor() as cur:
            cur.execute("SELECT timescaledb_post_restore()")
    with psycopg.connect(
        dsn, autocommit=True, connect_timeout=10,
    ) as conn, conn.cursor() as cur:
        # The real deployment upgrade path: old-version catalog restored
        # exactly, then upgraded in place to the cluster default so the
        # convergence diff compares same-version shapes. A no-op when the
        # pin and the cluster already agree. MUST be the first command in
        # a fresh session (timescaledb refuses ALTER EXTENSION UPDATE
        # mid-session after other queries).
        cur.execute("ALTER EXTENSION timescaledb UPDATE")
    return dsn


def _pinned_timescale_version(base_dsn: str, dump_path: Path) -> str:
    """The pin's TimescaleDB version, verified installable on the cluster.

    The pinned dump carries ``_timescaledb_catalog`` COPY data for the
    version it was captured under, so the restore MUST run under exactly
    that extension version — after which the restored side takes the
    real deployment upgrade path (``ALTER EXTENSION timescaledb
    UPDATE``) to the cluster default, exactly like an external user
    restoring an old dump onto an upgraded cluster. Typed refusals when
    the pin metadata is missing, the Postgres major differs, or the
    pinned extension version is not shipped by the cluster image (the
    timescaledb-ha images ship all supported versions; a cluster that
    dropped the pinned one needs an image fix or a documented re-pin).
    """
    meta_path = dump_path.with_suffix(".meta.json")
    if not meta_path.is_file():
        raise RuntimeError(
            f"pin metadata missing: {meta_path}; re-pin via "
            "scripts/refresh_convergence_dump.py (the pin and its "
            "metadata commit together)"
        )
    pinned = json.loads(meta_path.read_text(encoding="utf-8")).get(
        "server_versions", {}
    )
    pinned_ts = str(pinned.get("timescaledb") or "")
    with psycopg.connect(
        maintenance_dsn(base_dsn), connect_timeout=10
    ) as conn, conn.cursor() as cur:
        cur.execute("SHOW server_version")
        server = str(cur.fetchone()[0])
        cur.execute(
            "SELECT version FROM pg_available_extension_versions "
            "WHERE name = 'timescaledb'"
        )
        available = {str(row[0]) for row in cur.fetchall()}
    pinned_major = str(pinned.get("postgres", "")).split(".", 1)[0]
    if pinned_major != server.split(".", 1)[0]:
        raise RuntimeError(
            f"cluster Postgres {server} does not match the pin's "
            f"{pinned.get('postgres')} (major must match); align the "
            "cluster or re-pin via scripts/refresh_convergence_dump.py"
        )
    if pinned_ts not in available:
        raise RuntimeError(
            f"the pin was captured under timescaledb {pinned_ts}, which "
            "this cluster image does not ship "
            f"(available: {sorted(available)[-5:]}); the restore must "
            "run under the pinned version before upgrading — fix the "
            "image or re-pin via scripts/refresh_convergence_dump.py"
        )
    return pinned_ts


def _pg_schema_diff_binary() -> str | None:
    return os.environ.get("OKG_PG_SCHEMA_DIFF") or shutil.which(
        "pg-schema-diff"
    )


def pg_schema_diff(from_dsn: str, to_dsn: str) -> dict[str, Any]:
    """``pg-schema-diff plan`` between two live databases.

    ``status: diffed`` with ``statement_count`` (0 == converged), or
    ``status: unavailable`` when the binary is absent. Callers gating on
    convergence MUST fail on ``unavailable`` — an absent tool is not an
    empty diff.
    """
    binary = _pg_schema_diff_binary()
    if binary is None:
        return {
            "status": "unavailable",
            "detail": (
                "pg-schema-diff binary not found "
                "(set OKG_PG_SCHEMA_DIFF or install it)"
            ),
        }
    completed = subprocess.run(
        [
            binary,
            "plan",
            "--from-dsn", from_dsn,
            "--to-dsn", to_dsn,
            "--disable-plan-validation",
        ],
        capture_output=True,
        text=True,
        check=False,
    )
    plan = (completed.stdout or "").strip()
    statements = [
        line
        for line in plan.splitlines()
        if line.strip() and not line.startswith("#")
    ]
    if completed.returncode != 0:
        # A present-but-failing tool is NOT an empty diff (the same
        # rule as an absent tool): a crash with empty stdout must never
        # read as convergence.
        return {
            "status": "failed",
            "returncode": completed.returncode,
            "statement_count": len(statements),
            "plan": plan[:20000],
            "stderr": (completed.stderr or "").strip()[:4000],
        }
    return {
        "status": "diffed",
        "returncode": completed.returncode,
        "statement_count": len(statements),
        "plan": plan[:20000],
        "stderr": (completed.stderr or "").strip()[:4000],
    }


_DUMP_NOISE_RE = re.compile(
    r"^(--.*|SET .*|SELECT pg_catalog\.set_config.*|\\restrict.*"
    r"|\\unrestrict.*)$"
)

# `ALTER DEFAULT PRIVILEGES` issued without an explicit `FOR ROLE` is
# recorded against the role that issued it, and pg_dump names that role.
# The pinned convergence dump was captured under a login role named
# `postgres`, so it carries a `FOR ROLE postgres` entry permanently; a
# fresh install under a login role named `okg` carries only `FOR ROLE
# okg`. `--no-owner` suppresses `ALTER ... OWNER TO` but does not
# normalize this grantor name, so without folding it the lane reads a
# login-role difference as migration divergence (issue #1999). Fold the
# grantor ONLY: the schema, object class, privilege set and grantee are
# left intact, so authorization drift stays visible.
_DEFAULT_PRIVILEGES_GRANTOR_RE = re.compile(
    r'^(ALTER DEFAULT PRIVILEGES )FOR ROLE '
    r'(?:"(?:[^"]|"")+"|[^\s"]+)'
    r'( .*)$'
)
_GRANTOR_PLACEHOLDER = "FOR ROLE <grantor>"


def _fold_default_privileges_grantor(line: str) -> str | None:
    """``line`` with its default-privileges grantor role replaced by a
    placeholder, or ``None`` when ``line`` is not such a statement."""
    match = _DEFAULT_PRIVILEGES_GRANTOR_RE.match(line)
    if match is None:
        return None
    return f"{match.group(1)}{_GRANTOR_PLACEHOLDER}{match.group(2)}"


def _normalized_schema_dump(dsn: str) -> list[str]:
    pg_dump = shutil.which("pg_dump")
    if pg_dump is None:
        raise RuntimeError("pg_dump not found on PATH")
    # --no-owner only: privileges are part of the converged shape. Migration
    # reapplies the shared grant registry, and the textual engine must also see
    # privileges outside that registry so authorization drift stays visible.
    # The one privilege detail normalized afterwards is the default-privileges
    # grantor role name -- see _fold_default_privileges_grantor.
    completed = subprocess.run(
        [
            pg_dump,
            "--schema-only",
            "--no-owner",
            "--dbname", dsn,
        ],
        capture_output=True,
        text=True,
        check=False,
    )
    if completed.returncode != 0:
        raise RuntimeError(
            f"pg_dump --schema-only failed: {completed.stderr[-4000:]}"
        )
    lines: list[str] = []
    acl_lines: list[str] = []
    default_privilege_lines: set[str] = set()
    for line in completed.stdout.splitlines():
        stripped = line.rstrip()
        if not stripped or _DUMP_NOISE_RE.match(stripped):
            continue
        folded = _fold_default_privileges_grantor(stripped)
        if folded is not None:
            # One restored database can carry the same default-privileges
            # fact under two grantors (the role that captured the pin and
            # the role that migrated it). Folded, those are one fact, so
            # compare the entries as a deduplicated set.
            default_privilege_lines.add(folded)
            continue
        # ACL statements are one-line and self-contained, but pg_dump
        # emits them in ACL-array order, which reflects grant ORDER —
        # identical grant SETS applied in different order (fresh views.sql
        # vs restore replay) would otherwise diff. Compare them as a
        # sorted set.
        if stripped.startswith(("GRANT ", "REVOKE ")):
            acl_lines.append(stripped)
        else:
            lines.append(stripped)
    return (
        lines + sorted(acl_lines) + sorted(default_privilege_lines)
    )


def schema_dump_diff(dsn_a: str, dsn_b: str) -> dict[str, Any]:
    """Second engine: normalized ``pg_dump --schema-only`` text diff.

    Catches object classes pg-schema-diff does not model (views,
    functions). Normalization drops comments and session SET noise
    only; both sides are produced by the same pg_dump binary so the
    ordering is deterministic.
    """
    dump_a = _normalized_schema_dump(dsn_a)
    dump_b = _normalized_schema_dump(dsn_b)
    diff = list(
        difflib.unified_diff(
            dump_a, dump_b, fromfile="restored+migrated",
            tofile="fresh-install", lineterm="", n=2,
        )
    )
    return {
        "status": "diffed",
        "line_count": len(diff),
        "diff": "\n".join(diff[:400]),
    }


def check_convergence(
    base_dsn: str,
    *,
    run_suffix: str,
    dump_path: Path = PINNED_DUMP_PATH,
) -> dict[str, Any]:
    """The whole gate: build both sides, diff with both engines."""
    fresh_db = f"okg_test_conv_fresh_{run_suffix}"
    restored_db = f"okg_test_conv_restored_{run_suffix}"
    try:
        fresh_dsn = build_fresh_install(base_dsn, fresh_db)
        restored_dsn = restore_pinned_dump(
            base_dsn, restored_db, dump_path=dump_path
        )
        from okg.substrate.db.migrate import migrate

        migrate(restored_dsn)
        runtime_ensures(restored_dsn)

        structural = pg_schema_diff(restored_dsn, fresh_dsn)
        textual = schema_dump_diff(restored_dsn, fresh_dsn)
        converged = (
            structural["status"] == "diffed"
            and structural["statement_count"] == 0
            and textual["line_count"] == 0
        )
        return {
            "converged": converged,
            "pg_schema_diff": structural,
            "schema_dump_diff": textual,
        }
    finally:
        drop_database(base_dsn, fresh_db)
        drop_database(base_dsn, restored_db)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--base-dsn",
        default=os.environ.get(
            "OKG_TEST_DSN", "postgres://postgres:okg@127.0.0.1:5433/postgres"
        ),
        help="Cluster DSN; disposable okg_test_conv_* databases are "
        "created and dropped on it.",
    )
    parser.add_argument("--dump", type=Path, default=PINNED_DUMP_PATH)
    args = parser.parse_args(argv)

    import uuid

    result = check_convergence(
        args.base_dsn,
        run_suffix=uuid.uuid4().hex[:12],
        dump_path=args.dump,
    )
    print(json.dumps(result, indent=2, sort_keys=True))
    if result["pg_schema_diff"]["status"] != "diffed":
        print("pg-schema-diff unavailable: FAILING (absent tool is not "
              "an empty diff)", file=sys.stderr)
        return 2
    return 0 if result["converged"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
