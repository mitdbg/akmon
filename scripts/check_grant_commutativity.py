#!/usr/bin/env python3
"""Prove the two grant-bearing scripts commute.

`schema-grant-coherence` req.commutativity-gate.

The substrate has two authors of GRANT statements that both touch the MCP /
app_ro read surface:

  * ``src/okg/substrate/db/views.sql``            (run by ``migrate`` /
                                                   ``install_views``)
  * ``src/okg/substrate/runtime/sql/role_split.sql`` (run by the deployment
                                                   bootstrap)

Nothing sequences them. A deployment can bootstrap roles before its first
migrate, or migrate before adding the role split, and until this PACT the two
orders produced DIFFERENT security boundaries -- whichever ran last decided
whether the MCP read pool could read raw fact tables, source blobs and
watermarks.

This check builds two copies of the same migrated database, applies the two
scripts in opposite orders, and compares three things:

  1. Effective privileges on EXISTING relations and functions --
     ``(grantee, schema, object, privilege)`` walked out of the catalog ACLs.
  2. The full contents of ``pg_default_acl``. Two orders can agree on every
     current ACL and still disagree here, which silently changes what the NEXT
     created table grants. A default ACL appears in no relation's grant list
     and survives every REVOKE aimed at existing tables, so a diff that
     compares only (1) reports green on the more dangerous of the two bugs.
  3. A BEHAVIOURAL probe: create a fresh non-Chronos table AS ``okg_substrate``
     -- the role that creates relations at runtime -- and assert ``okg_mcp``
     cannot read it. This is the property (2) exists to protect, checked
     directly rather than inferred from catalog text.

Exit codes: 0 when the orders agree and the probe holds on both databases,
1 otherwise. ``--json`` prints a machine-readable report either way.

Mutation hooks (``--mutate``) inject a deliberately order-dependent statement
at the END of the role_split step so the gate's non-vacuity can be proven:
``grant`` is caught by (1), ``default-acl`` only by (2) and (3).
"""
from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path
from typing import Any
from uuid import uuid4

import psycopg
from psycopg import sql

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT / "src") not in sys.path:
    sys.path.insert(0, str(REPO_ROOT / "src"))
if str(REPO_ROOT / "scripts") not in sys.path:
    sys.path.insert(0, str(REPO_ROOT / "scripts"))

# The database-construction primitives (create/drop, extensions, the explicit
# GRANT_TARGET_ROLES environment) are the migration-convergence lane's, reused
# rather than re-derived: two lanes disagreeing about what a "fresh install"
# is would make both of their diffs untrustworthy.
import check_migration_convergence as conv  # noqa: E402

#: The deployment name both sides use. Identical on purpose: it makes the
#: per-deployment role comparable, so `okg_deployment_*` can be audited like
#: every other role instead of excluded. Excluding it removed COVERAGE, not
#: noise -- role_split.sql's column-level and negative grants all target it.
DEPLOYMENT_NAME = "commutea"

#: The roles whose privileges the comparison covers.
AUDITED_ROLES = (
    "app_ro",
    "app_rw",
    "catalog_admin",
    "okg_mcp",
    "okg_substrate",
    "okg_dbos",
    "projection_remediator",
    f"okg_deployment_{DEPLOYMENT_NAME}",
)

#: Schemas whose relations are compared. `dbos` is included because
#: role_split.sql grants and revokes on it.
AUDITED_SCHEMAS = ("okg", "okg_derived", "public", "dbos")

MUTATIONS = {
    # An order-dependent GRANT on existing relations. views.sql revokes exactly
    # this, so whichever script runs last wins. Caught by the relation-ACL diff.
    "grant": "GRANT SELECT ON ALL TABLES IN SCHEMA okg TO okg_mcp;",
    # An order-dependent DEFAULT ACL. Invisible in every relation's grants;
    # caught only by the pg_default_acl diff and the new-table probe.
    "default-acl": (
        "ALTER DEFAULT PRIVILEGES FOR ROLE okg_substrate IN SCHEMA okg "
        "GRANT SELECT ON TABLES TO okg_mcp;"
    ),
    # An order-dependent SCHEMA privilege. Every table ACL stays byte-identical
    # -- has_table_privilege answers only the table question -- while app_ro is
    # locked out of the schema entirely. Caught only by the schema-ACL section
    # and by the probe's positive control.
    "schema-usage": "REVOKE USAGE ON SCHEMA okg FROM app_ro;",
    # Order-INDEPENDENT overexposure. A pure commutativity diff cannot see it;
    # the absolute registry-drift arm must turn the gate red on both sides.
    "public-grant": "GRANT SELECT ON okg.node_facts TO PUBLIC;",
}

RELATION_ACL_QUERY = """
SELECT r.rolname                AS grantee,
       n.nspname                AS schema_name,
       c.relname                AS object_name,
       c.relkind::text          AS object_kind,
       acl.privilege_type::text AS privilege
  FROM pg_class c
  JOIN pg_namespace n ON n.oid = c.relnamespace
  CROSS JOIN LATERAL aclexplode(c.relacl) acl
  JOIN pg_roles r ON r.oid = acl.grantee
 WHERE n.nspname = ANY(%(schemas)s)
   AND r.rolname = ANY(%(roles)s)
   -- TimescaleDB internal chunk relations are not directly queryable and
   -- inherit their hypertable's ACL; their NAMES embed a serial chunk number,
   -- so comparing them would diff on chunk creation order, not on privilege.
   AND c.relname NOT LIKE '\\_hyper\\_%%'
   AND c.relname NOT LIKE '\\_compressed\\_hypertable\\_%%'
 ORDER BY 1, 2, 3, 5
"""

# Column-level grants live in pg_attribute, not pg_class. role_split.sql issues
# GRANT UPDATE (valid_to_gen) on the legacy live tables, so a diff that skipped
# this would miss an order-dependent column privilege.
COLUMN_ACL_QUERY = """
SELECT r.rolname                AS grantee,
       n.nspname                AS schema_name,
       c.relname || '.' || a.attname AS object_name,
       'column'                 AS object_kind,
       acl.privilege_type::text AS privilege
  FROM pg_attribute a
  JOIN pg_class c ON c.oid = a.attrelid
  JOIN pg_namespace n ON n.oid = c.relnamespace
  CROSS JOIN LATERAL aclexplode(a.attacl) acl
  JOIN pg_roles r ON r.oid = acl.grantee
 WHERE n.nspname = ANY(%(schemas)s)
   AND r.rolname = ANY(%(roles)s)
 ORDER BY 1, 2, 3, 5
"""

FUNCTION_ACL_QUERY = """
SELECT r.rolname                AS grantee,
       n.nspname                AS schema_name,
       p.proname || '(' || pg_get_function_identity_arguments(p.oid) || ')'
                               AS object_name,
       'function'               AS object_kind,
       acl.privilege_type::text AS privilege
  FROM pg_proc p
  JOIN pg_namespace n ON n.oid = p.pronamespace
  CROSS JOIN LATERAL aclexplode(p.proacl) acl
  JOIN pg_roles r ON r.oid = acl.grantee
 WHERE n.nspname = ANY(%(schemas)s)
   AND r.rolname = ANY(%(roles)s)
 ORDER BY 1, 2, 3, 5
"""

# Schema-level ACLs. Without this section the audit has a hole big enough to
# lock a role out entirely and still report OK: `has_table_privilege` answers
# only the TABLE question, so revoking `USAGE ON SCHEMA okg` from app_ro leaves
# every table ACL identical while app_ro can no longer reach a single relation.
# The behavioural probe below closes the same hole from the other side.
SCHEMA_ACL_QUERY = """
SELECT r.rolname                AS grantee,
       n.nspname                AS schema_name,
       'schema'                 AS object_name,
       'schema'                 AS object_kind,
       acl.privilege_type::text AS privilege
  FROM pg_namespace n
  CROSS JOIN LATERAL aclexplode(n.nspacl) acl
  JOIN pg_roles r ON r.oid = acl.grantee
 WHERE n.nspname = ANY(%(schemas)s)
   AND r.rolname = ANY(%(roles)s)
 ORDER BY 1, 2, 5
"""

DEFAULT_ACL_QUERY = """
SELECT pg_get_userbyid(d.defaclrole)       AS grantor,
       coalesce(n.nspname, '')             AS schema_name,
       d.defaclobjtype::text               AS object_type,
       d.defaclacl::text                   AS acl
  FROM pg_default_acl d
  LEFT JOIN pg_namespace n ON n.oid = d.defaclnamespace
 ORDER BY 1, 2, 3, 4
"""


def _rows(cur, query: str) -> list[tuple[Any, ...]]:
    cur.execute(
        query,
        {"schemas": list(AUDITED_SCHEMAS), "roles": list(AUDITED_ROLES)},
    )
    return [tuple(row) for row in cur.fetchall()]


def privilege_audit(dsn: str) -> dict[str, list[list[Any]]]:
    """The comparable privilege picture of one database."""
    with psycopg.connect(dsn, autocommit=True, connect_timeout=10) as conn:
        with conn.cursor() as cur:
            relation = _rows(cur, RELATION_ACL_QUERY)
            column = _rows(cur, COLUMN_ACL_QUERY)
            function = _rows(cur, FUNCTION_ACL_QUERY)
            schema = _rows(cur, SCHEMA_ACL_QUERY)
            cur.execute(DEFAULT_ACL_QUERY)
            default_acl = [tuple(row) for row in cur.fetchall()]
    return {
        "relation_privileges": sorted([list(r) for r in relation]),
        "column_privileges": sorted([list(r) for r in column]),
        "function_privileges": sorted([list(r) for r in function]),
        "schema_privileges": sorted([list(r) for r in schema]),
        "default_acl": sorted([list(r) for r in default_acl]),
    }


def registry_drift(dsn: str) -> list[list[Any]]:
    """Absolute allow-list violations; commutativity alone cannot see these."""
    with psycopg.connect(dsn, autocommit=True, connect_timeout=10) as conn:
        rows = conn.execute(
            "SELECT finding_kind, relation, role_name, detail "
            "FROM okg.grant_registry_drift() ORDER BY 1, 2, 3, 4"
        ).fetchall()
    return [list(row) for row in rows]


def new_table_probe(dsn: str) -> dict[str, Any]:
    """Create a fresh non-Chronos table AS okg_substrate; okg_mcp must not read it.

    ``SET ROLE`` rather than a separate connection: ``okg_substrate`` is NOLOGIN
    by design, and what matters is the CREATING role, because PostgreSQL default
    privileges are keyed by it. This is the exact shape of the leak the removed
    ``ALTER DEFAULT PRIVILEGES FOR ROLE okg_substrate ... GRANT SELECT`` created
    -- a table that does not exist when either script runs, silently readable.
    """
    table = f"grant_probe_{uuid4().hex[:12]}"
    results: dict[str, Any] = {}
    with psycopg.connect(dsn, autocommit=True, connect_timeout=10) as conn:
        with conn.cursor() as cur:
            cur.execute("SET ROLE okg_substrate")
            cur.execute(
                sql.SQL("CREATE TABLE okg.{} (probe_id integer)").format(
                    sql.Identifier(table)
                )
            )
            cur.execute("RESET ROLE")
        try:
            # A REAL SELECT as the role, not has_table_privilege. The catalog
            # answer covers the table ACL only; a missing `USAGE ON SCHEMA okg`
            # blocks the read while every table ACL still looks right. Attempt
            # the query the deployment would actually run.
            results["okg_mcp_can_read_new_substrate_table"] = _can_select_as(
                conn, "okg_mcp", f"okg.{table}"
            )
            results["app_ro_can_read_new_substrate_table"] = _can_select_as(
                conn, "app_ro", f"okg.{table}"
            )
            # Positive control: the roles must still reach a relation they ARE
            # supposed to read. Without it, "cannot read the probe table" is
            # equally consistent with "cannot read ANYTHING", which is how a
            # revoked schema USAGE would pass as a clean result.
            results["app_ro_can_read_a_registered_view"] = _can_select_as(
                conn, "app_ro", "okg.v_nodes", expect_rows=False
            )
            results["okg_mcp_can_read_graph_nodes"] = _can_select_as(
                conn, "okg_mcp", "okg.graph_nodes", expect_rows=False
            )
        finally:
            with conn.cursor() as cur:
                cur.execute(
                    sql.SQL("DROP TABLE okg.{}").format(sql.Identifier(table))
                )
    results["ok"] = (
        not results["okg_mcp_can_read_new_substrate_table"]
        and not results["app_ro_can_read_new_substrate_table"]
        and results["app_ro_can_read_a_registered_view"]
        and results["okg_mcp_can_read_graph_nodes"]
    )
    return results


def _can_select_as(
    conn: psycopg.Connection, role: str, relation: str, *, expect_rows: bool = False
) -> bool:
    """Can ``role`` actually run a SELECT on ``relation``?

    ``SET LOCAL ROLE`` + a real query. Anything that refuses the read --
    missing table grant, missing schema USAGE -- is a False. A relation whose
    read raises for a NON-privilege reason (the Chronos checkout guard on the
    graph views, which raises object_not_in_prerequisite_state) counts as
    reachable: the role got past the privilege system, which is what is being
    measured.
    """
    try:
        with conn.transaction():
            with conn.cursor() as cur:
                cur.execute(f"SET LOCAL ROLE {role}")
                cur.execute(f"SELECT 1 FROM {relation} LIMIT 1")
                cur.fetchall()
        return True
    except psycopg.errors.InsufficientPrivilege:
        return False
    except psycopg.errors.ObjectNotInPrerequisiteState:
        # The honest-read guard fired: privileges were sufficient.
        return True
    except psycopg.Error:
        return False


def concurrent_apply_probe(dsn: str, *, sessions: int = 4) -> dict[str, Any]:
    """N sessions apply the registry at once; none may raise.

    ORDER IS LOCK ORDER. Every GRANT takes a row lock on the target relation's
    ``pg_class`` tuple, so two sessions issuing the same grants in DIFFERENT
    orders deadlock (40P01) rather than serialising. That is not hypothetical:
    this PACT hit it, by deduplicating the Chronos catch-up's statement list
    with ``array_agg(DISTINCT ...)``, which SORTS. The applier's ``ORDER BY
    entry_ref`` is the property that makes concurrent application safe, and an
    ordering regression is invisible to a single-session run — the privilege
    picture it produces is identical.

    Concurrency is the right shape for this check: the ingest layer runs
    several sources as separate PROCESSES, and a deployment bootstrap can land
    on a database mid-migrate. Both are real races.
    """
    import threading

    errors: list[str] = []
    lock = threading.Lock()
    barrier = threading.Barrier(sessions)

    def _apply() -> None:
        try:
            with psycopg.connect(
                dsn, autocommit=True, connect_timeout=10
            ) as conn, conn.cursor() as cur:
                # A wedged backend must not outlive the checker. The daemon
                # bit bounds process exit; server-side timeouts bound the
                # normal cleanup path and make the error part of the report.
                cur.execute("SET statement_timeout = '90s'")
                cur.execute("SET lock_timeout = '60s'")
                # Start together: staggered starts would serialise by luck.
                barrier.wait(timeout=30)
                cur.execute("SELECT okg.apply_grant_registry()")
        except Exception as exc:  # noqa: BLE001 - reported, not swallowed
            with lock:
                errors.append(f"{type(exc).__name__}: {exc}")

    threads = [
        threading.Thread(target=_apply, daemon=True) for _ in range(sessions)
    ]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=120)
    alive = [thread for thread in threads if thread.is_alive()]
    if alive:
        errors.append(f"{len(alive)} session(s) did not finish within 120s")
    return {"sessions": sessions, "errors": errors, "ok": not errors}


def _apply_role_split(dsn: str, *, deployment: str, mutation: str | None) -> None:
    from okg.substrate.runtime.bootstrap import apply_role_split

    with psycopg.connect(dsn) as conn:
        apply_role_split(conn, deployment=deployment, dbos_schema="dbos")
        if mutation is not None:
            with conn.cursor() as cur:
                cur.execute(MUTATIONS[mutation])
        conn.commit()


def _apply_views(dsn: str) -> None:
    from okg.substrate.db.migrate import install_views

    install_views(dsn)


def build_base(base_dsn: str, dbname: str) -> str:
    """A migrated database, before either script is re-applied.

    Both sides start from the SAME state; only the order of the two
    re-applications differs.
    """
    dsn = conv.build_fresh_install(base_dsn, dbname)
    with psycopg.connect(dsn, autocommit=True, connect_timeout=10) as conn:
        with conn.cursor() as cur:
            # role_split grants on the DBOS schema, which the runtime creates.
            cur.execute("CREATE SCHEMA IF NOT EXISTS dbos")
    return dsn


def _diff(name: str, left: list[list[Any]], right: list[list[Any]]) -> dict[str, Any]:
    left_set = {tuple(row) for row in left}
    right_set = {tuple(row) for row in right}
    only_left = sorted(list(row) for row in left_set - right_set)
    only_right = sorted(list(row) for row in right_set - left_set)
    return {
        "section": name,
        "only_in_role_split_first": only_left,
        "only_in_views_first": only_right,
        "equal": not only_left and not only_right,
    }


def check_commutativity(
    base_dsn: str,
    *,
    prefix: str = "okg_grant_commute",
    mutation: str | None = None,
    keep: bool = False,
) -> dict[str, Any]:
    suffix = uuid4().hex[:8]
    db_a = f"{prefix}_a_{suffix}"
    db_b = f"{prefix}_b_{suffix}"
    report: dict[str, Any] = {
        "check": "grant_commutativity",
        "mutation": mutation,
        "databases": {"role_split_first": db_a, "views_first": db_b},
    }
    try:
        dsn_a = build_base(base_dsn, db_a)
        dsn_b = build_base(base_dsn, db_b)

        # Order A: role_split.sql, then views.sql.
        _apply_role_split(dsn_a, deployment=DEPLOYMENT_NAME, mutation=mutation)
        _apply_views(dsn_a)

        # Order B: views.sql, then role_split.sql.
        _apply_views(dsn_b)
        _apply_role_split(dsn_b, deployment=DEPLOYMENT_NAME, mutation=mutation)

        audit_a = privilege_audit(dsn_a)
        audit_b = privilege_audit(dsn_b)
        drift_a = registry_drift(dsn_a)
        drift_b = registry_drift(dsn_b)
        diffs = [
            _diff(section, audit_a[section], audit_b[section])
            for section in (
                "relation_privileges",
                "column_privileges",
                "function_privileges",
                "schema_privileges",
                "default_acl",
            )
        ]
        probe_a = new_table_probe(dsn_a)
        probe_b = new_table_probe(dsn_b)
        # Run once, on one database: the property is about two sessions racing
        # on ONE cluster's pg_class, which two independent databases cannot
        # exhibit.
        concurrency = concurrent_apply_probe(dsn_a)

        report["diffs"] = diffs
        report["registry_drift"] = {
            "role_split_first": drift_a,
            "views_first": drift_b,
        }
        report["probes"] = {"role_split_first": probe_a, "views_first": probe_b}
        report["concurrent_apply"] = concurrency
        report["ok"] = (
            all(d["equal"] for d in diffs)
            and not drift_a
            and not drift_b
            and probe_a["ok"]
            and probe_b["ok"]
            and concurrency["ok"]
        )
        report["failing_sections"] = [
            d["section"] for d in diffs if not d["equal"]
        ]
        return report
    finally:
        if not keep:
            conv.drop_database(base_dsn, db_a)
            conv.drop_database(base_dsn, db_b)


def _render_text(report: dict[str, Any]) -> str:
    lines: list[str] = []
    lines.append(
        "grant commutativity: "
        + ("OK" if report.get("ok") else "FAILED")
        + (f" (mutation={report['mutation']})" if report.get("mutation") else "")
    )
    for diff in report.get("diffs", []):
        status = "equal" if diff["equal"] else "DIFFERS"
        lines.append(f"  {diff['section']}: {status}")
        for row in diff["only_in_role_split_first"][:20]:
            lines.append(f"    only when role_split ran first: {row}")
        for row in diff["only_in_views_first"][:20]:
            lines.append(f"    only when views ran first:      {row}")
    for side, probe in report.get("probes", {}).items():
        lines.append(
            f"  behavioural probe [{side}]: "
            + ("ok" if probe["ok"] else "FAILED")
            + f" (new table: okg_mcp={probe['okg_mcp_can_read_new_substrate_table']},"
            f" app_ro={probe['app_ro_can_read_new_substrate_table']};"
            f" still reachable: app_ro v_nodes="
            f"{probe['app_ro_can_read_a_registered_view']},"
            f" okg_mcp graph_nodes={probe['okg_mcp_can_read_graph_nodes']})"
        )
    for side, findings in report.get("registry_drift", {}).items():
        lines.append(
            f"  registry drift [{side}]: "
            + ("clean" if not findings else f"{len(findings)} finding(s)")
        )
        for finding in findings[:10]:
            lines.append(f"    {finding}")
    concurrency = report.get("concurrent_apply")
    if concurrency is not None:
        lines.append(
            f"  concurrent apply x{concurrency['sessions']}: "
            + ("ok" if concurrency["ok"] else "FAILED")
        )
        for error in concurrency["errors"][:10]:
            lines.append(f"    {error}")
    return "\n".join(lines)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--dsn",
        default=os.environ.get(
            "OKG_TEST_DSN",
            os.environ.get(
                "OKG_DSN", "postgres://postgres:okg@localhost:5433/postgres"
            ),
        ),
        help="base DSN; databases are created beside it",
    )
    parser.add_argument("--prefix", default="okg_grant_commute")
    parser.add_argument(
        "--mutate",
        choices=sorted(MUTATIONS),
        default=None,
        help=(
            "inject a deliberate privilege fault at the end of the role_split "
            "step; used to prove both comparative and absolute gate arms"
        ),
    )
    parser.add_argument(
        "--keep",
        action="store_true",
        help="do not drop the two databases (for investigation)",
    )
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args(argv)

    report = check_commutativity(
        args.dsn, prefix=args.prefix, mutation=args.mutate, keep=args.keep
    )
    if args.json:
        print(json.dumps(report, sort_keys=True, default=str))
    else:
        print(_render_text(report))
    return 0 if report["ok"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
