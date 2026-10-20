"""Provenance-split audit — backstops Decision 2 at the Postgres level.

Per Decision 2 of `rebuild-substrate-on-dbos`, DBOS provenance
(workflow-execution) and OKG provenance (epistemic) live in separate
schemas and never share rows. The role split established by Phase 1
§2.2 enforces this at the Postgres level: the ``okg_dbos`` role has
no write privileges on OKG fact tables; per-deployment roles have only
narrow INSERT and column-level UPDATE on the fact tables they own.

This audit verifies the privilege contract holds using Postgres'
**effective-privilege** functions (``has_table_privilege``,
``has_column_privilege``) rather than ``information_schema.role_table_grants``.
The earlier ``role_table_grants``-based implementation had a blind spot
that the Phase 1 §2.10 review caught (BLOCKER A): it only saw *direct*
grants and missed:

  * **Role inheritance** — ``GRANT okg_substrate TO okg_dbos`` gives
    ``okg_dbos`` effective writes on OKG schemas that
    ``role_table_grants`` records under ``okg_substrate``, not under
    ``okg_dbos``. ``has_table_privilege('okg_dbos', ...)`` correctly
    walks the role hierarchy and returns the effective privilege.
  * **PUBLIC grants** — ``GRANT INSERT ON okg.escalation_live TO PUBLIC``
    grants the privilege to every role, but ``role_table_grants``
    records grantee='PUBLIC'. We now query
    ``information_schema.table_privileges`` (which captures PUBLIC)
    in addition to per-role effective checks.
  * **ALTER DEFAULT PRIVILEGES** — default-privilege grants on a
    schema affect tables created after the ALTER. Effective-privilege
    functions see the materialized grants on every existing table, so
    iterating tables and asking ``has_table_privilege`` for each one
    handles default-privilege drift the same way it handles direct
    grants.

Phase 1 §2.5 wires this audit into ``okg doctor`` so drift is surfaced
loudly on every operator run.

The audit is safe to run from any operator role with read access to
``information_schema``, ``pg_catalog``, and the relevant schemas. It is
strictly read-only.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass

from okg.substrate.db.connection import connect as db_connect


__all__ = [
    "AUDIT_OK_VERDICT",
    "ProvenanceSplitVerdict",
    "audit_provenance_split",
]


log = logging.getLogger(__name__)


AUDIT_OK_VERDICT = "ok"


# ---------------------------------------------------------------------------
# Audit queries — effective privileges via has_table_privilege /
# has_column_privilege, plus an information_schema lookup for PUBLIC.
# ---------------------------------------------------------------------------


# Check 1a: ``okg_dbos``'s effective write privileges on ``okg.*`` /
# ``okg_derived.*``. Uses ``has_table_privilege``, which:
#   - walks role inheritance (catches ``GRANT okg_substrate TO okg_dbos``)
#   - reflects materialized ALTER DEFAULT PRIVILEGES grants on existing tables
#   - returns true for any path by which the role has the privilege
_AUDIT_SQL_DBOS_EFFECTIVE_WRITES = """
WITH targets AS (
    SELECT n.nspname AS schema_name,
           c.relname AS table_name,
           c.oid     AS oid
    FROM pg_class c
    JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE n.nspname IN ('okg', 'okg_derived')
      AND c.relkind IN ('r', 'p')
),
privs(priv) AS (
    VALUES ('INSERT'), ('UPDATE'), ('DELETE'), ('TRUNCATE')
)
SELECT t.schema_name, t.table_name, p.priv
FROM targets t
CROSS JOIN privs p
WHERE has_table_privilege('okg_dbos', t.oid, p.priv)
ORDER BY t.schema_name, t.table_name, p.priv
"""


# Check 1b: PUBLIC pseudo-role grants on OKG / DBOS schemas. The
# effective-privilege functions accept role names, not PUBLIC; we read
# ``information_schema.table_privileges`` which captures grantee='PUBLIC'.
_AUDIT_SQL_PUBLIC_WRITES = """
SELECT table_schema, table_name, privilege_type
FROM information_schema.table_privileges
WHERE grantee = 'PUBLIC'
  AND privilege_type IN ('INSERT', 'UPDATE', 'DELETE', 'TRUNCATE')
  AND (
        table_schema IN ('okg', 'okg_derived')
        OR table_schema LIKE 'dbos%%'
      )
ORDER BY table_schema, table_name, privilege_type
"""


# Check 2: deployment role's effective writes on forbidden tables.
# Parameterized on the deployment role name. Forbidden set:
#   - substrate-only OKG tables (graph_generations, source_watermarks,
#     narrowings, catalog_*)
#   - every table in the DBOS workflow-state schema(s)
_AUDIT_SQL_DEPLOYMENT_FORBIDDEN_WRITES = """
WITH targets AS (
    SELECT n.nspname AS schema_name,
           c.relname AS table_name,
           c.oid     AS oid
    FROM pg_class c
    JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE c.relkind IN ('r', 'p')
      AND (
            (n.nspname = 'okg' AND c.relname IN (
                'graph_generations',
                'source_watermarks',
                'narrowings',
                'catalog_versions',
                'catalog_modules',
                'catalog_module_versions',
                'catalog_subtype_versions'
            ))
            OR n.nspname LIKE 'dbos%%'
          )
),
privs(priv) AS (
    VALUES ('INSERT'), ('UPDATE'), ('DELETE'), ('TRUNCATE')
)
SELECT t.schema_name, t.table_name, p.priv
FROM targets t
CROSS JOIN privs p
WHERE has_table_privilege(%s, t.oid, p.priv)
ORDER BY t.schema_name, t.table_name, p.priv
"""


# Check 3: deployment role's column-level UPDATE on
# ``okg.escalation_live`` must be restricted to ``valid_to_gen``
# only. Iterates every other column and uses
# ``has_column_privilege`` to detect any drift.
#
# escalation_live is included from Phase 2 onward (per the SDK build).
# On Phase-1 substrates the table doesn't exist; the audit's
# information_schema-derived target set is naturally empty for it.
_AUDIT_SQL_DEPLOYMENT_BROAD_ESCALATION_LIVE_UPDATES = """
WITH targets AS (
    SELECT n.nspname AS schema_name,
           c.relname AS table_name,
           a.attname AS column_name,
           c.oid     AS oid
    FROM pg_class c
    JOIN pg_namespace n ON n.oid = c.relnamespace
    JOIN pg_attribute a ON a.attrelid = c.oid
    WHERE n.nspname = 'okg'
      AND c.relname IN ('escalation_live')
      AND a.attnum > 0
      AND NOT a.attisdropped
      AND a.attname <> 'valid_to_gen'
)
SELECT schema_name, table_name, column_name
FROM targets
WHERE has_column_privilege(%s, oid, column_name, 'UPDATE')
ORDER BY schema_name, table_name, column_name
"""


# Diagnostic enrichment: enumerate pg_auth_members rows touching
# ``okg_substrate`` or ``okg_dbos``. Surfaces *which* membership grant
# caused an effective-privilege violation, so the operator can run a
# targeted REVOKE. Always informational — these rows are not violations
# on their own (operators may legitimately inherit substrate to run
# bootstrap), but they accompany violations to aid diagnosis.
_AUDIT_SQL_SERVICE_ROLE_MEMBERSHIPS = """
SELECT
    granted.rolname AS granted_role,
    member.rolname  AS member_role
FROM pg_auth_members am
JOIN pg_roles granted ON granted.oid = am.roleid
JOIN pg_roles member  ON member.oid  = am.member
WHERE granted.rolname IN ('okg_substrate', 'okg_dbos')
   OR member.rolname  IN ('okg_substrate', 'okg_dbos')
ORDER BY granted.rolname, member.rolname
"""


# Discover all deployment-role names that exist in this database.
_LIST_DEPLOYMENT_ROLES_SQL = """
SELECT rolname FROM pg_roles
WHERE rolname LIKE 'okg_deployment_%%'
ORDER BY rolname
"""


# ---------------------------------------------------------------------------
# Verdict
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class ProvenanceSplitVerdict:
    """Outcome of the provenance-split audit.

    Attributes
    ----------
    deployment : str
        Deployment the audit ran against.
    ok : bool
        True if the audit passes.
    violations : list[str]
        Human-readable description of each privilege drift found.
        Empty when ok=True.
    diagnostics : list[str]
        Informational notes that aid diagnosis when violations are
        present (e.g. service-role memberships that may explain how a
        privilege leaked). Not part of the pass/fail decision.
    """

    deployment: str
    ok: bool
    violations: list[str]
    diagnostics: list[str]

    def to_dict(self) -> dict:
        return {
            "provenance_split": (
                AUDIT_OK_VERDICT if self.ok else "violation"
            ),
            "deployment": self.deployment,
            "violations": list(self.violations),
            "diagnostics": list(self.diagnostics),
        }


# ---------------------------------------------------------------------------
# Audit entry point
# ---------------------------------------------------------------------------


def audit_provenance_split(
    *,
    deployment: str,
    dsn: str,
) -> ProvenanceSplitVerdict:
    """Run the provenance-split audit against ``dsn`` and return a verdict.

    Five checks total. The first three are mandated by `sdk-design.md`
    §5.2 / spec.md "Provenance audit"; the latter two close the BLOCKER
    A blind spot surfaced by the Phase 1 §2.10 review:

      1. ``okg_dbos`` has no effective INSERT/UPDATE/DELETE/TRUNCATE on
         ``okg.*`` or ``okg_derived.*`` (catches role-inheritance and
         materialized default-privilege drift).
      2. PUBLIC has no INSERT/UPDATE/DELETE/TRUNCATE on ``okg.*``,
         ``okg_derived.*``, or any ``dbos.*`` schema.
      3. Every ``okg_deployment_*`` role has no effective writes on
         substrate-only OKG tables (``graph_generations``,
         ``source_watermarks``, catalog tables) or on the DBOS schema.
      4. Every ``okg_deployment_*`` role's effective UPDATE on
         ``okg.escalation_live`` is restricted to the
         ``valid_to_gen`` column.
      5. Diagnostic-only: service-role membership graph touching
         ``okg_substrate`` / ``okg_dbos`` — surfaced to aid diagnosis
         of any violations from checks 1-4, never a violation on its own.

    All checks use ``has_table_privilege`` / ``has_column_privilege``
    on existing tables, so they reflect the post-materialization view
    of grants (including ALTER DEFAULT PRIVILEGES, role inheritance,
    and direct grants together).

    Idempotent and read-only.
    """
    violations: list[str] = []
    diagnostics: list[str] = []

    with db_connect(dsn, autocommit=True) as conn:
        with conn.cursor() as cur:
            # Check 1: okg_dbos effective writes on OKG schemas.
            cur.execute(_AUDIT_SQL_DBOS_EFFECTIVE_WRITES)
            for schema, table, priv in cur.fetchall():
                violations.append(
                    f"okg_dbos has effective {priv} on {schema}.{table} "
                    "(provenance-split rule: okg_dbos must not write "
                    "to OKG schemas — check role memberships, "
                    "PUBLIC grants, and direct grants)"
                )

            # Check 2: PUBLIC writes on OKG / DBOS schemas.
            cur.execute(_AUDIT_SQL_PUBLIC_WRITES)
            for schema, table, priv in cur.fetchall():
                violations.append(
                    f"PUBLIC has {priv} on {schema}.{table} "
                    "(boundary rule: no PUBLIC writes on OKG or DBOS "
                    "schemas — every write must trace to a named role)"
                )

            # Discover deployment roles for per-role checks.
            cur.execute(_LIST_DEPLOYMENT_ROLES_SQL)
            deployment_roles = [row[0] for row in cur.fetchall()]

            # Check 3: each deployment role's writes on forbidden tables.
            for role in deployment_roles:
                cur.execute(
                    _AUDIT_SQL_DEPLOYMENT_FORBIDDEN_WRITES, (role,)
                )
                for schema, table, priv in cur.fetchall():
                    violations.append(
                        f"{role} has effective {priv} on "
                        f"{schema}.{table} (boundary rule: deployment "
                        "roles must not write to substrate-only or "
                        "dbos tables)"
                    )

            # Check 4: column-level UPDATE on escalation_live.
            for role in deployment_roles:
                cur.execute(
                    _AUDIT_SQL_DEPLOYMENT_BROAD_ESCALATION_LIVE_UPDATES,
                    (role,),
                )
                for schema, table, column in cur.fetchall():
                    violations.append(
                        f"{role} has effective UPDATE({column}) on "
                        f"{schema}.{table} (boundary rule: UPDATE on "
                        "escalation_live must be restricted to "
                        "valid_to_gen only)"
                    )

            # Diagnostic (always run): service-role memberships.
            cur.execute(_AUDIT_SQL_SERVICE_ROLE_MEMBERSHIPS)
            for granted, member in cur.fetchall():
                diagnostics.append(
                    f"role membership: {member} inherits {granted} "
                    "(informational — operator memberships are normal; "
                    "service-role-to-service-role inheritance is "
                    "suspect)"
                )

    verdict = ProvenanceSplitVerdict(
        deployment=deployment,
        ok=not violations,
        violations=violations,
        diagnostics=diagnostics,
    )
    if violations:
        log.warning(
            "provenance-split audit FAILED for %s: %d violation(s); "
            "%d diagnostic(s)",
            deployment, len(violations), len(diagnostics),
        )
    else:
        log.info(
            "provenance-split audit OK for %s (%d diagnostic(s))",
            deployment, len(diagnostics),
        )
    return verdict
