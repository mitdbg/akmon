#!/usr/bin/env python3
"""Identity-convention gate for `okg schema quality`.

Enforces the identity conventions from
``docs/reference/database-schema.md#schema-design-rules`` against a migrated database's
catalog* --- not against schema.sql's text, because the conventions are
statements about the shape a deployment actually has, and runtime-ensured DDL
(audit F2) is exactly where the text and the catalog disagree.

    generation refs   no ordinal-typed generation reference in the GRANTED
                      surface, where the granted surface is every relation
                      okg_mcp can SELECT, closed under view dependency via
                      pg_depend (D2 amendment (c): "never exposed" is a catalog
                      check over the grant surface, not an aspiration).
    branch naming     every column named ``branch_id`` has
                      ``okg.branches.branch_id``'s type. Chronos TEXT branch
                      identifiers are named ``chronos_branch_id``.
    deployment scope  no user table carries a column literally named
                      ``deployment``.

Plus one explicit assertion from D2 amendment (a): ``okg.generation_ordinals``
is NOT in okg_mcp's granted surface.

Known divergences are registered in
``src/okg/substrate/db/identity_divergences.yaml`` with an owning PACT and a
reason. Anything NOT registered is a gate failure --- that is what makes this a
freeze gate rather than a snapshot of today's schema: a new relation cannot mint
a fourth identity convention, and a converged divergence cannot come back
(removing its row is how a convergence lane proves it landed).

Usage:
    python scripts/check_schema_identity.py --dsn postgres://...
    python scripts/check_schema_identity.py --dsn ... --json
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
import re
from typing import Any

import psycopg
import yaml

ROOT = Path(__file__).resolve().parents[1]
REGISTRY_PATH = ROOT / "src" / "okg" / "substrate" / "db" / "identity_divergences.yaml"

#: Column names that denote a generation reference. Note that the generation
#: catalog's OWN key (``okg.graph_generations.generation_id``) matches this
#: pattern too and is NOT excluded here: it is carried as an ordinary
#: registered divergence in identity_divergences.yaml, like every other
#: unconverged column, so that retiring it means deleting a row rather than
#: editing this file.
GENERATION_REF_COLUMN_RE = r"(^|_)generation(_id|_ordinal)$"

#: Postgres type names that carry an ordinal (a generation *position*) rather
#: than an identity.
ORDINAL_TYPES = ("smallint", "integer", "bigint", "numeric")

#: The read role whose grants define the freeze surface.
_GRANTED_ROLE = "okg_mcp"

#: Relations the read role must not hold a DIRECT grant on (D2 amendment (a)).
#:
#: Direct grant, not pg_depend closure, is the right predicate: the whole point
#: of the amendment is that the MCP sidecar stops reading the ordinal registry
#: as an ordinal-keyed table and reads it through okg.v_generation_interval_stab
#: instead. That view necessarily depends on the table, so a closure-based test
#: would refuse the very shape the amendment prescribes.
_DIRECT_GRANT_FORBIDDEN = ("okg.generation_ordinals",)


class RegistryError(RuntimeError):
    """The divergence registry is malformed."""


def load_registry(path: Path | None = None) -> dict[str, Any]:
    """Load and validate the registered-divergence file."""
    registry_path = path or REGISTRY_PATH
    raw = yaml.safe_load(registry_path.read_text(encoding="utf-8")) or {}
    if not isinstance(raw, dict):
        raise RegistryError(f"{registry_path}: top level must be a mapping")
    for section in (
        "generation_refs",
        "generation_functions",
        "branch_naming",
        "deployment_scoping",
    ):
        entries = raw.get(section) or []
        if not isinstance(entries, list):
            raise RegistryError(f"{registry_path}: {section} must be a list")
        for entry in entries:
            if not isinstance(entry, dict):
                raise RegistryError(f"{registry_path}: {section} entries must be maps")
            for field in ("relation", "owner", "reason"):
                value = entry.get(field)
                if not isinstance(value, str) or not value.strip():
                    raise RegistryError(
                        f"{registry_path}: {section} entry "
                        f"{entry.get('relation')!r} needs a non-empty {field!r}"
                    )
    return raw


def _registered_keys(registry: dict[str, Any], section: str) -> set[tuple[str, str]]:
    keys: set[tuple[str, str]] = set()
    for entry in registry.get(section) or []:
        keys.add((entry["relation"], entry.get("column") or ""))
    return keys


def granted_surface(cur: psycopg.Cursor[Any]) -> set[str]:
    """Every ``okg`` relation the MCP read role can reach, pg_depend-closed.

    Roots are the relations okg_mcp holds SELECT on. The closure walks
    ``pg_depend``/``pg_rewrite`` from each granted VIEW to the relations its
    rule reads, because a column is exposed exactly as much through a view as
    through the base table (D2 amendment (c)).
    """
    cur.execute(
        """
        WITH RECURSIVE roots AS (
          SELECT c.oid
            FROM pg_class c
            JOIN pg_namespace n ON n.oid = c.relnamespace
           WHERE n.nspname = 'okg'
             AND c.relkind IN ('r', 'v', 'm', 'p', 'f')
             AND has_table_privilege(%s, c.oid, 'SELECT')
        ),
        closure AS (
          SELECT oid FROM roots
          UNION
          SELECT d.refobjid
            FROM closure cl
            JOIN pg_rewrite r ON r.ev_class = cl.oid
            JOIN pg_depend d ON d.objid = r.oid
                            AND d.classid = 'pg_rewrite'::regclass
                            AND d.refclassid = 'pg_class'::regclass
           WHERE d.refobjid <> cl.oid
        )
        SELECT n.nspname || '.' || c.relname
          FROM closure cl
          JOIN pg_class c ON c.oid = cl.oid
          JOIN pg_namespace n ON n.oid = c.relnamespace
         WHERE n.nspname = 'okg'
           AND c.relkind IN ('r', 'v', 'm', 'p', 'f')
        """,
        (_GRANTED_ROLE,),
    )
    return {row[0] for row in cur.fetchall()}


def _role_exists(cur: psycopg.Cursor[Any]) -> bool:
    cur.execute("SELECT 1 FROM pg_roles WHERE rolname = %s", (_GRANTED_ROLE,))
    return cur.fetchone() is not None


def ordinal_generation_columns(cur: psycopg.Cursor[Any]) -> list[dict[str, str]]:
    """Every ordinal-typed generation *reference* column in schema ``okg``."""
    cur.execute(
        """
        SELECT n.nspname || '.' || c.relname AS relation,
               a.attname AS column,
               format_type(a.atttypid, a.atttypmod) AS type
          FROM pg_class c
          JOIN pg_namespace n ON n.oid = c.relnamespace
          JOIN pg_attribute a ON a.attrelid = c.oid
                             AND a.attnum > 0
                             AND NOT a.attisdropped
         WHERE n.nspname = 'okg'
           AND c.relkind IN ('r', 'v', 'm', 'p', 'f')
           AND a.attname ~ %s
           AND format_type(a.atttypid, a.atttypmod) = ANY(%s)
         ORDER BY 1, 2
        """,
        (GENERATION_REF_COLUMN_RE, list(ORDINAL_TYPES)),
    )
    return [
        {"relation": rel, "column": col, "type": typ}
        for rel, col, typ in cur.fetchall()
    ]


def divergent_branch_id_columns(cur: psycopg.Cursor[Any]) -> list[dict[str, str]]:
    """Columns named ``branch_id`` whose type differs from the surrogate key's.

    ``public._chronos*`` is out of scope by D4 (the vendored chronos layout is
    accepted as a documented contract), so only schema ``okg`` is examined.
    """
    cur.execute(
        """
        WITH canonical AS (
          SELECT format_type(a.atttypid, a.atttypmod) AS type
            FROM pg_attribute a
           WHERE a.attrelid = 'okg.branches'::regclass
             AND a.attname = 'branch_id'
             AND NOT a.attisdropped
        )
        SELECT n.nspname || '.' || c.relname AS relation,
               format_type(a.atttypid, a.atttypmod) AS type,
               (SELECT type FROM canonical) AS expected
          FROM pg_class c
          JOIN pg_namespace n ON n.oid = c.relnamespace
          JOIN pg_attribute a ON a.attrelid = c.oid
                             AND a.attnum > 0
                             AND NOT a.attisdropped
         WHERE n.nspname = 'okg'
           AND c.relkind IN ('r', 'v', 'm', 'p', 'f')
           AND a.attname = 'branch_id'
           AND format_type(a.atttypid, a.atttypmod)
               IS DISTINCT FROM (SELECT type FROM canonical)
         ORDER BY 1
        """
    )
    return [
        {"relation": rel, "column": "branch_id", "type": typ, "expected": expected}
        for rel, typ, expected in cur.fetchall()
    ]


def ordinal_generation_functions(cur: psycopg.Cursor[Any]) -> list[dict[str, str]]:
    """EXECUTE-granted functions that pass or return a generation as an ordinal.

    §5 puts functions in the freeze surface, not just relations: ``okg_mcp``
    holds ``GRANT EXECUTE ON ALL FUNCTIONS`` (views.sql), so a function taking
    a ``p_generation_id BIGINT`` or returning a bigint generation is exposed
    ordinal generation identity exactly as much as a column would be.
    """
    cur.execute(
        """
        SELECT n.nspname || '.' || p.proname AS relation,
               pg_get_function_identity_arguments(p.oid) AS args,
               pg_catalog.format_type(p.prorettype, NULL) AS rettype
          FROM pg_proc p
          JOIN pg_namespace n ON n.oid = p.pronamespace
         WHERE n.nspname = 'okg'
           AND has_function_privilege(%s, p.oid, 'EXECUTE')
         ORDER BY 1, 2
        """,
        (_GRANTED_ROLE,),
    )
    found: list[dict[str, str]] = []
    for name, args, rettype in cur.fetchall():
        offenders = []
        for arg in (args or "").split(","):
            arg = arg.strip()
            if not arg:
                continue
            parts = arg.split()
            if len(parts) < 2:
                continue
            argname, argtype = parts[0], " ".join(parts[1:])
            if re.search(GENERATION_REF_COLUMN_RE, argname) and (
                argtype in ORDINAL_TYPES
            ):
                offenders.append(f"{argname} {argtype}")
        # A function NAMED for the pin/generation that hands back an ordinal is
        # the same exposure as an ordinal column, e.g. okg.current_pin().
        if rettype in ORDINAL_TYPES and re.search(r"(pin|generation)", name):
            offenders.append(f"RETURNS {rettype}")
        if offenders:
            found.append({
                "relation": name,
                "column": "; ".join(offenders),
                "type": rettype,
            })
    return found


def bare_deployment_relations(cur: psycopg.Cursor[Any]) -> list[dict[str, str]]:
    """Relations carrying a column literally named ``deployment``."""
    cur.execute(
        """
        SELECT n.nspname || '.' || c.relname AS relation,
               format_type(a.atttypid, a.atttypmod) AS type
          FROM pg_class c
          JOIN pg_namespace n ON n.oid = c.relnamespace
          JOIN pg_attribute a ON a.attrelid = c.oid
                             AND a.attnum > 0
                             AND NOT a.attisdropped
         WHERE n.nspname = 'okg'
           AND c.relkind IN ('r', 'v', 'm', 'p', 'f')
           AND a.attname = 'deployment'
         ORDER BY 1
        """
    )
    return [{"relation": rel, "column": "deployment", "type": typ}
            for rel, typ in cur.fetchall()]


def run_identity_checks(
    *,
    dsn: str | None = None,
    conn: psycopg.Connection[Any] | None = None,
    registry_path: Path | None = None,
) -> dict[str, Any]:
    """Evaluate the three conventions; return a machine-readable envelope.

    Pass ``dsn`` for the normal case (the CLI opens its own connection). Pass an
    open ``conn`` instead when the caller needs the checks to observe
    uncommitted catalog state — the mutation-fixture tests create a violating
    relation inside a transaction and roll it back, which is both faster and
    higher-fidelity than provisioning a second database.
    """
    if (dsn is None) == (conn is None):
        raise ValueError("pass exactly one of dsn= or conn=")
    if conn is not None:
        return _run_identity_checks_on(conn, registry_path=registry_path)
    assert dsn is not None
    with psycopg.connect(dsn, autocommit=True) as owned:
        return _run_identity_checks_on(owned, registry_path=registry_path)


def _run_identity_checks_on(
    conn: psycopg.Connection[Any], *, registry_path: Path | None
) -> dict[str, Any]:
    registry = load_registry(registry_path)
    violations: list[dict[str, Any]] = []
    registered: list[dict[str, Any]] = []
    checks: dict[str, Any] = {}

    with conn.cursor() as cur:
        role_present = _role_exists(cur)
        surface = granted_surface(cur) if role_present else set()

        # --- 1. generation references in the granted surface --------------
        known = _registered_keys(registry, "generation_refs")
        exposed = []
        for row in ordinal_generation_columns(cur):
            if row["relation"] not in surface:
                # Outside the freeze surface. Still reported when registered so
                # the registry cannot rot, but never a violation on its own.
                if (row["relation"], row["column"]) in known:
                    registered.append({"check": "generation_refs", **row})
                continue
            exposed.append(row)
            entry = {"check": "generation_refs", **row}
            if (row["relation"], row["column"]) in known:
                registered.append(entry)
            else:
                violations.append(entry)
        checks["generation_refs"] = {
            "granted_relations": len(surface),
            "ordinal_generation_columns_in_surface": len(exposed),
            "granted_role_present": role_present,
        }

        # --- 1a. generation identity in EXECUTE-granted functions (§5) -----
        known_fn = _registered_keys(registry, "generation_functions")
        fn_divergent = ordinal_generation_functions(cur) if role_present else []
        for row in fn_divergent:
            entry = {"check": "generation_functions", **row}
            if (row["relation"], row["column"]) in known_fn:
                registered.append(entry)
            else:
                violations.append(entry)
        checks["generation_functions"] = {
            "ordinal_generation_functions": len(fn_divergent)
        }

        # --- 1b. generation_ordinals must not be directly granted (D2 (a)) -
        wrongly_granted: list[str] = []
        if role_present:
            for rel in _DIRECT_GRANT_FORBIDDEN:
                cur.execute("SELECT to_regclass(%s)", (rel,))
                if cur.fetchone()[0] is None:
                    continue
                cur.execute(
                    "SELECT has_table_privilege(%s, %s, 'SELECT')",
                    (_GRANTED_ROLE, rel),
                )
                if cur.fetchone()[0]:
                    wrongly_granted.append(rel)
        for rel in wrongly_granted:
            violations.append({
                "check": "direct_grant_forbidden",
                "relation": rel,
                "detail": (
                    f"{_GRANTED_ROLE} holds a direct SELECT grant on the "
                    "ordinal registry; read it through "
                    "okg.v_generation_interval_stab, which is keyed on the "
                    "TEXT generation ref"
                ),
            })
        checks["direct_grant_forbidden"] = {
            "checked": list(_DIRECT_GRANT_FORBIDDEN),
            "wrongly_granted": wrongly_granted,
        }

        # --- 2. branch naming --------------------------------------------
        known = _registered_keys(registry, "branch_naming")
        divergent = divergent_branch_id_columns(cur)
        for row in divergent:
            entry = {"check": "branch_naming", **row}
            if (row["relation"], "branch_id") in known:
                registered.append(entry)
            else:
                violations.append(entry)
        checks["branch_naming"] = {"type_divergent_branch_id_columns": len(divergent)}

        # --- 3. deployment scoping ---------------------------------------
        known = _registered_keys(registry, "deployment_scoping")
        bare = bare_deployment_relations(cur)
        for row in bare:
            entry = {"check": "deployment_scoping", **row}
            if (row["relation"], "") in known:
                registered.append(entry)
            else:
                violations.append(entry)
        checks["deployment_scoping"] = {"bare_deployment_relations": len(bare)}

        # --- 4. the registry must not rot --------------------------------
        #
        # A registry row that matches nothing in the catalog is the dangerous
        # case, not the harmless one: it is a standing exemption for a
        # divergence that no longer exists, ready to silently cover the same
        # divergence if it comes back. Removing the row is how a convergence
        # lane proves it landed, so a stale row means either the lane forgot
        # to remove it or the shape drifted underneath it. Either way the gate
        # must say so rather than quietly carrying it forever.
        matched = {
            ("generation_refs", r["relation"], r["column"])
            for r in ordinal_generation_columns(cur)
        }
        matched |= {
            ("branch_naming", r["relation"], "branch_id")
            for r in divergent_branch_id_columns(cur)
        }
        matched |= {
            ("deployment_scoping", r["relation"], "")
            for r in bare_deployment_relations(cur)
        }
        matched |= {
            ("generation_functions", r["relation"], r["column"])
            for r in fn_divergent
        }
        stale: list[dict[str, Any]] = []
        absent: list[str] = []
        for section in (
            "generation_refs",
            "generation_functions",
            "branch_naming",
            "deployment_scoping",
        ):
            for entry in registry.get(section) or []:
                column = entry.get("column") or (
                    "branch_id" if section == "branch_naming" else ""
                )
                if (section, entry["relation"], column) in matched:
                    continue
                if section == "generation_functions":
                    # to_regclass() only resolves relations; a registered
                    # function that no longer matches IS stale.
                    stale.append({
                        "check": "stale_registration",
                        "section": section,
                        "relation": entry["relation"],
                        "column": column,
                        "detail": (
                            "registered function divergence no longer matches; "
                            "delete the row from identity_divergences.yaml"
                        ),
                    })
                    continue
                # A relation that does not exist AT ALL has not converged --- it
                # simply has not been created yet. Several registered relations
                # are runtime-ensured (audit F2) and are absent until the first
                # publish, so treating "absent relation" as "stale row" would
                # red a freshly migrated database for no divergence at all, and
                # would make the gate depend on whether some earlier test
                # happened to publish first. Only a relation that EXISTS while
                # its registered divergence does not is a converged row that
                # someone forgot to delete.
                cur.execute("SELECT to_regclass(%s)", (entry["relation"],))
                if cur.fetchone()[0] is None:
                    absent.append(entry["relation"])
                    continue
                stale.append({
                        "check": "stale_registration",
                        "section": section,
                        "relation": entry["relation"],
                        "column": column,
                        "detail": (
                            "registered divergence is absent from the catalog; "
                            "delete the row from identity_divergences.yaml "
                            "(that is how a convergence lane proves it landed) "
                            "or correct it to the shape that is really there"
                        ),
                    })
        violations.extend(stale)
        checks["registry_hygiene"] = {
            "stale_registrations": len(stale),
            # Reported, never gating: these are registered relations that do
            # not exist on this database yet (runtime-ensured DDL).
            "registered_relations_absent": sorted(set(absent)),
        }

    # A missing read role means the grant surface is undefined, so the
    # generation-ref and direct-grant arms above examined NOTHING. Reporting
    # that as PASS would be absence dressed as agreement --- the check would
    # be loudest exactly where it can see least. Refuse instead.
    if not role_present:
        violations.append({
            "check": "granted_role_missing",
            "relation": _GRANTED_ROLE,
            "detail": (
                f"role {_GRANTED_ROLE} does not exist, so the granted surface "
                "is undefined and the generation-reference and direct-grant "
                "arms examined nothing; this is not a pass"
            ),
        })

    return {
        "ok": not violations,
        "violations": violations,
        "registered_divergences": registered,
        "registered_divergence_count": len(registered),
        "checks": checks,
    }


def _format_text(result: dict[str, Any]) -> str:
    lines = [
        "schema identity: " + ("PASS" if result["ok"] else "FAIL"),
        f"  registered divergences: {result['registered_divergence_count']}",
    ]
    for name, payload in sorted(result["checks"].items()):
        lines.append(f"  {name}: {json.dumps(payload, sort_keys=True)}")
    for violation in result["violations"]:
        target = violation.get("relation", "")
        column = violation.get("column")
        if column:
            target = f"{target}.{column}"
        lines.append(
            f"  UNREGISTERED {violation['check']}: {target} "
            f"{violation.get('type') or violation.get('detail') or ''}".rstrip()
        )
    if result["violations"]:
        lines.append(
            "  fix: converge the column, or register it in "
            "src/okg/substrate/db/identity_divergences.yaml with an owning "
            "PACT and a reason"
        )
    return "\n".join(lines)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dsn", required=True, help="migrated database DSN")
    parser.add_argument("--json", action="store_true", dest="as_json")
    args = parser.parse_args(argv)

    result = run_identity_checks(dsn=args.dsn)
    if args.as_json:
        print(json.dumps(result, indent=2, sort_keys=True))
    else:
        print(_format_text(result))
    return 0 if result["ok"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
