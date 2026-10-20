"""Standing schema-hygiene gates: born-VALID constraints, no dead relations.

Two gates, each with a static arm (over the shipped DDL, so a pull request
reds before any database exists) and a catalog arm (over a migrated database,
so a deployment that drifted reds too). Both arms must agree; a gate that can
only see one of them says so rather than passing on the half it can read.

  born_valid
    Every CHECK constraint the schema ships MUST be enforced. A constraint
    added `NOT VALID` is a catalog entry Postgres does not apply to existing
    rows, and one that is never VALIDATEd is decoration. The static arm
    refuses an unpaired `NOT VALID` in schema.sql --- unpaired meaning no
    ordered migration issues `VALIDATE CONSTRAINT` for that constraint name.
    The catalog arm refuses any `pg_constraint` row with `convalidated` false.

  no_dead_relations
    The relations retired by `schema-dead-weight-retirement` MUST stay
    retired. The static arm refuses their reappearance in the shipped DDL;
    the catalog arm refuses their presence on a migrated database. Pinned by
    name, because the failure this guards is a relation quietly coming back
    (a revert, a bad merge resolution, a copied block), which no
    zero-consumer heuristic would catch --- the relation would have no
    consumers then either.

Usage:
    python scripts/check_schema_relation_hygiene.py [--dsn DSN] [--json]

Without ``--dsn`` only the static arms run and the catalog arms report
``unavailable``, which fails the gate. That is deliberate: this is the same
fail-closed posture `okg schema quality` takes for every other gating tier.
"""
from __future__ import annotations

import argparse
import json
import re
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
SCHEMA_SQL = ROOT / "src" / "okg" / "substrate" / "db" / "schema.sql"
MIGRATIONS_DIR = ROOT / "src" / "okg" / "substrate" / "db" / "migrations"
EXEMPTIONS_YAML = (
    ROOT / "src" / "okg" / "substrate" / "db" / "not_valid_exemptions.yaml"
)

# The pinned absence list. Each entry is (catalog kind, identifier), where the
# identifier is what `to_regclass` / `to_regprocedure` resolves and the token
# the static arm greps for in the shipped DDL.
#
# okg.lease_audit is NOT on this list, though the audit (F15) named it. It has
# no catalog dependency -- okg.v_runtime_lock_surface_inventory stores the
# STRING 'okg.lease_audit' in a hardcoded VALUES list and never selects the
# table, so a RESTRICT drop of the table succeeds and the view keeps returning
# its four rows. What keeps the table is not a dependency but three live
# references plus one honesty problem: tests/substrate/conftest.py truncates
# it between tests, environment_clone.py disposition it, management.py
# classifies it, and dropping it would leave that published inventory
# advertising a `storage_relation` that no longer resolves -- app_ro readers
# would be told a table exists that does not. Fixing the inventory row is a
# views.sql edit, and views.sql belongs to another lane.
#
# The two function signatures are spelled with explicit `+` rather than
# adjacent literals: implicit concatenation inside a list/tuple reads as a
# missing comma to both humans and CodeQL
# (py/implicit-string-concatenation-in-list).
_COMPUTE_PAGERANK_SIG = (
    "okg.compute_pagerank(text, double precision, integer, double precision)"
)
_REFRESH_PAGERANK_SIG = (
    "okg.refresh_pagerank(text, text, double precision, integer, "
    + "double precision)"
)

RETIRED_RELATIONS: tuple[tuple[str, str], ...] = (
    ("table", "okg.pagerank_scores"),
    ("table", "okg.verifier_runs"),
    ("function", _COMPUTE_PAGERANK_SIG),
    ("function", _REFRESH_PAGERANK_SIG),
)

# The bare names the static arm greps for, derived from the pins above so the
# two arms cannot drift apart.
_RETIRED_TOKENS: tuple[str, ...] = (
    "pagerank_scores",
    "verifier_runs",
    "compute_pagerank",
    "refresh_pagerank",
)

_NOT_VALID_RE = re.compile(r"\bNOT\s+VALID\b", re.IGNORECASE)
_ADD_CONSTRAINT_RE = re.compile(
    r"\bADD\s+CONSTRAINT\s+(\w+)\b", re.IGNORECASE
)
# Matches both the literal form (`VALIDATE CONSTRAINT foo`) and the dynamic
# form this tree actually uses inside DO blocks
# (`format('... VALIDATE CONSTRAINT %I', ..., 'foo')`). Without the second,
# the "pair it with a validation migration" escape hatch would be advice no
# migration in this repo could follow -- 0025 itself validates dynamically.
_VALIDATE_CONSTRAINT_RE = re.compile(
    r"\bVALIDATE\s+CONSTRAINT\s+\"?(\w+)\"?", re.IGNORECASE
)
_DYNAMIC_VALIDATE_RE = re.compile(
    r"\bVALIDATE\s+CONSTRAINT\s+%I", re.IGNORECASE
)
# A quoted SQL identifier-ish literal, used to harvest the constraint names a
# dynamic VALIDATE iterates over.
_SQL_STRING_LITERAL_RE = re.compile(r"'([A-Za-z_][A-Za-z0-9_]*)'")


def load_exemptions() -> dict[str, dict[str, str]]:
    """Parse the born-VALID exemption registry, keyed by constraint name.

    Fail-loud: an unreadable or malformed registry raises. A gate that
    degraded to "no exemptions" would red the tree spuriously; one that
    degraded to "everything exempt" would pass anything. Neither is a
    verdict, so refuse instead.
    """
    import yaml

    raw = yaml.safe_load(EXEMPTIONS_YAML.read_text(encoding="utf-8")) or {}
    entries: dict[str, dict[str, str]] = {}
    for entry in raw.get("exemptions") or ():
        if not isinstance(entry, dict):
            raise ValueError(f"exemption entry is not a mapping: {entry!r}")
        missing = [
            key
            for key in ("relation", "constraint", "owner", "reason")
            if not str(entry.get(key) or "").strip()
        ]
        if missing:
            raise ValueError(
                f"exemption entry {entry.get('constraint')!r} is missing "
                f"{missing}; an exemption without an owner and a reason is "
                "indistinguishable from a forgotten constraint"
            )
        # Keyed on (relation, constraint), not the bare constraint name.
        # Postgres only requires a constraint name to be unique per relation,
        # so a bare-name key would let one registered exemption silently cover
        # a same-named constraint on a DIFFERENT table.
        entries[f"{entry['relation']}.{entry['constraint']}"] = {
            key: str(entry[key])
            for key in ("relation", "constraint", "owner", "reason")
        }
    return entries


def _strip_sql_comments(text: str) -> str:
    """Blank out `--` line comments and `/* */` block comments.

    Prose in a comment must not satisfy a gate, and must not trip one either:
    the migration that retires these relations names them in its own header.
    Trailing comments count -- an earlier version only dropped WHOLE-line
    `--` comments, so `DROP TABLE okg.x; -- mentions pagerank_scores` still
    tripped the pinned-absence arm. Comment bodies are replaced with spaces
    rather than deleted so byte offsets (and therefore statement boundaries)
    are preserved.
    """
    out: list[str] = []
    i, n = 0, len(text)
    in_line = in_block = in_string = False
    while i < n:
        ch = text[i]
        nxt = text[i + 1] if i + 1 < n else ""
        if in_line:
            if ch == "\n":
                in_line = False
                out.append(ch)
            else:
                out.append(" ")
        elif in_block:
            if ch == "*" and nxt == "/":
                in_block = False
                out.append("  ")
                i += 2
                continue
            out.append("\n" if ch == "\n" else " ")
        elif in_string:
            out.append(ch)
            if ch == "'":
                in_string = False
        elif ch == "-" and nxt == "-":
            in_line = True
            out.append("  ")
            i += 2
            continue
        elif ch == "/" and nxt == "*":
            in_block = True
            out.append("  ")
            i += 2
            continue
        else:
            if ch == "'":
                in_string = True
            out.append(ch)
        i += 1
    return "".join(out)


# Kept as the historical name so callers reading "strip comments" still work.
_strip_line_comments = _strip_sql_comments


def _validated_constraint_names() -> set[str]:
    """Constraint names any ordered migration issues VALIDATE CONSTRAINT for.

    Covers both the literal form and the dynamic `format(... VALIDATE
    CONSTRAINT %I ...)` form the DO-block migrations in this tree use. For the
    dynamic form the executed name is not knowable statically, so every SQL
    string literal in that migration is treated as a candidate name -- which
    is deliberately generous: this set only ever REMOVES a static finding, and
    the catalog arm is what proves the constraint is actually validated on a
    real database.
    """
    names: set[str] = set()
    for path in sorted(MIGRATIONS_DIR.glob("*.sql")):
        body = _strip_sql_comments(path.read_text(encoding="utf-8"))
        names |= set(_VALIDATE_CONSTRAINT_RE.findall(body))
        if _DYNAMIC_VALIDATE_RE.search(body):
            names |= set(_SQL_STRING_LITERAL_RE.findall(body))
    return names


def _statement_start(text: str, offset: int) -> int:
    """Index just past the last `;` before ``offset`` (statement boundary)."""
    return text.rfind(";", 0, offset) + 1


def _unpaired_not_valid(
    schema_text: str, exemptions: dict[str, dict[str, str]]
) -> list[dict[str, Any]]:
    """`NOT VALID` sites in schema.sql with no paired validation migration.

    A `NOT VALID` is attributed to an `ADD CONSTRAINT <name>` only when both
    appear in the SAME statement (no `;` between them). Scanning backwards
    without that bound would let an UNNAMED constraint
    (`ADD CHECK (...) NOT VALID`, whose name Postgres generates) inherit the
    name of some earlier, unrelated statement's constraint -- and then a
    registry entry for THAT name would silently exempt it.
    """
    stripped = _strip_sql_comments(schema_text)
    findings: list[dict[str, Any]] = []
    validated = _validated_constraint_names()
    exempt_names = {entry["constraint"] for entry in exemptions.values()}
    for match in _NOT_VALID_RE.finditer(stripped):
        statement = stripped[_statement_start(stripped, match.start()): match.start()]
        preceding = _ADD_CONSTRAINT_RE.findall(statement)
        name = preceding[-1] if preceding else None
        if name is not None and (name in validated or name in exempt_names):
            continue
        findings.append({
            "check": "born_valid",
            "arm": "static",
            "constraint": name,
            "detail": (
                f"schema.sql adds {name or '<unnamed>'} NOT VALID and no "
                "ordered migration issues VALIDATE CONSTRAINT for it, so the "
                "constraint is never enforced on existing rows; pair it with "
                "a validation migration, or register it in "
                "not_valid_exemptions.yaml with an owner and a reason"
            ),
        })
    return findings


def _resurrected_relations(schema_text: str) -> list[dict[str, Any]]:
    stripped = _strip_line_comments(schema_text)
    findings: list[dict[str, Any]] = []
    for token in _RETIRED_TOKENS:
        if token in stripped:
            findings.append({
                "check": "no_dead_relations",
                "arm": "static",
                "relation": token,
                "detail": (
                    f"{token} is on the retired-relation pin list but appears "
                    "in the shipped DDL again; a retired relation coming back "
                    "is a revert or a bad merge resolution, not a new feature"
                ),
            })
    return findings


def _catalog_arms(
    dsn: str, exemptions: dict[str, dict[str, str]]
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    import psycopg

    violations: list[dict[str, Any]] = []
    present: list[str] = []
    invalid: list[dict[str, str]] = []
    exempt_seen: list[str] = []

    with psycopg.connect(dsn) as conn, conn.cursor() as cur:
        for kind, identifier in RETIRED_RELATIONS:
            resolver = "to_regclass" if kind == "table" else "to_regprocedure"
            cur.execute(f"SELECT {resolver}(%s) IS NOT NULL", (identifier,))
            row = cur.fetchone()
            if row is None:
                raise RuntimeError(
                    f"catalog probe for {identifier!r} returned no row"
                )
            if row[0]:
                present.append(identifier)
                violations.append({
                    "check": "no_dead_relations",
                    "arm": "catalog",
                    "relation": identifier,
                    "detail": (
                        f"{identifier} still exists on this database; run "
                        "migrate to apply "
                        "0025_dead_relation_and_constraint_convergence"
                    ),
                })

        # convalidated is the only authority. "The constraint exists" is not
        # "the constraint is enforced" --- reading the former as the latter is
        # the defect this gate closes.
        cur.execute(
            """
            SELECT c.conname,
                   n.nspname || '.' || r.relname AS relation
              FROM pg_constraint c
              JOIN pg_class r ON r.oid = c.conrelid
              JOIN pg_namespace n ON n.oid = r.relnamespace
            -- Every non-system schema, not just okg/okg_derived. This arm
            -- exists to catch DRIFT, and a hand-applied constraint is most
            -- likely to land somewhere unexpected; scoping the scan to the
            -- schemas we already trust would make it blindest exactly where
            -- it is needed. Verified green across all schemas on a migrated
            -- database, so the wider scope costs nothing today.
             WHERE NOT c.convalidated
               AND n.nspname NOT IN ('pg_catalog', 'information_schema')
               AND n.nspname NOT LIKE 'pg_toast%'
               AND n.nspname NOT LIKE 'pg_temp%'
             ORDER BY 2, 1
            """
        )
        for conname, relation in cur.fetchall():
            if f"{relation}.{conname}" in exemptions:
                # Registered, owned, and argued. Reported so it stays visible,
                # never silently dropped from the output.
                exempt_seen.append(f"{relation}.{conname}")
                continue
            invalid.append({"constraint": conname, "relation": relation})
            violations.append({
                "check": "born_valid",
                "arm": "catalog",
                "constraint": conname,
                "relation": relation,
                "detail": (
                    f"{relation}.{conname} is NOT VALID on this database, so "
                    "Postgres does not enforce it against existing rows; run "
                    "migrate to apply "
                    "0025_dead_relation_and_constraint_convergence, or "
                    "register the constraint in not_valid_exemptions.yaml"
                ),
            })

    return violations, {
        "retired_relations_present": sorted(present),
        "unvalidated_constraints": invalid,
        "registered_exemptions_observed": sorted(exempt_seen),
    }


def run_relation_hygiene_checks(
    *, dsn: str | None = None, static_only: bool = False
) -> dict[str, Any]:
    schema_text = SCHEMA_SQL.read_text(encoding="utf-8")
    exemptions = load_exemptions()

    violations: list[dict[str, Any]] = []
    violations.extend(_unpaired_not_valid(schema_text, exemptions))
    violations.extend(_resurrected_relations(schema_text))

    checks: dict[str, Any] = {
        "static": {
            "not_valid_sites": len(
                _NOT_VALID_RE.findall(_strip_line_comments(schema_text))
            ),
            "pinned_retired_relations": len(RETIRED_RELATIONS),
            "registered_exemptions": sorted(exemptions),
        }
    }

    if dsn:
        catalog_violations, catalog_checks = _catalog_arms(dsn, exemptions)
        violations.extend(catalog_violations)
        checks["catalog"] = catalog_checks
        catalog_status = "checked"
    elif static_only:
        # An EXPLICIT declaration that only the static arms are being
        # certified -- the shape the repo already uses to split its static
        # gating tier (no Postgres on the lint runner) from the DB-backed tier
        # that runs under `okg schema quality` in the substrate-db lane. This
        # is not the same as a missing DSN: the caller has said which half it
        # is asking for, and the envelope still reports that the catalog arm
        # examined nothing.
        catalog_status = "not_requested"
        checks["catalog"] = {
            "detail": (
                "static-only run; the catalog arms were not requested and "
                "certify nothing -- run with --dsn (or `okg schema quality`) "
                "for the catalog half"
            )
        }
    else:
        # A gate that silently skips the arm it cannot run reports PASS on the
        # evidence it never gathered. Say unavailable and fail.
        catalog_status = "unavailable"
        checks["catalog"] = {
            "detail": (
                "no DSN supplied; the catalog arms of both gates examined "
                "nothing and cannot be certified"
            )
        }

    return {
        "ok": not violations and catalog_status in ("checked", "not_requested"),
        "catalog_status": catalog_status,
        "violations": violations,
        "checks": checks,
    }


def _format_text(result: dict[str, Any]) -> str:
    lines = [
        "schema relation hygiene: " + ("PASS" if result["ok"] else "FAIL"),
        f"  catalog arm: {result['catalog_status']}",
    ]
    for name, payload in sorted(result["checks"].items()):
        lines.append(f"  {name}: {json.dumps(payload, sort_keys=True)}")
    for violation in result["violations"]:
        target = violation.get("relation") or violation.get("constraint") or ""
        lines.append(
            f"  VIOLATION {violation['check']} ({violation['arm']}): "
            f"{target} --- {violation['detail']}"
        )
    return "\n".join(lines)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dsn", help="migrated database DSN")
    parser.add_argument(
        "--static-only",
        action="store_true",
        help=(
            "certify only the static (shipped-DDL) arms; the catalog arms are "
            "reported as not_requested rather than treated as passing"
        ),
    )
    parser.add_argument("--json", action="store_true", dest="as_json")
    args = parser.parse_args(argv)

    result = run_relation_hygiene_checks(
        dsn=args.dsn, static_only=args.static_only
    )
    if args.as_json:
        print(json.dumps(result, indent=2, sort_keys=True))
    else:
        print(_format_text(result))
    return 0 if result["ok"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
