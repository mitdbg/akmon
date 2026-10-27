#!/usr/bin/env python3
"""Refuse a migration that drops a relation something else re-creates.

Issue #1054. `0001_retire_job_runtime_ledger` dropped seven `okg.job_*` tables
while `schema.sql` still created them. Its postcondition checked absence inside
its own transaction, saw the tables gone, and recorded `absent: true` --
truthfully, for about a second. The next schema apply put all seven back, a
still-running writer refilled them, and the migration ledger carried a false
claim for a month.

Nothing caught it. The schema-migration convergence gate
(`scripts/check_migration_convergence.py`) is structurally blind here: it builds
the migrated and freshly installed sides from the same sources, so a drop and
its re-create cancel on both sides and the diff is empty either way.

This check compares the two directly. A migration's DROP targets are matched
against every place the shipped tree CREATEs a relation -- `schema.sql`,
`views.sql`, and the runtime `CREATE TABLE IF NOT EXISTS` sites in Python, which
are deliberately absent from `schema.sql` (see
`scripts/check_migration_convergence.py`) and would otherwise be an uncovered
path to the same defect.

A drop is exempt when the same migration file re-creates the relation itself --
that is a replace, not a permanent no-op.

Fails closed: an unreadable or statement-free source is a failure, and a run
that parsed zero DROP targets or zero CREATE targets is a failure, so an empty
scan can never read as clean.

Usage:
    python3 scripts/check_migration_drop_recreate.py [--json]
"""
from __future__ import annotations

import argparse
import ast
import json
import re
import sys
from dataclasses import dataclass, field
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
DB_DIR = REPO_ROOT / "src" / "okg" / "substrate" / "db"
MIGRATIONS_DIR = DB_DIR / "migrations"
SCHEMA_SQL = DB_DIR / "schema.sql"
VIEWS_SQL = DB_DIR / "views.sql"
PYTHON_ROOT = REPO_ROOT / "src" / "okg"

# Object kinds whose creation and destruction we pair up. FUNCTION is matched
# by bare name (arguments differ between DROP and CREATE spellings).
_DROPPABLE = r"(?:MATERIALIZED\s+VIEW|TABLE|VIEW|INDEX|SEQUENCE|FUNCTION)"

# `DROP TABLE a, b, c` names three relations, and identifiers may be quoted on
# either side of the dot. Capture the whole comma list and split it below --
# taking only the first name silently loses every relation after the comma.
_IDENT = r"(?:\"[^\"]+\"|[A-Za-z_][\w$]*)"
# `okg . job_runs` is valid Postgres; whitespace around the dot must not turn
# one relation into two unrelated names.
_QUALIFIED = rf"{_IDENT}(?:\s*\.\s*{_IDENT})?"
_DROP_RE = re.compile(
    rf"\bDROP\s+{_DROPPABLE}\s+(?:CONCURRENTLY\s+)?(?:IF\s+EXISTS\s+)?"
    rf"((?:{_QUALIFIED}\s*(?:\([^)]*\))?\s*,\s*)*"
    rf"{_QUALIFIED}\s*(?:\([^)]*\))?)",
    re.IGNORECASE,
)
# Dynamic DDL cannot be resolved statically. A migration that builds a DROP at
# runtime is reported unparseable rather than scanned as if it dropped nothing.
_DYNAMIC_DDL_RE = re.compile(
    r"\bEXECUTE\b[^;]*\b(?:DROP|CREATE)\s+"
    r"(?:MATERIALIZED\s+VIEW|TABLE|VIEW|INDEX|SEQUENCE|FUNCTION)",
    re.IGNORECASE | re.DOTALL,
)
_CREATE_RE = re.compile(
    rf"\bCREATE\s+(?:OR\s+REPLACE\s+)?(?:UNIQUE\s+)?(?:TEMP(?:ORARY)?\s+)?"
    rf"({_DROPPABLE})\s+(?:CONCURRENTLY\s+)?(?:IF\s+NOT\s+EXISTS\s+)?"
    r"([A-Za-z_][\w$]*(?:\.[A-Za-z_][\w$]*)?)"
    r"(?P<tail>[^;]*)",
    re.IGNORECASE,
)
# CREATE INDEX names the index unqualified and the table qualified
# (`CREATE INDEX foo_idx ON okg.bar`), while DROP INDEX qualifies the index
# (`DROP INDEX okg.foo_idx`). Recover the schema from the ON clause so the two
# spellings compare equal.
_ON_RE = re.compile(r"\bON\s+(?:ONLY\s+)?([A-Za-z_][\w$]*)\.", re.IGNORECASE)

# A source with none of these is not SQL we understood; refuse rather than
# scoring it clean.
_STATEMENT_RE = re.compile(
    r"\b(CREATE|DROP|ALTER|INSERT|UPDATE|DELETE|SET|DO|COMMENT|GRANT|REVOKE)\b",
    re.IGNORECASE,
)

# Runtime CREATE sites live inside Python string literals. Most of them do NOT
# spell the relation out -- they interpolate a module constant
# (`CREATE TABLE IF NOT EXISTS {ATTRIBUTE_INTEGRITY_STATE_TABLE}`). Matching
# only the literal form leaves the majority of the runtime corpus invisible,
# which is the same blindness this whole check exists to remove, so the target
# is captured either way and resolved through the module's constants below.
_PY_CREATE_RE = re.compile(
    r"\bCREATE\s+(?:OR\s+REPLACE\s+)?(?:UNIQUE\s+)?"
    rf"({_DROPPABLE})\s+(?:CONCURRENTLY\s+)?(?:IF\s+NOT\s+EXISTS\s+)?"
    r"(\{[^}\n]*\}|%s|okg\.[A-Za-z_][\w$]*|[A-Za-z_][\w$]*)",
    re.IGNORECASE,
)
# Spellings whose target is built outside the literal: `%s`, `{}`, `{f(x)}`,
# `{obj.attr}`, and `" ... " + NAME`. Each is captured and then either resolved
# or refused -- treating them as "no CREATE here" is the blindness this check
# exists to remove.
_UNRESOLVABLE_TARGET_RE = re.compile(r"^(?:%s|\{\s*\}|\{.*[(\[.].*\})$")


# Runtime CREATE targets that are computed, not named. Each entry is an
# explicit, reviewed acknowledgement: the site builds its relation name at run
# time, so this scanner cannot resolve it, and a human has confirmed no
# migration drops it by static name. A NEW dynamic site is NOT covered by these
# entries and reds the gate until someone reviews it -- the refusal is closed by
# a decision on the record, never by silence.
# The count is part of the acknowledgement: a NEW dynamic site in an
# already-acknowledged file must not inherit the old site's exemption.
ACKNOWLEDGED_DYNAMIC_CREATE_SITES: dict[tuple[str, str], tuple[int, str]] = {
    ("src/okg/substrate/db/management.py", "{view}"): (
        1,
        "honest_read_view_name(table) derives one view per Chronos-registered "
        "base table; no migration drops these by static name",
    ),
    (
        "src/okg/substrate/derived_artifacts/maintained_derivation.py",
        "{snapshot_temp}",
    ): (1, "session TEMP scratch table, never a durable okg.* relation"),
    ("src/okg/substrate/db/cold_history.py", "{}"): (
        3,
        "cold-history indexes on public.* partitions, named per partition at "
        "run time, and okg#3112 step 1's concurrent build of a missing "
        "visibility spec index (name = physical table + spec suffix); no "
        "migration drops them by static name",
    ),
    (
        "src/okg/substrate/declared_attr_indexes.py",
        "{'CONCURRENTLY ' if concurrently else ''}",
    ): (
        1,
        "okg#3112 declared-attribute expression indexes, named "
        "okg_dattr_<n|e>_<hash of attribute, type and key version> at run "
        "time; only the reconciler creates and drops them, through its "
        "manifest, and no migration drops them by static name",
    ),
    ("src/okg/substrate/derived_artifacts/relations.py", "{}"): (
        2,
        "derived-artifact relation and index names are supplied per artifact "
        "spec, including the schema; no migration drops them by static name",
    ),
    ("src/okg/substrate/views/read_policy.py", "{}"): (
        1,
        "admitted SQL read-model view schema/object identifiers come from the "
        "validated deployment plan; no migration drops these deployment-owned "
        "objects by static name",
    ),
    ("src/okg/substrate/semantic_ann.py", "{quote_ident(index_name)}"): (
        1,
        "PACT openrouter-embeddings-hnsw: the semantic-search HNSW index on "
        "the Chronos node segment, named chronos_interval_index_name(physical "
        "table, 'node_embedding_hnsw') at run time; no migration drops it by "
        "static name",
    ),
    ("src/okg/substrate/embeddings/store.py", "{name}"): (
        1,
        "PACT openrouter-embeddings-hnsw: one partial HNSW index per vector "
        "set on okg.text_embeddings, named set_index_name(set) at run time; "
        "only ensure_set_index drops (when invalid) and rebuilds it, and no "
        "migration drops it by static name",
    ),
    (
        "src/okg/substrate/graph_store_internal/schema.py",
        "{quote_ident(index_name)}",
    ): (
        4,
        "Chronos per-branch index names derived from the branch table, and "
        "the waiting-edge table's endpoint and source indexes (name = physical "
        "table + suffix); no migration drops them by static name",
    ),
}


def _module_string_constants(text: str) -> dict[str, str]:
    """Module-level ``NAME = "okg.thing"`` bindings, for interpolated targets."""
    try:
        tree = ast.parse(text)
    except SyntaxError:
        return {}
    constants: dict[str, str] = {}
    for node in tree.body:
        targets: list[ast.expr] = []
        if isinstance(node, ast.Assign):
            targets = list(node.targets)
            value = node.value
        elif isinstance(node, ast.AnnAssign) and node.value is not None:
            targets = [node.target]
            value = node.value
        else:
            continue
        if not isinstance(value, ast.Constant) or not isinstance(value.value, str):
            continue
        for target in targets:
            if isinstance(target, ast.Name):
                constants[target.id] = value.value
    return constants


def _strip_sql_comments(text: str) -> str:
    text = re.sub(r"--[^\n]*", " ", text)
    return re.sub(r"/\*.*?\*/", " ", text, flags=re.DOTALL)


def _normalize(name: str) -> str:
    """`okg."Job_Runs"`, `"okg".job_runs` and `okg.job_runs` are one relation."""
    parts = [part.strip().strip('"') for part in name.strip().split(".", 1)]
    if len(parts) == 2:
        return f"{parts[0].lower()}.{parts[1].lower()}"
    return f"public.{parts[0].lower()}"


def _drop_targets(fragment: str) -> list[str]:
    """Split a DROP's comma list into normalized relation names."""
    targets = []
    for piece in fragment.split(","):
        cleaned = re.sub(r"\(.*", "", piece).strip()
        if cleaned:
            targets.append(_normalize(cleaned))
    return targets


@dataclass
class Scan:
    drops: dict[str, list[str]] = field(default_factory=dict)
    creates: dict[str, list[str]] = field(default_factory=dict)
    unparseable: list[dict[str, str]] = field(default_factory=list)


def _scan_sql_creates(
    text: str,
    source: str,
    into: dict[str, list[str]],
    *,
    positions: dict[str, int] | None = None,
) -> None:
    for match in _CREATE_RE.finditer(text):
        kind = match.group(1).upper()
        raw = match.group(2)
        names = [raw]
        if kind == "INDEX" and "." not in raw.strip('"'):
            on_match = _ON_RE.search(match.group("tail") or "")
            if on_match:
                names = [f"{on_match.group(1)}.{raw}"]
            else:
                # An unqualified `CREATE INDEX foo ON bar` lands in whatever
                # schema is on the search_path. Register it under every schema a
                # DROP could name it in rather than guessing `public` and
                # missing `DROP INDEX okg.foo`.
                names = [f"okg.{raw}", raw]
        for name in names:
            normalized = _normalize(name)
            into.setdefault(normalized, []).append(source)
            if positions is not None:
                positions[normalized] = max(
                    positions.get(normalized, -1), match.start()
                )


def scan(
    *,
    migration_paths: list[Path] | None = None,
    create_sql_paths: list[Path] | None = None,
    python_paths: list[Path] | None = None,
) -> Scan:
    """Collect DROP targets from migrations and CREATE targets from the tree."""
    result = Scan()
    migrations = (
        sorted(MIGRATIONS_DIR.glob("*.sql"))
        if migration_paths is None
        else migration_paths
    )
    creates = (
        [SCHEMA_SQL, VIEWS_SQL] if create_sql_paths is None else create_sql_paths
    )
    pythons = (
        sorted(PYTHON_ROOT.rglob("*.py")) if python_paths is None else python_paths
    )

    for path in migrations:
        try:
            raw = path.read_text(encoding="utf-8")
        except (OSError, UnicodeDecodeError) as exc:
            result.unparseable.append({"path": str(path), "reason": str(exc)})
            continue
        text = _strip_sql_comments(raw)
        if not _STATEMENT_RE.search(text):
            result.unparseable.append(
                {"path": str(path), "reason": "no recognizable SQL statement"}
            )
            continue
        if _DYNAMIC_DDL_RE.search(text):
            result.unparseable.append(
                {
                    "path": str(path),
                    "reason": "builds DDL dynamically; targets are not statically known",
                }
            )
            continue
        # A drop the same file re-creates AFTERWARDS is a replace, not a
        # permanent no-op. Order matters: create-then-drop leaves it gone.
        self_created: dict[str, list[str]] = {}
        create_positions: dict[str, int] = {}
        _scan_sql_creates(text, str(path), self_created, positions=create_positions)
        for match in _DROP_RE.finditer(text):
            for target in _drop_targets(match.group(1)):
                if create_positions.get(target, -1) > match.start():
                    continue
                result.drops.setdefault(target, []).append(str(path))

    for path in creates:
        try:
            raw = path.read_text(encoding="utf-8")
        except (OSError, UnicodeDecodeError) as exc:
            result.unparseable.append({"path": str(path), "reason": str(exc)})
            continue
        text = _strip_sql_comments(raw)
        if not _STATEMENT_RE.search(text):
            result.unparseable.append(
                {"path": str(path), "reason": "no recognizable SQL statement"}
            )
            continue
        _scan_sql_creates(text, str(path), result.creates)

    for path in pythons:
        try:
            raw = path.read_text(encoding="utf-8")
        except (OSError, UnicodeDecodeError) as exc:
            result.unparseable.append({"path": str(path), "reason": str(exc)})
            continue
        constants: dict[str, str] | None = None
        acknowledged_seen: dict[tuple[str, str], int] = {}
        try:
            relative = str(path.relative_to(REPO_ROOT))
        except ValueError:
            relative = str(path)
        for match in _PY_CREATE_RE.finditer(raw):
            target = match.group(2)
            if target.startswith("{") or target == "%s":
                resolved = None
                if not _UNRESOLVABLE_TARGET_RE.match(target):
                    if constants is None:
                        constants = _module_string_constants(raw)
                    resolved = constants.get(target[1:-1].strip())
                if resolved is None:
                    key = (relative, target)
                    acknowledged = ACKNOWLEDGED_DYNAMIC_CREATE_SITES.get(key)
                    if acknowledged is not None:
                        acknowledged_seen[key] = acknowledged_seen.get(key, 0) + 1
                        if acknowledged_seen[key] <= acknowledged[0]:
                            continue
                    # Fail closed: an unresolvable runtime CREATE target means
                    # this scan cannot say whether a drop conflicts with it.
                    result.unparseable.append(
                        {
                            "path": str(path),
                            "reason": (
                                f"CREATE target {target} does not resolve to a "
                                f"module-level string constant; if it is built "
                                f"at run time, add a reviewed entry to "
                                f"ACKNOWLEDGED_DYNAMIC_CREATE_SITES"
                                + (
                                    f" (acknowledged {acknowledged[0]} site(s) "
                                    f"in this file, found more)"
                                    if acknowledged is not None
                                    else ""
                                )
                            ),
                        }
                    )
                    continue
                target = resolved
            if not target.lower().startswith("okg."):
                # Unqualified or non-okg targets in Python are temp tables and
                # DBOS/vendor relations; migrations here only drop okg.* objects.
                continue
            result.creates.setdefault(_normalize(target), []).append(str(path))
    return result


def check(**kwargs) -> dict:
    """Return the conflict report. `ok` is False on any conflict or refusal."""
    scanned = scan(**kwargs)
    conflicts = [
        {
            "relation": relation,
            "dropped_by": sorted(set(sources)),
            "created_by": sorted(set(scanned.creates[relation])),
        }
        for relation, sources in sorted(scanned.drops.items())
        if relation in scanned.creates
    ]
    refusals: list[str] = []
    if scanned.unparseable:
        refusals.append("unparseable sources")
    if not scanned.drops:
        # An empty scan and a clean tree look identical from the outside.
        refusals.append("no DROP targets parsed")
    if not scanned.creates:
        refusals.append("no CREATE targets parsed")
    return {
        "ok": not conflicts and not refusals,
        "conflicts": conflicts,
        "unparseable": scanned.unparseable,
        "refusals": refusals,
        "drop_target_count": len(scanned.drops),
        "create_target_count": len(scanned.creates),
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args()
    report = check()
    if args.json:
        print(json.dumps(report, indent=2, sort_keys=True))
    elif report["ok"]:
        print(
            f"ok: {report['drop_target_count']} DROP targets vs "
            f"{report['create_target_count']} CREATE targets, no conflicts"
        )
    else:
        for refusal in report["refusals"]:
            print(f"REFUSED: {refusal}", file=sys.stderr)
        for entry in report["unparseable"]:
            print(
                f"UNPARSEABLE: {entry['path']}: {entry['reason']}", file=sys.stderr
            )
        for conflict in report["conflicts"]:
            print(
                f"CONFLICT: {conflict['relation']} is dropped by "
                f"{', '.join(conflict['dropped_by'])} but created by "
                f"{', '.join(conflict['created_by'])}",
                file=sys.stderr,
            )
    return 0 if report["ok"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
