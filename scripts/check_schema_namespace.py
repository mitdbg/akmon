#!/usr/bin/env python3
"""Fail CI on an application table that is not created in the `okg` schema.

The namespace contract (docs/reference/database-schema.md#namespace-contract):
everything application-owned lives in `okg`. Exactly one family
is exempt -- the vendored Chronos physical and metadata tables, which the
upstream package creates unqualified in `public` -- plus a short, enumerated
list of deliberate `public` residents recorded in PUBLIC_SCHEMA_EXEMPTIONS
below. There is no wildcard: a bare `_chronos` prefix rule would let any
future table claim the exemption by naming itself well, so each family is
listed with its owner, its generator, and the reason it stays.

This is the static arm. It reads the SQL snapshot, the ordered migrations, and
every DDL string embedded in `src/okg` Python, and refuses a CREATE TABLE whose
target resolves to `public`. The runtime arm -- a DDL-capturing event trigger
that watches what the Python ensure paths actually create -- lives in
tests/substrate/db/test_namespace_qualification.py, because a statically clean
tree can still build an unqualified name at runtime.

Exit codes: 0 clean, 1 violations found, 2 the check itself failed.
"""
from __future__ import annotations

import argparse
import ast
import json
import re
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]

#: Schemas an application relation may be created in. `okg` is the contract;
#: `okg_derived` is the owned derived-relation schema; `dbos` belongs to the
#: DBOS runtime, which owns its own namespace and is out of scope (section 0
#: of the v1.0 design).
OWNED_SCHEMAS = frozenset({"okg", "okg_derived", "dbos"})

#: SQL files carrying application DDL. Deployment-local `views.sql` files are
#: globbed so a deployment cannot smuggle in an unqualified table.
SQL_GLOBS = (
    "src/okg/substrate/db/schema.sql",
    "src/okg/substrate/db/views.sql",
    "src/okg/substrate/db/migrations/*.sql",
    "src/okg/substrate/db/sql/*.sql",
    "src/okg/substrate/runtime/sql/*.sql",
    "deployments/*/views.sql",
)

#: Python trees whose embedded DDL is application DDL.
PYTHON_ROOTS = ("src/okg",)

#: `src/chronos_core` is vendored from mitdbg/chronos (see
#: src/chronos_core/VENDORED.md): local edits are clobbered on every upstream
#: refresh, so its DDL habits are an upstream question, not ours. It is
#: excluded from ruff for the same reason (pyproject.toml extend-exclude). D4
#: accepts its layout and files the qualification request upstream; the
#: RUNTIME arm still watches what it creates, against the registry below.
EXCLUDED_TREES = ("src/chronos_core",)


@dataclass(frozen=True)
class PublicExemption:
    """One enumerated relation family permitted to live in `public`."""

    #: Anchored regex matched against the bare relation name. Written as a
    #: pattern only where the generator mints names at runtime; every entry
    #: still names exactly one family, never a prefix class.
    pattern: str
    owner: str
    generator: str
    reason: str

    def matches(self, relation: str) -> bool:
        return re.fullmatch(self.pattern, relation) is not None


#: THE registry. Adding an entry is a namespace-contract change: it needs the
#: owner, the code that generates the name, and why `okg` is not an option.
PUBLIC_SCHEMA_EXEMPTIONS: tuple[PublicExemption, ...] = (
    PublicExemption(
        pattern=r"_chronos_branch_tables",
        owner="vendored chronos_core",
        generator="src/chronos_core/branching/_common.py",
        reason=(
            "Chronos' registry of branch-managed logical tables. Vendored "
            "(D4): qualifying it means a permanent local divergence or an "
            "upstream change."
        ),
    ),
    PublicExemption(
        pattern=r"_chronos_branch_indexes",
        owner="vendored chronos_core",
        generator="src/chronos_core/branching/_common.py",
        reason="Chronos' index registry; same vendoring argument as above.",
    ),
    PublicExemption(
        pattern=(
            r"_chronos_branch_interval_"
            r"(branches|segments|checkpoints|segment_id_alloc)"
        ),
        owner="vendored chronos_core",
        generator="src/chronos_core/branching/_interval_backend.py",
        reason=(
            "Branch/checkpoint metadata for the interval backend, the one "
            "backend OKG runs. Four relations, enumerated."
        ),
    ),
    PublicExemption(
        pattern=r"_chronos_branch_table_(schema_versions|bindings)",
        owner="vendored chronos_core",
        generator="src/chronos_core/branching/_interval_backend.py",
        reason=(
            "Per-logical-table schema version and binding metadata for the "
            "interval backend."
        ),
    ),
    PublicExemption(
        pattern=(
            r"_chronos_b_interval_okg_[a-z0-9_]+_[0-9a-f]{10}"
            r"(?:_[0-9a-f]{8})?"
        ),
        owner="vendored chronos_core",
        generator=(
            "src/chronos_core/branching/_interval_backend.py:1103, "
            "`_chronos_b_interval_{_physical_table_suffix(table)}`; the "
            "optional 8-hex tail is the schema-version variant at :1442"
        ),
        reason=(
            "The interval physicals -- one per registered logical table, so "
            "the names are minted at registration and cannot be listed "
            "literally. The pattern is anchored on what the generator "
            "actually produces: the `okg_` segment is the LOGICAL table's "
            "schema, and the hex tail is chronos' own hash "
            "(`_chronos_b_interval_okg_graph_nodes_1c8e1fc23f`). So this "
            "entry admits only a physical whose logical table is itself in "
            "`okg` -- it cannot be claimed by an arbitrary table that merely "
            "starts with `_chronos`. Bounded at 8-12 per database (census). "
            "This is the family D4 accepts, with its NUMERIC(32,0) ordinals."
        ),
    ),
    PublicExemption(
        pattern=r"okg_ephemeral_database",
        owner="okg.substrate.db.management",
        generator="src/okg/substrate/db/management.py",
        reason=(
            "The marker that grants the test-database janitor authority to "
            "drop a database. It must survive operations that drop and "
            "recreate the `okg` schema, which is precisely what it would not "
            "do from inside it."
        ),
    ),
)

#: DDL sites whose target cannot be resolved statically -- the schema is a
#: runtime identifier. Each is pinned to a file and a symbol so the exemption
#: cannot drift onto a different statement, and each names the schema the site
#: actually writes into.
@dataclass(frozen=True)
class UnresolvedExemption:
    path: str
    symbol: str
    resolves_to: str
    reason: str


UNRESOLVED_TARGET_EXEMPTIONS: tuple[UnresolvedExemption, ...] = (
    UnresolvedExemption(
        path="src/okg/substrate/derived_artifacts/relations.py",
        symbol="_ensure_relation_registered",
        resolves_to="okg_derived",
        reason=(
            "psycopg composes the identifier: "
            "sql.SQL('CREATE TABLE IF NOT EXISTS {}.{}').format("
            "sql.Identifier(DERIVED_RELATION_SCHEMA), ...), with "
            "DERIVED_RELATION_SCHEMA = 'okg_derived'."
        ),
    ),
    UnresolvedExemption(
        path="src/okg/substrate/attributes.py",
        symbol="_materialize_view_kind",
        resolves_to="okg",
        reason=(
            "the CREATE UNLOGGED TABLE name comes from "
            "_unlogged_stage_table_name(conn, target_kind), which returns "
            "f'okg._eff_stage_{target_kind}_{backend_pid}_{hex}'."
        ),
    ),
    UnresolvedExemption(
        path="src/okg/substrate/attributes.py",
        symbol="prepare_prelock_attribute_hash_cache",
        resolves_to="okg",
        reason=(
            "issue 2941 fix 2: the pre-lock hash stage is written "
            "f'CREATE UNLOGGED TABLE okg.{stage_name} ...' with the schema "
            "literal; only the session-unique name from "
            "_unlogged_stage_table_name is built at runtime."
        ),
    ),
)


@dataclass(frozen=True)
class Finding:
    path: str
    line: int
    relation: str
    message: str


@dataclass
class ScanResult:
    checked_files: int = 0
    statements: int = 0
    violations: list[Finding] = field(default_factory=list)
    exempt: list[Finding] = field(default_factory=list)

    @property
    def ok(self) -> bool:
        return not self.violations

    def as_dict(self) -> dict[str, Any]:
        return {
            "ok": self.ok,
            "checked_files": self.checked_files,
            "statements": self.statements,
            "violations": [
                {
                    "path": f.path,
                    "line": f.line,
                    "relation": f.relation,
                    "message": f.message,
                }
                for f in self.violations
            ],
            "exempt": [
                {"path": f.path, "line": f.line, "relation": f.relation}
                for f in self.exempt
            ],
        }


#: A CREATE TABLE, in each of the three forms Postgres accepts a table name in:
#: an inline column list, `PARTITION OF`, and `CREATE TABLE ... AS <query>`.
#: Every CREATE TABLE in the tree today takes the first form, but a check that
#: only knew that form would silently stop covering the day someone partitions
#: a table -- and silent loss of coverage is the failure mode this whole PACT
#: is about. Requiring one of the three continuations is also what keeps prose
#: like "only CREATE TABLE is supported" from parsing as DDL.
#:
#: Group 1 captures the TEMP/TEMPORARY modifier when present. Such a table is
#: created in the session's `pg_temp` schema by definition, never in `public`,
#: and it is gone at session end -- it is not a relation the namespace contract
#: governs, so those statements are skipped rather than exempted. (Skipping is
#: the right verb: an exemption records a decision to leave something in
#: `public`, and there is nothing here to leave.)
_CREATE_TABLE_RE = re.compile(
    r"""
    CREATE \s+
    (?: (?: UNLOGGED | GLOBAL | LOCAL ) \s+ )*
    ( TEMP | TEMPORARY )? \s*
    TABLE \s+
    (?: IF \s+ NOT \s+ EXISTS \s+ )?
    ( [^\s(;]+ )
    \s*
    (?:
        \(
      | PARTITION \s+ OF \b
      | AS \s+ (?: SELECT | WITH | TABLE | VALUES | EXECUTE | \( )
    )
    """,
    re.IGNORECASE | re.VERBOSE,
)

#: `SELECT ... INTO <table>` is a CTAS synonym that never says CREATE, and the
#: STATIC arm deliberately does not chase it. In PL/pgSQL the identical
#: spelling is variable assignment (`SELECT count(*) INTO n`), which creates
#: nothing; schema.sql is mostly PL/pgSQL, and a text-only detector reported 60
#: assignments as table creations. A check that cries wolf sixty times is worse
#: than no check, because reviewers learn to skip it.
#:
#: The RUNTIME arm covers this form exactly, and without ambiguity: PL/pgSQL
#: assignment fires no `ddl_command_end`, while a real `SELECT INTO` arrives
#: with its own command tag and the concrete relation it created. See
#: `_EVENT_TRIGGER_SQL` in tests/substrate/db/test_namespace_qualification.py.
#: This split is the honest one — it is recorded here so the gap is a decision
#: rather than an oversight.

#: A literal that ENDS at `CREATE TABLE` is a fragment being concatenated with
#: a name computed elsewhere (`"CREATE TABLE " + name(...) + " (...)"`). The
#: regex cannot match it, so without this the scan reports zero statements and
#: a clean result — the check failing OPEN on precisely the construction that
#: hides the target. Prose ("only CREATE TABLE is supported") does not end here.
_DANGLING_CREATE_RE = re.compile(
    r"CREATE \s+ (?: (?: UNLOGGED | GLOBAL | LOCAL | TEMP | TEMPORARY ) \s+ )* "
    r"TABLE (?: \s+ IF \s+ NOT \s+ EXISTS )? \s* $",
    re.IGNORECASE | re.VERBOSE,
)


def _strip_sql_comments(text: str) -> str:
    """Blank out `--` comments, preserving line numbering."""
    out: list[str] = []
    for line in text.splitlines():
        idx = line.find("--")
        out.append(line if idx < 0 else line[:idx])
    return "\n".join(out)


def _unquote(token: str) -> str:
    return token.strip().strip('"').strip("'")


def classify(relation_token: str) -> tuple[str, str]:
    """Return ``(schema, relation)`` for a CREATE TABLE target token.

    An unqualified token has schema ``""`` -- it resolves through
    ``search_path``, which on every OKG connection reaches ``public``.

    A token wrapped entirely in double quotes is ONE identifier no matter what
    it contains: `"okg.shadow"` is a public table whose name happens to hold a
    dot, not a table in `okg`. Splitting on the dot first would read the
    schema off the inside of a quoted name and wave it through.
    """
    token = relation_token.strip().rstrip(";")
    if len(token) >= 2 and token.startswith('"') and token.endswith('"'):
        inner = token[1:-1]
        if '"."' not in inner and '"' not in inner:
            return "", inner
    if "." in token:
        schema, _, relation = token.partition(".")
        return _unquote(schema), _unquote(relation)
    return "", _unquote(token)


def _is_dynamic(token: str) -> bool:
    return "{" in token or "%" in token


def _evaluate(
    path: Path,
    line: int,
    token: str,
    result: ScanResult,
    *,
    symbol: str = "",
) -> None:
    rel_path = _repo_relative(path)
    result.statements += 1
    schema, relation = classify(token)

    if _is_dynamic(token):
        for entry in UNRESOLVED_TARGET_EXEMPTIONS:
            if entry.path == rel_path and (not symbol or entry.symbol in symbol):
                result.exempt.append(
                    Finding(
                        rel_path,
                        line,
                        token,
                        f"unresolved target, registered as {entry.resolves_to}",
                    )
                )
                return
        result.violations.append(
            Finding(
                rel_path,
                line,
                token,
                "CREATE TABLE target is built at runtime and is not in "
                "UNRESOLVED_TARGET_EXEMPTIONS, so the check cannot prove it "
                "lands in okg. Qualify the literal, or register the site.",
            )
        )
        return

    if schema in OWNED_SCHEMAS:
        return

    for entry in PUBLIC_SCHEMA_EXEMPTIONS:
        if entry.matches(relation):
            result.exempt.append(
                Finding(rel_path, line, relation, f"exempt: {entry.owner}")
            )
            return

    where = f"the `{schema}` schema" if schema else "public (via search_path)"
    result.violations.append(
        Finding(
            rel_path,
            line,
            relation,
            f"application table `{relation}` is created in {where}; the "
            "namespace contract requires `okg.` qualification and this "
            "relation is not in PUBLIC_SCHEMA_EXEMPTIONS",
        )
    )


def _repo_relative(path: Path) -> str:
    try:
        return str(path.resolve().relative_to(ROOT))
    except ValueError:
        return str(path)


def _scan_text_body(
    path: Path,
    text: str,
    result: ScanResult,
    *,
    line_of: Any,
    symbol: str = "",
) -> None:
    """Run every detector over one body of SQL text.

    Shared by the SQL and Python scanners so a detector can never be wired
    into one and silently missing from the other.
    """
    for match in _CREATE_TABLE_RE.finditer(text):
        if match.group(1):  # TEMP / TEMPORARY: pg_temp, out of scope
            continue
        _evaluate(
            path, line_of(match), match.group(2), result, symbol=symbol
        )
    if _DANGLING_CREATE_RE.search(text):
        result.statements += 1
        result.violations.append(
            Finding(
                _repo_relative(path),
                line_of(None),
                "<concatenated>",
                "a CREATE TABLE literal ends without naming its target, so "
                "the name is concatenated from elsewhere and the check "
                "cannot prove it lands in okg. Put the qualified name in "
                "the literal, or register the site in "
                "UNRESOLVED_TARGET_EXEMPTIONS.",
            )
        )


def scan_sql_file(path: Path, result: ScanResult) -> None:
    text = _strip_sql_comments(path.read_text(encoding="utf-8"))
    result.checked_files += 1
    _scan_text_body(
        path,
        text,
        result,
        line_of=lambda m: (
            text.count("\n", 0, m.start()) + 1 if m is not None else 1
        ),
    )


def _module_string_constants(tree: ast.Module) -> dict[str, str]:
    """Module-level ``NAME = "literal"`` bindings.

    Qualification frequently arrives through such a constant --
    ``ATTRIBUTE_OBSERVATION_TABLE = "okg.attribute_observations"`` and nine
    siblings -- so a scan that renders every interpolation as opaque would
    report ten false violations and teach reviewers to ignore it.
    """
    constants: dict[str, str] = {}
    for node in tree.body:
        if not isinstance(node, ast.Assign):
            continue
        if not isinstance(node.value, ast.Constant):
            continue
        if not isinstance(node.value.value, str):
            continue
        for target in node.targets:
            if isinstance(target, ast.Name):
                constants[target.id] = node.value.value
    return constants


def _render(node: ast.AST, constants: dict[str, str]) -> str | None:
    """Render a string expression, substituting known module constants."""
    if isinstance(node, ast.Constant):
        return node.value if isinstance(node.value, str) else None
    if isinstance(node, ast.JoinedStr):
        parts: list[str] = []
        for value in node.values:
            if isinstance(value, ast.Constant) and isinstance(value.value, str):
                parts.append(value.value)
            elif isinstance(value, ast.FormattedValue):
                inner = value.value
                if isinstance(inner, ast.Name) and inner.id in constants:
                    parts.append(constants[inner.id])
                else:
                    parts.append("{dynamic}")
            else:
                parts.append("{dynamic}")
        return "".join(parts)
    if isinstance(node, ast.BinOp) and isinstance(node.op, ast.Add):
        left = _render(node.left, constants)
        right = _render(node.right, constants)
        if left is None or right is None:
            return None
        return left + right
    return None


def _enclosing_symbol(tree: ast.Module) -> dict[int, str]:
    """Map every line to the dotted name of its enclosing def/class."""
    mapping: dict[int, str] = {}

    def walk(node: ast.AST, prefix: str) -> None:
        for child in ast.iter_child_nodes(node):
            if isinstance(
                child, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)
            ):
                name = f"{prefix}.{child.name}" if prefix else child.name
                for line in range(
                    child.lineno, (child.end_lineno or child.lineno) + 1
                ):
                    mapping[line] = name
                walk(child, name)
            else:
                walk(child, prefix)

    walk(tree, "")
    return mapping


def scan_python_file(path: Path, result: ScanResult) -> None:
    source = path.read_text(encoding="utf-8")
    result.checked_files += 1
    try:
        tree = ast.parse(source)
    except SyntaxError as exc:
        result.violations.append(
            Finding(
                _repo_relative(path),
                exc.lineno or 1,
                "<unparseable>",
                f"cannot parse for namespace review: {exc}",
            )
        )
        return

    constants = _module_string_constants(tree)
    symbols = _enclosing_symbol(tree)
    docstrings = {
        id(node.body[0].value)
        for node in ast.walk(tree)
        if isinstance(
            node,
            (ast.Module, ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef),
        )
        and node.body
        and isinstance(node.body[0], ast.Expr)
        and isinstance(node.body[0].value, ast.Constant)
        and isinstance(node.body[0].value.value, str)
    }

    # `ast.walk` also yields the literal CHUNKS inside an f-string and inside a
    # successfully-rendered concatenation. Those chunks are fragments by
    # construction -- an f-string's first piece is exactly `"CREATE TABLE IF
    # NOT EXISTS "` -- so scanning them as statements in their own right makes
    # the dangling-literal detector fire on every healthy f-string in the tree.
    # The enclosing node is already scanned WITH its interpolations resolved,
    # so the chunks carry no information the whole does not.
    covered: set[int] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.JoinedStr) or (
            isinstance(node, ast.BinOp)
            and isinstance(node.op, ast.Add)
            and _render(node, constants) is not None
        ):
            for child in ast.walk(node):
                if child is not node:
                    covered.add(id(child))

    for node in ast.walk(tree):
        if not isinstance(node, (ast.Constant, ast.JoinedStr, ast.BinOp)):
            continue
        if id(node) in docstrings or id(node) in covered:
            continue
        text = _render(node, constants)
        if not text:
            continue
        if "CREATE" not in text.upper():
            continue
        line = getattr(node, "lineno", 1)
        _scan_text_body(
            path,
            text,
            result,
            line_of=lambda _m, _line=line: _line,
            symbol=symbols.get(line, ""),
        )


def _iter_targets(root: Path = ROOT) -> list[Path]:
    paths: list[Path] = []
    for pattern in SQL_GLOBS:
        if "*" in pattern:
            paths.extend(sorted(root.glob(pattern)))
        else:
            candidate = root / pattern
            if candidate.is_file():
                paths.append(candidate)
    for tree in PYTHON_ROOTS:
        paths.extend(sorted((root / tree).rglob("*.py")))
    excluded = tuple((root / t).resolve() for t in EXCLUDED_TREES)
    return [
        path
        for path in paths
        if not any(str(path.resolve()).startswith(str(x)) for x in excluded)
    ]


def scan_repo(root: Path = ROOT, paths: list[Path] | None = None) -> ScanResult:
    """Scan the application DDL surface. Importable; all arguments default."""
    result = ScanResult()
    for path in paths if paths is not None else _iter_targets(root):
        if path.suffix == ".sql":
            scan_sql_file(path, result)
        elif path.suffix == ".py":
            scan_python_file(path, result)
    return result


def registry_relation_is_exempt(relation: str) -> bool:
    """Runtime arm's predicate: may this bare relation live in `public`?"""
    return any(entry.matches(relation) for entry in PUBLIC_SCHEMA_EXEMPTIONS)


def _escape_github(value: str) -> str:
    return (
        value.replace("%", "%25")
        .replace("\r", "%0D")
        .replace("\n", "%0A")
        .replace(":", "%3A")
        .replace(",", "%2C")
    )


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "paths",
        nargs="*",
        type=Path,
        help="files to scan (default: the application DDL surface)",
    )
    parser.add_argument("--json", dest="json_output", action="store_true")
    parser.add_argument("--github-annotations", action="store_true")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    explicit = [
        p if p.is_absolute() else ROOT / p for p in args.paths
    ] or None
    try:
        result = scan_repo(paths=explicit)
    except OSError as exc:
        print(f"schema namespace: check failed: {exc}", file=sys.stderr)
        return 2

    if args.json_output:
        print(json.dumps(result.as_dict(), indent=2, sort_keys=True))
    elif result.ok:
        print(
            "schema namespace: ok "
            f"{result.statements} CREATE TABLE statement(s) across "
            f"{result.checked_files} file(s); "
            f"{len(result.exempt)} registered public-schema relation(s)"
        )
    else:
        print("schema namespace: FAIL", file=sys.stderr)
        for finding in result.violations:
            print(
                f"  - {finding.path}:{finding.line}: {finding.message}",
                file=sys.stderr,
            )

    if args.github_annotations:
        for finding in result.violations:
            print(
                f"::error file={_escape_github(finding.path)},"
                f"line={finding.line},"
                f"title={_escape_github('schema-namespace')}::"
                f"{_escape_github(finding.message)}"
            )

    return 0 if result.ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
