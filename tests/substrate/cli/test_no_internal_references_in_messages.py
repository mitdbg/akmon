"""Messages okg prints must not cite internal issue or work tracking.

An operator reading an error, a fix hint, ``--help`` text or a generated
file cannot act on "(issue #1288)" or "lane H stage 1": those point at the
project's own tracker and planning, not at anything the reader can use. The
message has to state the fact or the next step itself.

This guard walks every Python file under ``src/okg``, collects the string
constants that are not docstrings, and fails on an internal reference. An
f-string is checked as one text, its literal parts joined around a ``{}``
placeholder for each interpolated value, so ``f"(issue #{n})"`` is caught.
Docstrings and ``#`` comments are not user-facing and are not scanned. SQL
line comments inside a query string (a line that starts with ``-- ``) are
comments to the database, not output, and are stripped before matching.
"""

from __future__ import annotations

import ast
import functools
import re
from dataclasses import dataclass
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[3]
SOURCE_ROOT = REPO_ROOT / "src" / "okg"

#: An issue number, or the placeholder an f-string leaves where one goes.
_NUMBER = r"(?:\d+|\{\})"

#: Internal references a user-facing string must not carry. A bare number
#: needs four or five digits, or a "(#NN)" citation, so that a colour such as
#: "#333" or prose such as "item #42" is not a reference.
INTERNAL_REFERENCE = re.compile(
    r"\b(?i:issue|PR|pull request)\s+#" + _NUMBER
    + r"|\bokg#" + _NUMBER
    + r"|(?<![\w&/#])#\d{4,5}\b"
    + r"|\(#\d{2,5}\)"
    + r"|\blane (?!OR\b|AND\b|IS\b|IN\b)[A-Z]{1,3}\b"
    + r"|\bSprint\b"
)

#: A full-line SQL comment inside a query string.
SQL_LINE_COMMENT = re.compile(r"(?m)^[ \t]*--\s.*$")

#: (path relative to the repository root, matched text) -> reason it is
#: allowed. Keep this small; each entry must say why the string is not a
#: message a user reads.
ALLOWED: dict[tuple[str, str], str] = {
    (
        "src/okg/substrate/library/enrichers/_fact_emitters.py",
        "issue #901",
    ): (
        "_DEDUPE_SCOPE_DOC is appended to the emitter's __doc__; it is a "
        "docstring held in a constant, not output"
    ),
    ("src/okg/bench/ab/corpus.py", "PR #{}"): (
        "the number is runtime data: the corpus pull request being checked, "
        "not a citation of okg's tracker"
    ),
    ("src/okg/substrate/pact/cli.py", "PR #{}"): (
        "the number is runtime data: the user's own pull request that PACT "
        "is checking, not a citation of okg's tracker"
    ),
}


@dataclass(frozen=True)
class Finding:
    path: str
    line: int
    match: str
    text: str


def _docstring_nodes(tree: ast.AST) -> set[int]:
    owners = (ast.Module, ast.ClassDef, ast.FunctionDef, ast.AsyncFunctionDef)
    found: set[int] = set()
    for node in ast.walk(tree):
        if not isinstance(node, owners) or not node.body:
            continue
        first = node.body[0]
        if (
            isinstance(first, ast.Expr)
            and isinstance(first.value, ast.Constant)
            and isinstance(first.value.value, str)
        ):
            found.add(id(first.value))
    return found


def _joined_text(node: ast.JoinedStr) -> str:
    """An f-string's literal parts, with ``{}`` for each interpolation."""
    parts: list[str] = []
    for value in node.values:
        if isinstance(value, ast.Constant) and isinstance(value.value, str):
            parts.append(value.value)
        else:
            parts.append("{}")
    return "".join(parts)


def find_internal_references(source: str, path: str) -> list[Finding]:
    """Return every internal reference in a non-docstring string."""
    tree = ast.parse(source, filename=path)
    skipped = _docstring_nodes(tree)
    texts: list[tuple[int, str]] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.JoinedStr):
            # Its literal parts are checked once, joined, not one by one.
            skipped.update(id(value) for value in node.values)
            texts.append((node.lineno, _joined_text(node)))
    for node in ast.walk(tree):
        if (
            isinstance(node, ast.Constant)
            and isinstance(node.value, str)
            and id(node) not in skipped
        ):
            texts.append((node.lineno, node.value))
    findings: list[Finding] = []
    for line, value in sorted(texts):
        text = SQL_LINE_COMMENT.sub("", value)
        for match in INTERNAL_REFERENCE.finditer(text):
            findings.append(Finding(path, line, match.group(0), value))
    return findings


@functools.cache
def _scan_tree() -> tuple[Finding, ...]:
    """Scan ``src/okg`` once per test session; both tree tests read it."""
    findings: list[Finding] = []
    for file in sorted(SOURCE_ROOT.rglob("*.py")):
        relative = file.relative_to(REPO_ROOT).as_posix()
        findings.extend(
            find_internal_references(file.read_text(encoding="utf-8"), relative)
        )
    return tuple(findings)


def _unallowed(findings: tuple[Finding, ...] | list[Finding]) -> list[Finding]:
    return [f for f in findings if (f.path, f.match) not in ALLOWED]


def test_okg_messages_carry_no_internal_references() -> None:
    offenders = _unallowed(_scan_tree())
    assert not offenders, (
        "user-facing strings cite internal tracking; state the fact or the "
        "next step instead:\n"
        + "\n".join(
            f"  {f.path}:{f.line}: {f.match!r} in {f.text[:120]!r}"
            for f in offenders
        )
    )


def test_every_allowlist_entry_still_matches() -> None:
    """A stale entry would silently allow the next string that matches it."""
    seen = {(f.path, f.match) for f in _scan_tree()}
    stale = sorted(key for key in ALLOWED if key not in seen)
    assert not stale, f"allowlist entries that match nothing: {stale}"


SEEDED = '''
"""Module docstring citing issue #1234 is fine."""

QUERY = """
    SELECT 1
    -- okg#77: a SQL comment is fine
"""


def f():
    """Function docstring naming PR #55 is fine."""
    raise ValueError("the import refused (issue #1288)")


HELP = "build the indexes (okg#3112)"
HINT = f"rerun it; lane H stage 1 needs {HELP}"
NOTE = "planned for Sprint 22"
CITED = "never auto-picks a survivor (#710)"
BARE = "see #2013 for details"
FSTRING = f"refused (issue #{NUMBER})"
FSTRING_SPLIT = f"refused before {STEP} (issue #1971)"
FLAG = "use --apply to write; a wide lane OR a slow database"
COLOR = "fill: #333; stroke: #555"
ITEM = f"item #42 of {TOTAL}, step #7"
'''

SEEDED_EXPECTED = {
    "issue #1288", "okg#3112", "lane H", "Sprint", "(#710)", "#2013",
    "issue #{}", "issue #1971",
}


@pytest.mark.parametrize("expected", sorted(SEEDED_EXPECTED))
def test_seeded_internal_reference_is_found(expected: str) -> None:
    matches = {f.match for f in find_internal_references(SEEDED, "seeded.py")}
    assert expected in matches, matches


def test_seeded_docstrings_sql_comments_and_prose_are_not_flagged() -> None:
    matches = {f.match for f in find_internal_references(SEEDED, "seeded.py")}
    assert matches == SEEDED_EXPECTED


def test_seeded_reference_fails_the_tree_guard() -> None:
    """The guard's own filter refuses a finding that is not allowlisted."""
    findings = find_internal_references(SEEDED, "src/okg/seeded.py")
    assert findings
    assert _unallowed(findings) == findings
