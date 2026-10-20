"""Migration-class declaration and the data-motion classifier.

Per `schema-migration-hardening` / req.multiphase-class: the migration
system supports two declared classes in ``migrations/manifest.yaml``:

* ``single_transaction`` (the default when ``class`` is omitted) — the
  ordinary migration shape. It runs inside migrate's one serialized,
  time-bounded transaction and is covered by the single-commit invariant
  (audit F1).
* ``multi_phase`` — data motion too large for one transaction. The
  ordered runner never executes these inline; they run through the
  checkpointed, resumable driver in
  :mod:`okg.substrate.db.multiphase`, which commits per batch and
  records durable progress state.

The classifier here is the manifest check's teeth: an UNDECLARED
multi-statement data migration (two or more top-level data-motion
statements) is refused at apply time with a typed
:class:`~okg.substrate.errors.MigrationClassUndeclared` instead of
silently running batched data motion inside migrate's single
transaction. Data motion = a statement whose leading keyword is
INSERT/UPDATE/DELETE/MERGE, a ``COPY ... FROM``, or a ``WITH``-led
statement containing DML anywhere outside quoted regions (both the
CTE-wrapped backfill idiom ``WITH batch AS (...) UPDATE ...`` and
data-modifying CTEs ``WITH moved AS (DELETE ...) SELECT ...``).

Documented bounds (the classifier is an honesty check on declared
work, not an adversarial sandbox): DML inside function bodies and
``DO`` blocks is NOT counted — a dollar-quoted body is part of one
CREATE FUNCTION / DO statement, and a DO block cannot commit
internally, so it is inherently single-transaction. A single giant
statement is likewise not refused (the threshold is the
multi-statement batched shape), and ``CREATE TABLE ... AS`` is treated
as schema shape plus seed, not data motion over existing rows.
"""

from __future__ import annotations

import re
from typing import Any

from okg.substrate.errors import MigrationClassInvalid

MIGRATION_CLASS_SINGLE = "single_transaction"
# Durable progress state for multi-phase migrations. Lives in schema.sql
# (never runtime-ensured): the multi-phase driver REFUSES when it is
# absent instead of creating it — runtime ensure-DDL is the audit F2
# disease the single-source-ddl gate exists to kill.
MULTIPHASE_PROGRESS_TABLE = "okg.schema_migration_phases"
MIGRATION_CLASS_MULTI_PHASE = "multi_phase"
_KNOWN_CLASSES = frozenset({MIGRATION_CLASS_SINGLE, MIGRATION_CLASS_MULTI_PHASE})

# Top-level statement keywords that move data rather than shape schema.
_DATA_MOTION_KEYWORDS = frozenset({"INSERT", "UPDATE", "DELETE", "MERGE"})

# Refusal threshold: two or more top-level data-motion statements is the
# batched-backfill shape that must declare `class: multi_phase`. A single
# seed INSERT/UPDATE among DDL (catalog seeds, singleton fence rows) is an
# ordinary single-transaction migration.
_DATA_MOTION_REFUSAL_THRESHOLD = 2

_DOLLAR_TAG_RE = re.compile(r"\$[A-Za-z_][A-Za-z_0-9]*\$|\$\$")
_LEADING_WORD_RE = re.compile(r"[A-Za-z_]+")


def declared_migration_class(
    migration_id: str, entry: dict[str, Any]
) -> str:
    """The manifest entry's declared class, validated fail-closed.

    An omitted ``class`` means :data:`MIGRATION_CLASS_SINGLE`. Any other
    unrecognized value raises :class:`MigrationClassInvalid` — never a
    silent default (an unknown class defaulting to single-transaction
    would pull a declared-online migration back inside one transaction).
    A migration cannot be both ``multi_phase`` and ``destructive``: the
    destructive path routes through confirm-gated adapters that own
    their own transaction shape.
    """
    raw = entry.get("class")
    declared = MIGRATION_CLASS_SINGLE if raw is None else str(raw)
    if declared not in _KNOWN_CLASSES:
        raise MigrationClassInvalid(
            f"migration {migration_id!r} declares unrecognized "
            f"class {declared!r}",
            data={"migration_id": migration_id, "declared_class": declared},
            fix_hint=(
                f"use {MIGRATION_CLASS_SINGLE!r} or "
                f"{MIGRATION_CLASS_MULTI_PHASE!r} in the manifest entry"
            ),
        )
    if declared == MIGRATION_CLASS_MULTI_PHASE and bool(
        entry.get("destructive")
    ):
        raise MigrationClassInvalid(
            f"migration {migration_id!r} declares class multi_phase AND "
            "destructive; destructive migrations route through their "
            "reviewed adapters, not the multi-phase driver",
            data={"migration_id": migration_id, "declared_class": declared},
            fix_hint="pick one: multi_phase or destructive",
        )
    return declared


def top_level_statements(sql_text: str) -> list[str]:
    """Split SQL into top-level statements.

    Aware of line comments (``--``), block comments (``/* */``,
    nested), single-quoted strings (with ``''`` escapes), double-quoted
    identifiers, and dollar-quoted bodies (``$$ .. $$``,
    ``$tag$ .. $tag$``) — a semicolon inside any of those does not end a
    statement. This is deliberately a splitter, not a parser: the
    classifier only needs each statement's leading keyword.
    """
    statements: list[str] = []
    buf: list[str] = []
    i = 0
    n = len(sql_text)
    while i < n:
        ch = sql_text[i]
        nxt = sql_text[i + 1] if i + 1 < n else ""
        if ch == "-" and nxt == "-":
            end = sql_text.find("\n", i)
            end = n if end == -1 else end + 1
            buf.append(sql_text[i:end])
            i = end
            continue
        if ch == "/" and nxt == "*":
            depth = 1
            j = i + 2
            while j < n and depth:
                if sql_text[j] == "/" and sql_text[j : j + 2] == "/*":
                    depth += 1
                    j += 2
                elif sql_text[j] == "*" and sql_text[j : j + 2] == "*/":
                    depth -= 1
                    j += 2
                else:
                    j += 1
            buf.append(sql_text[i:j])
            i = j
            continue
        if ch == "'":
            j = i + 1
            while j < n:
                if sql_text[j] == "'":
                    if sql_text[j : j + 2] == "''":
                        j += 2
                        continue
                    j += 1
                    break
                j += 1
            buf.append(sql_text[i:j])
            i = j
            continue
        if ch == '"':
            j = i + 1
            while j < n:
                if sql_text[j] == '"':
                    j += 1
                    break
                j += 1
            buf.append(sql_text[i:j])
            i = j
            continue
        if ch == "$":
            match = _DOLLAR_TAG_RE.match(sql_text, i)
            if match is not None:
                tag = match.group(0)
                close = sql_text.find(tag, match.end())
                j = n if close == -1 else close + len(tag)
                buf.append(sql_text[i:j])
                i = j
                continue
        if ch == ";":
            statement = "".join(buf).strip()
            if statement:
                statements.append(statement)
            buf = []
            i += 1
            continue
        buf.append(ch)
        i += 1
    tail = "".join(buf).strip()
    if tail:
        statements.append(tail)
    return statements


def _statement_keyword(statement: str) -> str | None:
    """The statement's leading keyword, with comments stripped.

    Block-comment stripping is nested-aware, matching the splitter."""
    text = statement
    while True:
        text = text.lstrip()
        if text.startswith("--"):
            newline = text.find("\n")
            if newline == -1:
                return None
            text = text[newline + 1 :]
            continue
        if text.startswith("/*"):
            depth = 1
            j = 2
            n = len(text)
            while j < n and depth:
                if text[j : j + 2] == "/*":
                    depth += 1
                    j += 2
                elif text[j : j + 2] == "*/":
                    depth -= 1
                    j += 2
                else:
                    j += 1
            if depth:
                return None
            text = text[j:]
            continue
        break
    match = _LEADING_WORD_RE.match(text)
    return match.group(0).upper() if match else None


_EMBEDDED_DML_RE = re.compile(
    r"\b(INSERT|UPDATE|DELETE|MERGE)\b", re.IGNORECASE
)


def _mask_quoted_regions(statement: str) -> str:
    """Blank out strings, quoted identifiers, dollar bodies, comments."""
    out: list[str] = []
    i = 0
    n = len(statement)
    while i < n:
        ch = statement[i]
        nxt = statement[i + 1] if i + 1 < n else ""
        if ch == "-" and nxt == "-":
            end = statement.find("\n", i)
            i = n if end == -1 else end + 1
            out.append(" ")
            continue
        if ch == "/" and nxt == "*":
            depth = 1
            j = i + 2
            while j < n and depth:
                if statement[j : j + 2] == "/*":
                    depth += 1
                    j += 2
                elif statement[j : j + 2] == "*/":
                    depth -= 1
                    j += 2
                else:
                    j += 1
            i = j
            out.append(" ")
            continue
        if ch == "'":
            j = i + 1
            while j < n:
                if statement[j] == "'":
                    if statement[j : j + 2] == "''":
                        j += 2
                        continue
                    j += 1
                    break
                j += 1
            i = j
            out.append(" ")
            continue
        if ch == '"':
            j = statement.find('"', i + 1)
            i = n if j == -1 else j + 1
            out.append(" ")
            continue
        if ch == "$":
            match = _DOLLAR_TAG_RE.match(statement, i)
            if match is not None:
                tag = match.group(0)
                close = statement.find(tag, match.end())
                i = n if close == -1 else close + len(tag)
                out.append(" ")
                continue
        out.append(ch)
        i += 1
    return "".join(out)


def _is_data_motion_statement(statement: str) -> bool:
    keyword = _statement_keyword(statement)
    if keyword in _DATA_MOTION_KEYWORDS:
        return True
    if keyword == "COPY":
        # COPY ... FROM loads data; COPY ... TO only reads.
        return (
            re.search(r"\bFROM\b", _mask_quoted_regions(statement), re.I)
            is not None
        )
    if keyword == "WITH":
        # Both the CTE-wrapped backfill (WITH batch AS (...) UPDATE ...)
        # and data-modifying CTEs (WITH gone AS (DELETE ...) SELECT ...)
        # are data motion; a pure WITH ... SELECT is not.
        return (
            _EMBEDDED_DML_RE.search(_mask_quoted_regions(statement))
            is not None
        )
    return False


def classify_data_motion(sql_text: str) -> dict[str, int]:
    """Count top-level statements and the data-motion subset."""
    statements = top_level_statements(sql_text)
    data_motion = sum(
        1
        for statement in statements
        if _is_data_motion_statement(statement)
    )
    return {
        "statement_count": len(statements),
        "data_motion_statements": data_motion,
    }


def is_undeclared_data_migration(sql_text: str) -> dict[str, int] | None:
    """The classification when ``sql_text`` is a multi-statement data
    migration (which must declare ``class: multi_phase``), else None."""
    counts = classify_data_motion(sql_text)
    if counts["data_motion_statements"] >= _DATA_MOTION_REFUSAL_THRESHOLD:
        return counts
    return None
