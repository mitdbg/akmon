"""Catalog loader for `okg.identifier_patterns`.

A deployment ships an `identifier_patterns.yaml` next to its
`schemas/` and `aliases.json`. This module reads it, validates
each entry (regex compiles; `validator_kind` is in the
recognized set), and writes rows under a given catalog version.

Schema:

    patterns:
      - id_type: jira_ticket
        regex: '\\b(CMS|ALICE|ATLAS)-\\d+\\b'
        description: experiment JIRA ticket reference
      - id_type: doi
        regex: '\\b10\\.\\d{4,9}/[-._;()/:A-Za-z0-9]+\\b'
        validator_kind: doi_checksum

Bitemporal: rows live under `catalog_version_id` like everything
else in the catalog layer; old generations see the old pattern
set.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

import psycopg
from okg.substrate import fastyaml

from okg.substrate.errors import SchemaValidationFailed


VALID_VALIDATOR_KINDS = frozenset({
    "doi_checksum",
    "orcid_checksum",
    "isbn_checksum",
})


@dataclass(frozen=True)
class IdentifierPattern:
    """One regex pattern in the catalog."""

    id_type: str
    regex: str
    validator_kind: str | None = None
    description: str | None = None


def parse_yaml(path: Path) -> list[IdentifierPattern]:
    """Parse `identifier_patterns.yaml` into validated dataclasses.

    Raises `SchemaValidationFailed` on any of:
      - missing top-level `patterns:` list
      - missing required field on an entry
      - regex that doesn't compile
      - unknown `validator_kind`
    """
    if not path.is_file():
        raise SchemaValidationFailed(
            f"identifier_patterns.yaml not found: {path}",
            data={"path": str(path)},
            fix_hint="create the file or remove the reference from deployment.yaml",
        )

    with path.open() as f:
        doc = fastyaml.safe_load(f) or {}

    if "patterns" not in doc:
        raise SchemaValidationFailed(
            f"{path}: missing top-level 'patterns:' key",
            data={"path": str(path)},
            fix_hint="wrap entries under a top-level `patterns:` list",
        )
    raw = doc["patterns"]
    if not isinstance(raw, list):
        raise SchemaValidationFailed(
            f"{path}: 'patterns' must be a list",
            data={"path": str(path)},
        )

    out: list[IdentifierPattern] = []
    for i, entry in enumerate(raw):
        if not isinstance(entry, dict):
            raise SchemaValidationFailed(
                f"{path}: pattern[{i}] must be a mapping",
                data={"path": str(path), "index": i},
            )
        for k in ("id_type", "regex"):
            if k not in entry:
                raise SchemaValidationFailed(
                    f"{path}: pattern[{i}] missing '{k}'",
                    data={"path": str(path), "index": i, "missing": k},
                )
        id_type = str(entry["id_type"])
        regex = str(entry["regex"])
        validator_kind = entry.get("validator_kind")
        description = entry.get("description")

        try:
            re.compile(regex)
        except re.error as exc:
            raise SchemaValidationFailed(
                f"{path}: pattern[{i}] '{id_type}' has invalid regex: {exc}",
                data={
                    "path": str(path), "index": i,
                    "id_type": id_type, "regex": regex,
                    "regex_error": str(exc),
                },
                fix_hint="fix the regex or quote it differently",
            )

        if validator_kind is not None and validator_kind not in VALID_VALIDATOR_KINDS:
            raise SchemaValidationFailed(
                f"{path}: pattern[{i}] '{id_type}' has unknown "
                f"validator_kind {validator_kind!r}",
                data={
                    "path": str(path), "index": i,
                    "id_type": id_type, "validator_kind": validator_kind,
                    "valid_kinds": sorted(VALID_VALIDATOR_KINDS),
                },
                fix_hint=(
                    "use one of "
                    f"{sorted(VALID_VALIDATOR_KINDS)} or omit the field"
                ),
            )

        out.append(IdentifierPattern(
            id_type=id_type,
            regex=regex,
            validator_kind=validator_kind,
            description=description,
        ))

    return out


def write_patterns(
    conn: psycopg.Connection,
    patterns: Iterable[IdentifierPattern],
    catalog_version_id: int,
) -> int:
    """Insert pattern rows under the given catalog version.

    Idempotent under a single (id_type, regex, catalog_version_id)
    primary key. Returns the count of distinct rows inserted (rows
    that conflicted are not counted).
    """
    rows = [
        (p.id_type, p.regex, p.validator_kind, p.description, catalog_version_id)
        for p in patterns
    ]
    if not rows:
        return 0
    with conn.cursor() as cur:
        cur.executemany(
            """
            INSERT INTO okg.identifier_patterns
              (id_type, regex, validator_kind, description, catalog_version_id)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (id_type, regex, catalog_version_id) DO NOTHING
            """,
            rows,
        )
        # rowcount on executemany is implementation-dependent; for
        # the count we re-query.
        cur.execute(
            "SELECT count(*) FROM okg.identifier_patterns "
            "WHERE catalog_version_id = %s",
            (catalog_version_id,),
        )
        return cur.fetchone()[0]


def load_into_catalog(
    conn: psycopg.Connection,
    deployment_dir: Path,
    catalog_version_id: int,
    *,
    filename: str = "identifier_patterns.yaml",
) -> int:
    """Convenience: read `<deployment_dir>/<filename>` and write
    its patterns under the given catalog version. Returns the
    row count.

    Returns 0 (no error) if the file is absent — identifier
    patterns are an optional catalog feature.
    """
    path = deployment_dir / filename
    if not path.is_file():
        return 0
    patterns = parse_yaml(path)
    return write_patterns(conn, patterns, catalog_version_id)


def list_patterns(
    conn: psycopg.Connection, catalog_version_id: int,
) -> list[IdentifierPattern]:
    """Read back patterns at the given catalog version. Mostly
    useful for tests + diagnostics."""
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT id_type, regex, validator_kind, description
              FROM okg.identifier_patterns
             WHERE catalog_version_id = %s
             ORDER BY id_type, regex
            """,
            (catalog_version_id,),
        )
        return [
            IdentifierPattern(
                id_type=r[0], regex=r[1],
                validator_kind=r[2], description=r[3],
            )
            for r in cur.fetchall()
        ]
