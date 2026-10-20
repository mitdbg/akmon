"""Catalog loader for identifier_patterns.yaml + SqlIdentifierExtractor."""
from __future__ import annotations

from pathlib import Path
from textwrap import dedent

import psycopg
import pytest

from okg.substrate.catalog.identifier_patterns import (
    IdentifierPattern,
    list_patterns,
    load_into_catalog,
    parse_yaml,
    write_patterns,
)
from okg.substrate.errors import SchemaValidationFailed
from okg.substrate.projection.extractor import SqlIdentifierExtractor

pytestmark = pytest.mark.postgres


def _new_cv(conn: psycopg.Connection) -> int:
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO okg.catalog_versions (status, applied_at) "
            "VALUES ('applied', now()) RETURNING catalog_version_id"
        )
        return cur.fetchone()[0]


def test_parse_yaml_round_trips(tmp_path: Path) -> None:
    p = tmp_path / "identifier_patterns.yaml"
    # Single-quoted YAML scalar: backslash escapes are LITERAL.
    # `'\\b'` parses as the 2-char string `\b`.
    p.write_text(dedent("""
        patterns:
          - id_type: jira_ticket
            regex: '(CMS|ATLAS)-[0-9]+'
            description: experiment ticket
          - id_type: doi
            regex: '10\\.[0-9]{4,9}/[-._;()/:A-Za-z0-9]+'
            validator_kind: doi_checksum
        """))
    out = parse_yaml(p)
    assert len(out) == 2
    assert out[0] == IdentifierPattern(
        id_type="jira_ticket",
        regex="(CMS|ATLAS)-[0-9]+",
        validator_kind=None,
        description="experiment ticket",
    )
    assert out[1].validator_kind == "doi_checksum"
    assert out[1].regex.startswith("10")


def test_parse_yaml_rejects_bad_regex(tmp_path: Path) -> None:
    p = tmp_path / "identifier_patterns.yaml"
    p.write_text(dedent("""
        patterns:
          - id_type: bad
            regex: '['
        """))
    with pytest.raises(SchemaValidationFailed) as exc:
        parse_yaml(p)
    assert "invalid regex" in str(exc.value)


def test_parse_yaml_rejects_unknown_validator_kind(tmp_path: Path) -> None:
    p = tmp_path / "identifier_patterns.yaml"
    p.write_text(dedent("""
        patterns:
          - id_type: x
            regex: 'foo'
            validator_kind: unknown_thing
        """))
    with pytest.raises(SchemaValidationFailed) as exc:
        parse_yaml(p)
    assert "unknown" in str(exc.value).lower()


def test_parse_yaml_rejects_missing_id_type(tmp_path: Path) -> None:
    p = tmp_path / "identifier_patterns.yaml"
    p.write_text("patterns:\n  - regex: foo\n")
    with pytest.raises(SchemaValidationFailed) as exc:
        parse_yaml(p)
    assert "id_type" in str(exc.value)


def test_parse_yaml_rejects_missing_top_level(tmp_path: Path) -> None:
    p = tmp_path / "identifier_patterns.yaml"
    p.write_text("# nothing here\n")
    with pytest.raises(SchemaValidationFailed) as exc:
        parse_yaml(p)
    assert "patterns" in str(exc.value)


def test_load_into_catalog_round_trips(
    clean_runtime: str, tmp_path: Path,
) -> None:
    p = tmp_path / "identifier_patterns.yaml"
    p.write_text(dedent("""
        patterns:
          - id_type: jira
            regex: 'CMS-[0-9]+'
        """))
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        cv = _new_cv(conn)
        n = load_into_catalog(conn, tmp_path, cv)
        assert n == 1
        rows = list_patterns(conn, cv)
    assert len(rows) == 1
    assert rows[0].id_type == "jira"
    assert rows[0].regex == "CMS-[0-9]+"


def test_load_into_catalog_missing_file_is_no_op(
    clean_runtime: str, tmp_path: Path,
) -> None:
    """Identifier patterns are optional. Absence is not an error."""
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        cv = _new_cv(conn)
        n = load_into_catalog(conn, tmp_path, cv)
    assert n == 0


def test_write_patterns_idempotent(clean_runtime: str) -> None:
    """Re-loading the same patterns under the same catalog
    version is a no-op (no duplicate-key error)."""
    pats = [IdentifierPattern("jira", "CMS-[0-9]+")]
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        cv = _new_cv(conn)
        write_patterns(conn, pats, cv)
        write_patterns(conn, pats, cv)
        rows = list_patterns(conn, cv)
    assert len(rows) == 1


# ---------- SqlIdentifierExtractor ----------------------------------

def test_sql_extractor_returns_extracted_entities(
    clean_runtime: str,
) -> None:
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        cv = _new_cv(conn)
        write_patterns(conn, [
            IdentifierPattern("jira", "CMS-[0-9]+"),
        ], cv)

        extractor = SqlIdentifierExtractor(conn, cv)
        ents = extractor.extract("Filed CMS-1234 and CMS-99")

    assert len(ents) == 2
    values = sorted(e.value for e in ents)
    assert values == ["CMS-1234", "CMS-99"]
    for e in ents:
        assert e.type == "jira"
        assert e.confidence == 0.95
        # Spans recover values verbatim
        text = "Filed CMS-1234 and CMS-99"
        assert text[e.start:e.end] == e.value


def test_sql_extractor_filters_via_validator(clean_runtime: str) -> None:
    """A regex-matching string with a bad ORCID checksum gets
    filtered by the validator. A valid one passes through."""
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        cv = _new_cv(conn)
        write_patterns(conn, [
            IdentifierPattern(
                "orcid",
                "[0-9]{4}-[0-9]{4}-[0-9]{4}-[0-9]{3}[0-9X]",
                validator_kind="orcid_checksum",
            ),
        ], cv)

        extractor = SqlIdentifierExtractor(conn, cv)
        ents = extractor.extract(
            "valid: 0000-0002-1825-0097, invalid: 0000-0002-1825-0098"
        )

    assert len(ents) == 1
    assert ents[0].value == "0000-0002-1825-0097"


def test_sql_extractor_empty_text(clean_runtime: str) -> None:
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        cv = _new_cv(conn)
        extractor = SqlIdentifierExtractor(conn, cv)
        assert extractor.extract("") == []
