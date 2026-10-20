"""The `reads_safe_behind` declaration and the SQL allowed to carry it.

PACT unknown-s7-release-verdict. A migration declares
`reads_safe_behind: true` in migrations/manifest.yaml when code reading a
database without it returns the same answers. The release verdict then
treats a database behind it as `read_only` instead of `refuse`.

The loader checks the declaration against an ALLOW-list: after comments are
stripped, every statement must be CREATE TABLE, CREATE INDEX [CONCURRENTLY]
[IF NOT EXISTS], ALTER TABLE ... ADD COLUMN, COMMENT ON, GRANT, SET, BEGIN
or COMMIT. Anything else -- a backfill, a redefined view, a drop, a DO
block -- could let reads behind it return old answers with no error, so the
declaration is refused.
"""
from __future__ import annotations

import pytest

from okg.substrate.db import management
from okg.substrate.db.management import (
    current_known_migrations,
    reads_safe_behind_refusal,
)
from okg.substrate.errors import SubstrateError

_CLEAN = "CREATE TABLE okg.widgets (id bigint PRIMARY KEY);\n"


@pytest.mark.parametrize(
    "sql",
    [
        "DELETE FROM okg.nodes WHERE kind = 'x';",
        "TRUNCATE okg.nodes;",
        "MERGE INTO okg.a USING okg.b ON a.id = b.id WHEN MATCHED THEN DELETE;",
        "INSERT INTO okg.nodes (id) VALUES (1);",
        "CREATE OR REPLACE FUNCTION okg.f() RETURNS int LANGUAGE sql AS $$ SELECT 1 $$;",
        "ALTER TABLE okg.nodes ALTER COLUMN kind TYPE text;",
        "CREATE POLICY p ON okg.nodes USING (true);",
        "DO $$ BEGIN PERFORM 1; END $$;",
        "UPDATE okg.nodes SET kind = 'x';",
        "CREATE OR REPLACE VIEW okg.v AS SELECT 1;",
        "DROP TABLE okg.old;",
        "DROP INDEX IF EXISTS okg.idx;",
        "ALTER TABLE okg.nodes DROP COLUMN legacy;",
        # One allowed clause does not carry a forbidden one.
        "ALTER TABLE okg.nodes ADD COLUMN a int, ALTER COLUMN b TYPE bigint;",
        "ALTER TABLE okg.nodes ADD CONSTRAINT c CHECK (a > 0);",
        "CREATE VIEW okg.new_view AS SELECT 1;",
        # A forbidden statement after an allowed one is still found.
        _CLEAN + "UPDATE okg.nodes SET kind = 'x';",
    ],
)
def test_statements_off_the_allow_list_are_refused(sql: str) -> None:
    assert reads_safe_behind_refusal(sql), sql


@pytest.mark.parametrize(
    "sql",
    [
        _CLEAN,
        "CREATE INDEX idx ON okg.nodes (kind);",
        "CREATE INDEX CONCURRENTLY idx ON okg.nodes (kind);",
        "CREATE INDEX CONCURRENTLY IF NOT EXISTS idx ON okg.nodes (kind);",
        "ALTER TABLE okg.nodes ADD COLUMN note text;",
        'ALTER TABLE "okg"."nodes" ADD COLUMN a numeric(10, 2), '
        + "ADD COLUMN b text DEFAULT 'x, y';",
        "COMMENT ON TABLE okg.widgets IS 'drop me? no: UPDATE never';",
        "GRANT SELECT ON okg.widgets TO app_ro;",
        "SET lock_timeout = '5s';",
        "BEGIN; CREATE TABLE okg.t (id int); COMMIT;",
        # Comments are stripped before the check, so their words do not count.
        "-- DROP nothing, UPDATE nothing\n/* CREATE OR REPLACE VIEW */\n" + _CLEAN,
    ],
)
def test_allowed_forms_may_declare(sql: str) -> None:
    assert reads_safe_behind_refusal(sql) == []


def test_declaration_on_refused_sql_is_refused_by_name() -> None:
    with pytest.raises(SubstrateError) as caught:
        management._declared_reads_safe_behind(
            "0999_probe", {"reads_safe_behind": True}, "TRUNCATE okg.nodes;"
        )
    assert caught.value.data["migration_id"] == "0999_probe"
    assert caught.value.data["forbidden"] == ["TRUNCATE okg.nodes"]


def test_a_non_boolean_declaration_is_refused_not_coerced() -> None:
    with pytest.raises(SubstrateError) as caught:
        management._declared_reads_safe_behind(
            "0999_probe", {"reads_safe_behind": "yes"}, _CLEAN
        )
    assert caught.value.data["reads_safe_behind"] == "yes"


def test_absent_or_false_declaration_is_false() -> None:
    assert management._declared_reads_safe_behind("m", None, _CLEAN) is False
    assert management._declared_reads_safe_behind("m", {}, _CLEAN) is False
    assert (
        management._declared_reads_safe_behind(
            "m", {"reads_safe_behind": False}, "DROP TABLE x;"
        )
        is False
    )
    assert (
        management._declared_reads_safe_behind(
            "m", {"reads_safe_behind": True}, _CLEAN
        )
        is True
    )


def _manifest_declaring(migration_id: str):
    real = management.load_migrations_manifest()

    def _load():
        entries = {key: dict(value) for key, value in real.items()}
        entries[migration_id]["reads_safe_behind"] = True
        return entries

    return _load


def test_loader_refuses_a_declaration_on_a_real_backfill(monkeypatch) -> None:
    """0004 backfills inside a DO block; declaring it must stop the load."""
    monkeypatch.setattr(
        management,
        "load_migrations_manifest",
        _manifest_declaring("0004_effective_view_raw_attrs_nullable"),
    )
    with pytest.raises(SubstrateError) as caught:
        current_known_migrations()
    assert caught.value.data["migration_id"] == (
        "0004_effective_view_raw_attrs_nullable"
    )
    assert caught.value.data["forbidden"] == ["DO"]


def test_loader_carries_a_valid_declaration_into_status_rows(monkeypatch) -> None:
    monkeypatch.setattr(
        management,
        "load_migrations_manifest",
        _manifest_declaring("0028_enricher_input_quarantine"),
    )
    rows = {m.migration_id: m for m in current_known_migrations()}
    declared = rows["0028_enricher_input_quarantine"]
    assert declared.reads_safe_behind is True
    assert declared.to_dict()["reads_safe_behind"] is True
    assert rows["0004_effective_view_raw_attrs_nullable"].reads_safe_behind is False


def test_the_shipped_manifest_loads() -> None:
    """Loading checks every declaration the real manifest makes."""
    assert current_known_migrations()
