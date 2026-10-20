"""Tests for ``ConceptAliasesSource``.

Two test groups:

1. **Pure parsing tests** (no DB): exercise
   ``parse_aliases_file`` against valid + malformed
   inputs. Confirms operator-error modes (missing file,
   duplicate canonicals, alias-equals-canonical, alias
   collision across canonicals).
2. **DB integration tests** (``pytest.mark.postgres``):
   load substrate schema + concepts catalog into a
   fresh test DB, stage two canonical Concept rows,
   run ``ConceptAliasesSource``, confirm alias rows
   land in ``okg.aliases`` and missing-canonical
   typos fail loudly.

Per `extend-substrate-concepts-as-universal-hub`.
"""
from __future__ import annotations

from pathlib import Path
from typing import Any

import psycopg
import pytest
import yaml

from okg.substrate.library.sources.concept_aliases import (
    ConceptAliasesNoDsn,
    ConceptAliasesParseError,
    ConceptAliasesPathMissing,
    ConceptAliasesSource,
    parse_aliases_file,
    _apply_aliases,
)


# ---------------------------------------------------------------------------
# Parse tests (no DB)
# ---------------------------------------------------------------------------


def _write_yaml(path: Path, doc: Any) -> None:
    path.write_text(yaml.safe_dump(doc))


def test_parse_accepts_valid_file(tmp_path: Path) -> None:
    p = tmp_path / "aliases.yaml"
    _write_yaml(p, {
        "aliases": [
            {
                "canonical": "bitemporal-data",
                "aliases": ["bitemporal-records", "bitemporal-storage"],
            },
            {
                "canonical": "happens-before-ordering",
                "aliases": ["happens-before"],
            },
        ],
    })
    out = parse_aliases_file(p)
    assert len(out) == 2
    assert out[0]["canonical"] == "bitemporal-data"
    assert set(out[0]["aliases"]) == {
        "bitemporal-records", "bitemporal-storage",
    }


def test_parse_missing_file_raises_loud(tmp_path: Path) -> None:
    p = tmp_path / "nope.yaml"
    with pytest.raises(ConceptAliasesPathMissing) as exc:
        parse_aliases_file(p)
    assert "not found" in str(exc.value)


def test_parse_missing_top_level_aliases_key(tmp_path: Path) -> None:
    p = tmp_path / "aliases.yaml"
    _write_yaml(p, {"foo": "bar"})
    with pytest.raises(ConceptAliasesParseError) as exc:
        parse_aliases_file(p)
    assert "aliases:" in str(exc.value)


def test_parse_rejects_non_list_aliases(tmp_path: Path) -> None:
    p = tmp_path / "aliases.yaml"
    _write_yaml(p, {"aliases": "not-a-list"})
    with pytest.raises(ConceptAliasesParseError):
        parse_aliases_file(p)


def test_parse_rejects_entry_missing_canonical(tmp_path: Path) -> None:
    p = tmp_path / "aliases.yaml"
    _write_yaml(p, {
        "aliases": [
            {"aliases": ["foo", "bar"]},  # missing 'canonical'
        ],
    })
    with pytest.raises(ConceptAliasesParseError) as exc:
        parse_aliases_file(p)
    assert "canonical" in str(exc.value)


def test_parse_rejects_empty_aliases_list(tmp_path: Path) -> None:
    p = tmp_path / "aliases.yaml"
    _write_yaml(p, {
        "aliases": [
            {"canonical": "foo", "aliases": []},
        ],
    })
    with pytest.raises(ConceptAliasesParseError):
        parse_aliases_file(p)


def test_parse_rejects_duplicate_canonical(tmp_path: Path) -> None:
    p = tmp_path / "aliases.yaml"
    _write_yaml(p, {
        "aliases": [
            {"canonical": "foo", "aliases": ["bar"]},
            {"canonical": "foo", "aliases": ["baz"]},
        ],
    })
    with pytest.raises(ConceptAliasesParseError) as exc:
        parse_aliases_file(p)
    assert "duplicate canonical" in str(exc.value)


def test_parse_rejects_alias_equals_canonical(tmp_path: Path) -> None:
    p = tmp_path / "aliases.yaml"
    _write_yaml(p, {
        "aliases": [
            {"canonical": "foo", "aliases": ["foo"]},
        ],
    })
    with pytest.raises(ConceptAliasesParseError) as exc:
        parse_aliases_file(p)
    assert "equals its own canonical" in str(exc.value)


def test_parse_rejects_alias_collision_across_canonicals(
    tmp_path: Path,
) -> None:
    """An alias slug cannot appear under two different
    canonicals — the operator must decide which canonical
    owns it."""
    p = tmp_path / "aliases.yaml"
    _write_yaml(p, {
        "aliases": [
            {"canonical": "foo", "aliases": ["shared-alias"]},
            {"canonical": "bar", "aliases": ["shared-alias"]},
        ],
    })
    with pytest.raises(ConceptAliasesParseError) as exc:
        parse_aliases_file(p)
    msg = str(exc.value)
    assert "shared-alias" in msg
    assert "foo" in msg and "bar" in msg


# ---------------------------------------------------------------------------
# SourceAdapter contract tests
# ---------------------------------------------------------------------------


def test_source_adapter_requires_aliases_path() -> None:
    with pytest.raises(ConceptAliasesPathMissing):
        ConceptAliasesSource(aliases_path="")  # type: ignore[arg-type]


def test_source_adapter_run_without_dsn_fails_loud(
    tmp_path: Path,
) -> None:
    p = tmp_path / "aliases.yaml"
    _write_yaml(p, {
        "aliases": [{"canonical": "foo", "aliases": ["bar"]}],
    })
    source = ConceptAliasesSource(aliases_path=str(p))
    with pytest.raises(ConceptAliasesNoDsn):
        source.run("test-run")


def test_source_adapter_has_name_and_profile() -> None:
    source = ConceptAliasesSource(aliases_path="/tmp/x")
    assert source.name == "concept_aliases"
    # `reference_catalog`: full-snapshot read of an
    # operator-curated file (one of the five substrate
    # source profiles).
    assert source.profile == "reference_catalog"


def test_source_adapter_no_memex_paths_hardcoded() -> None:
    """Per umbrella D11, no memex-specific path appears in
    the source's code."""
    module_path = (
        Path(__file__).resolve().parents[4]
        / "src" / "okg" / "substrate" / "library"
        / "sources" / "concept_aliases.py"
    )
    text = module_path.read_text()
    for forbidden in ("~/.memex", "/Users/", "memex/references"):
        assert forbidden not in text, (
            f"forbidden path appears in concept_aliases.py: "
            f"{forbidden!r}"
        )


# ---------------------------------------------------------------------------
# DB integration test (postgres-marked, skipped without OKG_TEST_DSN)
# ---------------------------------------------------------------------------


pytestmark_postgres = pytest.mark.postgres


def _seed_concept(
    conn: psycopg.Connection, *, node_id: str, name: str,
) -> None:
    """Seed a canonical Concept node into the Chronos graph
    (default branch). `_apply_aliases` resolves the canonical
    via a current-default-branch checkout over okg.graph_nodes;
    for a real deployment the catalog apply step or A2's
    concept_extractor lands it."""
    from okg.substrate.graph_store import ChronosGraphStore
    from okg.substrate.library.sources.base import NodeFact

    store = ChronosGraphStore.from_connection(conn)
    try:
        store.apply_facts(
            [
                NodeFact(
                    node_id, "concept", {"name": name},
                    {"id": node_id}, {"v": 1},
                ),
            ],
            source="default",
        )
    finally:
        store.close()


@pytestmark_postgres
def test_apply_aliases_inserts_rows_into_okg_aliases(
    clean_runtime: str, tmp_path: Path,
) -> None:
    """Apply two canonical-with-aliases entries against a
    test DB. Confirms alias rows land in okg.aliases keyed
    to the canonical Concept's node_id."""
    p = tmp_path / "aliases.yaml"
    _write_yaml(p, {
        "aliases": [
            {
                "canonical": "bitemporal-data",
                "aliases": ["bitemporal-records", "bitemporal-storage"],
            },
        ],
    })
    entries = parse_aliases_file(p)
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        node_id = "concept:bitemporal-data-test-aliases"
        _seed_concept(conn, node_id=node_id, name="bitemporal-data")
        out = _apply_aliases(conn, entries=entries)
        # Idempotent re-runs may report 0 newly inserted +
        # 2 already-present; either way the rows are in
        # the alias table.
        assert (
            out["alias_rows_inserted"]
            + out["alias_rows_already_present"]
        ) == 2
        with conn.cursor() as cur:
            cur.execute(
                "SELECT alias, canonical FROM okg.aliases "
                " WHERE canonical = %s "
                " ORDER BY alias",
                (node_id,),
            )
            rows = cur.fetchall()
        assert ("bitemporal-records", node_id) in rows
        assert ("bitemporal-storage", node_id) in rows


@pytestmark_postgres
def test_apply_aliases_fails_loud_on_missing_canonical(
    clean_runtime: str, tmp_path: Path,
) -> None:
    """Per the no-silent-fallback discipline: if the YAML
    declares an alias for a canonical that has no live
    Concept row, _apply_aliases raises a precise error
    naming the missing canonical."""
    p = tmp_path / "aliases.yaml"
    _write_yaml(p, {
        "aliases": [
            {"canonical": "totally-nonexistent-concept", "aliases": ["x"]},
        ],
    })
    entries = parse_aliases_file(p)
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        with pytest.raises(Exception) as exc:
            _apply_aliases(conn, entries=entries)
        assert "totally-nonexistent-concept" in str(exc.value)
