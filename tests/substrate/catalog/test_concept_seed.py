"""Coverage tests for `okg.substrate.catalog.concept_seed`.

The pure parsing / slug / has_seed paths cover most of the file.
`upsert_concept_nodes` is exercised against `clean_runtime`.
"""
from __future__ import annotations

from pathlib import Path

import psycopg
import pytest

from okg.substrate.catalog.concept_seed import (
    _slug,
    has_seed,
    load_seed_path,
    parse_seed,
    upsert_concept_nodes,
)
from okg.substrate.errors import SchemaValidationFailed

pytestmark = pytest.mark.postgres


# ---------- _slug ----------


def test_slug_lowercases_and_collapses() -> None:
    assert _slug("Bad Weather") == "bad_weather"
    assert _slug("Crew-Rest") == "crew_rest"
    assert _slug("CHAPLAIN-services") == "chaplain_services"


def test_slug_drops_punctuation() -> None:
    assert _slug("Bad, Weather!") == "bad_weather"
    assert _slug("a___b") == "a_b"


def test_slug_rejects_empty_result() -> None:
    with pytest.raises(SchemaValidationFailed, match="empty slug"):
        _slug("!!!")
    with pytest.raises(SchemaValidationFailed):
        _slug("")


# ---------- load_seed_path / has_seed ----------


def test_has_seed_returns_false_when_missing(tmp_path: Path) -> None:
    assert has_seed(tmp_path) is False
    assert load_seed_path(tmp_path) == tmp_path / "concepts" / "seed.yaml"


def test_has_seed_returns_true_when_present(tmp_path: Path) -> None:
    (tmp_path / "concepts").mkdir()
    (tmp_path / "concepts" / "seed.yaml").write_text("concepts: []\n")
    assert has_seed(tmp_path) is True


# ---------- parse_seed ----------


def _write_seed(tmp_path: Path, content: str) -> Path:
    (tmp_path / "concepts").mkdir(exist_ok=True)
    (tmp_path / "concepts" / "seed.yaml").write_text(content)
    return tmp_path


def test_parse_seed_minimal_valid(tmp_path: Path) -> None:
    _write_seed(tmp_path,
        "concepts:\n"
        "  - name: Bad Weather\n"
        "    description: Adverse meteorological conditions affecting flight operations.\n"
    )
    seeds = parse_seed(tmp_path)
    assert len(seeds) == 1
    assert seeds[0]["node_id"] == "concept:bad_weather"
    assert seeds[0]["name"] == "bad weather"


def test_parse_seed_multiple_entries(tmp_path: Path) -> None:
    _write_seed(tmp_path,
        "concepts:\n"
        "  - name: Bad Weather\n"
        "    description: Adverse meteorological conditions.\n"
        "  - name: Crew Rest\n"
        "    description: Required downtime for aircrew between missions.\n"
    )
    seeds = parse_seed(tmp_path)
    assert len(seeds) == 2
    assert {s["node_id"] for s in seeds} == {
        "concept:bad_weather", "concept:crew_rest",
    }


def test_parse_seed_missing_file_raises(tmp_path: Path) -> None:
    with pytest.raises(SchemaValidationFailed, match="not found"):
        parse_seed(tmp_path)


def test_parse_seed_missing_top_level_key(tmp_path: Path) -> None:
    _write_seed(tmp_path, "not_concepts: []\n")
    with pytest.raises(SchemaValidationFailed, match="missing top-level"):
        parse_seed(tmp_path)


def test_parse_seed_concepts_not_list(tmp_path: Path) -> None:
    _write_seed(tmp_path, "concepts: not-a-list\n")
    with pytest.raises(SchemaValidationFailed, match="must be a list"):
        parse_seed(tmp_path)


def test_parse_seed_entry_not_mapping(tmp_path: Path) -> None:
    _write_seed(tmp_path,
        "concepts:\n"
        "  - just-a-string\n"
    )
    with pytest.raises(SchemaValidationFailed, match="must be a mapping"):
        parse_seed(tmp_path)


def test_parse_seed_missing_name(tmp_path: Path) -> None:
    _write_seed(tmp_path,
        "concepts:\n"
        "  - description: a description\n"
    )
    with pytest.raises(SchemaValidationFailed, match="missing or empty 'name'"):
        parse_seed(tmp_path)


def test_parse_seed_missing_description(tmp_path: Path) -> None:
    _write_seed(tmp_path,
        "concepts:\n"
        "  - name: Foo\n"
    )
    with pytest.raises(SchemaValidationFailed, match="missing or empty 'description'"):
        parse_seed(tmp_path)


def test_parse_seed_duplicate_name(tmp_path: Path) -> None:
    _write_seed(tmp_path,
        "concepts:\n"
        "  - name: Bad Weather\n"
        "    description: First.\n"
        "  - name: Bad Weather\n"
        "    description: Second.\n"
    )
    with pytest.raises(SchemaValidationFailed, match="duplicate"):
        parse_seed(tmp_path)


def test_parse_seed_empty_list_raises(tmp_path: Path) -> None:
    _write_seed(tmp_path, "concepts: []\n")
    with pytest.raises(SchemaValidationFailed, match="empty list"):
        parse_seed(tmp_path)


# ---------- upsert_concept_nodes ----------


def test_upsert_concept_nodes_inserts_rows(clean_runtime: str) -> None:
    seeds = [
        {
            "node_id": "concept:bad_weather",
            "name": "bad weather",
            "description": "Adverse meteorological conditions.",
        },
        {
            "node_id": "concept:crew_rest",
            "name": "crew rest",
            "description": "Required downtime for aircrew.",
        },
    ]
    with psycopg.connect(clean_runtime, autocommit=True) as conn, \
         conn.cursor() as cur:
        cur.execute(
            "INSERT INTO okg.catalog_versions (status, applied_at) "
            "VALUES ('applied', now()) RETURNING catalog_version_id"
        )
        cv = cur.fetchone()[0]

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        n = upsert_concept_nodes(
            conn, catalog_version_id=cv, seeds=seeds,
        )
    assert n == 2

    from okg.substrate.graph_store import ChronosGraphStore

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        graph = ChronosGraphStore.from_connection(
            conn, ensure_schema=False
        ).checkout()
        rows = graph.query(
            """
            SELECT node_id, attrs->>'name' AS name
              FROM okg.graph_nodes
             WHERE subtype = 'concept'
             ORDER BY node_id
            """
        )
    assert [r["node_id"] for r in rows] == [
        "concept:bad_weather", "concept:crew_rest",
    ]
    assert rows[0]["name"] == "bad weather"


def test_upsert_concept_nodes_idempotent(clean_runtime: str) -> None:
    """Running upsert twice with the same seeds is a no-op."""
    seeds = [{
        "node_id": "concept:x",
        "name": "x",
        "description": "the x concept",
    }]
    with psycopg.connect(clean_runtime, autocommit=True) as conn, \
         conn.cursor() as cur:
        cur.execute(
            "INSERT INTO okg.catalog_versions (status, applied_at) "
            "VALUES ('applied', now()) RETURNING catalog_version_id"
        )
        cv = cur.fetchone()[0]

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        n1 = upsert_concept_nodes(
            conn, catalog_version_id=cv, seeds=seeds,
        )
        n2 = upsert_concept_nodes(
            conn, catalog_version_id=cv, seeds=seeds,
        )
    # First insert returns ≥1; second is a no-op (≤1).
    assert n1 >= 1
    assert n2 <= 1
    # Either way, exactly one node exists in the graph for the concept.
    from okg.substrate.graph_store import ChronosGraphStore

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        graph = ChronosGraphStore.from_connection(
            conn, ensure_schema=False
        ).checkout()
        rows = graph.query(
            """
            SELECT count(*) AS n
              FROM okg.graph_nodes
             WHERE node_id = 'concept:x'
            """
        )
        assert int(rows[0]["n"]) == 1


# `test_upsert_concept_nodes_rejects_pseudo_future_overlap` was removed
# by the chronos cutover: it asserted the legacy bitemporal
# exclusion-constraint behavior (a future `valid_from_gen` pseudo row
# blocking a current open row) on okg.nodes_live. The Chronos graph has
# no per-generation visibility windows — `apply_facts` upserts by
# node_id with last-write-wins — so the obsolete overlap rule is gone.
