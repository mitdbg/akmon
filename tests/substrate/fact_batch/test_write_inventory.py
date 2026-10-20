from __future__ import annotations

import json
from pathlib import Path

import pytest

from okg.substrate.write_inventory import discover_write_callsites


ROOT = Path(__file__).resolve().parents[3]
#: The persisted inventory of every graph-write call site under src/okg/,
#: first recorded by PACT unify-producers-and-fact-batches.
INVENTORY = ROOT / "tests/fixtures/fact_batch/write_inventory.yaml"


def test_persisted_inventory_matches_every_live_callsite_exactly() -> None:
    document = json.loads(INVENTORY.read_text(encoding="utf-8"))
    persisted = document["entries"]
    discovered = discover_write_callsites(ROOT)

    assert document["schema_version"] == 1
    assert document["discovery_snapshot_count"] == len(persisted)
    assert {entry["fingerprint"] for entry in persisted} == {
        callsite.fingerprint for callsite in discovered
    }
    assert len(persisted) == len({entry["fingerprint"] for entry in persisted})
    for entry in persisted:
        assert entry["file"].startswith("src/okg/")
        assert entry["symbol"]
        assert entry["operation"]
        assert entry["target"]
        assert entry["authority_class"]
        assert entry["owner"]
        assert entry["exit_condition"]


def test_new_callsite_inside_an_inventory_file_is_not_allowlisted(
    tmp_path: Path,
) -> None:
    source_root = tmp_path / "src" / "okg" / "substrate"
    source_root.mkdir(parents=True)
    path = source_root / "classified.py"
    path.write_text(
        "def existing(store, facts):\n    store.apply_facts(facts)\n",
        encoding="utf-8",
    )
    before = discover_write_callsites(tmp_path)
    path.write_text(
        "def existing(store, facts):\n"
        "    store.apply_facts(facts)\n"
        "\n"
        "def seeded_bypass(store, facts):\n"
        "    store.apply_facts(facts)\n",
        encoding="utf-8",
    )

    after = discover_write_callsites(tmp_path)

    assert len(before) == 1
    assert len(after) == 2
    assert {item.fingerprint for item in after} - {item.fingerprint for item in before}


@pytest.mark.parametrize(
    "statement",
    [
        "INSERT INTO okg.node_facts VALUES (1)",
        "COPY okg.edge_facts FROM STDIN",
        "UPDATE okg.nodes_live SET attrs = '{}'",
        "DELETE FROM okg.graph_edges WHERE true",
        "TRUNCATE TABLE okg.edges_live",
        "MERGE INTO okg.graph_nodes USING incoming ON true",
    ],
)
def test_every_mutation_verb_is_discovered(
    tmp_path: Path,
    statement: str,
) -> None:
    source_root = tmp_path / "src" / "okg"
    source_root.mkdir(parents=True)
    (source_root / "bypass.py").write_text(
        f"def bypass(cur):\n    cur.execute({statement!r})\n",
        encoding="utf-8",
    )

    discovered = discover_write_callsites(tmp_path)

    assert len(discovered) == 1


def test_dynamic_sql_and_generation_publication_are_discovered(
    tmp_path: Path,
) -> None:
    source_root = tmp_path / "src" / "okg"
    source_root.mkdir(parents=True)
    (source_root / "dynamic.py").write_text(
        "def dynamic(cur, table, store):\n"
        "    sql = f'UPDATE {table} SET status = 1'\n"
        "    cur.execute(sql)\n"
        "    store.checkpoint_generation(7)\n",
        encoding="utf-8",
    )

    discovered = discover_write_callsites(tmp_path)

    assert {(item.operation, item.target) for item in discovered} == {
        ("UPDATE", "{dynamic}"),
        ("CHECKPOINT", "graph_generations"),
    }


def test_current_view_materialization_and_multi_target_truncate_are_discovered(
    tmp_path: Path,
) -> None:
    source_root = tmp_path / "src" / "okg"
    source_root.mkdir(parents=True)
    (source_root / "publisher.py").write_text(
        "def materialize(cur):\n"
        "    cur.execute('TRUNCATE okg.chronos_graph_edges_current, ' \n"
        "                'okg.chronos_graph_nodes_current')\n"
        "    cur.executemany(\n"
        "        'INSERT INTO okg.chronos_graph_nodes_current VALUES (%s)', [])\n"
        "    cur.executemany(\n"
        "        'INSERT INTO okg.chronos_graph_edges_current VALUES (%s)', [])\n",
        encoding="utf-8",
    )

    discovered = discover_write_callsites(tmp_path)

    assert {(item.operation, item.target) for item in discovered} == {
        ("TRUNCATE", "chronos_graph_edges_current"),
        ("TRUNCATE", "chronos_graph_nodes_current"),
        ("INSERT INTO", "chronos_graph_nodes_current"),
        ("INSERT INTO", "chronos_graph_edges_current"),
    }


def test_format_and_psycopg_composed_mutations_are_discovered(
    tmp_path: Path,
) -> None:
    source_root = tmp_path / "src" / "okg"
    source_root.mkdir(parents=True)
    (source_root / "composed.py").write_text(
        "from psycopg import sql\n"
        "def composed(cur, table):\n"
        "    cur.execute('UPDATE {} SET status = 1'.format(table))\n"
        "    cur.execute(sql.SQL('DELETE FROM {} WHERE true').format(\n"
        "        sql.Identifier(table)))\n",
        encoding="utf-8",
    )

    discovered = discover_write_callsites(tmp_path)

    assert {(item.operation, item.target) for item in discovered} == {
        ("UPDATE", "{dynamic}"),
        ("DELETE FROM", "{dynamic}"),
    }
