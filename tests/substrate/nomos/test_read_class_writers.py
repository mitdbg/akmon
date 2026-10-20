"""PACT lane-h-nomos-read-class-stored, design test T6: the writer inventory.

Every call under ``src/okg`` that can write a graph row version
(``upsert_rows`` / ``upsert_row`` / ``apply_table_edits`` on a Chronos session
or backend, or an SQL ``UPDATE`` / ``INSERT INTO`` on ``okg.graph_nodes`` /
``okg.graph_edges``) is pinned below with how it keeps the stored Nomos read
class equal to the row's content. A new writer fails this test until it is
classified, so it cannot silently write rows without a class -- or, for an
SQL UPDATE, carry the previous version's class forward (a Chronos UPDATE keeps
every column it does not SET).
"""
from __future__ import annotations

import re
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[3]
SRC = REPO / "src" / "okg"

_WRITE_CALL = re.compile(r"\b(upsert_rows|upsert_row|apply_table_edits)\(")
_GRAPH_SQL = re.compile(r"\b(UPDATE|INSERT\s+INTO)\s+okg\.graph_(nodes|edges)\b", re.I)

#: How each writer keeps the class right. ``stamp``: calls ``stamp_row`` /
#: ``stamp_rows`` on the rows it writes. ``copy``: writes whole rows it read
#: from the same tables (every registered column, the class included), so the
#: class travels with its content. ``set``: an SQL UPDATE that SETs the class
#: of its new content. ``other_table``: writes a table that is not a graph
#: table. ``deletes``: tombstones only (``upserts=[]``).
EXPECTED: dict[str, dict[str, int]] = {
    "substrate/graph_store_internal/writes.py": {
        # fact apply (every ingest, enricher and publish write), after the
        # publisher-derived lineage stamp and the declarer handoffs; ranked
        # edges (item 92a) join edge_upserts before the stamp (2 + 2). Item
        # 92a also writes winner rows on a declaration withdrawal
        # (withdraw_declarations) and when settling (_settle_ranked_edges),
        # each stamped first (2 + 2).
        "apply_table_edits:stamp": 4, "upsert_rows:stamp": 4,
        # edge/node tombstones, scoped deletes, and the edge-declarer
        # withdrawals and purges (deletes=..., upserts=[])
        "apply_table_edits:deletes": 6,
        # node and edge declarer tables (okg.graph_node_declarers,
        # okg.graph_edge_declarers): declarations, not graph row versions
        "apply_table_edits:other_table": 3, "upsert_rows:other_table": 3,
    },
    "substrate/runtime/embed_workflow.py": {"upsert_rows:stamp": 1},
    "substrate/nomos/source_lineage_repair.py": {
        "upsert_rows:stamp": 1, "UPDATE okg.graph_nodes:set": 1, "UPDATE okg.graph_edges:set": 1,
    },
    # branch restore: checkpoint rows read with _rows_by_key_for_keys; and
    # (item 92a) the edge-declarer table in reapply_edge_declarations (a
    # merge-back rollback), whose graph rows _settle_ranked_edges in
    # writes.py then writes (stamped)
    "substrate/graph_store_internal/generations.py": {
        "apply_table_edits:copy": 1, "apply_table_edits:other_table": 1,
    },
    # run undo: stored base or head rows read with _rows_by_key_for_keys
    # (whole rows), and since item 92a the declarer table in the same call.
    # Not pinned here: branch merge (read_branch.py) writes through
    # chronos_core's _apply_merge_changes, outside src/okg, copying whole
    # Chronos diff rows.
    "substrate/ingest/compensation.py": {"apply_table_edits:copy": 1},
    # merge-back quarantine: RowDiff.after rows from the Chronos diff
    "substrate/merge_back.py": {"upsert_rows:copy": 1},
    # maintained derivations write their own storage tables, not graph rows
    "substrate/derived_artifacts/maintained_derivation.py": {"upsert_rows:other_table": 2},
}


def _scan() -> dict[str, dict[str, int]]:
    found: dict[str, dict[str, int]] = {}
    for path in sorted(SRC.rglob("*.py")):
        text = path.read_text(encoding="utf-8")
        rel = str(path.relative_to(SRC))
        counts: dict[str, int] = {}
        for match in _WRITE_CALL.finditer(text):
            line_start = text.rfind("\n", 0, match.start()) + 1
            line = text[line_start:text.find("\n", match.end())]
            if line.lstrip().startswith(("def ", "#")):
                continue
            counts[match.group(1)] = counts.get(match.group(1), 0) + 1
        for match in _GRAPH_SQL.finditer(text):
            verb = "UPDATE" if match.group(1).upper() == "UPDATE" else "INSERT INTO"
            key = f"{verb} okg.graph_{match.group(2).lower()}"
            counts[key] = counts.get(key, 0) + 1
        if counts:
            found[rel] = counts
    return found


def _expected_totals() -> dict[str, dict[str, int]]:
    totals: dict[str, dict[str, int]] = {}
    for rel, kinds in EXPECTED.items():
        for key, n in kinds.items():
            verb = key.rsplit(":", 1)[0]
            totals.setdefault(rel, {})
            totals[rel][verb] = totals[rel].get(verb, 0) + n
    return totals


def test_every_graph_row_writer_is_classified() -> None:
    found = _scan()
    expected = _expected_totals()
    new = {rel: counts for rel, counts in found.items() if rel not in expected}
    assert not new, (
        "a new graph-table writer call: stamp its rows with "
        "okg.substrate.nomos.read_class.stamp_rows (or show it copies whole rows, "
        f"writes another table or only deletes) and pin it in EXPECTED: {new}"
    )
    for rel, counts in expected.items():
        assert found.get(rel) == counts, (rel, found.get(rel), counts)


def test_each_stamping_writer_calls_the_shared_helper() -> None:
    for rel, kinds in EXPECTED.items():
        text = (SRC / rel).read_text(encoding="utf-8")
        if any(key.endswith(":stamp") for key in kinds):
            assert re.search(r"\bstamp_rows?\(", text), rel
        if any(key.endswith(":set") for key in kinds):
            assert "READ_CLASS_COLUMN} = :read_class" in text, rel


def test_an_unclassified_writer_fails_the_inventory(tmp_path: Path, monkeypatch) -> None:
    """Failure case: a new module that upserts graph rows is reported."""
    import shutil

    fake_src = tmp_path / "okg"
    shutil.copytree(SRC, fake_src, ignore=shutil.ignore_patterns("__pycache__"))
    (fake_src / "substrate" / "new_writer.py").write_text(
        "def write(session, rows):\n    session.upsert_rows('okg.graph_nodes', rows)\n",
        encoding="utf-8",
    )
    monkeypatch.setattr(sys.modules[__name__], "SRC", fake_src)
    try:
        test_every_graph_row_writer_is_classified()
    except AssertionError as exc:
        assert "new_writer.py" in str(exc)
    else:  # pragma: no cover - the inventory must refuse
        raise AssertionError("an unclassified writer passed the inventory")
