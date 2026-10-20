from __future__ import annotations

from pathlib import Path

import psycopg
import pytest

from okg.substrate.graph_store import ChronosGraphStore
from okg.substrate.ingest import SourceRunner
from okg.substrate.library.sources.doc_corpus import DocCorpusSource
from okg.substrate.mcp.run_sql import _expand_composite_views, _rewrite_graph_views
from okg.substrate.publisher import Publisher
from tests.substrate._db_helpers import seed_catalog_version
from tests.pytest.catalog_fixture_builders import bind_test_catalog_writer  # noqa: E402


_VIEW_COLUMNS = [
    "cell_key",
    "table_occurrence_id",
    "doc_id",
    "path",
    "repo",
    "table_extraction_lane",
    "row_index",
    "row_label",
    "column_index",
    "column_header",
    "cell_text",
    "row_span",
    "column_span",
    "source_char_offset",
    "source_char_end",
    "source_page_index",
    "source_bbox",
    "source_content_sha256",
    "cache_signature",
]


def test_table_cell_view_contract_is_exact_and_doc_columns_only_append() -> None:
    root = Path(__file__).resolve().parents[2]
    sql = (root / "src/okg/substrate/db/views.sql").read_text()
    table_start = sql.index("CREATE OR REPLACE VIEW okg.v_table_cells")
    # Bounded by "the next view declaration", not by a NAMED one. This slice
    # used to end at `okg.v_changes`; that view was retired
    # (schema-canonical-read-path, decision D6) and the anchor raised
    # ValueError, so the test ERRORED rather than reporting a contract
    # violation. A positional bound cannot be invalidated by retiring an
    # unrelated neighbour.
    table_end = sql.index(
        "CREATE OR REPLACE VIEW okg.", sql.index("\n", table_start)
    )
    table_view = sql[table_start:table_end]
    positions = [table_view.index(f"AS {column}") for column in _VIEW_COLUMNS]
    assert positions == sorted(positions)
    assert "jsonb_array_elements(c.attrs->'table_grid'->'rows')" in table_view
    assert "c.attrs->>'table_grid_status' = 'parsed'" in table_view
    assert "ORDER BY e.dst, c.node_id" in table_view
    # The view is app_ro-readable because okg.grant_registry says so, not
    # because a GRANT statement sits in this file (schema-grant-coherence
    # replaced the statements and the `v_%` naming convention with the
    # registry). Bound the assertion to the substrate seed so another quoted
    # reference elsewhere in views.sql cannot make this pass vacuously.
    seed_start = sql.index("DO $okg_grant_registry_seed$")
    seed_end = sql.index("$okg_grant_registry_seed$;", seed_start)
    registry_seed = sql[seed_start:seed_end]
    app_ro_start = registry_seed.index("-- app_ro:")
    app_ro_end = registry_seed.index("-- okg_mcp:", app_ro_start)
    app_ro_seed = registry_seed[app_ro_start:app_ro_end]
    assert "'okg.v_table_cells'" in app_ro_seed, (
        "okg.v_table_cells has no okg.grant_registry row, so app_ro cannot "
        "read it; add it to the app_ro array in views.sql's "
        "okg_grant_registry_seed block"
    )

    doc_start = sql.index("CREATE OR REPLACE VIEW okg.v_doc_chunks")
    doc_end = sql.index("CREATE OR REPLACE VIEW okg.v_chunk_sequence", doc_start)
    doc_view = sql[doc_start:doc_end]
    established = [
        "AS chunk_id",
        "AS doc_id",
        "AS path",
        "AS repo",
        "AS text",
        "AS heading_path",
        "AS char_offset",
        "AS char_length",
        "AS chunker_name",
        "AS content_sha256",
        "AS chunk_kind",
        "AS char_end",
    ]
    appended = [
        "AS table_grid_status",
        "AS table_extraction_lane",
        "AS table_grid_error_code",
    ]
    established_positions = [doc_view.index(column) for column in established]
    appended_positions = [doc_view.index(column) for column in appended]
    assert established_positions == sorted(established_positions)
    assert min(appended_positions) > max(established_positions)


@pytest.mark.postgres
def test_published_table_cells_are_pinned_traceable_and_app_ro_readable(
    clean_runtime: str,
    tmp_path: Path,
) -> None:
    table = r"""\begin{table}
\begin{tabular}{lc}
Method & Score \\
Alpha & 10 \\
\end{tabular}
\end{table}"""
    malformed = r"""\begin{table}
\begin{tabular}{lc}Method & Score \\ missing \\ \end{tabular}
\end{table}"""
    source = (
        r"\begin{document}"
        + "\n"
        + table
        + "\n"
        + table
        + "\n"
        + malformed
        + "\n"
        + r"\end{document}"
    )
    corpus = tmp_path / "corpus"
    corpus.mkdir()
    (corpus / "paper.tex").write_text(source)
    extractors = tmp_path / "extractors"
    extractors.mkdir()
    (extractors / "extractors.yaml").write_text(
        """\
parser:
  - format: latex
    class: okg.substrate.library.extraction.parse.latex.LatexParser
chunker:
  - format: latex
    class: okg.substrate.library.extraction.chunk.latex_structural.LatexStructuralChunker
ner: []
"""
    )
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        seed_catalog_version(conn)

    SourceRunner(clean_runtime, source_name="test.table-cell-view").run(
        DocCorpusSource(
            repo="table-view",
            repo_root=corpus, non_git_walk=True,
            extractors_dir=extractors,
        ),
        mode="scope_complete",
    )
    published = Publisher(clean_runtime, "default", catalog_identity=bind_test_catalog_writer(clean_runtime, "default")).cycle()
    assert published["status"] == "published", published
    generation_id = published["generation_id"]

    with psycopg.connect(clean_runtime) as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_schema = 'okg' AND table_name = 'v_table_cells' "
            "ORDER BY ordinal_position"
        )
        assert [row[0] for row in cur.fetchall()] == _VIEW_COLUMNS
        cur.execute(
            "SELECT has_table_privilege('app_ro', "
            "'okg.v_table_cells'::regclass, 'SELECT')"
        )
        assert cur.fetchone()[0] is True
        # The installed SQL view remains bigint-pinned for legacy deployments;
        # this publisher is Chronos-native, so published graph rows are checked
        # through the concrete checkpoint below. Here app_ro proves the view is
        # executable under the least-privilege role.
        cur.execute("SET LOCAL okg.pin_generation = '0'")
        cur.execute("SET LOCAL ROLE app_ro")
        cur.execute("SELECT count(*) FROM okg.v_table_cells")
        assert cur.fetchone()[0] == 0

    query = (
        "SELECT cell_key, table_occurrence_id, doc_id, path, repo, "
        "table_extraction_lane, row_index, row_label, column_index, "
        "column_header, cell_text, row_span, column_span, "
        "source_char_offset, source_char_end, source_page_index, "
        "source_bbox, source_content_sha256, cache_signature "
        "FROM okg.v_table_cells WHERE repo = 'table-view' "
        "ORDER BY doc_id, table_occurrence_id, row_index, column_index"
    )
    with psycopg.connect(clean_runtime) as conn:
        graph_query = _rewrite_graph_views(_expand_composite_views(query, conn))
    store = ChronosGraphStore.connect(clean_runtime, ensure_schema=False)
    try:
        generation = store.checkout_generation(generation_id)
        rows = generation.query(graph_query)
        assert len(rows) == 4
        occurrence_ids = {row["table_occurrence_id"] for row in rows}
        assert len(occurrence_ids) == 2
        assert all(value.startswith("chunk:table:") for value in occurrence_ids)
        for row in rows:
            assert row["cell_key"] == (
                f"{row['table_occurrence_id']}:{row['row_index']}:"
                f"{row['column_index']}"
            )
            assert row["doc_id"] == "doc:table-view:paper.tex"
            assert row["path"] == "paper.tex"
            assert row["table_extraction_lane"] == "latex"
            assert row["source_char_offset"] is not None
            assert row["source_char_end"] > row["source_char_offset"]
            assert row["source_page_index"] is None
            assert row["source_bbox"] is None
            assert len(row["source_content_sha256"]) == 64
            assert row["cache_signature"] is None
        cells_by_occurrence = {
            occurrence_id: [
                (row["column_header"], row["cell_text"])
                for row in rows
                if row["table_occurrence_id"] == occurrence_id
            ]
            for occurrence_id in occurrence_ids
        }
        assert set(map(tuple, cells_by_occurrence.values())) == {
            (("Method", "Alpha"), ("Score", "10"))
        }
        assert generation.query(
            "SELECT count(*)::int AS n FROM okg.graph_nodes "
            "WHERE subtype = 'table_cell'"
        ) == [{"n": 0}]
    finally:
        store.close()
