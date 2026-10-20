from __future__ import annotations

import json
from pathlib import Path

import psycopg
import pytest

from okg.substrate.exchange.harness import database_doctor, describe_generation
from okg.substrate.graph_store import ChronosGraphStore
from okg.substrate.ingest import SourceRunner
from okg.substrate.library.sources.doc_corpus import DocCorpusSource, _chunk_node_id
from okg.substrate.mcp.run_sql import _expand_composite_views, _rewrite_graph_views
from okg.substrate.publisher import Publisher
from tests.substrate._db_helpers import seed_catalog_version
from tests.pytest.catalog_fixture_builders import bind_test_catalog_writer  # noqa: E402


pytestmark = pytest.mark.postgres


_LATEX = r"""\documentclass{article}
\begin{document}
\section{Proof}

First proof paragraph.

Second proof paragraph.

\begin{equation}
E = mc^2
\end{equation}

\begin{figure}
\includegraphics{not-read.png}
\caption{Proof figure.}
\end{figure}

\begin{table}
\begin{tabular}{cc}
a & b \\
\end{tabular}
\end{table}

\begin{itemize}
\item First proof item.
\item Second proof item.
\end{itemize}
\end{document}
"""

_EXTRACTORS = """\
parser:
  - format: latex
    class: okg.substrate.library.extraction.parse.latex.LatexParser
  - format: text
    class: okg.substrate.library.extraction.parse.text.TextParser
chunker:
  - format: latex
    class: okg.substrate.library.extraction.chunk.latex_structural.LatexStructuralChunker
  - format: default
    class: okg.substrate.library.extraction.chunk.window.WindowChunker
    config: {max_chars: 1500, overlap: 200}
ner: []
"""

_ORACLE = {
    "equation": 1,
    "figure_caption": 1,
    "list_item": 2,
    "paragraph": 2,
    "table": 1,
}


def _graph_counts(store: ChronosGraphStore) -> dict[str, int]:
    nodes = store.query("SELECT count(*) AS n FROM okg.graph_nodes")[0]["n"]
    edges = store.query("SELECT count(*) AS n FROM okg.graph_edges")[0]["n"]
    return {"nodes": int(nodes or 0), "edges": int(edges or 0)}


def test_isolated_latex_corpus_publishes_typed_graph_report(
    clean_runtime: str,
    tmp_path: Path,
) -> None:
    corpus = tmp_path / "corpus"
    corpus.mkdir()
    (corpus / "proof.tex").write_text(_LATEX)
    legacy_text = "Legacy paragraph identity remains stable."
    (corpus / "legacy.txt").write_text(legacy_text)
    extractors = tmp_path / "extractors"
    extractors.mkdir()
    (extractors / "extractors.yaml").write_text(_EXTRACTORS)

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        seed_catalog_version(conn)

    store = ChronosGraphStore.connect(clean_runtime, ensure_schema=False)
    try:
        before = _graph_counts(store)
    finally:
        store.close()

    SourceRunner(clean_runtime, source_name="test.doc-corpus-latex-proof").run(
        DocCorpusSource(
            repo="latex-proof",
            repo_root=corpus, non_git_walk=True,
            extractors_dir=extractors,
        ),
        mode="scope_complete",
    )
    published = Publisher(clean_runtime, "default", catalog_identity=bind_test_catalog_writer(clean_runtime, "default")).cycle()
    assert published["status"] == "published", published
    generation_id = published["generation_id"]
    described = describe_generation(clean_runtime, generation_id)
    assert described["status"] == "published", described

    store = ChronosGraphStore.connect(clean_runtime, ensure_schema=False)
    try:
        generation = store.checkout_generation(generation_id)
        after = _graph_counts(generation)
        assert after["nodes"] > before["nodes"]
        assert after["edges"] > before["edges"]

        legacy = generation.query(
            "SELECT node_id FROM okg.graph_nodes "
            "WHERE node_id = :node_id AND subtype = 'document_chunk'",
            {"node_id": _chunk_node_id(legacy_text)},
        )
        assert legacy == [{"node_id": _chunk_node_id(legacy_text)}]

        view_sql = (
            "SELECT chunk_kind, count(DISTINCT chunk_id)::int AS unique_nodes, "
            "count(*)::int AS memberships FROM okg.v_doc_chunks "
            "WHERE repo = 'latex-proof' AND path = 'proof.tex' "
            "GROUP BY chunk_kind ORDER BY chunk_kind"
        )
        with psycopg.connect(clean_runtime) as conn:
            expanded = _expand_composite_views(view_sql, conn)
        rows = generation.query(_rewrite_graph_views(expanded))
        unique_counts = {row["chunk_kind"]: row["unique_nodes"] for row in rows}
        membership_counts = {
            row["chunk_kind"]: row["memberships"] for row in rows
        }
        assert unique_counts == _ORACLE
        assert membership_counts == _ORACLE
    finally:
        store.close()

    with psycopg.connect(clean_runtime) as conn, conn.cursor() as cur:
        cur.execute("SELECT count(*) FROM okg.projection_pending")
        projection_queue = int(cur.fetchone()[0])
        cur.execute(
            "SELECT coalesce(sum(dlq_count), 0)::int "
            "FROM okg.v_runtime_queue_health"
        )
        dlq_count = int(cur.fetchone()[0])
    assert projection_queue == 0
    assert dlq_count == 0
    doctor = database_doctor(clean_runtime)
    assert doctor["ok"] is True, doctor

    report = {
        "deployment": "isolated-test:default",
        "generation_id": generation_id,
        "generation_status": described["status"],
        "before": before,
        "after": after,
        "delta": {
            "nodes": after["nodes"] - before["nodes"],
            "edges": after["edges"] - before["edges"],
        },
        "latex_unique_nodes_by_kind": unique_counts,
        "latex_memberships_by_kind": membership_counts,
        "latex_membership_oracle": _ORACLE,
        "legacy_paragraph_node_id": _chunk_node_id(legacy_text),
        "projection_queue": projection_queue,
        "dlq_count": dlq_count,
        "doctor": doctor,
    }
    artifact = Path(".scratch/doc-corpus-latex-publish.json")
    artifact.parent.mkdir(exist_ok=True)
    artifact.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n")
