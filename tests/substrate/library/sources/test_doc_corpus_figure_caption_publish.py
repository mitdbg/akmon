from __future__ import annotations

import base64
import hashlib
import json
from pathlib import Path

import psycopg
import pytest
import yaml

from okg.substrate.catalog.modules_apply import apply_composed_catalog
from okg.substrate.catalog.modules_compose import compose_catalog
from okg.substrate.exchange.harness import database_doctor, describe_generation
from okg.substrate.graph_store import ChronosGraphStore
from okg.substrate.ingest import SourceRunner
from okg.substrate.library.sources.doc_corpus import DocCorpusSource
from okg.substrate.publisher import Publisher
from tests.pytest.catalog_fixture_builders import bind_test_catalog_writer  # noqa: E402


pytestmark = pytest.mark.postgres


_EXTRACTORS = """\
parser:
  - format: latex
    class: okg.substrate.library.extraction.parse.latex.LatexParser
chunker:
  - format: latex
    class: okg.substrate.library.extraction.chunk.latex_structural.LatexStructuralChunker
ner: []
captioning:
  enabled: false
"""

_PNG = base64.b64decode(
    "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mNk+A8AAQUBAScY42YAAAAASUVORK5CYII="
)


def _write(root: Path, rel: str, content: str | bytes) -> None:
    path = root / rel
    path.parent.mkdir(parents=True, exist_ok=True)
    if isinstance(content, bytes):
        path.write_bytes(content)
    else:
        path.write_text(content, encoding="utf-8")


def _visual_id(content: bytes) -> str:
    return f"visual:{hashlib.sha256(content).hexdigest()[:16]}"


def _main_tex(*, second_target: str) -> str:
    return rf"""\documentclass{{article}}
\includegraphics{{outside.png}}
\begin{{document}}
\begin{{figure}}
\includegraphics{{inside-a.png}}
\includegraphics{{missing.png}}
\includegraphics{{{second_target}}}
\caption{{Shared caption.}}
\end{{figure}}
\end{{document}}
"""


def _shared_tex(*, include_target: str | None) -> str:
    graphic = f"\\includegraphics{{{include_target}}}\n" if include_target else ""
    return (
        "\\documentclass{article}\n"
        "\\begin{document}\n"
        "\\begin{figure}\n"
        f"{graphic}"
        "\\caption{Shared caption.}\n"
        "\\end{figure}\n"
        "\\end{document}\n"
    )


def _manifest_config(*, include_shared: bool = False) -> dict[str, object]:
    projects: list[dict[str, object]] = [
        {"project_dir": "paper", "entrypoints": ["main.tex"]},
    ]
    if include_shared:
        projects.append(
            {"project_dir": "shared", "entrypoints": ["main.tex"]},
        )
    return {
        "repository_authority": "caption-link-proof",
        "projects": projects,
    }


def _source(
    corpus: Path,
    extractors: Path,
    *,
    include_shared: bool = False,
) -> DocCorpusSource:
    return DocCorpusSource(
        repo="caption-link-proof",
        repo_root=corpus, non_git_walk=True,
        extractors_dir=extractors,
        project_manifest=_manifest_config(include_shared=include_shared),
    )


def _caption_edges(store: ChronosGraphStore) -> set[tuple[str, str]]:
    rows = store.query(
        "SELECT e.src, e.dst FROM okg.graph_edges e "
        "JOIN okg.graph_nodes s ON s.node_id = e.src "
        "JOIN okg.graph_nodes d ON d.node_id = e.dst "
        "WHERE e.edge_type = 'caption_of' "
        "AND s.subtype = 'document_chunk' "
        "AND s.attrs->>'chunk_kind' = 'figure_caption' "
        "AND d.subtype = 'visual_artifact' "
        "ORDER BY e.src, e.dst"
    )
    return {(str(row["src"]), str(row["dst"])) for row in rows}


def _publish(clean_runtime: str) -> tuple[str, dict[str, object]]:
    published = Publisher(clean_runtime, "default", catalog_identity=bind_test_catalog_writer(clean_runtime, "default")).cycle()
    assert published["status"] == "published", published
    generation_id = str(published["generation_id"])
    described = describe_generation(clean_runtime, generation_id)
    assert described["status"] == "published", described
    assert "narrowing_violation" not in json.dumps(described, sort_keys=True)
    return generation_id, described


def test_figure_caption_edges_publish_and_reconcile(
    clean_runtime: str,
    tmp_path: Path,
) -> None:
    corpus = tmp_path / "corpus"
    corpus.mkdir()
    extractors = tmp_path / "extractors"
    extractors.mkdir()
    (extractors / "extractors.yaml").write_text(_EXTRACTORS)

    first_a = _PNG + b"first-a"
    changed_a = _PNG + b"changed-a"
    inside_b = _PNG + b"inside-b"
    inside_c = _PNG + b"inside-c"
    outside = _PNG + b"outside"
    shared_visual = _PNG + b"shared"
    _write(corpus, "paper/main.tex", _main_tex(second_target="inside-b.png"))
    _write(corpus, "paper/inside-a.png", first_a)
    _write(corpus, "paper/inside-b.png", inside_b)
    _write(corpus, "paper/inside-c.png", inside_c)
    _write(corpus, "paper/outside.png", outside)

    deployment = tmp_path / "deployment"
    deployment.mkdir()
    (deployment / "schemas").mkdir()
    (deployment / "deployment.yaml").write_text(
        yaml.safe_dump(
            {
                "name": "caption-link-proof",
                "description": "Issue 896 figure-caption publication proof",
                "modules": [
                    "git_graph",
                    "openspec",
                    "agent_memory",
                    "extraction",
                    "reasoning",
                ],
            }
        )
    )
    composed = compose_catalog(deployment)
    expected_narrowing = (
        "document_chunk",
        "caption_of",
        "visual_artifact",
    )
    assert expected_narrowing in composed.narrowing_triples()
    with psycopg.connect(clean_runtime) as conn:
        catalog_version_id = apply_composed_catalog(conn, composed)
        conn.commit()
        with conn.cursor() as cur:
            cur.execute(
                "SELECT parent_subtype, edge_type, child_subtype "
                "FROM okg.narrowings WHERE catalog_version_id = %s "
                "AND edge_type = 'caption_of'",
                (catalog_version_id,),
            )
            assert cur.fetchall() == [expected_narrowing]

    store = ChronosGraphStore.connect(clean_runtime, ensure_schema=False)
    try:
        before_count = len(_caption_edges(store))
    finally:
        store.close()
    assert before_count == 0

    source_name = "proof.doc-corpus-caption-link"
    runner = SourceRunner(clean_runtime, source_name=source_name)
    runner.run(_source(corpus, extractors), mode="scope_complete")
    first_generation, first_described = _publish(clean_runtime)
    store = ChronosGraphStore.connect(clean_runtime, ensure_schema=False)
    try:
        first_checkout = store.checkout_generation(first_generation)
        first_edges = _caption_edges(first_checkout)
    finally:
        store.close()
    caption_id = next(iter(first_edges))[0]
    assert first_edges == {
        (caption_id, _visual_id(first_a)),
        (caption_id, _visual_id(inside_b)),
    }
    assert before_count < len(first_edges)
    assert all(dst != _visual_id(outside) for _, dst in first_edges)

    _write(corpus, "paper/main.tex", _main_tex(second_target="inside-c.png"))
    runner.run(_source(corpus, extractors), mode="scope_complete")
    replacement_generation, replacement_described = _publish(clean_runtime)
    store = ChronosGraphStore.connect(clean_runtime, ensure_schema=False)
    try:
        replacement_checkout = store.checkout_generation(replacement_generation)
        replacement_edges = _caption_edges(replacement_checkout)
    finally:
        store.close()
    assert replacement_edges == {
        (caption_id, _visual_id(first_a)),
        (caption_id, _visual_id(inside_c)),
    }
    assert (caption_id, _visual_id(inside_b)) not in replacement_edges

    _write(corpus, "paper/inside-a.png", changed_a)
    manifest_refresh_source = _source(corpus, extractors)
    runner.run(
        manifest_refresh_source,
        mode="scope_complete",
        sync_scope={"scope_keys": ["paper/inside-a.png"]},
    )
    assert manifest_refresh_source.last_run_stats[
        "scope_widened_for_project_manifest"
    ] is True
    assert manifest_refresh_source.last_run_stats["scope_status"] == "full"
    assert manifest_refresh_source.last_run_stats["files_changed"] > 1
    assert manifest_refresh_source.last_run_stats["emitted_edges"] > 0
    content_generation, content_described = _publish(clean_runtime)
    store = ChronosGraphStore.connect(clean_runtime, ensure_schema=False)
    try:
        content_checkout = store.checkout_generation(content_generation)
        content_edges = _caption_edges(content_checkout)
    finally:
        store.close()
    assert content_edges == {
        (caption_id, _visual_id(changed_a)),
        (caption_id, _visual_id(inside_c)),
    }
    assert (caption_id, _visual_id(first_a)) not in content_edges

    _write(corpus, "shared/main.tex", _shared_tex(include_target="shared.png"))
    _write(corpus, "shared/shared.png", shared_visual)
    runner.run(
        _source(corpus, extractors, include_shared=True),
        mode="scope_complete",
    )
    shared_generation, shared_described = _publish(clean_runtime)
    store = ChronosGraphStore.connect(clean_runtime, ensure_schema=False)
    try:
        shared_checkout = store.checkout_generation(shared_generation)
        shared_edges = _caption_edges(shared_checkout)
    finally:
        store.close()
    assert shared_edges == {
        *content_edges,
        (caption_id, _visual_id(shared_visual)),
    }

    _write(corpus, "shared/main.tex", _shared_tex(include_target=None))
    runner.run(
        _source(corpus, extractors, include_shared=True),
        mode="scope_complete",
    )
    union_generation, union_described = _publish(clean_runtime)
    store = ChronosGraphStore.connect(clean_runtime, ensure_schema=False)
    try:
        union_checkout = store.checkout_generation(union_generation)
        union_edges = _caption_edges(union_checkout)
    finally:
        store.close()
    assert union_edges == content_edges
    assert (caption_id, _visual_id(shared_visual)) not in union_edges

    with psycopg.connect(clean_runtime) as conn, conn.cursor() as cur:
        cur.execute("SELECT count(*) FROM okg.projection_pending")
        projection_queue = int(cur.fetchone()[0])
        cur.execute(
            "SELECT coalesce(sum(dlq_count), 0)::int FROM okg.v_runtime_queue_health"
        )
        dlq_count = int(cur.fetchone()[0])
    assert projection_queue == 0
    assert dlq_count == 0
    doctor = database_doctor(clean_runtime)
    assert doctor["ok"] is True, doctor

    report = {
        "deployment": "isolated-test:default",
        "catalog_version_id": catalog_version_id,
        "catalog_narrowing": list(expected_narrowing),
        "before_caption_of_count": before_count,
        "first": {
            "generation_id": first_generation,
            "status": first_described["status"],
            "caption_of_count": len(first_edges),
            "edges": sorted(first_edges),
        },
        "changed_target": {
            "generation_id": replacement_generation,
            "status": replacement_described["status"],
            "edges": sorted(replacement_edges),
        },
        "same_path_content_change": {
            "generation_id": content_generation,
            "status": content_described["status"],
            "edges": sorted(content_edges),
        },
        "shared_caption_added": {
            "generation_id": shared_generation,
            "status": shared_described["status"],
            "edges": sorted(shared_edges),
        },
        "shared_caption_reconciled": {
            "generation_id": union_generation,
            "status": union_described["status"],
            "edges": sorted(union_edges),
        },
        "projection_queue": projection_queue,
        "dlq_count": dlq_count,
        "doctor_ok": doctor["ok"],
    }
    artifact = Path(".scratch/issue-896-caption-link-publish.json")
    artifact.parent.mkdir(exist_ok=True)
    artifact.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n")
