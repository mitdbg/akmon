from __future__ import annotations

import json
from pathlib import Path

import pytest

from okg.views.planners.base import ViewDefinition
from okg.views.refresh import fresh
from okg.views.view_graphs import (
    build_collection_view_graph,
    load_view_graph_spec,
    view_graph_assembler,
    view_graph_payload_from_view,
    view_graph_planner,
    view_graph_to_corpus,
)


def test_view_graph_spec_loads_and_materializes(tmp_path: Path) -> None:
    spec_path = tmp_path / "demo_view_graph.yaml"
    spec_path.write_text(
        """
version: 1
view_id: demo.public_wiki
title: Demo Wiki
export_mode: public
artifact_policy: facts_and_links_only
flat_pages:
  - page_id: projects
    title: Projects
    route: projects
    collection: projects
    title_field: title
neighborhood_pages:
  - collection: projects
    route_prefix: projects
    page_id_prefix: project
    slug_field: slug
    title_field: title
    include_edge_types: [references]
""",
        encoding="utf-8",
    )

    spec = load_view_graph_spec(spec_path)
    graph = build_collection_view_graph(
        spec=spec,
        generation_id=42,
        collections={
            "projects": [
                {
                    "node_id": "lab_project:demo:one",
                    "title": "One",
                    "slug": "one",
                    "source_family": "demo",
                }
            ],
            "publications": [
                {
                    "node_id": "paper:demo:one",
                    "title": "Paper One",
                    "source_family": "demo",
                }
            ],
        },
        edges=[
            {
                "src": "lab_project:demo:one",
                "dst": "paper:demo:one",
                "edge_type": "references",
                "source_family": "demo",
            }
        ],
    )
    corpus = view_graph_to_corpus(graph)
    view_def = ViewDefinition(
        view_id=spec.view_id,
        planner=view_graph_planner,
        assembler=view_graph_assembler,
        view_def_hash=spec.stable_hash(),
    )

    view, stats = fresh(corpus, view_def, llm_client=None, generation_id=42)
    payload = view_graph_payload_from_view(view)
    markdown = view_graph_assembler(view).markdown

    assert stats.fragments_total == 3
    assert payload["view_id"] == "demo.public_wiki"
    assert payload["generation_id"] == 42
    assert len([node for node in payload["nodes"] if node["node_kind"] == "page"]) == 2
    assert "Projects" in markdown
    assert "Paper One" in markdown


def test_view_graph_spec_rejects_duplicate_flat_page_ids(tmp_path: Path) -> None:
    spec_path = tmp_path / "bad_view_graph.yaml"
    spec_path.write_text(
        """
version: 1
view_id: demo.bad
title: Bad
export_mode: public
artifact_policy: facts_and_links_only
flat_pages:
  - page_id: dup
    title: One
    route: one
    collection: projects
    title_field: title
  - page_id: dup
    title: Two
    route: two
    collection: projects
    title_field: title
""",
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="duplicate flat page ids"):
        load_view_graph_spec(spec_path)


def test_view_graph_payload_is_json_round_trippable(tmp_path: Path) -> None:
    spec_path = tmp_path / "roundtrip.yaml"
    spec_path.write_text(
        """
version: 1
view_id: demo.roundtrip
title: Roundtrip
export_mode: public
artifact_policy: facts_and_links_only
flat_pages:
  - page_id: sources
    title: Sources
    route: sources
    collection: sources
    title_field: title
""",
        encoding="utf-8",
    )

    spec = load_view_graph_spec(spec_path)
    graph = build_collection_view_graph(
        spec=spec,
        generation_id=None,
        collections={"sources": [{"node_id": "source:one", "title": "Source One"}]},
        edges=[],
    )

    payload = json.loads(json.dumps(graph.to_dict(), sort_keys=True))
    assert payload["artifact_policy"] == "facts_and_links_only"
    assert payload["nodes"][0]["node_id"] == "entity:demo.roundtrip:source:one"
