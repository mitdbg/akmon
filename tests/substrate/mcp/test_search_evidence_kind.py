from __future__ import annotations

import json
import time
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import psycopg
import pytest

from okg.substrate.catalog.modules_compose import compose_catalog, load_modules_root
from okg.substrate.mcp.evidence import classify_evidence_rows, curated_alias_evidence
from okg.substrate.mcp import open_session, search
from okg.substrate.mcp.server import build_server
from okg.substrate.graph_store import ChronosGraphStore
from okg.substrate.library.sources.base import NodeFact
from okg.substrate.publisher import Publisher
from tests.pytest.catalog_fixture_builders import SubtypeSpec, seed_catalog
from tests.substrate.mcp.search_fixture import make_search_deployment
from tests.pytest.catalog_fixture_builders import bind_test_catalog_writer  # noqa: E402


class _Cursor:
    def __init__(self, metadata: dict[str, dict[str, Any]]) -> None:
        self.metadata = metadata
        self.params: dict[str, Any] | None = None

    def __enter__(self) -> "_Cursor":
        return self

    def __exit__(self, *_args: Any) -> None:
        return None

    def execute(self, _sql: str, params: dict[str, Any]) -> None:
        self.params = params

    def fetchall(self) -> list[dict[str, Any]]:
        assert self.params is not None
        return [
            {"name": name, "metadata": self.metadata[name]}
            for name in self.params["subtypes"]
            if name in self.metadata
        ]


class _Conn:
    def __init__(self, metadata: dict[str, dict[str, Any]]) -> None:
        self.metadata = metadata
        self.calls = 0

    def cursor(self, *, row_factory: Any = None) -> _Cursor:
        assert row_factory is not None
        self.calls += 1
        return _Cursor(self.metadata)


def _session() -> SimpleNamespace:
    return SimpleNamespace(catalog_version_id=42)


def _rows() -> list[dict[str, Any]]:
    return [
        {"node_id": "n:paragraph", "subtype": "chunk", "source": "s", "attrs": {"text": "evidence", "chunk_kind": "paragraph"}},
        {"node_id": "n:figure", "subtype": "chunk", "source": "s", "attrs": {"text": "evidence", "chunk_kind": "figure_caption"}},
        {"node_id": "n:image", "subtype": "chunk", "source": "s", "attrs": {"text": "evidence", "chunk_kind": "image_caption"}},
        {"node_id": "n:table", "subtype": "chunk", "source": "s", "attrs": {"text": "evidence", "chunk_kind": "table"}},
        {"node_id": "n:entity", "subtype": "entity_mention", "source": "s", "attrs": {"text": "evidence"}},
        {"node_id": "n:model", "subtype": "claim", "source": "s", "attrs": {"text": "evidence", "provenance": "extracted_model"}},
        {"node_id": "n:unknown", "subtype": "unmapped", "source": "s", "attrs": {"text": "evidence"}},
    ]


def test_mixed_evidence_classes_are_distinct_across_methods_and_pages() -> None:
    conn = _Conn({
        "chunk": {
            "evidence_kind_by_attr": {
                "chunk_kind": {
                    "paragraph": "author_prose",
                    "figure_caption": "author_figure_caption",
                    "image_caption": "machine_vision_transcription",
                    "table": "table_transcription",
                }
            }
        },
        "entity_mention": {"evidence_kind": "extracted_entity"},
        "claim": {},
        "unmapped": {},
    })
    rows = _rows()
    expected = {
        "n:paragraph": "author_prose",
        "n:figure": "author_figure_caption",
        "n:image": "machine_vision_transcription",
        "n:table": "table_transcription",
        "n:entity": "extracted_entity",
        "n:model": "model_generated_inference",
        "n:unknown": "unknown",
    }
    for _ in ("lexical", "semantic", "attrs"):
        for page in (rows[:4], rows[4:]):
            out = classify_evidence_rows(conn, _session(), page)
            assert [row["node_id"] for row in out] == [row["node_id"] for row in page]
            for row in out:
                assert row["evidence_kind"] == expected[row["node_id"]]
                assert "subtype" in row["evidence_kind_provenance"]
    assert conn.calls == 6  # one bounded catalog read per method/page, never per hit
    assert curated_alias_evidence(alias="caption") ["evidence_kind"] == "curated_alias"
    assert expected["n:image"] != expected["n:paragraph"]


def test_catalog_metadata_drives_mapping_without_substrate_vocabulary(
    tmp_path: Path,
) -> None:
    deployment = tmp_path / "metadata-deployment"
    (deployment / "schemas").mkdir(parents=True)
    (tmp_path / "cross-links").mkdir()
    (deployment / "deployment.yaml").write_text(
        "name: metadata\nschema_dir: schemas\nmodules: []\n",
        encoding="utf-8",
    )
    (deployment / "schemas" / "nodes.yaml").write_text(
        """
classes:
  DeploymentEvidence:
    annotations:
      archetype: artifact
      okg_evidence_kind_by_attr:
        lane:
          human: operator_authored
          generated: automated_transcription
""",
        encoding="utf-8",
    )
    catalog = compose_catalog(
        deployment,
        modules_root=load_modules_root(),
        cross_links_root=tmp_path / "cross-links",
    )
    assert catalog.subtype_metadata["deployment_evidence"][
        "evidence_kind_by_attr"
    ] == {
        "lane": {"human": "operator_authored", "generated": "automated_transcription"}
    }


def test_unknown_preserves_raw_discriminator_and_supporting_provenance() -> None:
    conn = _Conn({"unmapped": {}})
    row = {
        "node_id": "n:unknown",
        "subtype": "unmapped",
        "source": "producer:one",
        "attrs": {"chunk_kind": "deployment-specific kind"},
    }
    out = classify_evidence_rows(conn, _session(), [row])[0]
    assert out["evidence_kind"] == "unknown"
    assert out["chunk_kind"] == "deployment-specific kind"
    assert out["evidence_kind_provenance"] == {
        "basis": "unclassified",
        "subtype": "unmapped",
        "source": "producer:one",
        "raw_discriminator": "deployment-specific kind",
    }


@pytest.mark.postgres
def test_attrs_search_hydrates_source_before_classification(
    clean_runtime: str,
) -> None:
    """A source omitted by the effective-attrs page is still provenance."""
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        seed_catalog(
            conn,
            subtypes=[SubtypeSpec(
                "chunk",
                metadata={"evidence_kind_by_attr": {"chunk_kind": {
                    "figure_caption": "author_figure_caption",
                }}},
            )],
        )
        ChronosGraphStore.from_connection(conn).apply_facts(
            [NodeFact(
                node_id="n:attrs-source",
                subtype="chunk",
                attrs={"text": "figure text", "chunk_kind": "figure_caption"},
                source_record_id={"id": "n:attrs-source"},
                source_revision={"v": 1},
            )],
            source="fixture:attrs-source",
            strict_endpoints=True,
        )
    published = Publisher(clean_runtime, "default", catalog_identity=bind_test_catalog_writer(clean_runtime, "default")).cycle()
    assert published["status"] == "published", published

    mcp = build_server(dsn=clean_runtime)
    try:
        result = mcp._tool_manager._tools["search"].fn(
            method="attrs",
            attr_filters={"chunk_kind": "figure_caption"},
            subtype_filter="chunk",
            select=["node_id", "evidence_kind", "evidence_kind_provenance"],
        )
    finally:
        mcp._okg_pool.close()  # type: ignore[attr-defined]

    assert result["results"]
    row = result["results"][0]
    assert row["evidence_kind"] == "author_figure_caption"
    assert row["evidence_kind_provenance"]["source"] == "fixture:attrs-source"


@pytest.mark.postgres
def test_real_published_corpus_reports_classes_and_bounded_payload(
    clean_runtime: str,
    tmp_path: Path,
) -> None:
    deployment = make_search_deployment(tmp_path, dsn=clean_runtime)
    specs = [
        SubtypeSpec(
            "chunk",
            metadata={"evidence_kind_by_attr": {"chunk_kind": {
                "paragraph": "author_prose",
                "figure_caption": "author_figure_caption",
                "image_caption": "machine_vision_transcription",
                "table": "table_transcription",
            }}},
        ),
        SubtypeSpec("entity_mention", metadata={"evidence_kind": "extracted_entity"}),
        SubtypeSpec("claim"),
    ]
    nodes = [
        NodeFact(
            node_id=node_id,
            subtype=subtype,
            attrs={"text": f"shared evidence search {node_id}", **attrs},
            source_record_id={"id": node_id},
            source_revision={"v": 1},
        )
        for node_id, subtype, attrs in [
            ("n:paragraph", "chunk", {"chunk_kind": "paragraph"}),
            ("n:figure", "chunk", {"chunk_kind": "figure_caption"}),
            ("n:image", "chunk", {"chunk_kind": "image_caption"}),
            ("n:table", "chunk", {"chunk_kind": "table"}),
            ("n:entity", "entity_mention", {}),
            ("n:model", "claim", {"provenance": "extracted_model"}),
        ]
    ]
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        seed_catalog(conn, subtypes=specs, aliases=[("caption", "n:figure", None)])
        ChronosGraphStore.from_connection(conn).apply_facts(
            nodes, source="system:evidence_fixture", strict_endpoints=True,
        )
    published = Publisher(clean_runtime, deployment, catalog_identity=bind_test_catalog_writer(clean_runtime, deployment)).cycle()
    assert published["status"] == "published"
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        session = open_session(conn, deployment_name=deployment)
        started = time.perf_counter()
        lexical = search(conn, session, "shared evidence", method="lexical", limit=6)
        elapsed = time.perf_counter() - started
        alias = search(conn, session, "caption", method="alias")
    assert len(lexical["results"]) == 6
    assert {row["evidence_kind"] for row in lexical["results"]} >= {
        "author_prose", "author_figure_caption", "machine_vision_transcription",
    }
    assert alias["results"][0]["evidence_kind"] == "curated_alias"
    assert alias["results"][0]["node"]["evidence_kind"] == "author_figure_caption"
    assert len(json.dumps(lexical, default=str).encode()) < 64 * 1024
    assert elapsed < 5.0
