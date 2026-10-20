"""Unit tests for `okg_created_at_from` /
`okg_last_modified_at_from` annotation extraction (no DB).

Per `auto-promote-semantic-timestamps` (2026-04-26).
"""
from __future__ import annotations

import textwrap
from pathlib import Path

import pytest

from okg.substrate.catalog.modules_compose import compose_catalog


_GIT_GRAPH_TS = textwrap.dedent("""\
    id: https://example.com/okg/modules/git_graph
    name: git_graph
    classes:
      GitCommit:
        annotations:
          archetype: process
          okg_created_at_from: authored_at
          okg_last_modified_at_from: committed_at
        attributes:
          commit_sha: {required: true}
          authored_at: {}
          committed_at: {}
""")


_NO_TS = textwrap.dedent("""\
    id: https://example.com/okg/modules/openspec
    name: openspec
    classes:
      OpenSpecChange:
        annotations:
          archetype: artifact
        attributes:
          change_id: {required: true}
""")


@pytest.fixture
def fake_modules_root(tmp_path):
    root = tmp_path / "ontologies"
    root.mkdir()
    (root / "git_graph").mkdir()
    (root / "git_graph" / "schema.yaml").write_text(_GIT_GRAPH_TS)
    (root / "openspec").mkdir()
    (root / "openspec" / "schema.yaml").write_text(_NO_TS)
    return root


@pytest.fixture
def empty_cross_links_root(tmp_path):
    root = tmp_path / "cross_links"
    root.mkdir()
    return root


def _write_deployment(tmp_path, *, modules):
    import yaml
    d = tmp_path / "dep"
    d.mkdir()
    (d / "deployment.yaml").write_text(
        yaml.safe_dump({"name": "dep", "modules": modules})
    )
    return d


def test_annotations_extracted_into_subtype_metadata(
    tmp_path, fake_modules_root, empty_cross_links_root,
):
    dep = _write_deployment(tmp_path, modules=["git_graph"])
    composed = compose_catalog(
        dep,
        modules_root=fake_modules_root,
        cross_links_root=empty_cross_links_root,
    )
    md = composed.subtype_metadata["git_commit"]
    assert md["created_at_from"] == "authored_at"
    assert md["last_modified_at_from"] == "committed_at"


def test_subtypes_without_annotations_have_no_keys(
    tmp_path, fake_modules_root, empty_cross_links_root,
):
    dep = _write_deployment(tmp_path, modules=["openspec"])
    composed = compose_catalog(
        dep,
        modules_root=fake_modules_root,
        cross_links_root=empty_cross_links_root,
    )
    md = composed.subtype_metadata.get("open_spec_change", {})
    assert "created_at_from" not in md
    assert "last_modified_at_from" not in md


def test_substrate_shipped_ontologies_carry_the_annotations():
    """Smoke: confirm the actual shipped ontology files have
    the annotations the proposal added."""
    import yaml
    root = (
        Path(__file__).resolve().parents[2]
        / "src" / "okg" / "substrate" / "library" / "ontologies"
    )
    git_graph = yaml.safe_load(
        (root / "git_graph" / "schema.yaml").read_text()
    )
    commit_anno = git_graph["classes"]["GitCommit"]["annotations"]
    assert commit_anno.get("okg_created_at_from") == "authored_at"
    assert commit_anno.get("okg_last_modified_at_from") == "committed_at"

    sessions = yaml.safe_load(
        (root / "agent_sessions" / "schema.yaml").read_text()
    )
    sess_anno = sessions["classes"]["AgentSession"]["annotations"]
    assert sess_anno.get("okg_created_at_from") == "started_at"
    assert sess_anno.get("okg_last_modified_at_from") == "ended_at"
