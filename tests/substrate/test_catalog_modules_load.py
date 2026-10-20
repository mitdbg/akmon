"""Tests for `okg catalog load` modules-composition (Phase 6 of
`migrate-okg-self-to-substrate`).

Covers the pure-Python compose path:

  - single-module load (git_graph alone)
  - multi-module load (the four built-in modules)
  - bridge load (deployment-side narrowings reference module
    subtypes)
  - missing module → CatalogModuleMissing raised before any work
  - bridge with unknown subtype → BridgeSubtypeUnknown raised

A second test exercises the CLI subcommand end-to-end via the
`main()` entrypoint (no Postgres required for the no-apply path).
"""
from __future__ import annotations

import argparse
import json
import textwrap
from pathlib import Path
from types import SimpleNamespace

import pytest

from okg.substrate.catalog.modules_compose import (
    compose_catalog,
    load_modules_root,
)
from okg.substrate.catalog.composition import ResolverError, resolve
from okg.substrate.deployments import DeploymentManifest
from okg.substrate.errors import (
    BridgeSubtypeUnknown,
    CatalogModuleMissing,
    NarrowingBridgeDirectorySymlink,
    NarrowingSchemaMisplaced,
)


# ---------- minimal fake module schema for hermetic tests ----------

_MINIMAL_GIT_GRAPH = textwrap.dedent("""\
    id: https://example.com/okg/modules/git_graph
    name: git_graph
    classes:
      SourceFile:
        annotations:
          archetype: artifact
        attributes:
          path: {required: true}
      GitCommit:
        annotations:
          archetype: process
        attributes:
          commit_sha: {required: true}
      EdgeNarrowings:
        annotations:
          okg_edge_narrowings: "true"
        attributes:
          commit_affects_file:
            annotations:
              edge_archetype: affects
              src_subtypes: GitCommit
              dst_subtypes: SourceFile
""")

_MINIMAL_OPENSPEC = textwrap.dedent("""\
    id: https://example.com/okg/modules/openspec
    name: openspec
    classes:
      OpenSpecChange:
        annotations:
          archetype: artifact
        attributes:
          change_id: {required: true}
      EdgeNarrowings:
        annotations:
          okg_edge_narrowings: "true"
        attributes:
          change_references_file:
            annotations:
              edge_archetype: references
              src_subtypes: OpenSpecChange
              dst_subtypes: SourceFile
""")

_DEPLOYMENT_NODES = textwrap.dedent("""\
    id: https://example.com/okg/example/nodes
    name: example_nodes
    classes:
      Book:
        annotations:
          archetype: artifact
        attributes:
          title: {required: true}
""")

_BRIDGE_OK = textwrap.dedent("""\
    id: https://example.com/okg/example/bridges/git_to_book
    name: example_bridge
    classes:
      EdgeNarrowings:
        annotations:
          okg_edge_narrowings: "true"
        attributes:
          file_references_book:
            annotations:
              edge_archetype: references
              src_subtypes: SourceFile
              dst_subtypes: Book
""")

_BRIDGE_BAD = textwrap.dedent("""\
    id: https://example.com/okg/example/bridges/bad
    name: bad_bridge
    classes:
      EdgeNarrowings:
        annotations:
          okg_edge_narrowings: "true"
        attributes:
          file_references_galaxy:
            annotations:
              edge_archetype: references
              src_subtypes: SourceFile
              dst_subtypes: Galaxy
""")


def test_load_modules_root_points_to_builtin_ontology_dir() -> None:
    root = load_modules_root()

    assert root.name == "ontologies"
    assert (root / "git_graph" / "schema.yaml").is_file()


def test_resolver_fails_loudly_on_malformed_module_schema(
    tmp_path: Path,
) -> None:
    root = tmp_path / "ontologies"
    module_dir = root / "broken"
    module_dir.mkdir(parents=True)
    (module_dir / "module.yaml").write_text(textwrap.dedent("""\
        name: broken
        version: 1.0.0
        provides:
          subtypes_file: schema.yaml
    """))
    (module_dir / "schema.yaml").write_text("classes:\n  Broken: : bad\n")

    with pytest.raises(ResolverError) as exc:
        resolve(["broken"], modules_root=root)

    assert "YAML parse error" in str(exc.value)
    assert exc.value.data["module_name"] == "broken"
    assert exc.value.data["schema_path"].endswith("schema.yaml")


def test_builtin_coordination_module_declares_reasoning_cross_link_subtypes(
) -> None:
    root = load_modules_root()
    resolved = resolve(["coordination", "reasoning"], modules_root=root)

    assert {
        "workspace_observation",
        "work_order",
        "review",
    } <= resolved.subtypes_by_module["coordination"]
    assert any(
        rr.source_module == "coordination"
        and rr.target_module == "reasoning"
        and rr.rule.id == "workspace_observation_derives_to_observation"
        for rr in resolved.cross_link_rules
    )


def test_builtin_identity_alias_narrowings_accept_same_subtype_edges(
    tmp_path: Path,
    empty_cross_links_root,
) -> None:
    deployment = _write_deployment(
        tmp_path,
        name="identity_alias_narrowings",
        modules=["person", "dataset", "scholarly"],
    )
    composed = compose_catalog(
        deployment,
        modules_root=load_modules_root(),
        cross_links_root=empty_cross_links_root,
    )

    triples = composed.narrowing_triples()
    assert ("person", "same_as", "person") in triples
    assert ("paper", "same_as", "paper") in triples


@pytest.fixture
def fake_modules_root(tmp_path):
    """Hermetic substrate-library modules tree."""
    root = tmp_path / "ontologies"
    root.mkdir()
    (root / "git_graph").mkdir()
    (root / "git_graph" / "schema.yaml").write_text(_MINIMAL_GIT_GRAPH)
    (root / "openspec").mkdir()
    (root / "openspec" / "schema.yaml").write_text(_MINIMAL_OPENSPEC)
    return root


@pytest.fixture
def empty_cross_links_root(tmp_path):
    """Hermetic empty cross-links dir so module-composition
    tests don't pick up substrate-shipped cross-link rules
    (per `declarative-cross-source-linker`, 2026-04-26)."""
    root = tmp_path / "cross_links"
    root.mkdir()
    return root


def _write_deployment(
    tmp_path: Path,
    *,
    name: str,
    modules: list[str],
    nodes_yaml: str | None = None,
    bridge_yaml: str | None = None,
    aliases_json: dict | None = None,
    identifier_patterns_yaml: str | None = None,
    schema_dir_name: str = "schemas",
) -> Path:
    d = tmp_path / name
    d.mkdir()
    manifest = {
        "name": name,
        "schema_dir": schema_dir_name,
        "modules": modules,
    }
    import yaml as _yaml
    (d / "deployment.yaml").write_text(_yaml.safe_dump(manifest))
    schemas = d / schema_dir_name
    schemas.mkdir(parents=True)
    if nodes_yaml is not None:
        (schemas / "nodes.yaml").write_text(nodes_yaml)
    if bridge_yaml is not None:
        bridges = schemas / "bridges"
        bridges.mkdir()
        (bridges / "test_bridge.yaml").write_text(bridge_yaml)
    if aliases_json is not None:
        (d / "aliases.json").write_text(json.dumps(aliases_json))
    if identifier_patterns_yaml is not None:
        (d / "identifier_patterns.yaml").write_text(
            identifier_patterns_yaml
        )
    return d


# ---------- compose: single-module load ----------

def test_compose_single_module_extracts_subtypes_and_narrowings(
    tmp_path, fake_modules_root, empty_cross_links_root,
) -> None:
    deployment = _write_deployment(
        tmp_path, name="solo", modules=["git_graph"],
    )
    composed = compose_catalog(deployment, modules_root=fake_modules_root, cross_links_root=empty_cross_links_root)

    assert composed.deployment_name == "solo"
    assert composed.modules == ["git_graph"]
    assert composed.subtypes == {"source_file", "git_commit"}
    assert composed.narrowing_triples() == {
        ("git_commit", "affects", "source_file"),
    }
    # Origin attribution is preserved.
    assert composed.subtype_origin["source_file"] == "git_graph"
    assert composed.narrowings[0].origin == "git_graph"
    assert composed.narrowings[0].narrowing_name == "commit_affects_file"


def test_compose_extracts_attribute_pii_classes_and_rejects_unknown(
    tmp_path, fake_modules_root, empty_cross_links_root,
) -> None:
    schema_path = fake_modules_root / "git_graph" / "schema.yaml"
    schema_path.write_text(
        _MINIMAL_GIT_GRAPH.replace(
            "path: {required: true}",
            "path:\n"
            "            required: true\n"
            "            annotations:\n"
            "              pii_class: direct_identifier",
        )
    )
    deployment = _write_deployment(
        tmp_path, name="privacy", modules=["git_graph"],
    )
    composed = compose_catalog(
        deployment,
        modules_root=fake_modules_root,
        cross_links_root=empty_cross_links_root,
    )
    assert composed.subtype_metadata["source_file"]["pii_classes"] == {
        "path": "direct_identifier",
    }

    schema_path.write_text(
        schema_path.read_text().replace(
            "pii_class: direct_identifier",
            "pii_class: definitely_not_a_class",
        )
    )
    with pytest.raises(ValueError, match="invalid pii_class"):
        compose_catalog(
            deployment,
            modules_root=fake_modules_root,
            cross_links_root=empty_cross_links_root,
        )


# ---------- compose: multi-module load ----------

def test_compose_multi_module_unions_subtypes_and_narrowings(
    tmp_path, fake_modules_root, empty_cross_links_root,
) -> None:
    deployment = _write_deployment(
        tmp_path, name="multi", modules=["git_graph", "openspec"],
    )
    composed = compose_catalog(deployment, modules_root=fake_modules_root, cross_links_root=empty_cross_links_root)

    assert composed.subtypes == {
        "source_file", "git_commit", "open_spec_change",
    }
    triples = composed.narrowing_triples()
    assert ("git_commit", "affects", "source_file") in triples
    assert ("open_spec_change", "references", "source_file") in triples
    # Origin is per-row.
    by_origin = {r.origin for r in composed.narrowings}
    assert by_origin == {"git_graph", "openspec"}


# ---------- compose: bridge load ----------

def test_compose_bridge_narrows_module_to_deployment_subtype(
    tmp_path, fake_modules_root, empty_cross_links_root,
) -> None:
    deployment = _write_deployment(
        tmp_path, name="withbridge",
        modules=["git_graph"],
        nodes_yaml=_DEPLOYMENT_NODES,
        bridge_yaml=_BRIDGE_OK,
    )
    composed = compose_catalog(deployment, modules_root=fake_modules_root, cross_links_root=empty_cross_links_root)

    # Deployment-private subtype is in the union.
    assert "book" in composed.subtypes
    assert composed.subtype_origin["book"] == "deployment:withbridge"

    # The bridge's narrowing surfaces.
    assert ("source_file", "references", "book") in (
        composed.narrowing_triples()
    )
    # And carries bridge origin.
    bridge_rows = [
        r for r in composed.narrowings if r.origin.startswith("bridge:")
    ]
    assert len(bridge_rows) == 1
    assert bridge_rows[0].origin == "bridge:test_bridge.yaml"

    # bridge_paths is populated for audit.
    assert len(composed.bridge_paths) == 1
    assert composed.bridge_paths[0].name == "test_bridge.yaml"


@pytest.mark.parametrize(
    "relative_path",
    [
        Path("misplaced.yaml"),
        Path("nested/misplaced.yaml"),
        Path("bridges/misplaced.yml"),
    ],
)
def test_compose_refuses_misplaced_narrowing_schema(
    tmp_path: Path,
    fake_modules_root: Path,
    empty_cross_links_root: Path,
    relative_path: Path,
) -> None:
    deployment = _write_deployment(
        tmp_path,
        name="misplaced",
        modules=["git_graph"],
    )
    schema_dir = deployment / "schemas"
    schema_path = schema_dir / relative_path
    schema_path.parent.mkdir(parents=True, exist_ok=True)
    schema_path.write_text(_BRIDGE_OK)

    with pytest.raises(NarrowingSchemaMisplaced) as exc:
        compose_catalog(
            deployment,
            modules_root=fake_modules_root,
            cross_links_root=empty_cross_links_root,
        )

    expected_path = schema_dir / "bridges" / "misplaced.yaml"
    assert exc.value.code == "narrowing_schema_misplaced"
    assert exc.value.data == {
        "schema_path": str(schema_path),
        "expected_path": str(expected_path),
        "schema_dir": str(schema_dir),
    }
    assert "instead of overwriting" in (exc.value.fix_hint or "")


def test_custom_schema_dir_and_resolved_manifest_refuse_misplaced_narrowing(
    tmp_path: Path,
    fake_modules_root: Path,
    empty_cross_links_root: Path,
) -> None:
    deployment = _write_deployment(
        tmp_path,
        name="custom-schema-dir",
        modules=["git_graph"],
        schema_dir_name="catalog/schema",
    )
    schema_dir = deployment / "catalog" / "schema"
    misplaced = schema_dir / "misplaced.yaml"
    misplaced.write_text(_BRIDGE_OK)
    manifest = DeploymentManifest(
        name="custom-schema-dir",
        description="",
        deployment_dir=deployment,
        schema_dir=schema_dir,
        source_registry=deployment / "source_registry.yaml",
        enricher_registry=None,
        taxonomy=None,
        aliases=None,
        modules=("git_graph",),
    )

    for deployment_input in (deployment, manifest):
        with pytest.raises(NarrowingSchemaMisplaced) as exc:
            compose_catalog(
                deployment_input,
                modules_root=fake_modules_root,
                cross_links_root=empty_cross_links_root,
            )
        assert exc.value.data["schema_dir"] == str(schema_dir)
        assert exc.value.data["schema_path"] == str(misplaced)


def test_narrowing_placement_uses_lexical_file_symlink_location(
    tmp_path: Path,
    fake_modules_root: Path,
    empty_cross_links_root: Path,
) -> None:
    target = tmp_path / "bridge-target.yaml"
    target.write_text(_BRIDGE_OK)
    canonical = _write_deployment(
        tmp_path,
        name="canonical-symlink",
        modules=["git_graph"],
        nodes_yaml=_DEPLOYMENT_NODES,
    )
    canonical_bridges = canonical / "schemas" / "bridges"
    canonical_bridges.mkdir()
    assert canonical_bridges.is_dir()
    assert not canonical_bridges.is_symlink()
    (canonical_bridges / "linked.yaml").symlink_to(target)

    composed = compose_catalog(
        canonical,
        modules_root=fake_modules_root,
        cross_links_root=empty_cross_links_root,
    )
    assert ("source_file", "references", "book") in (
        composed.narrowing_triples()
    )

    misplaced = _write_deployment(
        tmp_path,
        name="misplaced-symlink",
        modules=["git_graph"],
    )
    misplaced_link = misplaced / "schemas" / "linked.yaml"
    misplaced_link.symlink_to(target)
    with pytest.raises(NarrowingSchemaMisplaced) as exc:
        compose_catalog(
            misplaced,
            modules_root=fake_modules_root,
            cross_links_root=empty_cross_links_root,
        )
    assert exc.value.data["schema_path"] == str(misplaced_link)


def test_narrowing_placement_does_not_traverse_directory_symlinks(
    tmp_path: Path,
    fake_modules_root: Path,
    empty_cross_links_root: Path,
) -> None:
    external_dir = tmp_path / "external-schemas"
    external_dir.mkdir()
    (external_dir / "hidden.yaml").write_text(_BRIDGE_OK)
    deployment = _write_deployment(
        tmp_path,
        name="directory-symlink",
        modules=["git_graph"],
    )
    (deployment / "schemas" / "linked").symlink_to(
        external_dir,
        target_is_directory=True,
    )

    composed = compose_catalog(
        deployment,
        modules_root=fake_modules_root,
        cross_links_root=empty_cross_links_root,
    )
    assert ("source_file", "references", "book") not in (
        composed.narrowing_triples()
    )


def test_narrowing_placement_refuses_symlinked_bridges_without_target_read(
    tmp_path: Path,
    fake_modules_root: Path,
    empty_cross_links_root: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    external_dir = tmp_path / "external-bridges"
    external_dir.mkdir()
    (external_dir / "external.yaml").write_text(_BRIDGE_OK)
    deployment = _write_deployment(
        tmp_path,
        name="symlinked-bridges",
        modules=["git_graph"],
        nodes_yaml=_DEPLOYMENT_NODES,
    )
    schema_dir = deployment / "schemas"
    bridges_path = schema_dir / "bridges"
    bridges_path.symlink_to(external_dir, target_is_directory=True)
    external_path = external_dir / "external.yaml"
    external_lexical_path = bridges_path / "external.yaml"
    original_glob = Path.glob
    original_open = Path.open

    def guarded_glob(path: Path, pattern: str):
        if path == bridges_path:
            pytest.fail("symlinked bridge target was listed")
        return original_glob(path, pattern)

    def guarded_open(path: Path, *args, **kwargs):
        if path in {external_path, external_lexical_path}:
            pytest.fail("symlinked bridge target was read")
        return original_open(path, *args, **kwargs)

    monkeypatch.setattr(Path, "glob", guarded_glob)
    monkeypatch.setattr(Path, "open", guarded_open)

    with pytest.raises(NarrowingBridgeDirectorySymlink) as exc:
        compose_catalog(
            deployment,
            modules_root=fake_modules_root,
            cross_links_root=empty_cross_links_root,
        )

    assert exc.value.code == "narrowing_bridge_directory_symlink"
    assert exc.value.data == {
        "bridges_path": str(bridges_path),
        "schema_dir": str(schema_dir),
    }
    assert "replace" in (exc.value.fix_hint or "")
    assert "real directory" in (exc.value.fix_hint or "")


def test_narrowing_placement_refuses_broken_bridges_symlink(
    tmp_path: Path,
    fake_modules_root: Path,
    empty_cross_links_root: Path,
) -> None:
    deployment = _write_deployment(
        tmp_path,
        name="broken-bridges-symlink",
        modules=["git_graph"],
    )
    schema_dir = deployment / "schemas"
    bridges_path = schema_dir / "bridges"
    bridges_path.symlink_to(
        tmp_path / "missing-bridges-target",
        target_is_directory=True,
    )

    with pytest.raises(NarrowingBridgeDirectorySymlink) as exc:
        compose_catalog(
            deployment,
            modules_root=fake_modules_root,
            cross_links_root=empty_cross_links_root,
        )

    assert exc.value.data == {
        "bridges_path": str(bridges_path),
        "schema_dir": str(schema_dir),
    }


def test_narrowing_placement_reports_first_path_and_collision_safe_hint(
    tmp_path: Path,
    fake_modules_root: Path,
    empty_cross_links_root: Path,
) -> None:
    deployment = _write_deployment(
        tmp_path,
        name="ordered-misplaced",
        modules=["git_graph"],
    )
    schema_dir = deployment / "schemas"
    bridges = schema_dir / "bridges"
    bridges.mkdir()
    (bridges / "same.yaml").write_text(_BRIDGE_OK)
    first = schema_dir / "a" / "same.yaml"
    first.parent.mkdir()
    first.write_text(_BRIDGE_OK)
    (schema_dir / "same.yaml").write_text(_BRIDGE_OK)

    with pytest.raises(NarrowingSchemaMisplaced) as exc:
        compose_catalog(
            deployment,
            modules_root=fake_modules_root,
            cross_links_root=empty_cross_links_root,
        )

    assert exc.value.data["schema_path"] == str(first)
    assert exc.value.data["expected_path"] == str(bridges / "same.yaml")
    assert "instead of overwriting" in (exc.value.fix_hint or "")


@pytest.mark.postgres
def test_canonical_bridge_narrowing_publishes_expected_edge(
    clean_runtime: str,
    tmp_path: Path,
    fake_modules_root: Path,
    empty_cross_links_root: Path,
    capsys,
) -> None:
    import psycopg

    from okg.substrate.catalog.modules_apply import apply_composed_catalog
    from okg.substrate.consistency import TransformConsistencyGate
    from okg.substrate.graph_store import ChronosGraphStore
    from okg.substrate.ingest import SourceRunner
    from okg.substrate.library.sources.base import EdgeFact, NodeFact, SourceRun
    from okg.substrate.publish_proof import assert_publish_proof
    from okg.substrate.publisher import Publisher
    from tests.pytest.catalog_fixture_builders import bind_test_catalog_writer  # noqa: E402

    deployment = _write_deployment(
        tmp_path,
        name="default",
        modules=["git_graph"],
        nodes_yaml=_DEPLOYMENT_NODES,
        bridge_yaml=_BRIDGE_OK,
    )
    composed = compose_catalog(
        deployment,
        modules_root=fake_modules_root,
        cross_links_root=empty_cross_links_root,
    )
    assert ("source_file", "references", "book") in (
        composed.narrowing_triples()
    )
    with psycopg.connect(clean_runtime) as conn:
        apply_composed_catalog(conn, composed)

    publisher = lambda: Publisher(  # noqa: E731 - compact identical proof stack.
        clean_runtime,
        "default",
        consistency_gates=[TransformConsistencyGate()],
        catalog_identity=bind_test_catalog_writer(
            clean_runtime,
            "default",
        ),
    )
    baseline = publisher().cycle()
    assert baseline["status"] == "published", baseline

    class _CanonicalBridgeSource:
        name = "issue-1282-canonical-bridge"
        profile = "discovery_crawl"

        def run(self, run_id: str, *, mode: str = "scope_complete") -> SourceRun:
            del run_id
            return SourceRun(
                facts=[
                    NodeFact(
                        "file:issue-1282",
                        "source_file",
                        {"path": "issue-1282.md"},
                        {"id": "file:issue-1282"},
                        {"rev": 1},
                    ),
                    NodeFact(
                        "book:issue-1282",
                        "book",
                        {"title": "Issue 1282"},
                        {"id": "book:issue-1282"},
                        {"rev": 1},
                    ),
                    EdgeFact(
                        "file:issue-1282",
                        "book:issue-1282",
                        "references",
                        source_record_id={"id": "edge:issue-1282"},
                        source_revision={"rev": 1},
                    ),
                ],
                completed_scope=True,
                next_cursor={"rev": 1},
                run_mode=mode,
            )

    ingest = SourceRunner(
        clean_runtime,
        source_name="issue-1282-canonical-bridge",
    ).run(_CanonicalBridgeSource(), mode="scope_complete")
    assert ingest.nodes_appended == 2
    assert ingest.edges_appended == 1

    published = publisher().cycle()
    assert published["status"] == "published", published
    assert published["nodes_added"] == 2
    assert published["edges_added"] == 1
    generation_id = str(published["generation_id"])
    baseline_generation_id = str(baseline["generation_id"])

    store = ChronosGraphStore.connect(clean_runtime, ensure_schema=False)
    try:
        before = store.checkout_generation(baseline_generation_id).query(
            "SELECT count(*) AS n FROM okg.graph_edges"
        )
        after_checkout = store.checkout_generation(generation_id)
        after = after_checkout.query(
            "SELECT count(*) AS n FROM okg.graph_edges"
        )
        edge_rows = after_checkout.query(
            "SELECT src, edge_type, dst FROM okg.graph_edges "
            "WHERE src = 'file:issue-1282' "
            "AND edge_type = 'references' "
            "AND dst = 'book:issue-1282'"
        )
    finally:
        store.close()

    before_edges = int(before[0]["n"])
    after_edges = int(after[0]["n"])
    assert after_edges - before_edges == 1
    assert edge_rows == [{
        "src": "file:issue-1282",
        "edge_type": "references",
        "dst": "book:issue-1282",
    }]

    readiness = assert_publish_proof(published, dsn=clean_runtime)
    assert readiness["ok"] is True, readiness
    assert readiness["dlq_count"] == 0
    assert all(
        int(value or 0) == 0
        for value in readiness["queue_depth"].values()
    ), readiness

    from okg.substrate.cli.main import main

    doctor_rc = main(["--json", "doctor", "--dsn", clean_runtime])
    doctor = json.loads(capsys.readouterr().out.strip().splitlines()[-1])
    assert doctor_rc == 0, doctor
    assert doctor["ok"] is True, doctor

    graph_report = {
        "baseline_generation_id": baseline_generation_id,
        "generation_id": generation_id,
        "generation_status": published["status"],
        "before_edges": before_edges,
        "after_edges": after_edges,
        "edge_delta": after_edges - before_edges,
        "edge": edge_rows[0],
        "queue_depth": readiness["queue_depth"],
        "dlq_count": readiness["dlq_count"],
        "doctor": "clean",
        "readiness": "clean",
    }
    print(f"ISSUE1282_GRAPH_REPORT={json.dumps(graph_report, sort_keys=True)}")


# ---------- compose: failure modes ----------

def test_compose_missing_module_raises_before_any_work(
    tmp_path, fake_modules_root, empty_cross_links_root,
) -> None:
    deployment = _write_deployment(
        tmp_path, name="missing", modules=["does_not_exist"],
    )
    with pytest.raises(CatalogModuleMissing) as exc:
        compose_catalog(deployment, modules_root=fake_modules_root, cross_links_root=empty_cross_links_root)
    assert exc.value.data["module_name"] == "does_not_exist"
    assert "schema" in (exc.value.fix_hint or "").lower()


def test_compose_bridge_unknown_subtype_raises(
    tmp_path, fake_modules_root, empty_cross_links_root,
) -> None:
    deployment = _write_deployment(
        tmp_path, name="badbridge",
        modules=["git_graph"],
        bridge_yaml=_BRIDGE_BAD,
    )
    with pytest.raises(BridgeSubtypeUnknown) as exc:
        compose_catalog(deployment, modules_root=fake_modules_root, cross_links_root=empty_cross_links_root)
    # Identifies the offending subtype + bridge.
    assert exc.value.data["subtype_name"] == "galaxy"
    assert exc.value.data["bridge_path"].startswith("bridge:")


def test_compose_aliases_and_identifier_patterns(
    tmp_path, fake_modules_root, empty_cross_links_root,
) -> None:
    """aliases.json and identifier_patterns.yaml are passed through."""
    deployment = _write_deployment(
        tmp_path, name="extras",
        modules=["git_graph"],
        aliases_json={"agf": "change:add-greeting-feature"},
        identifier_patterns_yaml=textwrap.dedent("""\
            patterns:
              - regex: "DOI:[0-9.]+/[A-Za-z0-9.]+"
                id_type: doi
        """),
    )
    composed = compose_catalog(deployment, modules_root=fake_modules_root, cross_links_root=empty_cross_links_root)
    assert composed.aliases == [
        {"alias": "agf", "canonical": "change:add-greeting-feature"},
    ]
    assert composed.identifier_patterns == [
        {
            "id_type": "doi",
            "regex": "DOI:[0-9.]+/[A-Za-z0-9.]+",
            "validator_kind": None,
            "description": None,
        },
    ]


# ---------- compose: expand_dst_by_archetype affordance ----------


_EXPAND_DST_MODULE = textwrap.dedent("""\
    id: https://example.com/okg/modules/expand_dst_demo
    name: expand_dst_demo
    classes:
      Critique:
        annotations:
          archetype: claim
        attributes:
          critique_id: {required: true}
      Note:
        annotations:
          archetype: artifact
        attributes:
          note_id: {required: true}
      Hypothesis:
        annotations:
          archetype: claim
        attributes:
          hypothesis_id: {required: true}
      Person:
        annotations:
          archetype: actor
        attributes:
          person_id: {required: true}
      EdgeNarrowings:
        annotations:
          okg_edge_narrowings: "true"
        attributes:
          critique_critiques_artifact_or_claim:
            annotations:
              edge_archetype: critiques
              src_subtypes: Critique
              expand_dst_by_archetype: artifact,claim
""")


def test_compose_expands_dst_by_archetype_to_artifact_and_claim_subtypes(
    tmp_path, empty_cross_links_root,
) -> None:
    """S-A7-R8: an EdgeNarrowings attribute carrying
    `expand_dst_by_archetype: <archetypes>` expands at compose time
    to every loaded subtype whose `archetype` annotation matches.
    The `Critique critiques <artifact|claim>` narrowing in the
    pre-join MCP framework spec uses this affordance.
    """
    root = tmp_path / "ontologies"
    root.mkdir()
    (root / "expand_dst_demo").mkdir()
    (root / "expand_dst_demo" / "schema.yaml").write_text(
        _EXPAND_DST_MODULE,
    )
    deployment = _write_deployment(
        tmp_path, name="expand_dst_test",
        modules=["expand_dst_demo"],
    )
    composed = compose_catalog(
        deployment,
        modules_root=root,
        cross_links_root=empty_cross_links_root,
    )

    triples = composed.narrowing_triples()
    # critique (claim) -> Critique itself; note (artifact) and
    # hypothesis (claim) become permitted dsts via expansion.
    assert ("critique", "critiques", "note") in triples
    assert ("critique", "critiques", "hypothesis") in triples
    assert ("critique", "critiques", "critique") in triples
    # Person is archetype=actor; the expansion does NOT include it.
    assert ("critique", "critiques", "person") not in triples


# ---------- compose: production library composes cleanly ----------

def test_compose_okg_self_uses_real_substrate_library(tmp_path) -> None:
    """The okg-workspace deployment in this repo composes against the
    real substrate library without raising. This is the canonical
    end-to-end smoke test for Phase 6."""
    repo_root = Path(__file__).resolve().parents[2]
    deployment_dir = repo_root / "deployments" / "okg-workspace"
    if not (deployment_dir / "deployment.yaml").is_file():
        pytest.skip("okg-workspace deployment not present at this commit")
    composed = compose_catalog(deployment_dir)
    # All four modules surface; substrate library has all four.
    assert set(composed.modules) >= {
        "git_graph", "openspec", "agent_sessions", "compute_env",
    }
    # Composition is non-empty.
    assert len(composed.subtypes) > 0
    assert len(composed.narrowings) > 0
    assert "pact_change" in composed.subtypes
    assert "pact_task" in composed.subtypes
    triples = composed.narrowing_triples()
    assert ("pact_change", "contains", "spec_delta") in triples
    assert ("pact_change", "contains", "task_group") in triples
    assert ("acceptance_criterion", "contains", "verification_step") in triples
    assert ("task_group", "contains", "pact_task") in triples
    assert ("pact_task", "contains", "verification_step") in triples
    assert ("pact_task", "blocks", "pact_task") in triples
    assert ("pact_task", "requires", "pact_task") in triples
    assert ("pact_task", "enables", "pact_task") in triples
    assert ("pact_task", "supersedes", "pact_task") in triples
    assert ("pact_task", "conflicts_with", "pact_task") in triples
    assert ("proposal_task", "blocks", "proposal_task") in triples
    assert ("proposal_task", "requires", "proposal_task") in triples
    assert ("proposal_task", "enables", "proposal_task") in triples
    assert ("proposal_task", "supersedes", "proposal_task") in triples
    assert ("proposal_task", "conflicts_with", "proposal_task") in triples


# ---------- CLI subcommand: end-to-end (no DB) ----------

def test_catalog_input_resolves_active_sealed_environment(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from okg.substrate.cli.catalog import _resolve_catalog_input

    manifest = SimpleNamespace(
        deployment_dir=tmp_path,
        postgres_dsn="postgresql://staging.example/okg",
    )
    composed = SimpleNamespace(deployment_name="fixture-staging")
    monkeypatch.setattr(
        "okg.substrate.deployments.resolve_deployment",
        lambda name: manifest,
    )
    monkeypatch.setattr(
        "okg.substrate.catalog.modules_compose.compose_catalog",
        lambda target: composed,
    )

    actual, deployment_dir, dsn, writer_scope, environment_id = (
        _resolve_catalog_input(
        argparse.Namespace(deployment="fixture-staging", dsn=None)
        )
    )

    assert actual is composed
    assert deployment_dir == tmp_path
    assert dsn == manifest.postgres_dsn
    assert writer_scope == "deployment_root"
    assert environment_id is None


def test_cli_catalog_load_emits_summary(
    tmp_path, fake_modules_root, empty_cross_links_root, capsys, monkeypatch,
) -> None:
    """`okg catalog load --deployment <dir> --json` returns a
    summary on stdout when called via main()."""
    deployment = _write_deployment(
        tmp_path, name="cli", modules=["git_graph"],
    )
    monkeypatch.setattr(
        "okg.substrate.catalog.modules_compose.load_modules_root",
        lambda: fake_modules_root,
    )

    from okg.substrate.cli.main import main

    rc = main([
        "catalog", "load",
        "--deployment", str(deployment),
        "--json",
    ])
    out = capsys.readouterr().out
    assert rc == 0
    payload = json.loads(out)
    assert payload["deployment"] == "cli"
    assert payload["modules"] == ["git_graph"]
    assert payload["subtype_count"] == 2
    assert payload["narrowing_count"] == 1
    assert payload["applied"] is False


def test_cli_catalog_load_apply_without_dsn_errors_clearly(
    tmp_path, fake_modules_root, empty_cross_links_root, capsys, monkeypatch,
) -> None:
    """`--apply` requires --dsn or $OKG_DSN; missing DSN must
    surface a structured error pointing the operator at both
    flags."""
    deployment = _write_deployment(
        tmp_path, name="apply_test", modules=["git_graph"],
    )
    monkeypatch.setattr(
        "okg.substrate.catalog.modules_compose.load_modules_root",
        lambda: fake_modules_root,
    )
    monkeypatch.delenv("OKG_DSN", raising=False)

    from okg.substrate.cli.main import main

    rc = main([
        "catalog", "load",
        "--deployment", str(deployment),
        "--apply", "--json",
    ])
    err = capsys.readouterr().err
    assert rc != 0
    payload = json.loads(err)
    assert "DSN" in payload["error"]["message"]


def test_cli_catalog_load_missing_module_returns_error_envelope(
    tmp_path, fake_modules_root, empty_cross_links_root, capsys, monkeypatch,
) -> None:
    deployment = _write_deployment(
        tmp_path, name="missing_cli", modules=["nope"],
    )
    monkeypatch.setattr(
        "okg.substrate.catalog.modules_compose.load_modules_root",
        lambda: fake_modules_root,
    )

    from okg.substrate.cli.main import main

    rc = main([
        "catalog", "load",
        "--deployment", str(deployment),
        "--json",
    ])
    err = capsys.readouterr().err
    assert rc != 0
    payload = json.loads(err)
    assert payload["error"]["code"] == "catalog_module_missing"
    assert payload["error"]["data"]["module_name"] == "nope"
