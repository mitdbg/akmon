"""Structural attribute-completeness check.

Per `attribute-completeness-sweep` (2026-04-26). Walks every
ontology schema referenced by the okg-workspace deployment, builds
a `subtype → set[required_attr]` map, runs each source
adapter against the okg_self_micro fixture, and asserts
every yielded NodeFact carries every required attr non-None.

Catches B29 (acceptance_criterion.scenario_count) and B37
(task_group.group_index) plus any other declared-required-
but-unpopulated attrs in one pass.
"""

from __future__ import annotations

import re
import sys
from pathlib import Path

import pytest
import yaml


_FIXTURES = Path(__file__).resolve().parent / "fixtures"
if str(_FIXTURES) not in sys.path:
    sys.path.insert(0, str(_FIXTURES))


_ONTOLOGY_DIR = (
    Path(__file__).resolve().parents[2]
    / "src"
    / "okg"
    / "substrate"
    / "library"
    / "ontologies"
)


def _camel_to_snake(name: str) -> str:
    """`AcceptanceCriterion` → `acceptance_criterion`."""
    s = re.sub(r"(.)([A-Z][a-z]+)", r"\1_\2", name)
    s = re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", s)
    return s.lower()


def _load_required_attr_map() -> dict[str, dict[str, str | None]]:
    """subtype_name → {attr_name: gating_attr_or_None}.

    An attr marked `required: true` is unconditionally required
    (gating_attr is None). An attr that also declares
    `annotations.okg_required_when: <attr>` is *conditionally*
    required: required only on nodes where the gating attr is
    truthy. This encodes semantics like git_graph SourceFile's
    git_blob_sha / size_bytes / is_text, which only exist for a
    file present in the current worktree (current_in_worktree=true);
    a history-only stub for a deleted path legitimately has no blob.
    """
    out: dict[str, dict[str, str | None]] = {}
    for schema_path in sorted(_ONTOLOGY_DIR.glob("*/schema.yaml")):
        doc = yaml.safe_load(schema_path.read_text()) or {}
        classes = doc.get("classes") or {}
        for class_name, cls in classes.items():
            attrs = (cls or {}).get("attributes") or {}
            for attr_name, attr in attrs.items():
                if not (isinstance(attr, dict) and attr.get("required") is True):
                    continue
                annotations = attr.get("annotations") or {}
                gating_attr = annotations.get("okg_required_when")
                gating_attr = str(gating_attr) if gating_attr else None
                subtype = _camel_to_snake(class_name)
                out.setdefault(subtype, {})[attr_name] = gating_attr
    return out


@pytest.fixture(scope="module")
def fixture_paths(tmp_path_factory):
    from okg_self_micro import materialize  # type: ignore

    dest = tmp_path_factory.mktemp("attr_hygiene")
    return materialize(dest)


def _collect_emitted_nodes(fixture_paths) -> list:
    """Run each okg-workspace source adapter against the fixture
    and return all yielded NodeFact-shaped records (domain
    `NodeSpec` from modules layer)."""
    from okg.substrate.library.sources._openspec_emit import OpenSpecSource
    from okg.substrate.library.sources._git_graph_history_emit import GitHistorySource
    from okg.substrate.library.sources._git_graph_files_emit import GitFilesSource

    nodes: list = []
    nodes.extend(
        OpenSpecSource(
            openspec_root=fixture_paths.openspec_root,
            repo="okg",
            include_archived=True,
        ).nodes()
    )
    nodes.extend(
        GitHistorySource(
            repo_root=fixture_paths.repo_root,
            repo="okg",
        ).nodes()
    )
    nodes.extend(
        GitFilesSource(
            repo_root=fixture_paths.repo_root,
            repo="okg",
        ).nodes()
    )
    return nodes


def test_every_required_attr_is_populated(fixture_paths) -> None:
    required_by_subtype = _load_required_attr_map()
    nodes = _collect_emitted_nodes(fixture_paths)

    # subtype → list[(node_id, missing_attrs)]
    failures: dict[str, list[tuple[str, list[str]]]] = {}
    seen_subtypes: set[str] = set()
    for n in nodes:
        seen_subtypes.add(n.subtype)
        required = required_by_subtype.get(n.subtype)
        if not required:
            continue
        missing = []
        for attr, gating_attr in required.items():
            # Conditionally-required attrs only apply when their gating
            # attr is truthy on this node (e.g. git_blob_sha is required
            # only when current_in_worktree is true).
            if gating_attr is not None and not n.attrs.get(gating_attr):
                continue
            if n.attrs.get(attr) is None:
                missing.append(attr)
        if missing:
            failures.setdefault(n.subtype, []).append(
                (n.node_id, sorted(missing)),
            )

    if failures:
        report = []
        for subtype in sorted(failures):
            report.append(f"\n{subtype}:")
            for node_id, missing in failures[subtype][:5]:
                report.append(f"  {node_id}: missing {missing}")
            n_total = len(failures[subtype])
            if n_total > 5:
                report.append(f"  … and {n_total - 5} more {subtype} nodes")
        raise AssertionError("Nodes missing required attrs:" + "".join(report))

    # Sanity: the fixture should exercise the subtypes B29 + B37
    # care about, otherwise the test isn't actually checking
    # the cases the proposal cites.
    assert "acceptance_criterion" in seen_subtypes, (
        "fixture didn't yield any acceptance_criterion nodes; "
        "check the openspec source / fixture changes"
    )
    assert "task_group" in seen_subtypes, "fixture didn't yield any task_group nodes"


def _candidate_openspec_schemas() -> list[Path]:
    """Return existing openspec/schema.yaml paths the substrate
    should pin. The pre-substrate path
    (`src/okg/modules/openspec/schema.yaml`) was removed by the
    rearchitect-on-postgres branch; check it only if present so
    repos in mid-migration still get covered."""
    candidates = [
        _ONTOLOGY_DIR / "openspec" / "schema.yaml",
        Path(__file__).resolve().parents[2]
        / "src"
        / "okg"
        / "modules"
        / "openspec"
        / "schema.yaml",
    ]
    return [p for p in candidates if p.is_file()]


def test_scenario_count_marked_required_in_schemas() -> None:
    """B29: schema declaration is the contract; the test
    above checks emit. This test pins the schema flip so a
    later edit doesn't silently revert it."""
    schemas = _candidate_openspec_schemas()
    assert schemas, "no openspec schema.yaml found at any known path"
    for schema_path in schemas:
        doc = yaml.safe_load(schema_path.read_text())
        attrs = doc["classes"]["AcceptanceCriterion"]["attributes"]
        assert attrs["scenario_count"].get("required") is True, (
            f"{schema_path}: AcceptanceCriterion.scenario_count must be required: true"
        )


def test_group_index_marked_required_in_schemas() -> None:
    """B37: schema declaration check (already required;
    this test pins it)."""
    schemas = _candidate_openspec_schemas()
    assert schemas, "no openspec schema.yaml found at any known path"
    for schema_path in schemas:
        doc = yaml.safe_load(schema_path.read_text())
        for class_name in ("TaskGroup", "ProposalTask"):
            attrs = doc["classes"][class_name]["attributes"]
            assert attrs["group_index"].get("required") is True, (
                f"{schema_path}: {class_name}.group_index must be required: true"
            )
