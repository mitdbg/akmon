"""Substrate-shape parity tests for `OpenSpecSource` (Phase 3 of
`migrate-okg-self-to-substrate`).

The fixture corpus ships with one active change
(`add-greeting-feature`), one archived change
(`archive/add-formal-greeting`), and one canonical capability
spec (`specs/greetings/spec.md`). These tests assert:

  - SourceAdapter Protocol fields are populated.
  - cursor mode emits NodeFact for each change + capability.
  - Both critical change ids surface (active + archived).
  - The canonical capability node `cap:greetings` surfaces.
  - Substrate node_id set matches the domain emit set.
  - next_cursor carries file_hashes; second cursor-mode run
    short-circuits.
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest


_FIXTURES_DIR = Path(__file__).resolve().parents[2] / "fixtures"
if str(_FIXTURES_DIR) not in sys.path:
    sys.path.insert(0, str(_FIXTURES_DIR))


@pytest.fixture
def fixture_paths(tmp_path):
    from okg_self_micro import materialize  # type: ignore

    return materialize(tmp_path)


def test_adapter_protocol_fields_populated() -> None:
    from okg.substrate.library.sources.openspec import OpenSpecSource

    src = OpenSpecSource(repo="okg", openspec_root=Path("/tmp"))
    assert src.name == "openspec"
    assert src.profile == "discovery_crawl"


def test_adapter_emits_critical_change_nodes(fixture_paths) -> None:
    """Both fixture-pinned change ids appear in the first cursor run."""
    from okg_self_micro import CRITICAL_NODE_IDS  # type: ignore
    from okg.substrate.library.sources.openspec import OpenSpecSource

    src = OpenSpecSource(
        repo="okg",
        openspec_root=fixture_paths.openspec_root,
    )
    run = src.run("run-1", mode="cursor")
    emitted_ids = {f.node_id for f in run.facts if hasattr(f, "node_id")}

    expected_changes = {nid for nid in CRITICAL_NODE_IDS if nid.startswith("change:")}
    assert expected_changes  # sanity: fixture lists at least one
    missing = expected_changes - emitted_ids
    assert not missing, (
        f"adapter did not emit critical change ids: {sorted(missing)}; "
        f"emitted: {sorted(emitted_ids)[:15]}"
    )


def test_adapter_emits_canonical_capability(fixture_paths) -> None:
    """`specs/greetings/spec.md` surfaces as a `cap:greetings` node."""
    from okg.substrate.library.sources.openspec import OpenSpecSource

    src = OpenSpecSource(
        repo="okg",
        openspec_root=fixture_paths.openspec_root,
    )
    run = src.run("run-1", mode="cursor")
    emitted_ids = {f.node_id for f in run.facts if hasattr(f, "node_id")}
    assert "cap:greetings" in emitted_ids


def test_adapter_source_record_ids_match_node_identity_contract(
    fixture_paths,
) -> None:
    from okg.substrate.library.sources.base import EdgeFact, NodeFact
    from okg.substrate.library.sources.openspec import OpenSpecSource

    src = OpenSpecSource(
        repo="okg",
        openspec_root=fixture_paths.openspec_root,
    )
    facts = list(src.run("run-1", mode="scope_complete").facts)

    openspec_facts = [fact for fact in facts if isinstance(fact, (NodeFact, EdgeFact))]
    assert openspec_facts
    assert all("repo" in fact.source_record_id for fact in openspec_facts)
    assert all("node_id" in fact.source_record_id for fact in openspec_facts)
    assert all(
        fact.source_record_id["node_id"] == fact.node_id
        for fact in openspec_facts
        if isinstance(fact, NodeFact)
    )
    assert all(
        {"src", "dst", "edge_type"} <= set(fact.source_record_id)
        for fact in openspec_facts
        if isinstance(fact, EdgeFact)
    )


def test_scoped_run_emits_only_affected_change(fixture_paths) -> None:
    from okg.substrate.library.sources.openspec import OpenSpecSource

    src = OpenSpecSource(
        repo="okg",
        openspec_root=fixture_paths.openspec_root,
    )
    run = src.run(
        "run-scoped",
        mode="scope_incomplete",
        sync_scope={
            "scope_keys": ["changes/add-greeting-feature/proposal.md"],
        },
    )
    emitted_ids = {f.node_id for f in run.facts if hasattr(f, "node_id")}

    assert "change:add-greeting-feature" in emitted_ids
    assert "change:add-formal-greeting" not in emitted_ids
    assert "cap:greetings" not in emitted_ids
    assert run.completed_scope is False
    assert run.record_set is None


def test_unrecognized_scoped_run_noops_and_preserves_cursor(fixture_paths) -> None:
    from okg.substrate.library.sources.openspec import OpenSpecSource

    src = OpenSpecSource(
        repo="okg",
        openspec_root=fixture_paths.openspec_root,
    )
    cursor = {
        "file_hashes": {"add-greeting-feature": {"proposal.md": "old"}},
        "record_set": {"add-greeting-feature": {}},
        "record_set_ref": {"storage": "okg.source_record_set_blobs", "sha256": "abc"},
    }
    run = src.run(
        "run-scoped-noop",
        mode="scope_incomplete",
        cursor=cursor,
        sync_scope={"scope_keys": ["project.md"]},
    )

    assert list(run.facts) == []
    assert run.completed_scope is False
    assert run.record_set is None
    assert run.next_cursor == {
        "file_hashes": {"add-greeting-feature": {"proposal.md": "old"}},
        "record_set_ref": {"storage": "okg.source_record_set_blobs", "sha256": "abc"},
    }
    assert src.last_run_stats["scope_status"] == "ignored_scope_noop"


def test_scoped_run_preserves_unrelated_file_hashes(fixture_paths) -> None:
    from okg.substrate.library.sources.openspec import OpenSpecSource

    src = OpenSpecSource(
        repo="okg",
        openspec_root=fixture_paths.openspec_root,
    )
    baseline = src.run("baseline", mode="cursor")
    list(baseline.facts)
    baseline_hashes = dict((baseline.next_cursor or {})["file_hashes"])
    assert "add-greeting-feature" in baseline_hashes
    assert "add-formal-greeting" in baseline_hashes
    assert "cap:greetings" in baseline_hashes

    tasks_path = (
        fixture_paths.openspec_root / "changes" / "add-greeting-feature" / "tasks.md"
    )
    tasks_path.write_text(
        tasks_path.read_text() + "\n- [ ] 9.9 Scoped cursor probe.\n",
        encoding="utf-8",
    )

    scoped = src.run(
        "scoped-cursor",
        mode="scope_incomplete",
        cursor=baseline.next_cursor,
        sync_scope={"scope_keys": ["changes/add-greeting-feature/tasks.md"]},
    )
    list(scoped.facts)

    next_hashes = (scoped.next_cursor or {})["file_hashes"]
    assert next_hashes["add-greeting-feature"] != baseline_hashes[
        "add-greeting-feature"
    ]
    assert next_hashes["add-formal-greeting"] == baseline_hashes[
        "add-formal-greeting"
    ]
    assert next_hashes["cap:greetings"] == baseline_hashes["cap:greetings"]
    assert src.last_run_stats["scope_status"] == "scoped"
    assert src.last_run_stats["recognized_records"] == 1


def test_scoped_complete_merges_prior_record_set(fixture_paths) -> None:
    from okg.substrate.library.sources.openspec import OpenSpecSource

    src = OpenSpecSource(
        repo="okg",
        openspec_root=fixture_paths.openspec_root,
    )
    baseline = src.run("baseline", mode="scope_complete")
    assert baseline.record_set is not None
    cursor = dict(baseline.next_cursor or {})
    cursor["record_set"] = baseline.record_set.to_cursor()

    tasks_path = (
        fixture_paths.openspec_root / "changes" / "add-greeting-feature" / "tasks.md"
    )
    tasks_path.write_text(
        tasks_path.read_text() + "\n- [ ] 9.9 Scoped record-set probe.\n",
        encoding="utf-8",
    )
    scoped = src.run(
        "scoped-complete",
        mode="scope_complete",
        cursor=cursor,
        sync_scope={
            "scope_keys": ["changes/add-greeting-feature/tasks.md"],
        },
    )

    assert scoped.completed_scope is True
    assert scoped.record_set is not None
    assert set(baseline.record_set.records) <= set(scoped.record_set.records)
    assert scoped.record_set.records["add-greeting-feature"].primary is not None
    assert "record_set" not in (scoped.next_cursor or {})


def test_scoped_complete_removes_deleted_change_record(fixture_paths) -> None:
    import shutil

    from okg.substrate.library.sources.openspec import OpenSpecSource

    src = OpenSpecSource(
        repo="okg",
        openspec_root=fixture_paths.openspec_root,
    )
    baseline = src.run("baseline", mode="scope_complete")
    assert baseline.record_set is not None
    cursor = dict(baseline.next_cursor or {})
    cursor["record_set"] = baseline.record_set.to_cursor()

    change_dir = fixture_paths.openspec_root / "changes" / "add-greeting-feature"
    shutil.rmtree(change_dir)
    scoped = src.run(
        "delete-change",
        mode="scope_complete",
        cursor=cursor,
        sync_scope={
            "scope_keys": ["changes/add-greeting-feature/proposal.md"],
        },
    )

    assert scoped.completed_scope is True
    assert scoped.record_set is not None
    assert "add-greeting-feature" not in scoped.record_set.records
    assert "add-formal-greeting" in scoped.record_set.records
    assert "add-greeting-feature" not in (
        scoped.next_cursor or {}
    ).get("file_hashes", {})
    assert "record_set" not in (scoped.next_cursor or {})


def test_scope_complete_with_external_record_set_ref_preserves_no_change(
    fixture_paths,
    monkeypatch,
) -> None:
    from okg.substrate.library.sources import openspec as openspec_module
    from okg.substrate.library.sources.openspec import OpenSpecSource

    src = OpenSpecSource(
        repo="okg",
        openspec_root=fixture_paths.openspec_root,
    )
    baseline = src.run("baseline", mode="scope_complete")
    list(baseline.facts)
    assert baseline.next_cursor is not None

    def fail_record_set_rebuild(*args, **kwargs):  # noqa: ANN002, ANN003
        raise AssertionError("unchanged external record_set_ref run rebuilt")

    monkeypatch.setattr(
        openspec_module.OpenSpecSource,
        "_record_set_from_emit",
        fail_record_set_rebuild,
    )
    cursor = {
        **baseline.next_cursor,
        "record_set_ref": {
            "storage": "okg.source_record_set_blobs",
            "sha256": "abc",
        },
        "record_set_summary": {"record_count": 3},
    }

    unchanged = src.run("unchanged", mode="scope_complete", cursor=cursor)

    assert list(unchanged.facts) == []
    assert unchanged.completed_scope is True
    assert unchanged.record_set is None
    assert unchanged.next_cursor is not None
    assert unchanged.next_cursor["record_set_ref"] == cursor["record_set_ref"]
    assert unchanged.next_cursor["record_set_summary"] == cursor[
        "record_set_summary"
    ]


def test_openspec_no_change_preserves_record_set_authority(
    fixture_paths,
    monkeypatch,
) -> None:
    from okg.substrate.library.sources import openspec as openspec_module
    from okg.substrate.library.sources.openspec import OpenSpecSource

    src = OpenSpecSource(
        repo="okg",
        openspec_root=fixture_paths.openspec_root,
    )
    baseline = src.run("baseline", mode="scope_complete")
    list(baseline.facts)
    assert baseline.next_cursor is not None

    def fail_record_set_rebuild(*args, **kwargs):  # noqa: ANN002, ANN003
        raise AssertionError("no-change run rebuilt record-set authority")

    monkeypatch.setattr(
        openspec_module.OpenSpecSource,
        "_record_set_from_emit",
        fail_record_set_rebuild,
    )
    cursor = {
        **baseline.next_cursor,
        "record_set_ref": {
            "storage": "okg.source_record_set_blobs",
            "sha256": "abc",
        },
        "record_set_summary": {
            "record_count": len(baseline.record_set.records)
            if baseline.record_set else 0,
        },
    }

    unchanged = src.run("unchanged", mode="scope_complete", cursor=cursor)

    assert list(unchanged.facts) == []
    assert unchanged.completed_scope is True
    assert unchanged.record_set is None
    assert unchanged.next_cursor is not None
    assert unchanged.next_cursor["record_set_ref"] == cursor["record_set_ref"]
    assert unchanged.next_cursor["record_set_summary"] == cursor[
        "record_set_summary"
    ]
    assert src.last_run_stats["nodes_emitted"] == 0
    assert src.last_run_stats["edges_emitted"] == 0


def test_scope_complete_with_external_record_set_ref_replaces_changed_record(
    fixture_paths,
) -> None:
    from okg.substrate.library.sources.openspec import OpenSpecSource

    src = OpenSpecSource(
        repo="okg",
        openspec_root=fixture_paths.openspec_root,
    )
    baseline = src.run("baseline", mode="scope_complete")
    list(baseline.facts)
    assert baseline.next_cursor is not None
    assert baseline.record_set is not None

    cursor = {
        **baseline.next_cursor,
        "record_set_ref": {
            "storage": "okg.source_record_set_blobs",
            "sha256": "abc",
        },
        "record_set_summary": {
            "record_count": len(baseline.record_set.records),
        },
    }

    tasks_path = (
        fixture_paths.openspec_root / "changes" / "add-greeting-feature" / "tasks.md"
    )
    tasks_path.write_text(
        tasks_path.read_text() + "\n- [ ] 9.10 External record-set delta.\n",
        encoding="utf-8",
    )

    changed = src.run("changed", mode="scope_complete", cursor=cursor)

    assert changed.completed_scope is True
    assert changed.record_set is not None
    assert set(changed.record_set.records) == {"add-greeting-feature"}
    assert set(changed.record_set_replacement_keys or ()) == {
        "add-greeting-feature",
    }
    assert changed.next_cursor is not None
    assert "record_set_ref" not in changed.next_cursor


def test_changed_record_keys_include_emitted_records() -> None:
    from okg.substrate.library.sources.openspec import OpenSpecSource

    keys = OpenSpecSource._changed_record_keys(
        {"file_hashes": {"demo-change": {"proposal.md": "same"}}},
        current={"file_hashes": {"demo-change": {"proposal.md": "same"}}},
        emitted_keys={"demo-change"},
    )

    assert keys == {"demo-change"}


def test_adapter_subtypes_match_schema(fixture_paths) -> None:
    """Every emitted change carries subtype 'open_spec_change'."""
    from okg.substrate.library.sources.openspec import OpenSpecSource
    from okg.substrate.library.sources.base import NodeFact

    src = OpenSpecSource(
        repo="okg",
        openspec_root=fixture_paths.openspec_root,
    )
    run = src.run("run-1", mode="cursor")
    change_nodes = [
        f
        for f in run.facts
        if isinstance(f, NodeFact) and f.node_id.startswith("change:")
    ]
    assert change_nodes
    assert all(n.subtype == "open_spec_change" for n in change_nodes)


def test_adapter_node_set_matches_domain(fixture_paths) -> None:
    """Graph-shape parity: substrate adapter's emitted node_id set
    equals the domain domain class's emit set on the same fixture."""
    from okg.substrate.library.sources._openspec_emit import (
        OpenSpecSource as DomainOpenSpecSource,
    )
    from okg.substrate.library.sources.openspec import OpenSpecSource

    domain = DomainOpenSpecSource(
        openspec_root=fixture_paths.openspec_root,
        repo="okg",
    )
    domain_node_ids = {n.node_id for n in domain.nodes()}

    src = OpenSpecSource(
        repo="okg",
        openspec_root=fixture_paths.openspec_root,
    )
    run = src.run("run-1", mode="cursor")
    substrate_node_ids = {
        f.node_id for f in run.facts if hasattr(f, "node_id") and f.op == "I"
    }
    assert substrate_node_ids == domain_node_ids


def test_adapter_edge_set_matches_domain(fixture_paths) -> None:
    """Same parity check on the edges side."""
    from okg.substrate.library.sources._openspec_emit import (
        OpenSpecSource as DomainOpenSpecSource,
    )
    from okg.substrate.library.sources.openspec import OpenSpecSource

    domain = DomainOpenSpecSource(
        openspec_root=fixture_paths.openspec_root,
        repo="okg",
    )
    list(domain.nodes())  # populate
    domain_edge_keys = {(e.src, e.dst, e.edge_type) for e in domain.edges()}

    src = OpenSpecSource(
        repo="okg",
        openspec_root=fixture_paths.openspec_root,
    )
    run = src.run("run-1", mode="cursor")
    substrate_edge_keys = {
        (f.src, f.dst, f.edge_type) for f in run.facts if hasattr(f, "src")
    }
    assert substrate_edge_keys == domain_edge_keys


def test_archived_changes_emitted_when_include_archived(fixture_paths) -> None:
    """Archived change `add-formal-greeting` shows up only when
    include_archived is true (default)."""
    from okg.substrate.library.sources.openspec import OpenSpecSource

    src_with = OpenSpecSource(
        repo="okg",
        openspec_root=fixture_paths.openspec_root,
        include_archived=True,
    )
    ids_with = {
        f.node_id
        for f in src_with.run("rw", mode="cursor").facts
        if hasattr(f, "node_id")
    }
    assert "change:add-formal-greeting" in ids_with

    src_without = OpenSpecSource(
        repo="okg",
        openspec_root=fixture_paths.openspec_root,
        include_archived=False,
    )
    ids_without = {
        f.node_id
        for f in src_without.run("rwo", mode="cursor").facts
        if hasattr(f, "node_id")
    }
    assert "change:add-formal-greeting" not in ids_without


def test_reopened_change_prefers_active_emit(tmp_path: Path) -> None:
    from okg.substrate.library.sources.base import NodeFact
    from okg.substrate.library.sources.openspec import OpenSpecSource

    openspec_root = tmp_path / "openspec"
    active = openspec_root / "changes" / "demo-change"
    archived = openspec_root / "changes" / "archive" / "2026-01-01-demo-change"
    active.mkdir(parents=True)
    archived.mkdir(parents=True)
    (archived / "proposal.md").write_text(
        "# Change: Archived Demo\n\n## Why is this broken?\n\nArchived text.\n",
    )
    (active / "proposal.md").write_text(
        "# Change: Active Demo\n\n## Why is this broken?\n\nActive text.\n",
    )

    src = OpenSpecSource(
        repo="okg",
        openspec_root=openspec_root,
        include_archived=True,
    )
    facts = list(src.run("run-reopened", mode="scope_complete").facts)
    reopened = [
        fact
        for fact in facts
        if isinstance(fact, NodeFact)
        and fact.node_id == "change:demo-change"
        and fact.op == "I"
    ]

    assert len(reopened) == 1
    assert reopened[0].attrs["title"] == "Active Demo"


def test_filtered_external_reference_manifest_invalidates_inner_cursor(
    tmp_path: Path,
) -> None:
    from okg.substrate.library.sources.base import EdgeFact
    from okg.substrate.library.sources.openspec import OpenSpecSource

    openspec_root = tmp_path / "openspec"
    change_dir = openspec_root / "changes" / "demo-change"
    change_dir.mkdir(parents=True)
    (change_dir / "proposal.md").write_text(
        "# Change: Demo\n\n"
        "## Why\n\n"
        "The implementation is in @/src/demo.py.\n",
        encoding="utf-8",
    )
    source = OpenSpecSource(
        repo="okg",
        openspec_root=openspec_root,
        filter_unresolved_crossrefs=True,
    )

    missing = source.run("missing", mode="cursor")
    missing_facts = list(missing.facts)
    missing_cursor = missing.next_cursor or {}
    assert not any(
        isinstance(fact, EdgeFact)
        and fact.dst == "file:okg:src/demo.py"
        for fact in missing_facts
    )
    assert (
        missing_cursor["external_reference_manifest"]["demo-change"][
            "src/demo.py"
        ]
        == "missing"
    )

    external = tmp_path / "src" / "demo.py"
    external.parent.mkdir()
    external.write_text("value = 1\n", encoding="utf-8")
    added = source.run("added", mode="cursor", cursor=missing_cursor)
    added_facts = list(added.facts)
    assert any(
        isinstance(fact, EdgeFact)
        and fact.src == "change:demo-change"
        and fact.dst == "file:okg:src/demo.py"
        and fact.edge_type == "references"
        for fact in added_facts
    )
    assert (added.next_cursor or {})["external_reference_manifest"][
        "demo-change"
    ]["src/demo.py"].startswith("present:")

    external.unlink()
    deleted = source.run("deleted", mode="cursor", cursor=added.next_cursor)
    deleted_facts = list(deleted.facts)
    # The changed external manifest invalidates the domain's unchanged-file
    # shortcut, so the owning record is re-emitted and the now-unresolvable
    # REFERENCES edge is absent from its replacement footprint.
    assert any(
        getattr(fact, "node_id", None) == "change:demo-change"
        for fact in deleted_facts
    )
    assert not any(
        isinstance(fact, EdgeFact)
        and fact.dst == "file:okg:src/demo.py"
        for fact in deleted_facts
    )
    assert (
        (deleted.next_cursor or {})["external_reference_manifest"][
            "demo-change"
        ]["src/demo.py"]
        == "missing"
    )


def _moved_tree_repo(tmp_path: Path) -> Path:
    """A repository whose OpenSpec tree sits at `history/openspec/`.

    The proposal mentions one file outside the tree and one inside it, both
    spelled the way proposals were written (tree at `openspec/`).
    """
    repo = tmp_path / "repo"
    change_dir = repo / "history" / "openspec" / "changes" / "demo-change"
    change_dir.mkdir(parents=True)
    (change_dir / "proposal.md").write_text(
        "# Change: Demo\n\n"
        "## Why\n\n"
        "The implementation is in @/src/demo.py and the design is in\n"
        "@/openspec/changes/demo-change/design.md.\n",
        encoding="utf-8",
    )
    (change_dir / "design.md").write_text("# Design\n", encoding="utf-8")
    (repo / "src").mkdir()
    (repo / "src" / "demo.py").write_text("value = 1\n", encoding="utf-8")
    return repo


def _reference_dsts(source) -> set[str]:
    from okg.substrate.library.sources.base import EdgeFact

    return {
        fact.dst
        for fact in source.run("run", mode="reconcile").facts
        if isinstance(fact, EdgeFact) and fact.edge_type == "references"
    }


def test_repo_root_resolves_mentions_when_the_tree_has_moved(
    tmp_path: Path,
) -> None:
    """PACT layout-history-openspec: the tree moved, the mentions did not.

    With `repo_root` set, a mention outside the tree resolves against the
    repository, and a mention inside it is relocated to where the tree now
    sits -- the path git_graph emits for that file. Node identity and the
    change's `path` attribute stay relative to the tree's parent, so the
    move re-keys no node.
    """
    from okg.substrate.library.sources.base import NodeFact
    from okg.substrate.library.sources.openspec import OpenSpecSource

    repo = _moved_tree_repo(tmp_path)
    source = OpenSpecSource(
        repo="okg",
        openspec_root=repo / "history" / "openspec",
        repo_root=repo,
        filter_unresolved_crossrefs=True,
    )

    assert _reference_dsts(source) == {
        "file:okg:src/demo.py",
        "file:okg:history/openspec/changes/demo-change/design.md",
    }
    change = next(
        fact
        for fact in source.run("run", mode="reconcile").facts
        if isinstance(fact, NodeFact) and fact.node_id == "change:demo-change"
    )
    assert change.attrs["path"] == "openspec/changes/demo-change"


def test_moved_tree_without_repo_root_loses_the_outside_mention(
    tmp_path: Path,
) -> None:
    """The failure `repo_root` exists for: the parent is not the repository.

    Without it, mentions resolve against `history/`, so the file outside the
    tree reads as missing and its edge is filtered, and the edge into the
    tree names the pre-move path, which nothing emits once the tree moves.
    """
    from okg.substrate.library.sources.openspec import OpenSpecSource

    repo = _moved_tree_repo(tmp_path)
    source = OpenSpecSource(
        repo="okg",
        openspec_root=repo / "history" / "openspec",
        filter_unresolved_crossrefs=True,
    )

    assert _reference_dsts(source) == {
        "file:okg:openspec/changes/demo-change/design.md",
    }


def test_default_layout_edges_are_unchanged_by_repo_root(tmp_path: Path) -> None:
    """At `<repo>/openspec/`, naming the repository changes nothing."""
    from okg.substrate.library.sources.openspec import OpenSpecSource

    repo = tmp_path / "repo"
    change_dir = repo / "openspec" / "changes" / "demo-change"
    change_dir.mkdir(parents=True)
    (change_dir / "proposal.md").write_text(
        "# Change: Demo\n\n## Why\n\nSee @/src/demo.py and\n"
        "@/openspec/changes/demo-change/design.md.\n",
        encoding="utf-8",
    )
    (change_dir / "design.md").write_text("# Design\n", encoding="utf-8")
    (repo / "src").mkdir()
    (repo / "src" / "demo.py").write_text("value = 1\n", encoding="utf-8")

    expected = {
        "file:okg:src/demo.py",
        "file:okg:openspec/changes/demo-change/design.md",
    }
    for kwargs in ({}, {"repo_root": repo}):
        source = OpenSpecSource(
            repo="okg",
            openspec_root=repo / "openspec",
            filter_unresolved_crossrefs=True,
            **kwargs,
        )
        assert _reference_dsts(source) == expected, kwargs


def test_repo_root_that_does_not_contain_the_tree_is_refused(
    tmp_path: Path,
) -> None:
    """No repo-relative location exists for a tree outside the repository."""
    from okg.substrate.library.sources.openspec import OpenSpecSource

    tree = tmp_path / "elsewhere" / "openspec"
    tree.mkdir(parents=True)
    repo = tmp_path / "repo"
    repo.mkdir()
    with pytest.raises(ValueError, match="is not inside repo_root"):
        OpenSpecSource(repo="okg", openspec_root=tree, repo_root=repo)
    with pytest.raises(ValueError, match="is repo_root itself"):
        OpenSpecSource(repo="okg", openspec_root=repo, repo_root=repo)


def test_relocate_tree_path_moves_only_paths_inside_the_tree() -> None:
    from okg.substrate.library.sources.openspec import relocate_tree_path

    assert relocate_tree_path("openspec", "history/openspec") == "history/openspec"
    assert (
        relocate_tree_path("openspec/changes/x/proposal.md", "history/openspec")
        == "history/openspec/changes/x/proposal.md"
    )
    assert relocate_tree_path("src/openspec/x.py", "history/openspec") == (
        "src/openspec/x.py"
    )
    assert relocate_tree_path("openspecs/x.md", "history/openspec") == (
        "openspecs/x.md"
    )
    assert relocate_tree_path("openspec/x.md", "openspec") == "openspec/x.md"


def test_openspec_warm_probe_avoids_materialized_snapshot_and_tracks_external_bytes(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from okg.substrate.library.sources import openspec as openspec_module
    from okg.substrate.library.sources.change_probe import tokens_equal
    from okg.substrate.library.sources.openspec import OpenSpecSource

    openspec_root = tmp_path / "openspec"
    change_dir = openspec_root / "changes" / "demo-change"
    change_dir.mkdir(parents=True)
    (change_dir / "proposal.md").write_text(
        "# Change: Demo\n\n"
        "## Why\n\n"
        "The implementation is in @/src/demo.py.\n",
        encoding="utf-8",
    )
    external = tmp_path / "src" / "demo.py"
    external.parent.mkdir()
    external.write_text("value = 1\n", encoding="utf-8")
    source = OpenSpecSource(
        repo="okg",
        openspec_root=openspec_root,
        filter_unresolved_crossrefs=True,
    )

    def _unexpected_materialization(*_args, **_kwargs):
        raise AssertionError("poll token must not materialize a source snapshot")

    monkeypatch.setattr(
        openspec_module.tempfile,
        "TemporaryDirectory",
        _unexpected_materialization,
    )
    before = source.change_probe.build_token()
    external.write_text("value = 2\n", encoding="utf-8")
    after = source.change_probe.build_token()

    assert not tokens_equal(before, after)


def test_openspec_token_only_and_materialized_snapshot_identity_match(
    tmp_path: Path,
) -> None:
    from okg.substrate.library.sources.change_probe import tokens_equal
    from okg.substrate.library.sources.openspec import OpenSpecSource

    openspec_root = tmp_path / "openspec"
    change_dir = openspec_root / "changes" / "demo-change"
    change_dir.mkdir(parents=True)
    (change_dir / "proposal.md").write_text(
        "# Change: Demo\n\n"
        "## Why\n\n"
        "The implementation is in @/src/demo.py.\n",
        encoding="utf-8",
    )
    external = tmp_path / "src" / "demo.py"
    external.parent.mkdir()
    external.write_text("value = 1\n", encoding="utf-8")
    source = OpenSpecSource(
        repo="okg",
        openspec_root=openspec_root,
        include_archived=True,
        filter_unresolved_crossrefs=True,
    )

    token_only = source.change_probe.build_token()
    snapshot = source.change_probe.capture_snapshot()
    try:
        assert tokens_equal(token_only, snapshot.token)
        assert snapshot.external_reference_manifest["demo-change"][
            "src/demo.py"
        ].startswith("present:")
        assert (snapshot.openspec_root.parent / "src" / "demo.py").read_text(
            encoding="utf-8"
        ) == "value = 1\n"
    finally:
        snapshot.close()


def test_openspec_semantic_identity_ignores_run_id_and_mtime(
    fixture_paths,
) -> None:
    import os

    from okg.substrate.library.sources.base import NodeFact
    from okg.substrate.library.sources.openspec import OpenSpecSource

    first = list(
        OpenSpecSource(
            repo="okg",
            openspec_root=fixture_paths.openspec_root,
        ).run("run-a", mode="scope_complete").facts
    )
    proposal = (
        fixture_paths.openspec_root
        / "changes"
        / "add-greeting-feature"
        / "proposal.md"
    )
    before = proposal.stat()
    os.utime(
        proposal,
        ns=(before.st_atime_ns, before.st_mtime_ns + 1_000_000_000),
    )
    second_source = OpenSpecSource(
        repo="okg",
        openspec_root=fixture_paths.openspec_root,
    )
    second_run = second_source.run("run-b", mode="scope_complete")
    second = list(second_run.facts)

    first_nodes = {
        fact.node_id: (fact.attrs, fact.source_revision)
        for fact in first
        if isinstance(fact, NodeFact)
    }
    second_nodes = {
        fact.node_id: (fact.attrs, fact.source_revision)
        for fact in second
        if isinstance(fact, NodeFact)
    }
    assert first_nodes == second_nodes
    assert all("last_modified_at" not in attrs for attrs, _ in first_nodes.values())
    assert all("run_id" not in revision for _, revision in first_nodes.values())
    from okg.substrate.library.sources.change_probe import tokens_equal

    assert second_run.applied_token_scope == "global"
    assert second_run.applied_token_authority == "exact"
    assert second_run.applied_raw_token is not None
    assert tokens_equal(
        second_run.applied_raw_token,
        second_source.change_probe.build_token(),
    )


def test_reopened_change_cursor_run_does_not_emit_archived_duplicate(
    tmp_path: Path,
) -> None:
    from okg.substrate.library.sources.openspec import OpenSpecSource

    openspec_root = tmp_path / "openspec"
    active = openspec_root / "changes" / "demo-change"
    archived = openspec_root / "changes" / "archive" / "2026-01-01-demo-change"
    active.mkdir(parents=True)
    archived.mkdir(parents=True)
    (archived / "proposal.md").write_text(
        "# Change: Archived Demo\n\n## Why is this broken?\n\nArchived text.\n",
        encoding="utf-8",
    )
    (active / "proposal.md").write_text(
        "# Change: Active Demo\n\n## Why is this broken?\n\nActive text.\n",
        encoding="utf-8",
    )

    src = OpenSpecSource(
        repo="okg",
        openspec_root=openspec_root,
        include_archived=True,
    )
    baseline = src.run("baseline", mode="scope_complete")
    list(baseline.facts)
    assert baseline.next_cursor is not None
    cursor = {
        **baseline.next_cursor,
        "record_set_ref": {
            "storage": "okg.source_record_set_blobs",
            "sha256": "abc",
        },
        "record_set_summary": {"record_count": 1},
    }

    unchanged = src.run("unchanged", mode="scope_complete", cursor=cursor)

    assert list(unchanged.facts) == []
    assert unchanged.record_set is None
    assert unchanged.record_set_replacement_keys is None


def test_duplicate_archived_change_uses_latest_and_cursor_noops(
    tmp_path: Path,
) -> None:
    from okg.substrate.library.sources.base import NodeFact
    from okg.substrate.library.sources.openspec import OpenSpecSource

    openspec_root = tmp_path / "openspec"
    older = openspec_root / "changes" / "archive" / "2026-01-01-demo-change"
    newer = openspec_root / "changes" / "archive" / "2026-02-01-demo-change"
    older.mkdir(parents=True)
    newer.mkdir(parents=True)
    (older / "proposal.md").write_text(
        "# Change: Older Demo\n\n## Why is this broken?\n\nOlder text.\n",
        encoding="utf-8",
    )
    (newer / "proposal.md").write_text(
        "# Change: Newer Demo\n\n## Why is this broken?\n\nNewer text.\n",
        encoding="utf-8",
    )

    src = OpenSpecSource(
        repo="okg",
        openspec_root=openspec_root,
        include_archived=True,
    )
    baseline = src.run("baseline", mode="scope_complete")
    baseline_facts = list(baseline.facts)
    reopened = [
        fact
        for fact in baseline_facts
        if isinstance(fact, NodeFact)
        and fact.node_id == "change:demo-change"
        and fact.op == "I"
    ]
    assert len(reopened) == 1
    assert reopened[0].attrs["title"] == "Newer Demo"
    assert baseline.next_cursor is not None
    cursor = {
        **baseline.next_cursor,
        "record_set_ref": {
            "storage": "okg.source_record_set_blobs",
            "sha256": "abc",
        },
        "record_set_summary": {"record_count": 1},
    }

    unchanged = src.run("unchanged", mode="scope_complete", cursor=cursor)

    assert list(unchanged.facts) == []
    assert unchanged.record_set is None
    assert unchanged.record_set_replacement_keys is None


def test_next_cursor_carries_file_hashes(fixture_paths) -> None:
    from okg.substrate.library.sources.openspec import OpenSpecSource

    src = OpenSpecSource(
        repo="okg",
        openspec_root=fixture_paths.openspec_root,
    )
    run = src.run("run-1", mode="cursor")
    list(run.facts)
    cursor = run.next_cursor or {}
    assert "file_hashes" in cursor
    assert isinstance(cursor["file_hashes"], dict)
    # Both fixture changes plus the canonical capability are
    # represented in file_hashes.
    assert "add-greeting-feature" in cursor["file_hashes"]
    assert "cap:greetings" in cursor["file_hashes"]


def test_second_cursor_run_with_unchanged_hashes_emits_no_changes(
    fixture_paths,
) -> None:
    """Round-trip the cursor: second cursor run with the same
    file_hashes must emit zero new change/capability NodeFacts."""
    from okg.substrate.library.sources.base import NodeFact
    from okg.substrate.library.sources.openspec import OpenSpecSource

    src = OpenSpecSource(
        repo="okg",
        openspec_root=fixture_paths.openspec_root,
    )
    first = src.run("run-1", mode="cursor")
    list(first.facts)

    src2 = OpenSpecSource(
        repo="okg",
        openspec_root=fixture_paths.openspec_root,
    )
    second = src2.run("run-2", mode="cursor", cursor=first.next_cursor)
    new_change_or_cap = [
        f
        for f in second.facts
        if isinstance(f, NodeFact)
        and (f.node_id.startswith("change:") or f.node_id.startswith("cap:"))
    ]
    assert new_change_or_cap == []


def test_reconcile_mode_completed_scope_is_true(fixture_paths) -> None:
    from okg.substrate.library.sources.openspec import OpenSpecSource

    src = OpenSpecSource(
        repo="okg",
        openspec_root=fixture_paths.openspec_root,
    )
    run = src.run("rr", mode="reconcile")
    assert run.completed_scope is True
    assert run.record_set is not None
    assert "add-greeting-feature" in run.record_set.records


def test_record_set_includes_reasoning_records(fixture_paths) -> None:
    """Reasoning artifacts are emitted by OpenSpec and must be in the
    completed-scope record set so reconcile runs do not classify them
    as stale source facts."""
    from okg.substrate.library.sources.openspec import OpenSpecSource

    hyp_dir = fixture_paths.openspec_root / "hypotheses" / "agent-contract"
    hyp_dir.mkdir(parents=True)
    (hyp_dir / "proposal.md").write_text(
        """---
schema_version: 1
id: agent-contract
statement: "Agents follow explicit contracts."
status: open
principles_applied:
  - names-matter
---

Body.
""",
        encoding="utf-8",
    )
    obs_dir = fixture_paths.openspec_root / "observations"
    obs_dir.mkdir()
    (obs_dir / "agent-contract-observed.md").write_text(
        """---
schema_version: 1
id: agent-contract-observed
status: recorded
statement: "The contract was observed."
source_kind: curated
source_ref: tests
instantiates_principles:
  - names-matter
---

Body.
""",
        encoding="utf-8",
    )
    insights_dir = fixture_paths.openspec_root / "insights"
    insights_dir.mkdir()
    (insights_dir / "agent-contract-insight.md").write_text(
        """---
schema_version: 1
id: agent-contract-insight
statement: "Contracts make agent work inspectable."
synthesizes_connection_refs:
  - contract-to-outcome
cites_observation_refs:
  - agent-contract-observed
status: draft
---

Body.
""",
        encoding="utf-8",
    )

    src = OpenSpecSource(
        repo="okg",
        openspec_root=fixture_paths.openspec_root,
    )
    run = src.run("rr", mode="reconcile")

    assert run.record_set is not None
    assert "hyp:agent-contract" in run.record_set.records
    assert "obs:agent-contract-observed" in run.record_set.records
    assert "insight:agent-contract-insight" in run.record_set.records
    assert (
        run.record_set.records["hyp:agent-contract"].primary.node_id
        == "hyp:agent-contract"
    )


def test_cursor_mode_does_not_emit_retractions(fixture_paths) -> None:
    """Even when stale ids are detected, cursor mode is not
    authorized to emit op='R' rows."""
    from okg.substrate.library.sources.base import NodeFact
    from okg.substrate.library.sources.openspec import OpenSpecSource

    src = OpenSpecSource(
        repo="okg",
        openspec_root=fixture_paths.openspec_root,
    )
    run = src.run("rc", mode="cursor")
    retractions = [f for f in run.facts if isinstance(f, NodeFact) and f.op == "R"]
    assert retractions == []
    assert run.record_set is None


@pytest.mark.postgres
def test_record_set_retracts_deleted_task(
    clean_runtime: str,
    fixture_paths,
) -> None:
    import psycopg

    from okg.substrate.ingest import SourceRunner
    from okg.substrate.library.sources.openspec import OpenSpecSource

    with psycopg.connect(clean_runtime, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute(
            "INSERT INTO okg.catalog_versions (status, applied_at) "
            "VALUES ('applied', now())"
        )

    source_name = "test.openspec-record-set"
    runner = SourceRunner(clean_runtime, source_name=source_name)
    first = runner.run(
        OpenSpecSource(repo="okg", openspec_root=fixture_paths.openspec_root),
        mode="scope_complete",
    )
    assert first.next_cursor is not None
    assert "record_set" in first.next_cursor

    tasks_path = (
        fixture_paths.openspec_root / "changes" / "add-greeting-feature" / "tasks.md"
    )
    tasks_path.write_text(
        tasks_path.read_text().replace("- [ ] 1.2 Add usage docs.\n", "")
    )

    second = runner.run(
        OpenSpecSource(repo="okg", openspec_root=fixture_paths.openspec_root),
        mode="scope_complete",
    )
    assert second.retractions_node > 0
    assert second.retractions_edge > 0

    assert (
        _graph_node_exists(
            clean_runtime,
            node_id="task:add-greeting-feature:1.2",
        )
        is False
    )


def _graph_node_exists(dsn: str, *, node_id: str) -> bool:
    import psycopg

    from okg.substrate.graph_store import ChronosGraphStore

    with psycopg.connect(dsn, autocommit=True) as conn:
        rows = ChronosGraphStore.from_connection(
            conn,
            ensure_schema=False,
        ).query(
            """
            SELECT 1 AS present
              FROM okg.graph_nodes
             WHERE node_id = :node_id
             LIMIT 1
            """,
            {"node_id": node_id},
        )
    return bool(rows)
