from __future__ import annotations

import os
from pathlib import Path

import pytest

from okg.substrate.errors import SubstrateError
from okg.substrate.pact import cli, manifest_inventory, operations
from okg.substrate.pact.models import (
    Change,
    EvidencePolicy,
    PactHierarchy,
    PactManifest,
    Requirement,
    Task,
    TestPlanEntry as PactTestPlanEntry,
)
from okg.substrate.pact.store import render_manifest


def _manifest(
    change_id: str,
    *,
    hierarchy: PactHierarchy | None = None,
) -> PactManifest:
    requirement_id = f"req.{change_id}"
    return PactManifest(
        pact_version=4,
        target_pact_version=4,
        change=Change(
            id=change_id,
            title=change_id,
            status="proposal",
            tier=1,
            rationale="Exercise lifecycle-aware read resolution.",
            root_cause="The test fixture requires one exact manifest location.",
        ),
        hierarchy=hierarchy,
        requirements=[
            Requirement(
                id=requirement_id,
                statement="The fixture MUST carry executed proof.",
                evidence_policy=EvidencePolicy(required=["executed_test"]),
            )
        ],
        tasks=[
            Task(
                id=f"task.{change_id}",
                title="Prove the fixture",
                verification="Run the focused lifecycle-resolution test.",
                requirements=[requirement_id],
            )
        ],
        test_plan=[
            PactTestPlanEntry(
                id=f"test.{change_id}",
                requirement=requirement_id,
                test_id=(
                    "tests/pact/test_archive_read_resolution.py::"
                    "test_archived_evidence_status_names_reopen_not_an_illegal_write"
                ),
                evidence=["pytest"],
            )
        ],
    )


def _write_manifest(
    root: Path,
    manifest: PactManifest,
    *,
    collection: str = "changes",
) -> Path:
    path = root / "pact" / collection / manifest.change.id / "pact.yaml"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(render_manifest(manifest), encoding="utf-8")
    return path


@pytest.mark.parametrize("collection", ["changes", "archive"])
def test_read_loader_resolves_one_active_or_archived_manifest(
    tmp_path: Path,
    collection: str,
) -> None:
    expected = _write_manifest(tmp_path, _manifest("demo"), collection=collection)

    resolved = manifest_inventory.load_manifest_for_read(tmp_path, "demo")

    assert resolved.manifest.change.id == "demo"
    assert resolved.path == expected
    assert resolved.collection == collection
    assert resolved.archived is (collection == "archive")


def test_read_loader_refuses_missing_and_archive_lock_only(
    tmp_path: Path,
) -> None:
    lock = tmp_path / "pact" / "archive" / "demo" / "pact.yaml.lock"
    lock.parent.mkdir(parents=True)
    lock.touch()

    with pytest.raises(SubstrateError, match="PACT change not found: demo"):
        manifest_inventory.load_manifest_for_read(tmp_path, "demo")


def test_read_loader_refuses_duplicate_lifecycle_candidates(tmp_path: Path) -> None:
    manifest = _manifest("demo")
    active = _write_manifest(tmp_path, manifest)
    archived = _write_manifest(tmp_path, manifest, collection="archive")

    with pytest.raises(SubstrateError, match="duplicate active and archived") as exc:
        manifest_inventory.load_manifest_for_read(tmp_path, "demo")

    assert exc.value.data["paths"] == [str(active), str(archived)]


def test_read_loader_rejects_traversal_before_any_filesystem_read(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def unexpected_read(*_args, **_kwargs):
        raise AssertionError("an invalid id reached the filesystem")

    monkeypatch.setattr(
        manifest_inventory,
        "read_manifest_payload",
        unexpected_read,
    )

    with pytest.raises(SubstrateError, match="invalid PACT change id"):
        manifest_inventory.load_manifest_for_read(tmp_path, "../outside")


@pytest.mark.parametrize("collection", ["changes", "archive"])
@pytest.mark.skipif(os.name != "posix", reason="POSIX no-follow descriptor test")
def test_read_loader_refuses_a_symlinked_candidate(
    tmp_path: Path,
    collection: str,
) -> None:
    outside = tmp_path / "outside" / collection / "demo"
    outside.mkdir(parents=True)
    (outside / "pact.yaml").write_text(
        render_manifest(_manifest("demo")),
        encoding="utf-8",
    )
    collection_root = tmp_path / "pact" / collection
    collection_root.mkdir(parents=True)
    (collection_root / "demo").symlink_to(outside, target_is_directory=True)

    with pytest.raises(SubstrateError, match="unable to read PACT manifest candidate"):
        manifest_inventory.load_manifest_for_read(tmp_path, "demo")


@pytest.mark.parametrize(
    ("payload", "message"),
    [
        (b"change: [unterminated", "invalid YAML"),
        (
            b"pact_version: 4\ntarget_pact_version: 4\n"
            b"change:\n  id: demo\n  title: []\n",
            "failed schema validation",
        ),
    ],
)
def test_read_loader_refuses_invalid_manifest_content(
    tmp_path: Path,
    payload: bytes,
    message: str,
) -> None:
    path = tmp_path / "pact" / "archive" / "demo" / "pact.yaml"
    path.parent.mkdir(parents=True)
    path.write_bytes(payload)

    with pytest.raises(SubstrateError, match=message):
        manifest_inventory.load_manifest_for_read(tmp_path, "demo")


def test_read_loader_refuses_a_manifest_id_mismatch(tmp_path: Path) -> None:
    source = _write_manifest(tmp_path, _manifest("other"), collection="archive")
    target = source.parent.parent / "demo" / "pact.yaml"
    target.parent.mkdir()
    source.replace(target)

    with pytest.raises(SubstrateError, match="id does not match resolved path"):
        manifest_inventory.load_manifest_for_read(tmp_path, "demo")


def test_read_loader_refuses_an_unreadable_candidate(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _write_manifest(tmp_path, _manifest("demo"))
    original = manifest_inventory.read_manifest_payload

    def deny_active(
        repo_root: Path,
        change_id: str,
        path: Path,
        *,
        collection: str = "changes",
    ) -> bytes:
        if collection == "changes":
            raise PermissionError("permission denied by fixture")
        return original(
            repo_root,
            change_id,
            path,
            collection=collection,
        )

    monkeypatch.setattr(manifest_inventory, "read_manifest_payload", deny_active)

    with pytest.raises(SubstrateError, match="unable to read PACT manifest candidate"):
        manifest_inventory.load_manifest_for_read(tmp_path, "demo")


def test_gate_loader_uses_lifecycle_aware_resolution(tmp_path: Path) -> None:
    _write_manifest(tmp_path, _manifest("demo"), collection="archive")

    assert cli._load_manifest_for_gate(tmp_path, "demo").change.id == "demo"


def test_post_edit_loader_refuses_duplicate_lifecycle_candidates(
    tmp_path: Path,
) -> None:
    manifest = _manifest("demo")
    _write_manifest(tmp_path, manifest)
    _write_manifest(tmp_path, manifest, collection="archive")

    with pytest.raises(SubstrateError, match="duplicate active and archived"):
        operations._load_manifest_for_post_edit(tmp_path, "demo")


@pytest.mark.skipif(os.name != "posix", reason="POSIX no-follow descriptor test")
def test_post_edit_loader_refuses_an_active_symlink(tmp_path: Path) -> None:
    outside = tmp_path / "outside" / "demo"
    outside.mkdir(parents=True)
    (outside / "pact.yaml").write_text(
        render_manifest(_manifest("demo")),
        encoding="utf-8",
    )
    changes = tmp_path / "pact" / "changes"
    changes.mkdir(parents=True)
    (changes / "demo").symlink_to(outside, target_is_directory=True)

    with pytest.raises(SubstrateError, match="unable to read PACT manifest candidate"):
        operations._load_manifest_for_post_edit(tmp_path, "demo")


def test_single_change_hierarchy_rollup_reads_an_archived_child(
    tmp_path: Path,
) -> None:
    child = _manifest(
        "child",
        hierarchy=PactHierarchy(role="child", parent="parent"),
    )
    _write_manifest(tmp_path, child, collection="archive")
    parent = _manifest(
        "parent",
        hierarchy=PactHierarchy(role="program", children=["child"]),
    )

    rollup = cli._hierarchy_rollup(tmp_path, parent)

    assert rollup["child_rollup"][0]["change"] == "child"
    assert rollup["child_rollup"][0]["status"] == "proposal"
    assert "error" not in rollup["child_rollup"][0]


def test_archived_evidence_status_names_reopen_not_an_illegal_write(
    tmp_path: Path,
) -> None:
    archive = _write_manifest(tmp_path, _manifest("demo"), collection="archive")
    before = archive.read_bytes()

    result = operations.evidence_status(tmp_path, "demo")

    assert result["verdict"] == "refused"
    assert result["archived"] is True
    assert result["next_commands"] == [
        "okg pact decide demo --verdict reopen --json"
    ]
    assert all("evidence attach" not in command for command in result["next_commands"])
    assert archive.read_bytes() == before


@pytest.mark.parametrize(
    "mutation",
    [
        lambda root: operations.migrate_version(root, "demo", target_version=4),
        lambda root: operations.update_task_status(
            root,
            "demo",
            "task.demo",
            status="in_progress",
        ),
    ],
)
def test_mutating_manifest_operations_remain_active_only(
    tmp_path: Path,
    mutation,
) -> None:
    archive = _write_manifest(tmp_path, _manifest("demo"), collection="archive")
    before = archive.read_bytes()

    with pytest.raises(SubstrateError, match="PACT change not found: demo"):
        mutation(tmp_path)

    assert archive.read_bytes() == before
