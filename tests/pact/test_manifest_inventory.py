from __future__ import annotations

import os
from pathlib import Path

import pytest

from okg.substrate.errors import SubstrateError
from okg.substrate.pact import cli, decisions
from okg.substrate.pact import manifest_inventory
from okg.substrate.pact.evaluation_context import PactEvaluationContext
from okg.substrate.pact.models import (
    Change,
    PactHierarchy,
    PactManifest,
    Requirement,
)
from okg.substrate.pact import store
from okg.substrate.pact.store import render_manifest


def _manifest(change_id: str, *, title: str | None = None) -> PactManifest:
    return PactManifest(
        pact_version=4,
        target_pact_version=4,
        change=Change(
            id=change_id,
            title=title or change_id,
            status="proposal",
            tier=1,
        ),
        requirements=[
            Requirement(
                id="req-cache-isolation",
                statement="The captured manifest is isolated per consumer.",
            )
        ],
    )


def _write_manifest(root: Path, change_id: str, *, title: str | None = None) -> Path:
    return _write_model(root, _manifest(change_id, title=title))


def _write_model(root: Path, manifest: PactManifest) -> Path:
    change_id = manifest.change.id
    path = root / "pact" / "changes" / change_id / "pact.yaml"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(render_manifest(manifest), encoding="utf-8")
    return path


def _write_archive_model(root: Path, manifest: PactManifest) -> Path:
    path = root / "pact" / "archive" / manifest.change.id / "pact.yaml"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(render_manifest(manifest), encoding="utf-8")
    return path


def _capture(root: Path) -> manifest_inventory.ManifestInventory:
    return manifest_inventory.capture_manifest_inventory(root)


def test_inventory_freshly_discovers_add_remove_and_missing_entries(tmp_path: Path) -> None:
    _write_manifest(tmp_path, "b-present")
    (tmp_path / "pact" / "changes" / "a-missing").mkdir(parents=True)

    first = _capture(tmp_path)
    assert [entry.change_id for entry in first] == ["a-missing", "b-present"]
    assert first.entries[0].missing
    assert first.entries[0].error is not None
    assert first.entries[0].relative_path == "pact/changes/a-missing/pact.yaml"

    _write_manifest(tmp_path, "0-added")
    (tmp_path / "pact" / "changes" / "a-missing").rmdir()
    second = _capture(tmp_path)
    assert [entry.change_id for entry in second] == ["0-added", "b-present"]


def test_inventory_reads_exact_bytes_once_and_reports_digest(tmp_path: Path, monkeypatch) -> None:
    path = _write_manifest(tmp_path, "demo")
    payload = path.read_bytes()
    original_reader = manifest_inventory.read_manifest_payload
    reads = 0

    def counted_reader(
        repo_root: Path,
        change_id: str,
        candidate: Path,
        *,
        collection: str = "changes",
    ) -> bytes:
        nonlocal reads
        if candidate == path:
            reads += 1
        return original_reader(
            repo_root,
            change_id,
            candidate,
            collection=collection,
        )

    monkeypatch.setattr(manifest_inventory, "read_manifest_payload", counted_reader)
    entry = _capture(tmp_path).entries[0]
    assert reads == 1
    assert entry.payload == payload
    assert entry.content_digest == manifest_inventory._digest(payload)


def test_inventory_memoizes_success_but_isolates_nested_mutation(
    tmp_path: Path,
    monkeypatch,
) -> None:
    _write_manifest(tmp_path, "demo", title="original")
    original_loader = manifest_inventory.load_manifest_bytes
    parses = 0

    def counted_loader(payload: bytes, *, source_path: Path) -> PactManifest:
        nonlocal parses
        parses += 1
        return original_loader(payload, source_path=source_path)

    monkeypatch.setattr(manifest_inventory, "load_manifest_bytes", counted_loader)
    first = _capture(tmp_path)
    first.entries[0].manifest.change.title = "mutated"  # type: ignore[union-attr]
    first.entries[0].manifest.requirements[0].statement = "mutated nested value"  # type: ignore[union-attr]
    second = _capture(tmp_path)

    assert parses == 1
    assert second.entries[0].manifest.change.title == "original"  # type: ignore[union-attr]
    assert second.entries[0].manifest.requirements[0].statement == (  # type: ignore[union-attr]
        "The captured manifest is isolated per consumer."
    )


def test_inventory_invalidates_same_path_same_mtime_content_changes(tmp_path: Path) -> None:
    path = _write_manifest(tmp_path, "demo", title="before")
    first = _capture(tmp_path).entries[0]
    stat = path.stat()
    _write_manifest(tmp_path, "demo", title="after")
    os.utime(path, ns=(stat.st_atime_ns, stat.st_mtime_ns))

    second = _capture(tmp_path).entries[0]
    assert first.content_digest != second.content_digest
    assert second.manifest.change.title == "after"  # type: ignore[union-attr]


def test_inventory_parse_failure_recovers_after_correction(tmp_path: Path) -> None:
    path = tmp_path / "pact" / "changes" / "demo" / "pact.yaml"
    path.parent.mkdir(parents=True)
    path.write_bytes(b"change: [not valid")
    failed = _capture(tmp_path).entries[0]
    assert failed.status == "invalid"
    assert failed.error is not None

    path.write_bytes(render_manifest(_manifest("demo")).encode("utf-8"))
    recovered = _capture(tmp_path).entries[0]
    assert recovered.ok
    assert recovered.manifest.change.id == "demo"  # type: ignore[union-attr]


def test_inventory_schema_invalid_and_malformed_rows_are_path_bound_and_ordered(
    tmp_path: Path,
) -> None:
    malformed = tmp_path / "pact" / "changes" / "b-malformed" / "pact.yaml"
    malformed.parent.mkdir(parents=True)
    malformed.write_bytes(b"change: [not valid")
    unsupported = tmp_path / "pact" / "changes" / "c-unsupported" / "pact.yaml"
    unsupported.parent.mkdir(parents=True)
    unsupported.write_text(
        "pact_version: 99\nchange:\n  id: c-unsupported\n  title: bad\n",
        encoding="utf-8",
    )
    _write_manifest(tmp_path, "d-valid")

    inventory = _capture(tmp_path)
    assert [entry.change_id for entry in inventory] == [
        "b-malformed",
        "c-unsupported",
        "d-valid",
    ]
    assert [entry.status for entry in inventory] == ["invalid", "invalid", "ok"]
    assert all(
        entry.error is not None and str(entry.path) in str(entry.error)
        for entry in inventory.entries[:2]
    )


def test_inventory_cache_scopes_repository_and_relative_path(tmp_path: Path, monkeypatch) -> None:
    left = tmp_path / "left"
    right = tmp_path / "right"
    _write_manifest(left, "same", title="left")
    _write_manifest(right, "same", title="right")
    _write_manifest(left, "other", title="same-bytes")
    (right / "pact" / "changes" / "other").mkdir(parents=True, exist_ok=True)
    (right / "pact" / "changes" / "other" / "pact.yaml").write_bytes(
        (left / "pact" / "changes" / "other" / "pact.yaml").read_bytes()
    )

    original_loader = manifest_inventory.load_manifest_bytes
    parses = 0

    def counted_loader(payload: bytes, *, source_path: Path) -> PactManifest:
        nonlocal parses
        parses += 1
        return original_loader(payload, source_path=source_path)

    monkeypatch.setattr(manifest_inventory, "load_manifest_bytes", counted_loader)
    left_inventory = _capture(left)
    right_inventory = _capture(right)

    left_by_id = {entry.change_id: entry for entry in left_inventory}
    right_by_id = {entry.change_id: entry for entry in right_inventory}
    assert left_by_id["same"].manifest.change.title == "left"  # type: ignore[union-attr]
    assert right_by_id["same"].manifest.change.title == "right"  # type: ignore[union-attr]
    assert parses == 4  # two paths in each repository identity


def test_inventory_cache_respects_aggregate_source_byte_budget(
    tmp_path: Path,
    monkeypatch,
) -> None:
    _write_manifest(tmp_path, "demo")
    manifest_inventory.clear_manifest_inventory_cache()
    monkeypatch.setattr(manifest_inventory, "MAX_PARSE_CACHE_BYTES", 1)
    original_loader = manifest_inventory.load_manifest_bytes
    parses = 0

    def counted_loader(payload: bytes, *, source_path: Path) -> PactManifest:
        nonlocal parses
        parses += 1
        return original_loader(payload, source_path=source_path)

    monkeypatch.setattr(manifest_inventory, "load_manifest_bytes", counted_loader)
    _capture(tmp_path)
    _capture(tmp_path)

    assert parses == 2


@pytest.mark.skipif(os.name != "posix", reason="no-follow handles are POSIX-only")
def test_inventory_refuses_symlinked_change_directories_and_manifest_files(
    tmp_path: Path,
) -> None:
    outside_directory = tmp_path / "outside-directory"
    outside_manifest = _write_manifest(outside_directory, "external")
    changes = tmp_path / "pact" / "changes"
    changes.mkdir(parents=True, exist_ok=True)
    (changes / "a-directory-link").symlink_to(outside_manifest.parent)
    file_link = changes / "b-file-link" / "pact.yaml"
    file_link.parent.mkdir()
    file_link.symlink_to(outside_manifest)
    (changes / "c-entry-link").symlink_to(outside_manifest)

    inventory = _capture(tmp_path)

    assert [entry.change_id for entry in inventory] == [
        "a-directory-link",
        "b-file-link",
        "c-entry-link",
    ]
    assert [entry.status for entry in inventory] == [
        "unreadable",
        "unreadable",
        "unreadable",
    ]
    assert all(entry.error is not None for entry in inventory)


def test_list_consumes_each_manifest_and_validation_report_once(
    tmp_path: Path,
    monkeypatch,
) -> None:
    parent = _manifest("parent")
    parent.hierarchy = PactHierarchy(role="program", children=["child"])
    child = _manifest("child")
    child.hierarchy = PactHierarchy(role="child", parent="parent")
    _write_model(tmp_path, parent)
    _write_model(tmp_path, child)
    manifest_inventory.clear_manifest_inventory_cache()

    parse_calls = 0
    original_loader = manifest_inventory.load_manifest_bytes

    def counted_loader(payload: bytes, *, source_path: Path) -> PactManifest:
        nonlocal parse_calls
        parse_calls += 1
        return original_loader(payload, source_path=source_path)

    validation_calls = 0
    original_validate = cli.validate_manifest

    def counted_validate(*args, **kwargs):
        nonlocal validation_calls
        validation_calls += 1
        return original_validate(*args, **kwargs)

    monkeypatch.setattr(manifest_inventory, "load_manifest_bytes", counted_loader)
    monkeypatch.setattr(cli, "validate_manifest", counted_validate)
    monkeypatch.setattr(
        cli,
        "derive_alignment",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            AssertionError("hierarchy reopened an already validated child")
        ),
    )
    monkeypatch.setattr(
        store,
        "load_manifest_file",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            AssertionError("file authority reopened a captured manifest")
        ),
    )
    monkeypatch.setattr(
        decisions,
        "require_ledger_connection",
        lambda **_kwargs: (_ for _ in ()).throw(SubstrateError("offline")),
    )
    statuses: dict[str, dict] = {}
    original_status = cli._status

    def observed_status(*args, **kwargs):
        status = original_status(*args, **kwargs)
        statuses[status["change"]] = status
        return status

    monkeypatch.setattr(cli, "_status", observed_status)

    result = cli._list_changes(
        tmp_path,
        include_graph=False,
        status_filter="all",
    )

    assert [row["change"] for row in result["changes"]] == ["child", "parent"]
    assert parse_calls == 2
    assert validation_calls == 2
    assert statuses["parent"]["hierarchy"]["child_rollup"][0]["change"] == "child"


def test_list_honors_an_explicit_empty_inventory(tmp_path: Path, monkeypatch) -> None:
    _write_manifest(tmp_path, "must-not-be-recaptured")
    empty = manifest_inventory.ManifestInventory(repo_root=tmp_path, entries=())
    monkeypatch.setattr(
        decisions,
        "require_ledger_connection",
        lambda **_kwargs: (_ for _ in ()).throw(SubstrateError("offline")),
    )

    result = cli._list_changes(
        tmp_path,
        include_graph=False,
        status_filter="all",
        manifest_inventory=empty,
    )

    assert result["changes"] == []


def test_list_hierarchy_uses_captured_archived_child_without_live_reopen(
    tmp_path: Path,
    monkeypatch,
) -> None:
    parent = _manifest("parent")
    parent.hierarchy = PactHierarchy(role="program", children=["archived-child"])
    child = _manifest("archived-child")
    child.hierarchy = PactHierarchy(role="child", parent="parent")
    _write_model(tmp_path, parent)
    archive_path = _write_archive_model(tmp_path, child)
    archive_payload = archive_path.read_bytes()
    archive_parse_calls = 0
    original_load_bytes = cli.load_manifest_bytes

    def counted_load_bytes(payload: bytes, *, source_path: Path):
        nonlocal archive_parse_calls
        if payload == archive_payload:
            archive_parse_calls += 1
        return original_load_bytes(payload, source_path=source_path)

    validation_calls: dict[str, int] = {}
    original_validate = cli.validate_manifest

    def counted_validate(manifest: PactManifest, *args, **kwargs):
        validation_calls[manifest.change.id] = (
            validation_calls.get(manifest.change.id, 0) + 1
        )
        return original_validate(manifest, *args, **kwargs)

    monkeypatch.setattr(cli, "load_manifest_bytes", counted_load_bytes)
    monkeypatch.setattr(cli, "validate_manifest", counted_validate)
    monkeypatch.setattr(
        decisions,
        "require_ledger_connection",
        lambda **_kwargs: (_ for _ in ()).throw(SubstrateError("offline")),
    )
    monkeypatch.setattr(
        cli,
        "load_manifest",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            AssertionError("hierarchy reopened an archived child")
        ),
    )
    statuses: dict[str, dict] = {}
    original_status = cli._status

    def observed_status(*args, **kwargs):
        status = original_status(*args, **kwargs)
        statuses[status["change"]] = status
        return status

    monkeypatch.setattr(cli, "_status", observed_status)

    result = cli._list_changes(
        tmp_path,
        include_graph=False,
        status_filter="all",
    )

    assert [row["change"] for row in result["changes"]] == ["parent"]
    rollup = statuses["parent"]["hierarchy"]["child_rollup"]
    assert rollup[0]["change"] == "archived-child"
    assert rollup[0]["status"] == "proposal"
    assert archive_parse_calls == 1
    assert validation_calls["archived-child"] == 1


@pytest.mark.skipif(os.name != "posix", reason="no-follow handles are POSIX-only")
def test_list_hierarchy_refuses_symlinked_archived_child(
    tmp_path: Path,
    monkeypatch,
) -> None:
    parent = _manifest("parent")
    parent.hierarchy = PactHierarchy(role="program", children=["archived-child"])
    _write_model(tmp_path, parent)
    outside = _write_manifest(tmp_path / "outside", "archived-child")
    archive_link = (
        tmp_path / "pact" / "archive" / "archived-child" / "pact.yaml"
    )
    archive_link.parent.mkdir(parents=True)
    archive_link.symlink_to(outside)
    monkeypatch.setattr(
        decisions,
        "require_ledger_connection",
        lambda **_kwargs: (_ for _ in ()).throw(SubstrateError("offline")),
    )
    monkeypatch.setattr(
        cli,
        "load_manifest",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            AssertionError("hierarchy followed a live archive symlink")
        ),
    )
    statuses: dict[str, dict] = {}
    original_status = cli._status

    def observed_status(*args, **kwargs):
        status = original_status(*args, **kwargs)
        statuses[status["change"]] = status
        return status

    monkeypatch.setattr(cli, "_status", observed_status)

    cli._list_changes(tmp_path, include_graph=False, status_filter="all")

    rollup = statuses["parent"]["hierarchy"]["child_rollup"]
    assert rollup[0]["change"] == "archived-child"
    assert rollup[0]["status"] == "missing"


def test_list_preserves_sorted_invalid_rows_without_aborting(
    tmp_path: Path,
    monkeypatch,
) -> None:
    _write_manifest(tmp_path, "a-valid")
    invalid = tmp_path / "pact" / "changes" / "b-invalid" / "pact.yaml"
    invalid.parent.mkdir(parents=True)
    invalid.write_bytes(b"change: [invalid")
    _write_manifest(tmp_path, "c-valid")
    monkeypatch.setattr(
        decisions,
        "require_ledger_connection",
        lambda **_kwargs: (_ for _ in ()).throw(SubstrateError("offline")),
    )

    result = cli._list_changes(
        tmp_path,
        include_graph=False,
        status_filter="all",
    )

    assert [row["change"] for row in result["changes"]] == [
        "a-valid",
        "b-invalid",
        "c-valid",
    ]
    invalid_row = result["changes"][1]
    assert invalid_row["state"] == "unreadable"
    assert invalid_row["error_count"] == 1


def test_manifest_only_context_does_not_capture_unrelated_files(tmp_path: Path) -> None:
    manifest_ref = "pact/changes/demo/pact.yaml"
    context = PactEvaluationContext(manifest_files={manifest_ref: b"change: {}"})

    manifest_lookup = context.manifest_file_lookup(tmp_path, manifest_ref)
    absent_manifest_lookup = context.manifest_file_lookup(
        tmp_path,
        "pact/changes/absent/pact.yaml",
    )
    unrelated_lookup = context.file_lookup(tmp_path, "tests/pact/test_demo.py")

    assert manifest_lookup.exists
    assert manifest_lookup.content == b"change: {}"
    assert absent_manifest_lookup.captured is True
    assert absent_manifest_lookup.content is None
    assert unrelated_lookup.captured is False
