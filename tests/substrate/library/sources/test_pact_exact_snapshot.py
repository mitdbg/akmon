"""Adversarial coverage for PACTSource's exact evaluation snapshot."""

from __future__ import annotations

import hashlib
import os
import shutil
import subprocess
from pathlib import Path

import pytest
import yaml

from okg.substrate.library.sources.base import EdgeFact, NodeFact
from okg.substrate.library.sources.change_probe import (
    CURSOR_TOKEN_KEY,
    ChangeProbeError,
    run_probe,
    tokens_equal,
)
from okg.substrate.library.sources.pact import PACTSource


@pytest.fixture(autouse=True)
def _ledger_off(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("OKG_PACT_DECISION_PROJECTION", "off")


def _base_manifest(change_id: str = "demo") -> dict:
    return {
        "pact_version": 4,
        "target_pact_version": 4,
        "change": {"id": change_id, "title": change_id},
        "requirements": [{
            "id": f"req.{change_id}",
            "statement": "The captured evaluation remains exact.",
            "evidence_policy": {"required": ["executed_test"]},
        }],
        "tasks": [{
            "id": f"task.{change_id}",
            "title": "Prove the captured evaluation",
            "status": "todo",
            "verification": "pytest -q",
            "requirements": [f"req.{change_id}"],
        }],
    }


def _write_manifest(
    repo: Path,
    manifest: dict,
    *,
    branch: str = "changes",
) -> Path:
    change_id = manifest["change"]["id"]
    path = repo / "pact" / branch / change_id / "pact.yaml"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(yaml.safe_dump(manifest), encoding="utf-8")
    return path


def _commit(repo: Path, message: str = "initial") -> None:
    subprocess.run(["git", "-C", str(repo), "add", "-A"], check=True)
    subprocess.run(
        ["git", "-C", str(repo), "commit", "-q", "-m", message],
        check=True,
    )


def _init_git(repo: Path) -> None:
    subprocess.run(["git", "-C", str(repo), "init", "-q"], check=True)
    subprocess.run(
        ["git", "-C", str(repo), "config", "user.email", "test@example.com"],
        check=True,
    )
    subprocess.run(
        ["git", "-C", str(repo), "config", "user.name", "PACT test"],
        check=True,
    )
    _commit(repo)


def _digest(path: Path) -> str:
    return "sha256:" + hashlib.sha256(path.read_bytes()).hexdigest()


def _formal_repo(tmp_path: Path) -> tuple[Path, dict[str, Path]]:
    repo = tmp_path / "repo"
    paths = {
        "module": repo / "formal" / "Demo.tla",
        "config": repo / "formal" / "Demo.cfg",
        "checker": repo / "scripts" / "check.py",
        "refinement": repo / "tests" / "test_demo.py",
        # Absolute paths inside the repo exercise the absolute-reference path.
        "artifact": (repo / "artifacts" / "formal-report.yaml").absolute(),
    }
    contents = {
        "module": "---- MODULE Demo ----\n====\n",
        "config": "SPECIFICATION Spec\n",
        "checker": "print('pass')\n",
        "refinement": "def test_demo():\n    assert True\n",
        "artifact": "status: pass\n",
    }
    for name, path in paths.items():
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(contents[name], encoding="utf-8")

    checker_command = "python scripts/check.py"
    refinement_ref = "tests/test_demo.py::test_demo"
    manifest = {
        "pact_version": 3,
        "target_pact_version": 3,
        "change": {"id": "demo", "title": "Demo"},
        "requirements": [{
            "id": "req.demo",
            "statement": "The formal proof remains fresh.",
            "evidence_policy": {"required": ["formal_check"]},
        }],
        "tasks": [{
            "id": "task.demo",
            "title": "Prove Demo",
            "status": "done",
            "verification": "pytest -q",
            "requirements": ["req.demo"],
            "evidence": ["ev.formal.demo"],
        }],
        "formal": [{
            "id": "formal.demo",
            "requirement": "req.demo",
            "property_id": "DemoInvariant",
            "module": "formal/Demo.tla",
            "config": "formal/Demo.cfg",
            "checker_command": checker_command,
            "refinement_tests": [refinement_ref],
        }],
        "evidence": [{
            "id": "ev.formal.demo",
            "kind": "formal_check",
            "status": "pass",
            "refs": ["formal.demo", "req.demo", "task.demo"],
            "artifact": str(paths["artifact"]),
            "data": {
                "property_id": "DemoInvariant",
                "checker_command": checker_command,
                "fresh": True,
                "refinement_tests": [refinement_ref],
                "module_digest": _digest(paths["module"]),
                "config_digest": _digest(paths["config"]),
                "artifact_digest": _digest(paths["artifact"]),
                "checker_script_digests": {
                    "scripts/check.py": _digest(paths["checker"]),
                },
            },
        }],
    }
    _write_manifest(repo, manifest)
    _init_git(repo)
    return repo, paths


def _node(facts: list, node_id: str) -> NodeFact:
    return next(
        fact
        for fact in facts
        if isinstance(fact, NodeFact) and fact.node_id == node_id
    )


def test_manifest_membership_placement_and_mtime_semantics(tmp_path: Path) -> None:
    repo = tmp_path / "repo"
    demo_path = _write_manifest(repo, _base_manifest())
    _init_git(repo)
    source = PACTSource(repo="demo", pact_root=repo / "pact")

    initial = source.change_probe.build_token()
    stat = demo_path.stat()
    os.utime(demo_path, ns=(stat.st_atime_ns, stat.st_mtime_ns + 1_000_000))
    assert tokens_equal(initial, source.change_probe.build_token())

    other = _base_manifest("other")
    other_path = _write_manifest(repo, other)
    added = source.change_probe.build_token()
    assert not tokens_equal(initial, added)

    archive_path = repo / "pact" / "archive" / "other" / "pact.yaml"
    archive_path.parent.mkdir(parents=True)
    shutil.move(other_path, archive_path)
    moved = source.change_probe.build_token()
    assert not tokens_equal(added, moved)

    archive_path.unlink()
    assert tokens_equal(initial, source.change_probe.build_token())


@pytest.mark.parametrize(
    "input_name",
    ["module", "config", "checker", "refinement", "artifact"],
)
def test_referenced_input_matrix_busts_exact_token(
    tmp_path: Path,
    input_name: str,
) -> None:
    repo, paths = _formal_repo(tmp_path)
    source = PACTSource(repo="demo", pact_root=repo / "pact")
    before = source.change_probe.build_token()

    with paths[input_name].open("a", encoding="utf-8") as stream:
        stream.write(f"# changed {input_name}\n")

    assert not tokens_equal(before, source.change_probe.build_token())


def test_environment_truthiness_not_secret_value_is_tokenized(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repo = tmp_path / "repo"
    manifest = _base_manifest()
    manifest["tasks"][0]["environment"] = [{
        "id": "env.PACT_EXACT_TEST_ENV",
        "kind": "env_var",
        "name": "PACT_EXACT_TEST_ENV",
        "required": True,
    }]
    _write_manifest(repo, manifest)
    _init_git(repo)
    source = PACTSource(repo="demo", pact_root=repo / "pact")

    monkeypatch.delenv("PACT_EXACT_TEST_ENV", raising=False)
    absent = source.change_probe.build_token()
    monkeypatch.setenv("PACT_EXACT_TEST_ENV", "first-secret-value")
    present = source.change_probe.build_token()
    monkeypatch.setenv("PACT_EXACT_TEST_ENV", "different-secret-value")
    assert not tokens_equal(absent, present)
    assert tokens_equal(present, source.change_probe.build_token())


def test_exact_ledger_rows_detect_insert_update_and_delete(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repo = tmp_path / "repo"
    _write_manifest(repo, _base_manifest())
    _init_git(repo)
    source = PACTSource(repo="demo", pact_root=repo / "pact")
    rows: list[dict] = []

    def load_rows():
        return (
            {"demo": [dict(row) for row in rows]} if rows else {},
            {"status": "ok", "watermark": {"max_id": 0, "count": len(rows)}},
        )

    monkeypatch.setattr(source, "_load_ledger_decision_rows", load_rows)
    empty = source.change_probe.build_token()
    rows.append({
        "decision_id": 1,
        "change_id": "demo",
        "verdict": "accept",
        "principal": "operator",
        "note": "first",
    })
    inserted = source.change_probe.build_token()
    rows[0]["note"] = "updated in place"
    updated = source.change_probe.build_token()
    rows.clear()
    deleted = source.change_probe.build_token()

    assert not tokens_equal(empty, inserted)
    assert not tokens_equal(inserted, updated)
    assert not tokens_equal(updated, deleted)
    assert tokens_equal(empty, deleted)


def test_no_change_probe_and_applied_authority_contract(tmp_path: Path) -> None:
    repo = tmp_path / "repo"
    _write_manifest(repo, _base_manifest())
    _init_git(repo)
    source = PACTSource(repo="demo", pact_root=repo / "pact")
    token = source.change_probe.build_token()

    verdict = run_probe(
        source.change_probe,
        cursor={CURSOR_TOKEN_KEY: token},
        sync_scope=None,
    )
    assert verdict.changed is False
    assert source.probe_short_circuit_safe is True

    full = source.run("full")
    list(full.facts)
    assert full.applied_token_authority == "exact"
    assert full.applied_token_scope == "global"
    assert full.applied_raw_token is not None

    scoped = source.run(
        "scoped",
        sync_scope={"scope_keys": ["pact/changes/demo/pact.yaml"]},
    )
    list(scoped.facts)
    assert scoped.applied_token_authority == "unavailable"
    assert scoped.applied_token_scope == "unavailable"
    assert scoped.applied_raw_token is None


def test_capture_post_fence_rejects_mid_capture_change(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repo = tmp_path / "repo"
    _write_manifest(repo, _base_manifest())
    _init_git(repo)
    source = PACTSource(repo="demo", pact_root=repo / "pact")
    original = source._capture_evaluation_snapshot
    project = repo / "pact" / "project.yaml"
    mutated = False

    def mutate_after_full_capture(probe, *, token_only):
        nonlocal mutated
        snapshot = original(probe, token_only=token_only)
        if not token_only and not mutated:
            project.write_text("pact_version: 4\n", encoding="utf-8")
            mutated = True
        return snapshot

    monkeypatch.setattr(
        source,
        "_capture_evaluation_snapshot",
        mutate_after_full_capture,
    )
    with pytest.raises(
        ChangeProbeError,
        match="changed during snapshot capture",
    ):
        source.change_probe.capture_snapshot()


def test_reference_read_error_fails_closed(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repo, paths = _formal_repo(tmp_path)
    source = PACTSource(repo="demo", pact_root=repo / "pact")
    target = paths["artifact"]
    original = Path.read_bytes

    def fail_target(path: Path) -> bytes:
        if path == target:
            raise PermissionError("denied")
        return original(path)

    monkeypatch.setattr(Path, "read_bytes", fail_target)
    with pytest.raises(
        ChangeProbeError,
        match="could not read referenced file",
    ):
        source.change_probe.build_token()


def test_external_absolute_evidence_artifact_is_not_dereferenced(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repo = tmp_path / "repo"
    external = tmp_path / "operator-only" / "boundary-report.json"
    manifest = _base_manifest()
    manifest["evidence"] = [{
        "id": "ev.graph.demo",
        "kind": "graph_report",
        "status": "pass",
        "refs": ["req.demo"],
        "artifact": str(external.absolute()),
        "summary": "The remote boundary report was clean.",
    }]
    _write_manifest(repo, manifest)
    _init_git(repo)
    source = PACTSource(repo="demo", pact_root=repo / "pact")
    original = Path.stat

    def deny_external(path: Path, *args, **kwargs):
        if path == external.absolute():
            raise PermissionError("operator-only evidence")
        return original(path, *args, **kwargs)

    monkeypatch.setattr(Path, "stat", deny_external)

    token = source.change_probe.build_token()
    facts = list(source.run("portable-snapshot").facts)

    assert token
    assert any(
        isinstance(fact, NodeFact) and fact.node_id == "pact-change:demo"
        for fact in facts
    )


def test_manifest_directory_enumeration_error_fails_closed(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from okg.substrate.library.sources import pact as pact_source

    repo = tmp_path / "repo"
    _write_manifest(repo, _base_manifest())
    _init_git(repo)
    source = PACTSource(repo="demo", pact_root=repo / "pact")
    denied = repo / "pact" / "changes"
    original = pact_source.os.scandir

    def deny_manifest_directory(path):
        if Path(path) == denied:
            raise PermissionError("denied")
        return original(path)

    monkeypatch.setattr(pact_source.os, "scandir", deny_manifest_directory)
    with pytest.raises(
        ChangeProbeError,
        match="could not enumerate manifest directory",
    ):
        source.change_probe.build_token()


def test_non_mapping_project_policy_is_not_silent_default(
    tmp_path: Path,
) -> None:
    repo = tmp_path / "repo"
    _write_manifest(repo, _base_manifest())
    project = repo / "pact" / "project.yaml"
    project.write_text("- not-a-mapping\n", encoding="utf-8")
    _init_git(repo)

    snapshot = PACTSource(
        repo="demo",
        pact_root=repo / "pact",
    ).change_probe.capture_snapshot()

    errors = snapshot.evaluation_context.evidence_registry.config_errors
    assert errors
    assert "must be a mapping" in errors[0]


def test_tilde_evidence_artifact_uses_captured_home(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repo, paths = _formal_repo(tmp_path)
    captured_home = tmp_path / "captured-home"
    artifact = captured_home / "formal-report.yaml"
    artifact.parent.mkdir()
    artifact.write_bytes(paths["artifact"].read_bytes())
    manifest_path = repo / "pact" / "changes" / "demo" / "pact.yaml"
    manifest = yaml.safe_load(manifest_path.read_text(encoding="utf-8"))
    manifest["evidence"][0]["artifact"] = "~/formal-report.yaml"
    manifest["evidence"][0]["data"]["artifact_digest"] = _digest(artifact)
    manifest_path.write_text(
        yaml.safe_dump(manifest),
        encoding="utf-8",
    )
    monkeypatch.setenv("HOME", str(captured_home))

    run = PACTSource(repo="demo", pact_root=repo / "pact").run("tilde")
    requirement = _node(list(run.facts), "pact-req:demo:req.demo")

    assert requirement.attrs["satisfaction_status"] == "satisfied"
    assert run.applied_token_authority == "exact"


@pytest.mark.parametrize("reference_mode", ["absolute", "escape"])
def test_invalid_formal_reference_is_not_read_before_validation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    reference_mode: str,
) -> None:
    repo = tmp_path / "repo"
    outside = tmp_path / "outside.tla"
    outside.write_text("sensitive\n", encoding="utf-8")
    manifest = _base_manifest()
    manifest["formal"] = [{
        "id": "formal.demo",
        "requirement": "req.demo",
        "property_id": "DemoInvariant",
        "module": (
            str(outside)
            if reference_mode == "absolute"
            else "../outside.tla"
        ),
    }]
    _write_manifest(repo, manifest)
    _init_git(repo)
    source = PACTSource(repo="demo", pact_root=repo / "pact")
    original = Path.read_bytes
    outside_reads = 0

    def record_outside_read(path: Path) -> bytes:
        nonlocal outside_reads
        if path == outside:
            outside_reads += 1
        return original(path)

    monkeypatch.setattr(Path, "read_bytes", record_outside_read)
    token = source.change_probe.build_token()
    run = source.run(f"invalid-formal-{reference_mode}")
    list(run.facts)

    assert token
    assert outside_reads == 0
    assert run.applied_token_authority == "exact"


def test_unavailable_ledger_fails_closed(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repo = tmp_path / "repo"
    _write_manifest(repo, _base_manifest())
    _init_git(repo)
    source = PACTSource(repo="demo", pact_root=repo / "pact")
    monkeypatch.setattr(
        source,
        "_load_ledger_decision_rows",
        lambda: ({}, {"status": "unavailable", "reason": "test outage"}),
    )

    with pytest.raises(ChangeProbeError, match="test outage"):
        source.change_probe.build_token()


def test_no_manifests_do_not_require_irrelevant_ledger(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repo = tmp_path / "repo"
    repo.mkdir()
    (repo / "README.md").write_text(
        "repository without PACTs\n",
        encoding="utf-8",
    )
    _init_git(repo)
    source = PACTSource(repo="demo", pact_root=repo / "pact")

    def fail_if_called():
        raise AssertionError(
            "ledger rows cannot affect a manifest-free projection"
        )

    monkeypatch.setattr(source, "_load_ledger_decision_rows", fail_if_called)
    snapshot = source.change_probe.capture_snapshot()
    facts = list(source.run("manifest-free").facts)

    assert snapshot.manifests == ()
    assert snapshot.ledger_status["status"] == "not_applicable_no_manifests"
    assert [fact.node_id for fact in facts if isinstance(fact, NodeFact)] == [
        "cap:pact"
    ]


def test_aba_during_fact_evaluation_uses_captured_reference_bytes(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repo, paths = _formal_repo(tmp_path)
    source = PACTSource(repo="demo", pact_root=repo / "pact")
    artifact = paths["artifact"]
    original_bytes = artifact.read_bytes()
    original_facts = source._facts_for_manifest
    mutated = False

    def facts_during_aba(*args, **kwargs):
        nonlocal mutated
        if not mutated:
            mutated = True
            artifact.write_text("status: changed mid-read\n", encoding="utf-8")
        try:
            yield from original_facts(*args, **kwargs)
        finally:
            artifact.write_bytes(original_bytes)

    monkeypatch.setattr(source, "_facts_for_manifest", facts_during_aba)
    run = source.run("aba")
    facts = list(run.facts)

    requirement = _node(facts, "pact-req:demo:req.demo")
    assert requirement.attrs["satisfaction_status"] == "satisfied"
    assert artifact.read_bytes() == original_bytes
    assert tokens_equal(run.applied_raw_token, source.change_probe.build_token())


def test_home_redaction_uses_captured_authority_during_aba(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repo = tmp_path / "repo"
    captured_home = tmp_path / "captured-home"
    transient_home = tmp_path / "transient-home"
    manifest = _base_manifest()
    manifest["change"]["title"] = f"{captured_home}/private-note"
    _write_manifest(repo, manifest)
    _init_git(repo)
    monkeypatch.setenv("HOME", str(captured_home))
    source = PACTSource(repo="demo", pact_root=repo / "pact")
    original_facts = source._facts_for_manifest

    def facts_during_home_aba(*args, **kwargs):
        monkeypatch.setenv("HOME", str(transient_home))
        try:
            yield from original_facts(*args, **kwargs)
        finally:
            monkeypatch.setenv("HOME", str(captured_home))

    monkeypatch.setattr(source, "_facts_for_manifest", facts_during_home_aba)
    run = source.run("home-aba")
    facts = list(run.facts)

    assert _node(facts, "pact-change:demo").attrs["title"] == (
        "<home>/private-note"
    )
    assert tokens_equal(run.applied_raw_token, source.change_probe.build_token())


def test_supersession_uses_captured_manifest_membership(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repo = tmp_path / "repo"
    predecessor = _base_manifest("demo-b")
    successor = _base_manifest("demo-a")
    successor["change"]["rationale"] = "This change supersedes demo-b."
    predecessor_path = _write_manifest(repo, predecessor)
    _write_manifest(repo, successor)
    _init_git(repo)
    source = PACTSource(repo="demo", pact_root=repo / "pact")
    original_facts = source._facts_for_manifest
    deleted = False

    def delete_after_capture(manifest, **kwargs):
        nonlocal deleted
        if manifest.change.id == "demo-a" and not deleted:
            predecessor_path.unlink()
            deleted = True
        yield from original_facts(manifest, **kwargs)

    monkeypatch.setattr(source, "_facts_for_manifest", delete_after_capture)
    run = source.run("supersession")
    facts = list(run.facts)
    supersession = [
        fact
        for fact in facts
        if isinstance(fact, EdgeFact) and fact.edge_type == "supersedes"
    ]

    assert {(edge.src, edge.dst) for edge in supersession} == {
        ("pact-change:demo-a", "pact-change:demo-b"),
    }
