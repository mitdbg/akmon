from __future__ import annotations

import hashlib
from pathlib import Path

import pytest
import yaml

from okg.substrate.pact import review, scope_drift
from okg.substrate.pact.evaluation_context import PactEvaluationContext
from okg.substrate.pact.evidence_kinds import DEFAULT_REGISTRY
from okg.substrate.pact.fsm import DecisionEvent
from okg.substrate.pact.models import Change, Evidence, PactManifest
from okg.substrate.pact.review import build_queue, manifest_condition
from okg.substrate.pact.scope_drift import drift_report, staleness_report
from okg.substrate.pact.validation import validate_manifest


def _environment_manifest() -> PactManifest:
    return PactManifest(
        change=Change(id="demo", title="Demo", tier=1),
        tasks=[
            {
                "id": "task.publish",
                "title": "Publish",
                "verification": "publish",
                "environment": [
                    {
                        "id": "env.CAPTURED_TOKEN",
                        "kind": "env_var",
                        "env": "CAPTURED_TOKEN",
                        "description": "captured token",
                    }
                ],
            }
        ],
    )


def _formal_manifest() -> PactManifest:
    return PactManifest(
        target_pact_version=4,
        change=Change(id="demo", title="Demo", tier=1),
        requirements=[
            {
                "id": "req.demo",
                "statement": "Formal proof is required.",
                "evidence_policy": {"required": ["formal_check"]},
                "acceptance": [
                    {
                        "id": "acc.demo",
                        "description": "formal proof",
                        "evidence": ["formal.demo"],
                    }
                ],
            }
        ],
        formal=[
            {
                "id": "formal.demo",
                "requirement": "req.demo",
                "property_id": "DemoInvariant",
                "module": "formal/demo/Demo.tla",
                "config": "formal/demo/Demo.cfg",
                "checker_command": "uv run python scripts/check_demo.py",
                "refinement_tests": [
                    "tests/pact/test_demo.py::test_demo"
                ],
            }
        ],
    )


def _digest(payload: bytes) -> str:
    return "sha256:" + hashlib.sha256(payload).hexdigest()


def _queue_manifest_bytes(change_id: str = "demo") -> bytes:
    return yaml.safe_dump({
        "pact_version": 0,
        "change": {
            "id": change_id,
            "title": "Captured review",
            "status": "proposal",
            "rationale": "captured",
            "root_cause": "captured",
        },
        "requirements": [
            {
                "id": f"req.{change_id}",
                "title": "Captured",
                "statement": "Captured evaluation is deterministic.",
                "evidence_policy": {"required": ["executed_test"]},
            }
        ],
        "tasks": [
            {
                "id": f"task.{change_id}",
                "title": "Captured",
                "status": "done",
                "verification": "pytest",
                "requirements": [f"req.{change_id}"],
                "evidence": [f"ev.{change_id}"],
            }
        ],
        "evidence": [
            {
                "id": f"ev.{change_id}",
                "kind": "executed_test",
                "status": "pass",
                "refs": [f"req.{change_id}"],
                "summary": "captured proof",
            }
        ],
    }).encode("utf-8")


def _staleness_manifest() -> PactManifest:
    return PactManifest(
        change={
            "id": "demo",
            "title": "Demo",
            "affected_code": ["src/demo.py"],
        },
        requirements=[
            {
                "id": "req.demo",
                "statement": "Demo remains correct.",
                "evidence_policy": {"required": ["executed_test"]},
            }
        ],
        tasks=[
            {
                "id": "task.demo",
                "title": "Demo",
                "status": "done",
                "verification": "pytest",
                "requirements": ["req.demo"],
                "evidence": ["ev.demo"],
            }
        ],
        evidence=[
            {
                "id": "ev.demo",
                "kind": "executed_test",
                "status": "pass",
                "refs": ["req.demo"],
                "data": {"attached_at": "2026-01-01T00:00:00Z"},
            }
        ],
    )


def test_captured_environment_is_strict_and_immutable(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("CAPTURED_TOKEN", "live-value")
    missing = PactEvaluationContext(env_truthiness={})

    report = validate_manifest(_environment_manifest(), context=missing)

    assert report.alignment["tasks"]["task.publish"][
        "environment_blockers"
    ][0]["name"] == "CAPTURED_TOKEN"
    with pytest.raises(TypeError):
        missing.env_truthiness["CAPTURED_TOKEN"] = True  # type: ignore[index]

    monkeypatch.delenv("CAPTURED_TOKEN")
    present = PactEvaluationContext(
        env_truthiness={"CAPTURED_TOKEN": True},
    )
    report = validate_manifest(_environment_manifest(), context=present)
    assert report.alignment["tasks"]["task.publish"][
        "environment_blockers"
    ] == []


def test_validation_consumes_captured_files_without_live_fallback(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    files = {
        "formal/demo/Demo.tla": b"---- MODULE Demo ----\n====\n",
        "formal/demo/Demo.cfg": b"SPECIFICATION Spec\n",
        "scripts/check_demo.py": b"raise SystemExit(0)\n",
        "tests/pact/test_demo.py": b"def test_demo():\n    pass\n",
    }
    context = PactEvaluationContext(
        files=files,
        evidence_registry=DEFAULT_REGISTRY,
    )

    def _unexpected_live_read(*_args, **_kwargs):
        raise AssertionError("captured validation reopened live state")

    monkeypatch.setattr(Path, "is_file", _unexpected_live_read)
    report = validate_manifest(
        _formal_manifest(),
        repo_root=tmp_path,
        context=context,
    )
    artifact_codes = {
        issue.code
        for issue in report.issues
        if issue.code.startswith("missing_formal")
    }
    assert artifact_codes == set()

    captured_missing = PactEvaluationContext(
        files={
            key: value
            for key, value in files.items()
            if key != "formal/demo/Demo.tla"
        },
        evidence_registry=DEFAULT_REGISTRY,
    )
    report = validate_manifest(
        _formal_manifest(),
        repo_root=tmp_path,
        context=captured_missing,
    )
    assert "missing_formal_module" in {
        issue.code for issue in report.issues
    }


def test_formal_freshness_hashes_captured_artifact_bytes(
    tmp_path: Path,
) -> None:
    files = {
        "formal/demo/Demo.tla": b"---- MODULE Demo ----\n====\n",
        "formal/demo/Demo.cfg": b"SPECIFICATION Spec\n",
        "scripts/check_demo.py": b"raise SystemExit(0)\n",
        "tests/pact/test_demo.py": b"def test_demo():\n    pass\n",
        "artifacts/demo.log": b"captured checker output\n",
    }
    manifest = _formal_manifest()
    manifest.evidence.append(Evidence(**{
        "id": "ev.formal.demo",
        "kind": "formal_check",
        "status": "pass",
        "refs": ["formal.demo", "req.demo"],
        "artifact": "artifacts/demo.log",
        "data": {
            "property_id": "DemoInvariant",
            "checker_command": "uv run python scripts/check_demo.py",
            "fresh": True,
            "refinement_tests": ["tests/pact/test_demo.py::test_demo"],
            "module_digest": _digest(files["formal/demo/Demo.tla"]),
            "config_digest": _digest(files["formal/demo/Demo.cfg"]),
            "artifact_digest": _digest(files["artifacts/demo.log"]),
            "checker_script_digests": {
                "scripts/check_demo.py": _digest(
                    files["scripts/check_demo.py"]
                ),
            },
        },
    }))
    context = PactEvaluationContext(
        files=files,
        evidence_registry=DEFAULT_REGISTRY,
    )

    report = validate_manifest(
        manifest,
        repo_root=tmp_path,
        context=context,
    )

    assert "stale_formal_evidence" not in {
        issue.code for issue in report.issues
    }
    assert report.alignment["requirements"]["req.demo"][
        "satisfaction_status"
    ] == "satisfied"


def test_review_queue_uses_captured_manifests_and_ledger(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    approved = DecisionEvent(
        change_id="demo",
        verdict="approve",
        principal="operator",
        authority="operator",
        from_state="awaiting_review",
        to_state="retired",
        decided_at="2026-01-02T00:00:00+00:00",
    )
    context = PactEvaluationContext(
        files={"pact/changes/demo/pact.yaml": _queue_manifest_bytes()},
        file_mtimes={"pact/changes/demo/pact.yaml": 100.0},
        evidence_registry=DEFAULT_REGISTRY,
        ledger_authoritative=True,
        ledger_available=True,
        decision_events_by_change={"demo": (approved,)},
        worktree_identity={"suffix": "captured"},
        evaluation_time=200.0,
    )

    def _unexpected_ledger(*_args, **_kwargs):
        raise AssertionError("captured review reopened the decision ledger")

    monkeypatch.setattr(
        review.ledger,
        "require_ledger_connection",
        _unexpected_ledger,
    )
    queue = build_queue(tmp_path, context=context)

    assert queue["warnings"] == []
    assert queue["queue"][0]["change"] == "demo"
    assert queue["queue"][0]["decision"] == "archive"
    assert queue["queue"][0]["age_seconds"] == 100.0


def test_review_queue_uses_captured_legacy_tier_cohort(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    document = yaml.safe_load(_queue_manifest_bytes())
    document["pact_version"] = 4
    document["target_pact_version"] = 4
    manifest_bytes = yaml.safe_dump(document, sort_keys=False).encode("utf-8")
    context = PactEvaluationContext(
        files={
            "pact/changes/demo/pact.yaml": manifest_bytes,
            "pact/project.yaml": (
                b"pact_defaults:\n"
                b"  legacy_missing_tier: 1\n"
                b"  legacy_missing_tier_snapshot: pact/legacy-tierless.txt\n"
            ),
            "pact/legacy-tierless.txt": b"demo\n",
        },
        evidence_registry=DEFAULT_REGISTRY,
        ledger_authoritative=True,
        ledger_available=True,
    )

    def _unexpected_live_settings(*_args, **_kwargs):
        raise AssertionError("captured tier cohort reopened live settings")

    from okg.substrate.pact import project_settings

    monkeypatch.setattr(
        project_settings,
        "load_project_settings",
        _unexpected_live_settings,
    )
    queue = build_queue(tmp_path, context=context)

    row = next(row for row in queue["queue"] if row["change"] == "demo")
    assert row["decision"] == "review"
    assert "missing_lifecycle_tier" not in row.get("flags", [])


def test_review_queue_keeps_live_defaults_without_context(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    manifest_path = tmp_path / "pact" / "changes" / "demo" / "pact.yaml"
    manifest_path.parent.mkdir(parents=True)
    manifest_path.write_bytes(_queue_manifest_bytes())

    class _Connection:
        def close(self) -> None:
            return None

    monkeypatch.setattr(
        review.ledger,
        "require_ledger_connection",
        lambda **_kwargs: _Connection(),
    )
    monkeypatch.setattr(
        review.ledger,
        "load_all_decisions",
        lambda _conn: {},
    )
    monkeypatch.setattr(
        review.ledger,
        "load_intents",
        lambda _conn: [],
    )

    queue = build_queue(tmp_path)

    assert queue["queue"][0]["change"] == "demo"
    assert queue["queue"][0]["decision"] == "review"


def test_captured_git_authority_drives_drift_and_staleness_fail_closed(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    manifest = _staleness_manifest()
    context = PactEvaluationContext(
        evidence_registry=DEFAULT_REGISTRY,
        git_authoritative=True,
        git_head_sha="a" * 40,
        git_base_ref="b" * 40,
        git_base_branch="dev",
        git_changed_paths=("src/demo.py", "src/rogue.py"),
        affected_code_timestamps={
            ("src/demo.py",): "2026-02-01T00:00:00Z",
        },
    )

    def _unexpected_git(*_args, **_kwargs):
        raise AssertionError("captured evaluation reopened Git")

    monkeypatch.setattr(scope_drift, "_git", _unexpected_git)
    drift = drift_report(tmp_path, manifest, context=context)
    stale = staleness_report(
        tmp_path,
        manifest,
        "req.demo",
        context=context,
    )
    condition = manifest_condition(
        manifest,
        repo_root=tmp_path,
        context=context,
    )

    assert drift["head_sha"] == "a" * 40
    assert drift["uncovered"] == ["src/rogue.py"]
    assert stale["revoked"] is True
    assert stale["head_sha"] == "a" * 40
    assert condition["evidence_satisfied"] is False

    incomplete = PactEvaluationContext(
        evidence_registry=DEFAULT_REGISTRY,
        git_authoritative=True,
        git_head_sha="c" * 40,
        git_base_ref="b" * 40,
        git_changed_paths=None,
        affected_code_timestamps={},
    )
    assert drift_report(
        tmp_path,
        manifest,
        context=incomplete,
    )["authority_missing"] is True
    missing_timestamp = staleness_report(
        tmp_path,
        manifest,
        "req.demo",
        context=incomplete,
    )
    assert missing_timestamp["authority_missing"] is True
    assert missing_timestamp["revoked"] is True

    unavailable = PactEvaluationContext(
        evidence_registry=DEFAULT_REGISTRY,
        git_authoritative=False,
        git_available=False,
    )
    assert drift_report(
        tmp_path,
        manifest,
        context=unavailable,
    )["authority_missing"] is True
    unavailable_timestamp = staleness_report(
        tmp_path,
        manifest,
        "req.demo",
        context=unavailable,
    )
    assert unavailable_timestamp["authority_missing"] is True
    assert unavailable_timestamp["revoked"] is True
