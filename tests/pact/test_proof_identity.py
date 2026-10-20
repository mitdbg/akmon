from __future__ import annotations

import os
import subprocess
from copy import deepcopy
from pathlib import Path

import pytest

from okg.substrate.pact.evaluation_context import PactEvaluationContext
from okg.substrate.pact.models import PactManifest
from okg.substrate.pact.proof_identity import (
    ProofIdentityError,
    build_code_proof_receipt,
    canonical_digest,
    receipt_payload,
    validate_code_proof_receipts,
)
from okg.substrate.pact.store import intent_digest, load_manifest_file, render_manifest
from okg.substrate.pact.scope_drift import staleness_report


CHANGE_ID = "proof-demo"
ACTIVE_MANIFEST = f"pact/changes/{CHANGE_ID}/pact.yaml"
ARCHIVE_MANIFEST = f"pact/archive/{CHANGE_ID}/pact.yaml"
REPORT = f"pact/changes/{CHANGE_ID}/reports/proof.json"


def _git(root: Path, *args: str) -> str:
    proc = subprocess.run(
        ["git", "-C", str(root), *args],
        check=True,
        capture_output=True,
        text=True,
    )
    return proc.stdout.strip()


def _commit(root: Path, message: str, *, date: str | None = None) -> str:
    _git(root, "add", "-A")
    env = os.environ.copy()
    if date is not None:
        env["GIT_AUTHOR_DATE"] = date
        env["GIT_COMMITTER_DATE"] = date
    subprocess.run(
        ["git", "-C", str(root), "commit", "-m", message],
        check=True,
        capture_output=True,
        text=True,
        env=env,
    )
    return _git(root, "rev-parse", "HEAD")


def _manifest() -> PactManifest:
    manifest = PactManifest.model_validate(
        {
            "pact_version": 4,
            "target_pact_version": 4,
            "change": {
                "id": CHANGE_ID,
                "title": "Proof identity",
                "status": "verification",
                "tier": 2,
                "rationale": "Prove code once and close metadata later.",
                "root_cause": "Code and closure identity were recursive.",
                "affected_code": [
                    ACTIVE_MANIFEST,
                    ARCHIVE_MANIFEST,
                    "src/feature.py",
                ],
            },
            "requirements": [
                {
                    "id": "req.proof",
                    "statement": "Code proof MUST remain exact.",
                    "evidence_policy": {"required": ["executed_test"]},
                }
            ],
            "tasks": [
                {
                    "id": "task.proof",
                    "title": "Prove",
                    "status": "done",
                    "verification": "pytest tests/pact/test_proof_identity.py -q",
                    "requirements": ["req.proof"],
                    "evidence": ["ev.proof"],
                }
            ],
            "test_plan": [
                {
                    "id": "test.proof",
                    "requirement": "req.proof",
                    "test_id": (
                        "tests/pact/test_proof_identity.py::"
                        "test_metadata_only_closure_reuses_code_proof"
                    ),
                    "command": "pytest tests/pact/test_proof_identity.py -q",
                    "evidence": ["executed_test"],
                }
            ],
            "evidence": [
                {
                    "id": "ev.proof",
                    "kind": "executed_test",
                    "status": "pass",
                    "refs": ["req.proof"],
                    "data": {
                        "attached_at": "2026-01-03T00:00:00+00:00",
                        "junit_test_ids": [
                            "tests/pact/test_proof_identity.py::"
                            "test_metadata_only_closure_reuses_code_proof"
                        ],
                    },
                }
            ],
        }
    )
    manifest.evidence.append(
        type(manifest.evidence[0]).model_validate(
            {
                "id": "ev.proof.intent-review",
                "kind": "intent_review",
                "status": "pass",
                "refs": [CHANGE_ID],
                "summary": "operator approved the digest-bound proof intent",
                "data": {
                    "decision": "approved",
                    "reviewer": "operator",
                    "reviewed_digest": intent_digest(manifest),
                },
            }
        )
    )
    return manifest


def _write_manifest(root: Path, manifest: PactManifest) -> None:
    path = root / ACTIVE_MANIFEST
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(render_manifest(manifest), encoding="utf-8")


def _repo_with_closure(tmp_path: Path) -> tuple[Path, str, str, PactManifest]:
    root = tmp_path / "repo"
    root.mkdir()
    _git(root, "init")
    _git(root, "config", "user.name", "PACT test")
    _git(root, "config", "user.email", "pact@example.test")
    (root / "README.md").write_text("base\n", encoding="utf-8")
    base = _commit(root, "base", date="2026-01-01T00:00:00+00:00")

    manifest = _manifest()
    _write_manifest(root, manifest)
    source = root / "src" / "feature.py"
    source.parent.mkdir(parents=True)
    source.write_text("VALUE = 1\n", encoding="utf-8")
    proven = _commit(
        root,
        "proven code",
        date="2026-01-02T00:00:00+00:00",
    )

    receipt = build_code_proof_receipt(
        root,
        manifest,
        base_ref=base,
        proven_code_ref=proven,
        closure_allowlist=[ACTIVE_MANIFEST, ARCHIVE_MANIFEST, REPORT],
    )
    manifest.proof_receipts.code = [receipt]
    _write_manifest(root, manifest)
    report = root / REPORT
    report.parent.mkdir(parents=True)
    report.write_text('{"proof":"pass"}\n', encoding="utf-8")
    closure = _commit(
        root,
        "metadata closure",
        date="2026-01-04T00:00:00+00:00",
    )
    return root, proven, closure, load_manifest_file(root / ACTIVE_MANIFEST)


def _resign(manifest: PactManifest) -> None:
    receipt = manifest.proof_receipts.code[0]
    receipt.code_digest = canonical_digest(
        [row.model_dump(mode="json") for row in receipt.code_tuples]
    )
    receipt.receipt_digest = canonical_digest(
        receipt_payload(receipt, include_receipt_digest=False)
    )


def test_metadata_only_closure_reuses_code_proof(tmp_path: Path) -> None:
    root, proven, closure, manifest = _repo_with_closure(tmp_path)

    result = validate_code_proof_receipts(root, manifest, closure)

    assert result.applies is True
    assert result.closure_sha == closure
    assert result.proven_code_heads == (proven,)
    assert result.receipt_digests == (
        manifest.proof_receipts.code[0].receipt_digest,
    )


def test_valid_receipt_keeps_proven_evidence_fresh_after_metadata_closure(
    tmp_path: Path,
) -> None:
    root, proven, _closure, manifest = _repo_with_closure(tmp_path)
    without_receipt = deepcopy(manifest)
    without_receipt.proof_receipts.code = []
    legacy = staleness_report(root, without_receipt, "req.proof")
    assert legacy["revoked"] is True
    assert legacy["code_changed_at"] == "2026-01-04T00:00:00+00:00"

    report = staleness_report(root, manifest, "req.proof")

    assert report["revoked"] is False
    assert report["fresh_count"] == 1
    assert report["code_changed_at"] == "2026-01-02T00:00:00+00:00"
    assert report["freshness_authority"] == "validated_code_proof"
    assert report["proof_identity"]["validation"]["proven_code_heads"] == [
        proven
    ]


def test_task_done_consumes_validated_proven_code_freshness(
    tmp_path: Path,
    monkeypatch,
) -> None:
    from okg.substrate.pact import operations

    root, proven, _closure, _manifest_at_head = _repo_with_closure(tmp_path)
    monkeypatch.setattr(
        operations,
        "_workflow_ledger_authority",
        lambda _root: {"fresh": True, "status": "fresh"},
    )
    monkeypatch.setattr(
        operations,
        "_workflow_graph_projection_status",
        lambda _root, _manifest: {
            "ok": True,
            "status": "fresh",
            "stale": False,
            "projected_generation_id": None,
            "recommended_commands": [],
        },
    )
    monkeypatch.setattr(
        operations,
        "_record_coordination_event",
        lambda *_args, **_kwargs: {"status": "skipped"},
    )

    gate = operations.task_completion_gate(root, CHANGE_ID, "task.proof")

    assert gate["ok"] is True, gate
    assert "stale_evidence" not in gate
    proof = gate["workflow_authority"]["code_proof_freshness"]
    assert proof["ok"] is True
    assert proof["validation"]["proven_code_heads"] == [proven]


def test_uncommitted_allowlisted_pact_evidence_keeps_proof_fresh(
    tmp_path: Path,
) -> None:
    root, _proven, _closure, manifest = _repo_with_closure(tmp_path)
    manifest.evidence[0].summary = "metadata-only closure note"
    _write_manifest(root, manifest)

    report = staleness_report(root, manifest, "req.proof")

    assert report["revoked"] is False
    assert report["proof_identity"]["validation"]["local_closure_paths"] == [
        ACTIVE_MANIFEST
    ]


def test_uncommitted_post_proof_code_revokes_freshness(tmp_path: Path) -> None:
    root, _proven, _closure, manifest = _repo_with_closure(tmp_path)
    (root / "src" / "feature.py").write_text("VALUE = 2\n", encoding="utf-8")

    report = staleness_report(root, manifest, "req.proof")

    assert report["revoked"] is True
    assert report["freshness_authority"] == "invalid_code_proof"
    assert report["proof_identity"]["error"]["code"] == (
        "code_proof_local_closure_scope_changed"
    )


def test_uncommitted_verification_edit_revokes_freshness(tmp_path: Path) -> None:
    root, _proven, _closure, manifest = _repo_with_closure(tmp_path)
    manifest.tasks[0].verification = "pytest tests/pact -q"
    _write_manifest(root, manifest)

    report = staleness_report(root, manifest, "req.proof")

    assert report["revoked"] is True
    assert report["proof_identity"]["error"]["code"] == (
        "code_proof_closure_manifest_mismatch"
    )


def test_captured_receipt_context_fails_closed_without_reopening_git(
    tmp_path: Path,
    monkeypatch,
) -> None:
    from okg.substrate.pact import proof_identity, scope_drift

    root, _proven, closure, manifest = _repo_with_closure(tmp_path)
    context = PactEvaluationContext(
        git_authoritative=True,
        git_head_sha=closure,
    )

    def _live_git_forbidden(*_args, **_kwargs):
        raise AssertionError("captured evaluation reopened live Git")

    monkeypatch.setattr(scope_drift, "_git", _live_git_forbidden)
    monkeypatch.setattr(proof_identity, "_run_git", _live_git_forbidden)

    report = scope_drift.staleness_report(
        root,
        manifest,
        "req.proof",
        context=context,
    )

    assert report["revoked"] is True
    assert report["freshness_authority"] == "invalid_code_proof"
    assert report["proof_identity"]["error"]["code"] == (
        "code_proof_captured_validation_unavailable"
    )


@pytest.mark.parametrize(
    "path",
    [
        "src/post_proof.py",
        "tests/test_post_proof.py",
        ".github/workflows/post-proof.yml",
        "scripts/post_proof.py",
    ],
)
def test_post_proof_code_test_workflow_or_script_edit_blocks(
    tmp_path: Path,
    path: str,
) -> None:
    root, _proven, _closure, manifest = _repo_with_closure(tmp_path)
    changed = root / path
    changed.parent.mkdir(parents=True, exist_ok=True)
    changed.write_text("changed\n", encoding="utf-8")
    head = _commit(root, "post-proof bytes")

    with pytest.raises(ProofIdentityError) as exc:
        validate_code_proof_receipts(root, manifest, head)

    assert exc.value.code == "code_proof_closure_scope_changed"


def test_unlisted_closure_report_blocks(tmp_path: Path) -> None:
    root, _proven, _closure, manifest = _repo_with_closure(tmp_path)
    report = root / f"pact/changes/{CHANGE_ID}/reports/not-enumerated.json"
    report.write_text("{}\n", encoding="utf-8")
    head = _commit(root, "unlisted report")

    with pytest.raises(ProofIdentityError) as exc:
        validate_code_proof_receipts(root, manifest, head)

    assert exc.value.code == "code_proof_closure_scope_changed"


def test_allowlisted_metadata_mode_change_blocks(tmp_path: Path) -> None:
    root, _proven, _closure, manifest = _repo_with_closure(tmp_path)
    report = root / REPORT
    report.chmod(0o755)
    head = _commit(root, "executable report")

    with pytest.raises(ProofIdentityError) as exc:
        validate_code_proof_receipts(root, manifest, head)

    assert exc.value.code == "code_proof_closure_metadata_mode_changed"


def test_verification_byte_edit_changes_intent_and_blocks(tmp_path: Path) -> None:
    root, _proven, _closure, manifest = _repo_with_closure(tmp_path)
    manifest.tasks[0].verification = "pytest tests/pact -q"
    _write_manifest(root, manifest)
    head = _commit(root, "changed verification")
    closure_manifest = load_manifest_file(root / ACTIVE_MANIFEST)

    with pytest.raises(ProofIdentityError) as exc:
        validate_code_proof_receipts(root, closure_manifest, head)

    assert exc.value.code == "code_proof_intent_digest_mismatch"


def test_mode_or_tombstone_rewrite_with_fresh_hash_still_blocks(tmp_path: Path) -> None:
    root, _proven, _closure, manifest = _repo_with_closure(tmp_path)
    receipt = manifest.proof_receipts.code[0]
    code_row = next(row for row in receipt.code_tuples if row.path == "src/feature.py")
    code_row.mode = "100755"
    code_row.object_id = "deletion"
    _resign(manifest)
    _write_manifest(root, manifest)
    head = _commit(root, "forged tuple")
    forged = load_manifest_file(root / ACTIVE_MANIFEST)

    with pytest.raises(ProofIdentityError) as exc:
        validate_code_proof_receipts(root, forged, head)

    assert exc.value.code == "code_proof_tuple_mismatch"


def test_receipt_mutation_without_rehash_blocks_before_git(tmp_path: Path) -> None:
    _root, _proven, _closure, manifest = _repo_with_closure(tmp_path)
    manifest.proof_receipts.code[0].closure_allowlist.append(
        f"pact/changes/{CHANGE_ID}/reports/forged.json"
    )

    with pytest.raises(ProofIdentityError) as exc:
        validate_code_proof_receipts(tmp_path, manifest, "HEAD")

    assert exc.value.code in {
        "invalid_code_proof_closure_allowlist_order",
        "code_proof_receipt_digest_mismatch",
    }


def test_nonancestor_closure_blocks(tmp_path: Path) -> None:
    root, proven, _closure, manifest = _repo_with_closure(tmp_path)
    _git(root, "checkout", "--orphan", "unrelated")
    _git(root, "rm", "-rf", ".")
    _write_manifest(root, manifest)
    unrelated = _commit(root, "unrelated")
    assert unrelated != proven

    with pytest.raises(ProofIdentityError) as exc:
        validate_code_proof_receipts(root, manifest, unrelated)

    assert exc.value.code == "code_proof_nonancestor"


def test_no_receipt_keeps_legacy_exact_head_path_compatible() -> None:
    result = validate_code_proof_receipts(Path("/not/a/repo"), _manifest(), "HEAD")
    assert result.applies is False
    assert result.closure_sha is None


def test_verification_script_cannot_be_allowlisted_as_evidence_metadata(
    tmp_path: Path,
) -> None:
    root = tmp_path / "repo"
    root.mkdir()
    _git(root, "init")
    _git(root, "config", "user.name", "PACT test")
    _git(root, "config", "user.email", "pact@example.test")
    (root / "README.md").write_text("base\n", encoding="utf-8")
    base = _commit(root, "base")
    manifest = _manifest()
    verifier = f"pact/changes/{CHANGE_ID}/evidence/verify.py"
    manifest.tasks[0].verification = f"python {verifier}"
    _write_manifest(root, manifest)
    source = root / "src" / "feature.py"
    source.parent.mkdir(parents=True)
    source.write_text("VALUE = 1\n", encoding="utf-8")
    proven = _commit(root, "proven code")

    with pytest.raises(ProofIdentityError) as exc:
        build_code_proof_receipt(
            root,
            manifest,
            base_ref=base,
            proven_code_ref=proven,
            closure_allowlist=[ACTIVE_MANIFEST, ARCHIVE_MANIFEST, verifier],
        )

    assert exc.value.code == "code_proof_closure_path_is_verification"
