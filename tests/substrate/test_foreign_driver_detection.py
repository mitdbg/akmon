"""A checkout other than this release driving the database must be visible.

okg-workspace sat unpublished for weeks behind a Nomos fingerprint
mismatch whose actual cause was mundane: on 2026-08-04 the sync-root
checkout claimed catalog ownership and activated a newer rollout baseline
against production, while the sealed release kept deriving the older one.
Readiness reported only `active_policy_mismatch`, which reads like
tampering or corruption, so the recovery went looking for a denied read
path that was in fact permitting every standard subject.

The two conditions need different repairs and must not share a report:

  version_skew   — a legitimate writer running different CODE. Align the
                   release; do not distrust the policy.
  foreign_writer — a writer from a different deployment ROOT. The P0 #419
                   cross-boundary condition; never wave it through.
"""
from __future__ import annotations

from okg.substrate.catalog.foreign_driver import (
    FOREIGN_WRITER,
    VERSION_SKEW,
    WriterProvenance,
    compare_writers,
    recorded_writer_provenance,
    running_writer_provenance,
)

SEALED_ROOT = (
    "/Users/example/.local/share/okg/artifacts/25203ebf/repo/deployments/okg-workspace"
)
SYNC_ROOT = (
    "/Users/example/.local/share/okg/sync-roots/okg-workspace/deployments/okg-workspace"
)
SEALED_REVISION = "f7cffbc35c7a8e950b72b9a50a21a2ccbe0e232a"
SYNC_REVISION = "b8e13a8552c277a807bf839830f50853c7590f1e"


def test_a_newer_checkout_on_the_same_root_is_a_version_skew() -> None:
    """Same root, different code: align the release, not the policy."""
    comparison = compare_writers(
        recorded=WriterProvenance(
            deployment_root=SEALED_ROOT,
            code_revision=SYNC_REVISION,
            branch="okg-workspace-live-dev-20260722",
        ),
        running=WriterProvenance(
            deployment_root=SEALED_ROOT,
            code_revision=SEALED_REVISION,
        ),
    )

    assert comparison.ok is False
    assert comparison.status == "version_skew"
    assert comparison.reason_code == VERSION_SKEW
    assert comparison.differing_fields == ["code_revision"]
    # BOTH revisions named — a skew report showing one side is unactionable.
    assert SYNC_REVISION in comparison.message
    assert SEALED_REVISION in comparison.message
    assert "okg-workspace-live-dev-20260722" in comparison.message


def test_a_different_deployment_root_is_a_foreign_writer() -> None:
    """The P0 cross-boundary condition must not be reported as a skew."""
    comparison = compare_writers(
        recorded=WriterProvenance(
            deployment_root=SYNC_ROOT,
            code_revision=SYNC_REVISION,
        ),
        running=WriterProvenance(
            deployment_root=SEALED_ROOT,
            code_revision=SEALED_REVISION,
        ),
    )

    assert comparison.ok is False
    assert comparison.status == "foreign_writer"
    assert comparison.reason_code == FOREIGN_WRITER
    assert comparison.reason_code != VERSION_SKEW
    assert SYNC_ROOT in comparison.message
    assert SEALED_ROOT in comparison.message


def test_an_aligned_writer_reports_ok() -> None:
    provenance = WriterProvenance(
        deployment_root=SEALED_ROOT, code_revision=SEALED_REVISION,
    )

    comparison = compare_writers(recorded=provenance, running=provenance)

    assert comparison.ok is True
    assert comparison.status == "aligned"
    assert comparison.differing_fields == []


def test_no_recorded_claim_is_not_reported_as_drift() -> None:
    """Unclaimed is a separate condition with its own repair."""
    comparison = compare_writers(
        recorded=None,
        running=WriterProvenance(
            deployment_root=SEALED_ROOT, code_revision=SEALED_REVISION,
        ),
    )

    assert comparison.ok is True
    assert comparison.status == "no_recorded_writer"
    assert comparison.reason_code is None


def test_a_missing_field_on_either_side_is_not_drift() -> None:
    """Absence of evidence must not fire on claims predating the field."""
    comparison = compare_writers(
        recorded=WriterProvenance(deployment_root=SEALED_ROOT),
        running=WriterProvenance(
            deployment_root=SEALED_ROOT, code_revision=SEALED_REVISION,
        ),
    )

    assert comparison.ok is True
    assert comparison.status == "aligned"


def test_both_sides_are_always_present_in_the_payload() -> None:
    comparison = compare_writers(
        recorded=WriterProvenance(
            deployment_root=SEALED_ROOT, code_revision=SYNC_REVISION,
        ),
        running=WriterProvenance(
            deployment_root=SEALED_ROOT, code_revision=SEALED_REVISION,
        ),
    )

    payload = comparison.to_dict()

    assert payload["recorded_writer"]["code_revision"] == SYNC_REVISION
    assert payload["running_writer"]["code_revision"] == SEALED_REVISION
    assert payload["reason_code"] == VERSION_SKEW


def test_provenance_is_read_from_the_ownership_metadata() -> None:
    """The real okg-workspace ownership row shape."""
    owner = {
        "deployment_name": "okg-workspace",
        "metadata": {
            "deployment_root": SYNC_ROOT,
            "worktree_root": "/Users/example/.local/share/okg/sync-roots/okg-workspace",
            "git_commit": SYNC_REVISION,
            "git_branch": "okg-workspace-live-dev-20260722",
        },
    }

    recorded = recorded_writer_provenance(owner)

    assert recorded is not None
    assert recorded.deployment_root == SYNC_ROOT
    assert recorded.code_revision == SYNC_REVISION
    assert recorded.branch == "okg-workspace-live-dev-20260722"


def test_running_provenance_picks_up_the_release_environment() -> None:
    running = running_writer_provenance(
        deployment_root=SEALED_ROOT,
        code_revision=SEALED_REVISION,
        environ={
            "OKG_ENVIRONMENT_ID": "okg-workspace-production",
            "OKG_RELEASE_ARTIFACT_ID": "sha256:25203ebf",
        },
    )

    assert running.environment_id == "okg-workspace-production"
    assert running.release_artifact_id == "sha256:25203ebf"


def test_the_okg_workspace_incident_is_classified_as_foreign_writer() -> None:
    """End to end on the recorded 2026-08-04 event."""
    owner = {
        "metadata": {
            "deployment_root": SYNC_ROOT,
            "git_commit": SYNC_REVISION,
            "git_branch": "okg-workspace-live-dev-20260722",
        },
    }

    comparison = compare_writers(
        recorded=recorded_writer_provenance(owner),
        running=running_writer_provenance(
            deployment_root=SEALED_ROOT,
            code_revision=SEALED_REVISION,
            environ={"OKG_ENVIRONMENT_ID": "okg-workspace-production"},
        ),
    )

    assert comparison.status == "foreign_writer"
    # The report names the sync-root, which is the fact that would have
    # ended the misdiagnosis immediately.
    assert "sync-roots" in comparison.message
