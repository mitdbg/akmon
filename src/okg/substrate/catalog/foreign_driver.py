"""Detect a checkout other than this release driving a deployment's database.

A deployment can be written to by a checkout that is not the release the
environment activated. That is invisible until something refuses, because
nothing compares the two: the graph keeps answering reads, the worker
keeps its own code revision, and the divergence only surfaces the next
time an identity has to match.

okg-workspace hit this. On 2026-08-04 18:33 the catalog ownership claim
was taken from the SYNC-ROOT checkout
(`~/.local/share/okg/sync-roots/okg-workspace`, branch
`okg-workspace-live-dev-20260722`, commit b8e13a85), and twelve minutes
later at 18:45 a thirteen-rule Nomos rollout baseline was activated from
the same place. The sealed production release runs code_revision
f7cffbc3 and derives a twelve-rule baseline, so readiness reported
`nomos.runtime.active_policy_mismatch` — a fingerprint disagreement with
no indication that the cause was simply a NEWER checkout doing
maintenance. The deployment then sat unpublished for weeks while the
mismatch read as possible corruption.

The distinction this module draws:

  version_skew   — a legitimate writer whose CODE differs from the
                   release this process is running. Repair is to align
                   the release, not to distrust the policy.
  foreign_writer — a writer from a different deployment root, i.e. a
                   genuine cross-boundary write. This is the P0 #419
                   condition and must NOT be waved through.

Both are reported by naming BOTH sides. A fingerprint that disagrees with
no recorded claimant at all is neither: that is tampering or loss, and it
is left to the ownership checks that already cover it.
"""
from __future__ import annotations

import os
from dataclasses import dataclass, field
from typing import Any

VERSION_SKEW = "writer_version_skew"
FOREIGN_WRITER = "writer_foreign_root"


@dataclass(frozen=True)
class WriterProvenance:
    """Where a writer wrote from, as recorded or as running."""

    deployment_root: str | None = None
    worktree_root: str | None = None
    code_revision: str | None = None
    branch: str | None = None
    release_artifact_id: str | None = None
    environment_id: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "deployment_root": self.deployment_root,
            "worktree_root": self.worktree_root,
            "code_revision": self.code_revision,
            "branch": self.branch,
            "release_artifact_id": self.release_artifact_id,
            "environment_id": self.environment_id,
        }


@dataclass(frozen=True)
class DriverComparison:
    ok: bool
    status: str
    reason_code: str | None = None
    message: str = ""
    recorded: WriterProvenance | None = None
    running: WriterProvenance | None = None
    differing_fields: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema": "okg.writer_driver_comparison.v1",
            "ok": self.ok,
            "status": self.status,
            "reason_code": self.reason_code,
            "message": self.message,
            # Both sides, always named. A skew report that shows only one
            # revision cannot be acted on.
            "recorded_writer": self.recorded.to_dict() if self.recorded else None,
            "running_writer": self.running.to_dict() if self.running else None,
            "differing_fields": list(self.differing_fields),
        }


def running_writer_provenance(
    *,
    deployment_root: str | None = None,
    code_revision: str | None = None,
    environ: dict[str, str] | None = None,
) -> WriterProvenance:
    """Provenance of the process asking the question."""
    env = dict(environ if environ is not None else os.environ)
    return WriterProvenance(
        deployment_root=deployment_root,
        code_revision=code_revision,
        release_artifact_id=env.get("OKG_RELEASE_ARTIFACT_ID") or None,
        environment_id=env.get("OKG_ENVIRONMENT_ID") or None,
    )


def recorded_writer_provenance(owner: dict[str, Any] | None) -> WriterProvenance | None:
    """Provenance recorded on the ownership claim, if any."""
    if not isinstance(owner, dict):
        return None
    metadata = owner.get("metadata")
    if not isinstance(metadata, dict):
        metadata = {}
    return WriterProvenance(
        deployment_root=metadata.get("deployment_root"),
        worktree_root=metadata.get("worktree_root"),
        code_revision=metadata.get("git_commit"),
        branch=metadata.get("git_branch"),
        release_artifact_id=metadata.get("release_artifact_id"),
        environment_id=metadata.get("environment_id"),
    )


def compare_writers(
    *,
    recorded: WriterProvenance | None,
    running: WriterProvenance,
) -> DriverComparison:
    """Classify the difference between the recorded and running writers."""
    if recorded is None:
        # No claim to compare against. Unclaimed is a separate condition
        # with its own repair; saying nothing here keeps them distinct.
        return DriverComparison(
            ok=True,
            status="no_recorded_writer",
            message="no ownership claim recorded; nothing to compare",
            running=running,
        )

    differing: list[str] = []
    for label, left, right in (
        ("deployment_root", recorded.deployment_root, running.deployment_root),
        ("code_revision", recorded.code_revision, running.code_revision),
        ("release_artifact_id",
         recorded.release_artifact_id, running.release_artifact_id),
    ):
        # Only compare when BOTH sides know the value. A missing field is
        # absence of evidence; reporting it as drift would fire on every
        # deployment whose claim predates the field.
        if left and right and str(left) != str(right):
            differing.append(label)

    if not differing:
        return DriverComparison(
            ok=True,
            status="aligned",
            message="the recorded writer matches the running release",
            recorded=recorded,
            running=running,
        )

    if "deployment_root" in differing:
        return DriverComparison(
            ok=False,
            status="foreign_writer",
            reason_code=FOREIGN_WRITER,
            message=(
                "the recorded writer claimed from a different deployment root: "
                f"recorded {recorded.deployment_root!r}, "
                f"running {running.deployment_root!r}"
            ),
            recorded=recorded,
            running=running,
            differing_fields=differing,
        )

    return DriverComparison(
        ok=False,
        status="version_skew",
        reason_code=VERSION_SKEW,
        message=(
            "the recorded writer ran different code from this release: "
            f"recorded revision {recorded.code_revision!r}"
            f"{f' on {recorded.branch!r}' if recorded.branch else ''}, "
            f"running revision {running.code_revision!r}"
        ),
        recorded=recorded,
        running=running,
        differing_fields=differing,
    )
