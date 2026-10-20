from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
import subprocess
from typing import Any


# What a READ decision ledger answered about the merge approval binding the
# change's current intent digest (issue #1443). One surface, four answers, so
# each cause names its own repair instead of collapsing into `ledger_not_fresh`
# — a token classified operator-action-only, which published an owner-and-prose
# remedy for a database that was working and no command at all.
#
# `not_judged` is not a defect: `gate task-done` and the cutover report answer a
# different question, and a merge approval is no part of their bar. It is the
# default only so those callers read naturally; every production construction
# passes the field explicitly, and the merge boundary's `authority_payload`
# seam requires it, so a new branch cannot inherit a permissive answer.
LEDGER_APPROVAL_NOT_JUDGED = "not_judged"
LEDGER_APPROVAL_APPROVED = "approved"
# No approval binds the current intent digest: none was ever recorded, a later
# decision superseded it, or the manifest changed after it. A NEW decision is
# the repair, so `okg pact decide` is a real command here.
LEDGER_APPROVAL_ABSENT = "absent"
# An approval DOES bind the current digest, but the change still sits in
# `pact/changes/` instead of `pact/archive/`. Recommending a decide verdict
# here is the inert-command loop: the FSM-legal verdict in that state is
# `defer`, which records a defer and leaves the gate refusing identically. The
# repair is the archive step.
LEDGER_APPROVAL_NOT_ARCHIVED = "not_archived"

LEDGER_APPROVALS = (
    LEDGER_APPROVAL_NOT_JUDGED,
    LEDGER_APPROVAL_APPROVED,
    LEDGER_APPROVAL_ABSENT,
    LEDGER_APPROVAL_NOT_ARCHIVED,
)


@dataclass(frozen=True)
class GitEvidence:
    commit: str | None = None
    branch: str | None = None
    diff_ref: str | None = None
    file_paths: tuple[str, ...] = ()
    test_artifacts: tuple[str, ...] = ()
    live_checked: bool = False

    def present(self) -> bool:
        return self.live_checked and bool(
            self.commit
            or self.branch
            or self.diff_ref
            or self.file_paths
            or self.test_artifacts
        )


@dataclass(frozen=True)
class WorkflowAuthorityInput:
    file_fresh: bool
    graph_required: bool
    graph_ready: bool
    graph_fresh: bool
    ledger_fresh: bool
    generation_pin_matches: bool
    evidence_artifacts_fresh: bool
    pact_coordination_agree: bool
    # True when the ledger is unreachable BECAUSE no DSN resolved in this
    # shell (issue #643, req.pactdx.ledger-hints). Distinguishes the
    # "nothing configured" blocker from a stale-but-configured ledger;
    # `pact_coordination_disagreement` stays reserved for genuine
    # pact-vs-coordination disagreement.
    ledger_no_dsn: bool = False
    # What the READ ledger answered about the merge approval — one of
    # LEDGER_APPROVALS. A separate fact from `ledger_fresh`: a ledger that
    # answered the query IS fresh, and "no approval binds this digest" is an
    # ANSWER, not staleness. Read only when `ledger_fresh` is true, because an
    # unreachable ledger cannot testify to what it holds.
    ledger_approval: str = LEDGER_APPROVAL_NOT_JUDGED
    projection_authority: str = "default_graph"
    event_plane_authority_allowed: bool = False
    # KNOWN GAP, tracked as a follow-up: `formal/pact/WorkflowAuthority.tla`
    # does NOT yet model this arm, so there is no NoUnauthorizedExternalAuthority
    # invariant beside the event-plane one. Extending the model changes the
    # module and config digests that `pact-no-decision-status-truth` pins its
    # formal evidence against, so it must be done in the same change that
    # re-runs TLC and re-ingests that proof — re-sealing it by hand would forge
    # the receipt. The arm's refusals are covered by
    # `tests/pact/test_external_repository_authority.py` in the meantime.
    #
    # The external-repository receipt surface (issue #1424). `allowed` defaults
    # OFF so a caller that never heard of the surface cannot enable it;
    # `bound` defaults permissively because it is read ONLY once `allowed` is
    # true, and the surface is only ever selected by an explicit declaration
    # in the manifest.
    external_authority_allowed: bool = False
    external_receipt_bound: bool = True
    intent_event_fresh: bool = True
    intent_digest_matches: bool = True
    worktree_head_matches: bool = True
    worktree_clean: bool = True
    code_claims: tuple[str, ...] = ()
    git_evidence: GitEvidence = field(default_factory=GitEvidence)


# Blockers that are facts about the GRAPH PROJECTION, not about the change.
# The projection is a derived read-only view: a projector that is not running,
# a node not yet published, or a node published before the last manifest edit
# are all states of the projector, and none of them is evidence that the change
# is unfit to merge (issue #1476 - a green head waited because "the local graph
# worker is absent").
#
# `evaluate_workflow_authority` still refuses to ACCEPT on these: authority it
# could not read is authority it does not have, and the TLA model in
# `formal/pact/WorkflowAuthority.tla` says exactly that. What changes is the
# merge boundary's VERDICT. A gate whose only unmet items are on this list
# reports `deferred` - it holds on everything it could check and names what it
# could not - rather than `refused`, which reads as "this change is bad".
# Callers own that distinction, so the membership lives here in one place.
# `intent_event_not_fresh` is deliberately NOT here. It fires only when the
# event plane is being used INSTEAD of the graph as the authority surface, so
# it is a fact about the authority itself, not about a derived copy of it.
PROJECTION_ONLY_BLOCKERS = frozenset(
    {
        "graph_not_ready",
        "graph_not_fresh",
        "generation_pin_mismatch",
    }
)


def projection_only(blockers: Any) -> bool:
    """Is every blocker a fact about the projection rather than the change?

    An EMPTY blocker list is not projection-only: nothing was deferred, so a
    caller that treated it as such would downgrade a clean pass to exit 3.
    """

    items = tuple(blockers or ())
    return bool(items) and set(items) <= PROJECTION_ONLY_BLOCKERS


@dataclass(frozen=True)
class WorkflowAuthorityDecision:
    accepted: bool
    blockers: tuple[str, ...]
    required_live_checks: tuple[str, ...]


def evaluate_workflow_authority(
    packet: WorkflowAuthorityInput,
) -> WorkflowAuthorityDecision:
    blockers: list[str] = []
    graph_backed = packet.graph_required and packet.projection_authority in {
        "default_graph",
        "worktree_branch_graph",
        "published_generation_evidence",
    }
    published_generation_backed = (
        graph_backed and packet.projection_authority == "published_generation_evidence"
    )
    event_requested = (
        packet.graph_required
        and packet.projection_authority == "event_plane_intent"
    )
    event_backed = event_requested and packet.event_plane_authority_allowed
    external_requested = (
        packet.graph_required
        and packet.projection_authority == "external_repository_receipt"
    )
    external_backed = external_requested and packet.external_authority_allowed
    # Both surfaces prove the change through the identity of THIS checkout
    # rather than through a projection of it, so both answer the same three
    # identity questions. Keeping the arm membership in one name stops a
    # second surface from quietly inheriting only some of them.
    identity_backed = event_backed or external_backed
    known_authority = (
        not packet.graph_required
        or graph_backed
        or event_backed
        or external_backed
        or packet.projection_authority == "not_required"
    )
    if not packet.file_fresh:
        blockers.append("file_not_fresh")
    if not known_authority:
        blockers.append("graph_authority_unknown")
    if event_requested and not packet.event_plane_authority_allowed:
        blockers.append("event_plane_not_allowed")
    if external_requested and not packet.external_authority_allowed:
        blockers.append("external_authority_not_allowed")
    if graph_backed and not packet.graph_ready:
        blockers.append("graph_not_ready")
    if graph_backed and not packet.graph_fresh:
        blockers.append("graph_not_fresh")
    if event_backed and not packet.intent_event_fresh:
        blockers.append("intent_event_not_fresh")
    if external_backed and not packet.external_receipt_bound:
        blockers.append("external_receipt_not_bound")
    if identity_backed and not packet.intent_digest_matches:
        blockers.append("intent_digest_mismatch")
    if identity_backed and not packet.worktree_head_matches:
        blockers.append("worktree_head_mismatch")
    if identity_backed and not packet.worktree_clean:
        blockers.append("worktree_not_clean")
    if not packet.ledger_fresh:
        blockers.append(
            "ledger_dsn_not_configured"
            if packet.ledger_no_dsn
            else "ledger_not_fresh"
        )
    # Only a ledger that was actually READ can testify to what it holds. An
    # unreachable ledger says nothing about approval, so both blockers are
    # gated on freshness rather than emitted alongside it.
    if packet.ledger_fresh and packet.ledger_approval == LEDGER_APPROVAL_ABSENT:
        blockers.append("no_ledger_approval")
    if packet.ledger_fresh and (
        packet.ledger_approval == LEDGER_APPROVAL_NOT_ARCHIVED
    ):
        blockers.append("approval_not_archived")
    if graph_backed and not packet.generation_pin_matches:
        blockers.append("generation_pin_mismatch")
    if not packet.evidence_artifacts_fresh:
        blockers.append("evidence_artifacts_stale")
    if not packet.pact_coordination_agree:
        blockers.append("pact_coordination_disagreement")
    if packet.code_claims and not packet.git_evidence.present():
        blockers.append("missing_git_evidence")

    checks: list[str] = []
    if not packet.file_fresh:
        checks.append("reload pact.yaml")
    if packet.code_claims:
        checks.extend(["git status", "focused tests"])
    if not known_authority:
        checks.append("select graph authority surface")
    if event_requested and not packet.event_plane_authority_allowed:
        checks.append("use sync-root graph projection")
    if external_requested and not packet.external_authority_allowed:
        checks.append("fix the external authority declaration")
    if external_backed and not packet.external_receipt_bound:
        checks.append("re-record the external approval receipt")
    if graph_backed and not packet.graph_ready:
        checks.append(
            "verify exact published generation"
            if published_generation_backed
            else "run graph readiness check"
        )
    if graph_backed and not packet.graph_fresh:
        checks.append(
            "verify exact published generation"
            if published_generation_backed
            else "refresh graph projection"
        )
    if event_backed and not packet.intent_event_fresh:
        checks.append("refresh pact intent event")
    if identity_backed and not packet.intent_digest_matches:
        checks.append("refresh pact intent digest")
    if identity_backed and not packet.worktree_head_matches:
        checks.append("refresh pact intent head")
    if identity_backed and not packet.worktree_clean:
        checks.append("clean worktree")
    if not packet.ledger_fresh:
        checks.append(
            "configure a database connection (export OKG_DSN=... or "
            "okg deployment env <deployment> --write-local)"
            if packet.ledger_no_dsn
            else "refresh decision ledger"
        )
    if packet.ledger_fresh and packet.ledger_approval == LEDGER_APPROVAL_ABSENT:
        checks.append("record an operator decision with okg pact decide")
    if packet.ledger_fresh and (
        packet.ledger_approval == LEDGER_APPROVAL_NOT_ARCHIVED
    ):
        checks.append("archive the approved change")
    if graph_backed and not packet.generation_pin_matches:
        checks.append(
            "reconcile published generation pin"
            if published_generation_backed
            else "repin graph generation"
        )
    if not packet.evidence_artifacts_fresh:
        checks.append("rerun evidence checks")
    if not packet.pact_coordination_agree:
        checks.append("reconcile pact and coordination state")

    return WorkflowAuthorityDecision(
        accepted=not blockers,
        blockers=tuple(blockers),
        required_live_checks=tuple(dict.fromkeys(checks)),
    )


def workflow_authority_payload(
    decision: WorkflowAuthorityDecision,
) -> dict[str, object]:
    return {
        "accepted": decision.accepted,
        "blockers": list(decision.blockers),
        "projection_only": projection_only(decision.blockers),
        "required_live_checks": list(decision.required_live_checks),
    }


def manifest_file_authority(
    repo_root: Path,
    change_id: str,
    manifest: Any,
    *,
    captured_path: Path | None = None,
    captured_state_digest: str | None = None,
) -> dict[str, object]:
    from okg.substrate.pact.store import load_manifest_file, state_digest

    expected_digest = state_digest(manifest)
    if captured_path is not None:
        return {
            "fresh": (
                captured_state_digest is not None
                and captured_state_digest == expected_digest
            ),
            "path": str(captured_path),
            "state_digest": expected_digest,
            "file_state_digest": captured_state_digest,
        }
    candidates = [
        repo_root / "pact" / "changes" / change_id / "pact.yaml",
        repo_root / "pact" / "archive" / change_id / "pact.yaml",
    ]
    for path in candidates:
        if not path.is_file():
            continue
        loaded = load_manifest_file(path)
        actual_digest = state_digest(loaded)
        return {
            "fresh": actual_digest == expected_digest,
            "path": str(path),
            "state_digest": expected_digest,
            "file_state_digest": actual_digest,
        }
    return {
        "fresh": False,
        "path": None,
        "state_digest": expected_digest,
        "file_state_digest": None,
    }


def git_evidence_for_manifest(
    repo_root: Path,
    manifest: Any,
) -> GitEvidence:
    affected_code = tuple(getattr(manifest.change, "affected_code", ()) or ())
    try:
        head = subprocess.run(
            ["git", "-C", str(repo_root), "rev-parse", "HEAD"],
            capture_output=True,
            text=True,
            timeout=5,
            check=False,
        )
        branch = subprocess.run(
            ["git", "-C", str(repo_root), "rev-parse", "--abbrev-ref", "HEAD"],
            capture_output=True,
            text=True,
            timeout=5,
            check=False,
        )
        status = subprocess.run(
            ["git", "-C", str(repo_root), "status", "--porcelain=v1"],
            capture_output=True,
            text=True,
            timeout=5,
            check=False,
        )
    except (OSError, subprocess.SubprocessError):
        return GitEvidence(file_paths=affected_code, live_checked=False)

    live_checked = head.returncode == 0 and branch.returncode == 0 and status.returncode == 0
    return GitEvidence(
        commit=head.stdout.strip() if head.returncode == 0 else None,
        branch=branch.stdout.strip() if branch.returncode == 0 else None,
        diff_ref="working-tree" if status.stdout.strip() else None,
        file_paths=affected_code,
        live_checked=live_checked,
    )
