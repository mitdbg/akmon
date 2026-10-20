"""The disposition policy for legacy PACT hygiene findings (#978).

`pact_hygiene_issues` (validation.py) has reported three findings as warnings
since PR #733 because the tree carried a backlog no gate could clear without
either inventing evidence or falsely advancing a lifecycle status. This module
is the policy that says, mechanically, what to do with each finding, and the
rule that stops the backlog growing.

Three classes, decided from manifest fields plus git -- never from judgement:

``archive_as_is``
    The change landed long ago and its evidence is no longer the thing under
    review. Condition: every task is ``done``, the manifest is LANDED (its
    newest commit is an ancestor of the integration branch and older than
    ``FREEZE_DAYS``), it is not SEALED, and no done task lacks evidence.
    Action: ``change.status`` advances from ``proposal`` to ``verification``.
    No evidence is written and NOTHING is moved. The physical move to
    ``pact/archive/`` is deliberately excluded: relocation rewrites artifact
    paths recorded inside sealed evidence, and re-sealing a digest by hand
    would forge the proof it binds.

``recorded_downgrade``
    Evidence existed or was owed, and cannot be recovered. Condition: the
    finding is not ``archive_as_is`` and no existing checker can regenerate the
    missing proof. Action: the status advance above where the status finding
    fires, plus one ``kind: waiver, status: waived`` evidence record per
    unevidenced done task naming the reason. A waiver is an explicit, visible
    non-proof; it never claims a run happened.

``real_proof_rerun``
    An existing checker can regenerate the proof. Condition: the unevidenced
    done task links a ``test_plan`` entry that carries a command and whose
    ``test_id`` still resolves to a file in the tree. Action: NO EVIDENCE is
    written. The command is reported so it can be run and ingested through
    ``okg pact evidence ingest``. Fabricating the row instead is the one thing
    this policy exists to prevent. A stale-status finding on the SAME manifest
    is a separate finding with its own class, and is still repaired: classes
    are assigned per finding, not per file.

Both fields the policy ever writes -- ``change.status`` and the top-level
``evidence`` list -- are OUTSIDE the intent digest (`store.intent_digest`), so
applying it drops no recorded operator approval.

Landedness FAILS CLOSED: when git cannot answer -- an unreachable
integration ref, no commit for the path there, an unparseable date -- the
manifest is treated as not landed, which routes it to ``recorded_downgrade``
rather than to a silent archive. Sealedness is decided from the manifest's own
declared artifact paths and formal bindings; it reads no files.

The policy has one PRECONDITION, which is not a class: it applies only to a
manifest that already validates clean. 63 active manifests carry
error-severity validation defects (`task_done_missing_evidence`,
`stale_formal_evidence`, ...) that CI never sees, because `validate-pacts`
only validates the manifests a pull request CHANGES. Editing one would drag
its inherited errors into the check and turn the sweep itself red -- so a
sweep that touched them could never be merged, and a sweep allowed to touch
them would be blamed for defects it did not create. Those records need an
author, not a sweep; `blocked_invalid_manifest` reports each one with its
codes and leaves it exactly as it was.

The second half of this module is the forward rule -- `freshness_issues` --
enforced by `scripts/ci/verdict_policy.py validate-pacts` on the manifests a
pull request actually changes. It is scoped to changed manifests on purpose:
the legacy corpus is dispositioned by the policy above, and a corpus-wide flip
would refuse every unrelated PACT edit.
"""

from __future__ import annotations

import json
import subprocess
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from okg.substrate.pact.models import PactManifest
from okg.substrate.pact.store import intent_digest
from okg.substrate.pact.validation import (
    PASSING_EVIDENCE,
    ValidationIssue,
    _task_valid_evidence_refs,
)

CLASS_ARCHIVE_AS_IS = "archive_as_is"
CLASS_RECORDED_DOWNGRADE = "recorded_downgrade"
CLASS_REAL_PROOF_RERUN = "real_proof_rerun"

# How old a merged manifest must be before "landed long ago" is true. Short
# enough that genuinely finished work clears within a sprint, long enough that
# a change still being reviewed is never swept.
FREEZE_DAYS = 30

# The lifecycle value a swept manifest advances to. `verification` is the
# terminal value the v4 schema admits (models.Change.status); there is no
# `archived` status, because a change leaves the active tree by being MOVED,
# never by declaring itself finished (corpus_hygiene.ACTIVE_STATUSES).
SWEPT_STATUS = "verification"

# Kind + status of the record a recorded downgrade writes. `waiver` is a
# canonical registered kind (pact/project.yaml evidence_kinds) and `waived` is
# in PASSING_EVIDENCE, so the row clears the hygiene predicate while remaining
# visibly not a proof: it satisfies no `executed_test` evidence policy.
DOWNGRADE_KIND = "waiver"
DOWNGRADE_STATUS = "waived"


@dataclass(frozen=True)
class TaskDisposition:
    """What the policy says about one `task_done_empty_evidence` finding."""

    task_id: str
    disposition: str
    reason: str
    command: str | None = None


@dataclass(frozen=True)
class ManifestDisposition:
    """The policy's answer for one manifest."""

    change_id: str
    disposition: str
    reason: str
    status_finding: bool
    status_from: str | None = None
    status_to: str | None = None
    sealed: bool = False
    landed: bool | None = None
    tasks: tuple[TaskDisposition, ...] = field(default_factory=tuple)

    @property
    def residual(self) -> tuple[TaskDisposition, ...]:
        """Findings the policy refuses to close mechanically."""
        return tuple(
            task for task in self.tasks
            if task.disposition == CLASS_REAL_PROOF_RERUN
        )

    def as_dict(self) -> dict[str, Any]:
        return {
            "change": self.change_id,
            "disposition": self.disposition,
            "reason": self.reason,
            "status_finding": self.status_finding,
            "status_from": self.status_from,
            "status_to": self.status_to,
            "sealed": self.sealed,
            "landed": self.landed,
            "tasks": [
                {
                    "task": task.task_id,
                    "disposition": task.disposition,
                    "reason": task.reason,
                    "command": task.command,
                }
                for task in self.tasks
            ],
        }


def status_finding_fires(manifest: PactManifest) -> bool:
    """Rule 1 of `pact_hygiene_issues`, restated for the policy."""
    return bool(
        manifest.tasks
        and manifest.change.status == "proposal"
        and all(task.status == "done" for task in manifest.tasks)
    )


def task_is_evidenced(manifest: PactManifest, task: Any) -> bool:
    """Rule 2's predicate: does any passing evidence reach this task?"""
    valid_refs = _task_valid_evidence_refs(manifest, task)
    if task.evidence:
        return True
    return any(
        evidence.status in PASSING_EVIDENCE
        and bool(set(evidence.refs or ()) & valid_refs)
        for evidence in manifest.evidence
    )


def unevidenced_done_tasks(manifest: PactManifest) -> list[Any]:
    return [
        task
        for task in manifest.tasks
        if task.status == "done" and not task_is_evidenced(manifest, task)
    ]


def rerun_command(
    manifest: PactManifest, task: Any, repo_root: Path
) -> str | None:
    """The command an EXISTING checker would run to prove this task.

    Only a `test_plan` entry bound to one of the task's own requirements
    counts, and only when it carries a command AND its `test_id` still names a
    file in the tree. A plan entry pointing at a deleted test cannot regenerate
    anything, so it is not a re-run candidate -- reporting it as one would send
    an operator to a command that cannot pass.
    """
    requirements = set(task.requirements or ())
    for entry in manifest.test_plan or ():
        if not entry.requirement or entry.requirement not in requirements:
            continue
        command = (entry.command or "").strip()
        if not command:
            continue
        target = (entry.test_id or "").split("::")[0].strip()
        if target and (repo_root / target).is_file():
            return command
    return None


def is_sealed(manifest: PactManifest, change_id: str) -> bool:
    """Would relocating this manifest rewrite a path recorded inside proof?

    Any formal binding, or any evidence artifact stored under the change's own
    directory, makes the manifest sealed. Archiving such a change rewrites the
    artifact paths its evidence names, and recomputing the digest afterwards
    forges the seal -- so the policy never labels it `archive_as_is`.
    """
    if manifest.formal:
        return True
    prefix = f"pact/changes/{change_id}/"
    for evidence in manifest.evidence:
        artifact = (evidence.artifact or "").strip()
        if artifact.startswith(prefix):
            return True
    return False


def _git(repo_root: Path, *args: str) -> str | None:
    try:
        completed = subprocess.run(
            ["git", *args],
            cwd=repo_root,
            check=False,
            capture_output=True,
            text=True,
        )
    except OSError:
        return None
    if completed.returncode != 0:
        return None
    return completed.stdout.strip()


def _parse_stamp(stamp: str) -> datetime | None:
    """An ISO-8601 stamp as an aware datetime; naive values are read as UTC."""
    text = stamp.strip().replace("Z", "+00:00")
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return None
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)


def manifest_is_landed(
    repo_root: Path,
    change_id: str,
    *,
    integration_ref: str = "origin/dev",
    now: datetime | None = None,
    freeze_days: int = FREEZE_DAYS,
) -> bool | None:
    """Is this change's record merged and older than the freeze window?

    Answered from the INTEGRATION BRANCH's own history -- the newest commit on
    `integration_ref` that touched this manifest -- never from the current
    branch. Asking the current branch instead makes the answer depend on
    whether the sweep has committed yet: the sweep's own commit is not an
    ancestor of the integration ref, so every manifest it touched would flip
    to "not landed" and the class of a record would change between a dry run
    and a re-run of the same tree.

    Returns None when git cannot answer -- an unreachable integration ref, no
    commit for the path there, an unparseable date. None is NOT "no": callers
    route it away from `archive_as_is`, because "nobody could check" must
    never read as "checked and fine".
    """
    relative = f"pact/changes/{change_id}/pact.yaml"
    stamp = _git(
        repo_root, "log", "-1", "--format=%cI", integration_ref, "--", relative
    )
    if not stamp:
        return None
    parsed = _parse_stamp(stamp)
    if parsed is None:
        return None
    reference = now or datetime.now(timezone.utc)
    return parsed <= reference - timedelta(days=freeze_days)


def classify(
    manifest: PactManifest,
    *,
    change_id: str,
    repo_root: Path,
    landed: bool | None,
) -> ManifestDisposition | None:
    """Apply the policy to one manifest. None when it carries no finding."""
    status_fires = status_finding_fires(manifest)
    sealed = is_sealed(manifest, change_id)
    tasks: list[TaskDisposition] = []
    for task in unevidenced_done_tasks(manifest):
        command = rerun_command(manifest, task, repo_root)
        if command:
            tasks.append(
                TaskDisposition(
                    task.id,
                    CLASS_REAL_PROOF_RERUN,
                    "a bound test_plan entry still resolves to a file in the tree",
                    command,
                )
            )
        else:
            tasks.append(
                TaskDisposition(
                    task.id,
                    CLASS_RECORDED_DOWNGRADE,
                    "no test_plan entry bound to this task resolves to a runnable checker",
                    None,
                )
            )
    if not status_fires and not tasks:
        return None

    if any(task.disposition == CLASS_REAL_PROOF_RERUN for task in tasks):
        disposition = CLASS_REAL_PROOF_RERUN
        reason = (
            "at least one unevidenced done task can be proved by an existing checker"
        )
    elif tasks:
        disposition = CLASS_RECORDED_DOWNGRADE
        reason = (
            "every unevidenced done task has lost its checker; "
            "the gap is recorded, not filled"
        )
    elif landed is True and not sealed:
        disposition = CLASS_ARCHIVE_AS_IS
        reason = (
            f"landed on the integration branch more than {FREEZE_DAYS} days ago "
            "and carries no sealed artifact"
        )
    elif sealed:
        disposition = CLASS_RECORDED_DOWNGRADE
        reason = (
            "sealed evidence: relocating this record would rewrite a path its "
            "proof binds"
        )
    elif landed is None:
        disposition = CLASS_RECORDED_DOWNGRADE
        reason = "git could not confirm the record is merged; unknown is not landed"
    else:
        disposition = CLASS_RECORDED_DOWNGRADE
        reason = f"the record is younger than the {FREEZE_DAYS}-day freeze window"

    return ManifestDisposition(
        change_id=change_id,
        disposition=disposition,
        reason=reason,
        status_finding=status_fires,
        status_from=manifest.change.status if status_fires else None,
        status_to=SWEPT_STATUS if status_fires else None,
        sealed=sealed,
        landed=landed,
        tasks=tuple(tasks),
    )


# --------------------------------------------------------------------------
# The forward rule: a changed manifest must carry proof no older than the
# change that last edited its reviewed intent.
# --------------------------------------------------------------------------
#
# The rule is stated over CONTENT, not over dates. The obvious implementation
# -- compare an evidence row's `ingested_at` against the commit date of the
# newest commit that moved the intent digest -- is wrong twice over. The proof
# always RUNS before the commit that records it, so a single commit that edits
# intent and closes a task is refused by construction, and the only way out is
# an undocumented second commit. And `git rebase` rewrites committer dates, so
# rebasing a green pull request onto dev would turn every evidence row stale at
# once. Comparing the evidence RECORDS instead answers the same question --
# "was the proof produced for this version of the intent?" -- and is immune to
# both.


# The reviewed-intent surface, stated field by field.
#
# `store.intent_digest` answers a DIFFERENT question -- "does an operator's
# recorded approval still bind?" -- and hashes everything a manifest declares
# except lifecycle state. This rule asks the narrower question "did the claim
# this proof was produced against move?".
#
# The criterion applied below: a field is PLUMBING when it is a reproduction
# recipe, lifecycle state, or part of the proof record itself. Everything else
# -- scope, requirements, a task's acceptance, and the bindings that say which
# checker and which evidence kinds prove a requirement -- is reviewed intent.
# Editing plumbing must not revoke a proof that already passed; editing intent
# must.
#
# #1589 is the instance: a lane updated five reproduce-command PATHS inside a
# landed change's `test_plan` to track a renamed artifact directory, and the
# rule demanded a fresh proof from three tasks whose bound artifacts had
# already expired. Nothing a reviewer approved had moved.
#
# Both halves of every split are declared, for the five models
# `_intent_projection` reaches.
# `test_every_model_field_is_classified_as_intent_or_plumbing` pins the union
# against each model's own field set, and
# `test_each_field_moves_the_digest_iff_it_is_intent` mutates every declared
# field and checks the digest behaves as declared -- so adding a field to one
# of these five models reds a test rather than silently landing on one side.
# The nested models (`Requirement`, `Scenario`, `AcceptanceCriterion`,
# `EvidencePolicy`, `EnvironmentCheck`, `TaskDependency`, `Waiver`,
# `PactHierarchy`) are hashed whole, as intent.
INTENT_FIELDS: dict[str, frozenset[str]] = {
    "PactManifest": frozenset(
        {
            "change",
            "hierarchy",
            "requirements",
            "tasks",
            "dependencies",
            "test_plan",
            "formal",
            "waivers",
        }
    ),
    "Change": frozenset(
        {
            "id",
            "title",
            "rationale",
            "root_cause",
            "labels",
            "tier",
            "method_pack",
            "method_metadata",
            "affected_areas",
            "affected_code",
            "migration_notes",
        }
    ),
    "Task": frozenset(
        {
            "id",
            "title",
            "owner",
            "labels",
            "method_pack",
            "method_metadata",
            "verification",
            "requirements",
            "environment",
            "coordination_gates",
        }
    ),
    # `evidence` names the evidence KINDS that satisfy this entry, and
    # `validation` routes through it when deciding whether a requirement is
    # proved. Weakening it is a claim about proof, so it is reviewed intent.
    "TestPlanEntry": frozenset(
        {"id", "requirement", "test_id", "fixture", "evidence"}
    ),
    "FormalBinding": frozenset(
        {"id", "requirement", "property_id", "module", "config", "refinement_tests"}
    ),
}

PLUMBING_FIELDS: dict[str, frozenset[str]] = {
    # `pact_version`/`target_pact_version` move when the corpus is migrated;
    # `evidence`, `proof_receipts` and `edits` ARE the proof record, and
    # hashing them would make ingesting a proof invalidate the intent it
    # proves. `prose` is a summary no gate reads.
    "PactManifest": frozenset(
        {
            "pact_version",
            "target_pact_version",
            "evidence",
            "proof_receipts",
            "edits",
            "prose",
        }
    ),
    "Change": frozenset({"status"}),
    "Task": frozenset(
        {
            "status",
            "evidence",
            "blocked_reason",
            "completion_satisfied",
            "coordination_generation_id",
        }
    ),
    # The reproduce recipe only. `test_id` and `evidence` -- which checker,
    # and which evidence kinds satisfy it -- are the reviewed binding.
    "TestPlanEntry": frozenset({"command"}),
    "FormalBinding": frozenset({"checker_command"}),
}


def _intent_projection(manifest: PactManifest) -> PactManifest:
    """A copy with every plumbing field flattened to a constant.

    `store.intent_digest` already neutralises `change.status`, each task's
    `status`/`evidence`/`blocked_reason`, and the manifest's `evidence`,
    `proof_receipts` and `edits`; `dump_manifest` drops
    `completion_satisfied`. This adds the rest of `PLUMBING_FIELDS`, so the
    digest is still computed by the one implementation in `store`.
    """
    projection = manifest.model_copy(deep=True)
    projection.pact_version = 0
    projection.target_pact_version = None
    projection.prose = {}
    for task in projection.tasks:
        task.coordination_generation_id = None
    for entry in projection.test_plan:
        entry.command = None
    for binding in projection.formal:
        binding.checker_command = None
    return projection


def reviewed_intent_digest(manifest: PactManifest) -> str:
    """`store.intent_digest` over the reviewed-intent surface only.

    See `INTENT_FIELDS` / `PLUMBING_FIELDS` above for the split and why it is
    narrower than `intent_digest`'s.
    """
    return intent_digest(_intent_projection(manifest))


def _passing_proof(manifest: PactManifest, task: Any) -> set[str]:
    """A comparable fingerprint of every passing evidence row reaching a task.

    The id alone is not enough: `evidence ingest` reuses an id when it
    refreshes a row, so an unchanged id can still carry a new run. The
    artifact, command and recorded data are folded in so that re-running the
    proof produces a different fingerprint even when the row keeps its name.
    """
    refs = _task_valid_evidence_refs(manifest, task)
    fingerprints: set[str] = set()
    for evidence in manifest.evidence:
        if evidence.status not in PASSING_EVIDENCE:
            continue
        if not set(evidence.refs or ()) & refs:
            continue
        fingerprints.add(
            json.dumps(
                [
                    evidence.id,
                    evidence.kind,
                    evidence.status,
                    evidence.artifact,
                    evidence.command,
                    evidence.summary,
                    evidence.data,
                ],
                sort_keys=True,
                default=str,
            )
        )
    return fingerprints


def _newly_done_task_ids(head: PactManifest, base: PactManifest | None) -> set[str]:
    """Tasks this change moved to `done`. Every task when the file is new."""
    if base is None:
        return {task.id for task in head.tasks if task.status == "done"}
    before = {task.id: task.status for task in base.tasks}
    return {
        task.id
        for task in head.tasks
        if task.status == "done" and before.get(task.id) != "done"
    }


def freshness_issues(
    head: PactManifest,
    *,
    base: PactManifest | None,
    change_id: str,
    repo_root: Path | None = None,
) -> list[ValidationIssue]:
    """Blocking hygiene for a manifest a pull request CHANGES.

    Two rules, both errors:

    1. `change_status_stale_all_tasks_done` -- every task done while the change
       is still `proposal`. Enforced on any changed manifest: it is one word to
       fix and needs no proof.
    2. `task_done_without_fresh_evidence` -- a task this change CLOSES must
       carry at least one passing evidence record, and a task that was already
       closed must gain one when the change edits reviewed intent. Absent proof
       fails; unchanged proof under a changed intent fails.

    Those are the two ways the backlog grows: a task closed with no proof, and
    intent moved out from under proof that already passed. A status-or-
    evidence-only edit to a legacy manifest -- what the disposition sweep does
    -- is neither, so the sweep is not refused by the rule it installs.

    "Reviewed intent" is `reviewed_intent_digest`, not the file's bytes: an
    edit confined to reproduce commands, evidence plumbing or `change.status`
    moves no digest and revokes nothing. A manifest the base does not contain
    (`base is None`) has no prior done state at all, so only the missing-proof
    rule can reach it.
    """
    issues: list[ValidationIssue] = []
    if status_finding_fires(head):
        issues.append(
            ValidationIssue(
                "change_status_stale_all_tasks_done",
                (
                    f"change {change_id} has every task done but change.status is "
                    "still 'proposal'; advance it before this change merges"
                ),
                "change.status",
                severity="error",
            )
        )

    intent_edited = base is None or reviewed_intent_digest(
        head
    ) != reviewed_intent_digest(base)
    newly_done = _newly_done_task_ids(head, base)
    if not intent_edited and not newly_done:
        return issues

    base_tasks = {task.id: task for task in (base.tasks if base else ())}
    for index, task in enumerate(head.tasks):
        if task.status != "done":
            continue
        if task.id not in newly_done and not intent_edited:
            continue
        proof = _passing_proof(head, task)
        if not proof and not task.evidence:
            issues.append(
                ValidationIssue(
                    "task_done_without_fresh_evidence",
                    (
                        f"task {task.id} is done in this change but no passing "
                        "evidence refs it; attach the proving run or revert "
                        "the status"
                    ),
                    f"tasks[{index}].evidence",
                    severity="error",
                )
            )
            continue
        if task.id in newly_done or base is None:
            # A manifest absent on the base has no prior done state to have
            # gone stale, so the stale-intent arm cannot reach it.
            continue
        # Already closed before this change, and the change edits reviewed
        # intent: the proof must have been produced for the NEW intent.
        previous = base_tasks.get(task.id)
        if previous is None:
            continue
        if proof - _passing_proof(base, previous):
            continue
        # Say what can actually be done. When no test_plan entry bound to this
        # task's requirements names a runnable checker, "re-run the proof" is
        # an instruction nobody can carry out -- #1589's tasks were bound to
        # artifacts that had already expired -- so the refusal names the
        # binding as the blocker instead of demanding an impossible run.
        command = (
            rerun_command(head, task, repo_root) if repo_root is not None else None
        )
        if command:
            remedy = (
                f"run `{command}` and ingest the result, or leave the intent "
                "alone"
            )
        elif repo_root is not None:
            remedy = (
                "no test_plan entry bound to its requirements names a runnable "
                "checker, so the recorded proof cannot be regenerated; repair "
                "the binding, or leave the intent alone"
            )
        else:
            remedy = "re-run the proof and ingest it, or leave the intent alone"
        issues.append(
            ValidationIssue(
                "task_done_without_fresh_evidence",
                (
                    f"task {task.id} was already done and this change edits the "
                    "reviewed intent, but every passing evidence record that "
                    f"refs it is unchanged; {remedy}"
                ),
                f"tasks[{index}].evidence",
                severity="error",
            )
        )
    return issues
