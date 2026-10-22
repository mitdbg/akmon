"""PACT v0 manifest operations."""

from __future__ import annotations

import json
import hashlib
import os
import re
import shlex
import shutil
import stat
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable, Iterable
from xml.etree import ElementTree

import yaml
from okg.substrate import fastyaml

from okg.substrate.errors import SubstrateError
from okg.substrate.pact.models import (
    AcceptanceCriterion,
    Change,
    Evidence,
    EvidencePolicy,
    FormalBinding,
    PactEdit,
    PactManifest,
    Requirement,
    Scenario,
    Task,
    TaskDependency,
    TestPlanEntry,
    Waiver,
)
from okg.substrate.pact import envelope
from okg.substrate.pact.evidence_kinds import load_registry
from okg.substrate.pact.coordination_gates import resolve_coordination_gates
from okg.substrate.pact.manifest_inventory import load_manifest_for_read
from okg.substrate.pact.project_settings import pact_version_defaults
from okg.substrate.pact.store import (
    change_creation_lock,
    change_dir,
    intent_digest,
    load_manifest,
    load_manifest_file,
    manifest_write_lock,
    manifest_path,
    save_manifest,
    state_digest,
    render_manifest,
)
from okg.substrate.pact.validation import (
    PARTIAL_EVIDENCE,
    _checker_script_refs,
    acceptance_proof_tokens,
    validate_manifest,
)
from okg.substrate.pact.workflow_authority import (
    LEDGER_APPROVAL_NOT_JUDGED,
    WorkflowAuthorityInput,
    evaluate_workflow_authority,
    git_evidence_for_manifest,
    manifest_file_authority,
    workflow_authority_payload,
)


def _record_coordination_event(
    change_id: str,
    event_type: str,
    *,
    task_id: str | None = None,
    intent_digest_value: str | None = None,
    payload: dict | None = None,
    repo_root: Path | None = None,
) -> dict:
    """PACT verb exhaust into the shared coordination plane.

    Per `polish-pact-agent-ux` req.polish.deferred-closure: an unreachable
    ledger queues a durable local closure intent (never a silent drop),
    and a reachable ledger first replays whatever is queued.
    """
    from okg.substrate.pact import closure_intents, decisions as ledger

    worktree = ledger.current_worktree_identity()
    if worktree is None:
        return {"status": "skipped", "reason": "no git checkout detected"}
    try:
        conn = ledger.require_ledger_connection(repo_root=repo_root)
    except Exception as exc:  # noqa: BLE001 - offline authoring is allowed.
        reason = str(exc)[:160]
        fix_hint = getattr(exc, "fix_hint", None)
        # The CLASSIFIED cause and its documented fix, carried onto the queued
        # intent. Without them the deferral downstream had only a truncated
        # reason string, and `repair.ledger_remedy` fell through to a defaulted
        # remedy — so `evidence attach` and `gate task-done`, in the same
        # shell, named different fixes for the same dead ledger
        # (req.refusal.complete: the remedy is derived from the actual cause).
        cause = (getattr(exc, "data", None) or {}).get("cause")
        deployment = (getattr(exc, "data", None) or {}).get("deployment")
        authority = {
            "reason": reason,
            **({"cause": cause} if cause else {}),
            **({"deployment": deployment} if deployment else {}),
            **({"fix_hint": fix_hint} if fix_hint else {}),
        }
        if repo_root is None:
            return {
                "status": "unavailable",
                "reason": reason,
                **({"fix_hint": fix_hint} if fix_hint else {}),
                "ledger_authority": authority,
            }
        intent = closure_intents.queue_intent(
            repo_root,
            change_id=change_id,
            kind="coordination_event",
            task_id=task_id,
            event_type=event_type,
            intent_digest=intent_digest_value,
            payload=payload or {},
            ledger_reason=reason,
            ledger_authority=authority,
        )
        return {
            "status": "deferred",
            "reason": reason,
            **({"fix_hint": fix_hint} if fix_hint else {}),
            "ledger_authority": authority,
            "intent_id": intent["intent_id"],
            "intent_path": intent["path"],
            "sync_command": f"okg pact evidence sync {change_id} --json",
        }
    replayed: dict | None = None
    try:
        if repo_root is not None:
            replayed = _replay_pending_intents(repo_root, conn, ledger)
        event = ledger.record_coordination_event(
            conn,
            event_type=event_type,
            change_id=change_id,
            task_id=task_id,
            intent_digest=intent_digest_value,
            payload=payload or {},
            worktree=worktree,
        )
    except Exception as exc:  # noqa: BLE001
        # A failure AFTER connecting is the same disease as a failed
        # connect: the event must not be silently dropped.
        reason = str(exc)[:160]
        if repo_root is None:
            return {"status": "unavailable", "reason": reason}
        intent = closure_intents.queue_intent(
            repo_root,
            change_id=change_id,
            kind="coordination_event",
            task_id=task_id,
            event_type=event_type,
            intent_digest=intent_digest_value,
            payload=payload or {},
            ledger_reason=reason,
        )
        return {
            "status": "deferred",
            "reason": reason,
            "intent_id": intent["intent_id"],
            "intent_path": intent["path"],
            "sync_command": f"okg pact evidence sync {change_id} --json",
        }
    finally:
        conn.close()
    result = {
        "status": "recorded",
        "event_type": event_type,
        "worktree": worktree.get("suffix"),
        "branch": worktree.get("branch"),
        **event,
    }
    if replayed and (replayed.get("replayed") or replayed.get("failed")):
        result["replayed_closure_intents"] = replayed
    return result


def _replay_pending_intents(
    repo_root: Path,
    conn,
    ledger,
    change_id: str | None = None,
) -> dict:
    """Flush queued closure intents into the ledger. Idempotent: a synced
    intent moves out of pending/, so replaying again writes nothing. Every
    per-intent step is isolated — a malformed intent file must never take
    down the healthy verb that triggered the replay."""
    from okg.substrate.pact import closure_intents

    replayed: list[dict] = []
    failed: list[dict] = []
    for record in closure_intents.pending_intents(repo_root, change_id):
        if record.get("kind") not in {"coordination_event", "task_done"}:
            failed.append(
                {
                    "intent_id": record.get("intent_id"),
                    "reason": record.get("error", "unknown intent kind"),
                    "path": record.get("path"),
                    "fix_hint": "inspect the intent file; delete it to unwedge sync",
                }
            )
            continue
        try:
            payload = dict(record.get("payload") or {})
            payload["deferred"] = {
                "queued_at": record.get("created_at"),
                "ledger_reason": record.get("ledger_reason"),
                "intent_id": record.get("intent_id"),
            }
            event = ledger.record_coordination_event(
                conn,
                event_type=record.get("event_type") or "progress",
                change_id=record["change"],
                task_id=record.get("task"),
                intent_digest=record.get("intent_digest"),
                payload=payload,
            )
            closure_intents.mark_synced(repo_root, record, ledger_result=event)
            replayed.append(
                {
                    "intent_id": record.get("intent_id"),
                    "change": record.get("change"),
                    **event,
                }
            )
        except Exception as exc:  # noqa: BLE001 - keep the rest flushing
            failed.append(
                {
                    "intent_id": record.get("intent_id"),
                    "reason": str(exc)[:160],
                }
            )
    return {"replayed": replayed, "failed": failed}


def sync_closure_intents(repo_root: Path, change_id: str | None = None) -> dict:
    """Explicit replay verb (`okg pact evidence sync`). Loud on failure."""
    from okg.substrate.pact import closure_intents, decisions as ledger

    pending = closure_intents.pending_intents(repo_root, change_id)
    if not pending:
        return envelope.recorded(
            text="evidence sync: no closure intents queued",
            change=change_id,
            pending=0,
            replayed=[],
            failed=[],
        )
    try:
        conn = ledger.require_ledger_connection(repo_root=repo_root)
    except Exception as exc:  # noqa: BLE001
        reason = str(exc)[:160]
        # The glossary token, not a verb-local spelling: this is the same
        # ledger blocker the workflow-authority vocabulary already names, and
        # it is the design's operator-action-required case — no `okg pact`
        # command can clear an unconfigured DSN.
        cause = (getattr(exc, "data", None) or {}).get("cause")
        token = (
            "ledger_dsn_not_configured"
            if cause == "no_dsn_configured"
            else "ledger_not_fresh"
        )
        from okg.substrate.pact import repair as repair_mod

        remedy = repair_mod.ledger_remedy(
            {
                "reason": reason,
                "deployment": (getattr(exc, "data", None) or {}).get("deployment"),
                "fix_hint": getattr(exc, "fix_hint", None),
            }
        )
        return envelope.refused(
            text=f"evidence sync refused: ledger unavailable ({len(pending)} queued)",
            message=reason,
            blocked_by=[token],
            # OPERATOR ACTION, not a repair: no `okg pact` command clears an
            # unconfigured DSN, and re-emitting `evidence sync` here is the
            # audited infinite loop. The remedy is carried as text with a
            # named owner instead.
            next_commands=[],
            why=(
                f"{remedy['remedy']} (owner: {remedy['owner']}); "
                "no okg pact command can clear this blocker, so none is offered"
            ),
            change=change_id,
            pending=len(pending),
            replayed=[],
            failed=[],
            status="ledger_unavailable",
            reason=reason,
            owner=remedy["owner"],
            remedy=remedy["remedy"],
            operator_action=remedy,
            workflow_authority={
                "blockers": [token],
                "ledger_authority": {
                    "fresh": False,
                    "status": "unavailable",
                    "reason": reason,
                    "deployment": remedy["deployment"],
                    "fix_hint": remedy["remedy"],
                },
            },
        )
    try:
        outcome = _replay_pending_intents(repo_root, conn, ledger, change_id)
    finally:
        conn.close()
    still_pending = len(closure_intents.pending_intents(repo_root, change_id))
    failed = outcome["failed"]
    return envelope.from_ok(
        not failed,
        text=(
            f"evidence sync: replayed {len(outcome['replayed'])}, "
            f"failed {len(failed)}, pending {still_pending}"
        ),
        blocked_by=[
            f"closure_intent_replay_failed:{row.get('intent_id')}" for row in failed
        ],
        next_commands=(
            [f"okg pact evidence sync {change_id} --json"] if failed else []
        ),
        change=change_id,
        pending=still_pending,
        **outcome,
    )


_LEDGER_ONLY_BLOCKERS = {
    "ledger_not_fresh",
    "ledger_dsn_not_configured",
    "pact_coordination_disagreement",
}


def maybe_defer_task_done(
    repo_root: Path,
    change_id: str,
    task_id: str,
    result: dict,
) -> dict | None:
    """Queue a task-done closure intent when the ONLY thing refusing the
    gate is ledger availability: proof is complete, coordination gates
    pass, but the ledger cannot record the closure. The refusal stands
    (the task is not marked), but the proof is not discarded — rerunning
    the gate after `evidence sync` derives done with no reconcile edit."""
    from okg.substrate.pact import closure_intents, decisions as ledger

    if result.get("ok"):
        return None
    if result.get("missing_proof") or result.get("blockers"):
        return None
    # A manifest that does not VALIDATE is refused, never deferred: deferral
    # claims the subject holds and only the durable write is queued. With a
    # dangling evidence ref and a dead ledger this returned an intent, the
    # gate emitted `deferred` (exit 3, which a blocking hook ALLOWS), and the
    # real cause was overwritten by the authority blockers. Refusal wins.
    if any(
        (row or {}).get("severity") == "error"
        for row in (result.get("validation_errors") or [])
    ):
        return None
    authority = result.get("workflow_authority") or {}
    ledger_authority = authority.get("ledger_authority") or {}
    if ledger_authority.get("fresh") is True:
        return None
    authority_blockers = set(authority.get("blockers") or [])
    if not authority_blockers or not authority_blockers <= _LEDGER_ONLY_BLOCKERS:
        return None
    if not (result.get("coordination_gates") or {}).get("ok", False):
        return None
    if ledger.current_worktree_identity() is None:
        return None
    intent = closure_intents.queue_intent(
        repo_root,
        change_id=change_id,
        kind="task_done",
        task_id=task_id,
        event_type="progress",
        payload={
            "command": "gate task-done",
            "deferred_ok": True,
            "task_status": result.get("task_status"),
        },
        ledger_reason=str(ledger_authority.get("reason") or "")[:160],
        ledger_authority=dict(ledger_authority),
    )
    return {
        "status": "deferred",
        "reason": ledger_authority.get("reason"),
        # The gate's own classified authority, carried so every reader of this
        # deferral derives the SAME remedy from the SAME cause.
        "ledger_authority": dict(ledger_authority),
        "intent_id": intent["intent_id"],
        "intent_path": intent["path"],
        "sync_command": f"okg pact evidence sync {change_id} --json",
    }


def slugify(value: str, *, fallback: str = "item") -> str:
    slug = re.sub(r"[^A-Za-z0-9]+", "-", value.strip().lower()).strip("-")
    return slug or fallback


BUILTIN_CHANGE_TEMPLATES = ("feature", "bug", "protocol", "migration")
_LOCAL_TEMPLATE_NAME_RE = re.compile(r"[A-Za-z0-9][A-Za-z0-9_-]*")


def available_change_templates(repo_root: Path) -> tuple[str, ...]:
    """Built-ins plus safe direct children under ``pact/templates``."""
    names = set(BUILTIN_CHANGE_TEMPLATES)
    templates_root = repo_root / "pact" / "templates"
    if templates_root.is_dir():
        for candidate in templates_root.iterdir():
            if (
                _LOCAL_TEMPLATE_NAME_RE.fullmatch(candidate.name)
                and candidate.is_dir()
                and not candidate.is_symlink()
            ):
                names.add(candidate.name)
    return tuple(sorted(names))


def _duplicate_change_error(
    repo_root: Path,
    change_id: str,
    path: Path,
) -> SubstrateError:
    """`new` on an id that already exists — a BAD ARGUMENT, not a dead end.

    The repair is to USE the change that is already there, so the refusal
    carries `okg pact view <id>`: one command, same subject, and it answers
    the question the caller actually had. The audited hint pointed the other
    way (scaffold another change), which is how duplicate ids got authored in
    the first place — a second scaffold is never the repair.
    """
    from okg.substrate.pact import repair as repair_mod

    prefix = repair_mod.repo_command(repo_root)
    command = f"{prefix} pact view {shlex.quote(change_id)} --format=status --json"
    return SubstrateError(
        f"PACT change already exists: {change_id}",
        data={
            "path": str(path),
            "change": change_id,
            "cause": "duplicate_change_id",
            # The existing change IS the correction: the classifier reads this
            # to re-invoke against it instead of scaffolding a second copy.
            "existing_change": change_id,
            "next_commands": repair_mod.executable_commands([command]),
        },
        fix_hint=command,
    )


def _local_template_source(repo_root: Path, template: str) -> Path:
    if not _LOCAL_TEMPLATE_NAME_RE.fullmatch(template):
        raise SubstrateError(
            f"invalid PACT template name: {template!r}",
            data={
                "template": template,
                "available": list(available_change_templates(repo_root)),
            },
            fix_hint="use a direct template name containing letters, digits, '-' or '_'",
        )
    source = repo_root / "pact" / "templates" / template
    if not source.is_dir() or source.is_symlink():
        raise SubstrateError(
            f"unknown PACT template: {template!r}",
            data={
                "template": template,
                "available": list(available_change_templates(repo_root)),
            },
        )
    return source


def _validated_local_template(
    repo_root: Path,
    template: str,
) -> tuple[Path, PactManifest]:
    source = _local_template_source(repo_root, template)
    for entry in source.rglob("*"):
        if entry.is_symlink():
            raise SubstrateError(
                f"PACT template {template!r} contains a symlink",
                data={"template": template, "path": str(entry)},
            )
        if not entry.is_dir() and not entry.is_file():
            raise SubstrateError(
                f"PACT template {template!r} contains a non-regular entry",
                data={"template": template, "path": str(entry)},
            )
    source_manifest = source / "pact.yaml"
    if not source_manifest.is_file() or source_manifest.is_symlink():
        raise SubstrateError(
            f"PACT template {template!r} has no regular pact.yaml",
            data={"template": template, "path": str(source_manifest)},
        )
    manifest = load_manifest_file(source_manifest)
    report = validate_manifest(manifest, repo_root=repo_root)
    if not report.ok:
        raise SubstrateError(
            f"PACT template {template!r} failed validation",
            data={
                "template": template,
                "path": str(source_manifest),
                "errors": [
                    issue.to_dict()
                    for issue in report.issues
                    if issue.severity == "error"
                ],
            },
        )
    return source, manifest


def _create_from_local_template(
    repo_root: Path,
    change_id: str,
    *,
    template: str,
    title: str | None,
) -> PactManifest:
    source, manifest = _validated_local_template(repo_root, template)
    manifest.change.id = change_id
    if title is not None:
        manifest.change.title = title

    changes_root = repo_root / "pact" / "changes"
    changes_root.mkdir(parents=True, exist_ok=True)
    staging_root = repo_root / "pact" / ".template-staging"
    staging_root.mkdir(parents=True, exist_ok=True)
    staging = Path(tempfile.mkdtemp(prefix="change-", dir=staging_root))
    destination = change_dir(repo_root, change_id)
    try:
        for entry in source.iterdir():
            if entry.name == "pact.yaml":
                continue
            target = staging / entry.name
            if entry.is_dir():
                shutil.copytree(entry, target)
            else:
                shutil.copy2(entry, target)
        (staging / "pact.yaml").write_text(
            render_manifest(manifest),
            encoding="utf-8",
        )

        with change_creation_lock(repo_root, change_id):
            if destination.exists():
                raise _duplicate_change_error(repo_root, change_id, destination)
            staging.replace(destination)
        return load_manifest(repo_root, change_id)
    except Exception as exc:
        if isinstance(exc, SubstrateError):
            raise
        raise SubstrateError(
            f"failed to materialize PACT template {template!r}",
            data={
                "template": template,
                "change": change_id,
                "exception_type": type(exc).__name__,
            },
        ) from exc
    finally:
        shutil.rmtree(staging, ignore_errors=True)


def create_change(
    repo_root: Path,
    change_id: str,
    *,
    template: str,
    title: str | None = None,
) -> PactManifest:
    if template not in BUILTIN_CHANGE_TEMPLATES:
        return _create_from_local_template(
            repo_root,
            change_id,
            template=template,
            title=title,
        )
    path = manifest_path(repo_root, change_id)
    # Creation is serialized outside the not-yet-existent destination. The
    # manifest lock remains the mutation authority once creation publishes.
    with change_creation_lock(repo_root, change_id):
        with manifest_write_lock(repo_root, change_id, must_exist=False):
            if path.exists():
                raise _duplicate_change_error(repo_root, change_id, path)
            version_defaults = pact_version_defaults(repo_root)
            manifest = PactManifest(
                pact_version=version_defaults.artifact_format_version,
                target_pact_version=version_defaults.target_pact_version,
                change=Change(
                    id=change_id,
                    title=title or change_id.replace("-", " ").title(),
                    status="proposal",
                    rationale=f"{template} change",
                    tier=2,
                ),
            )
            # In a repo without DECLARED pact defaults (no pact/project.yaml,
            # or one without a `pact_defaults` block — adversarial-review M3)
            # the starter must be a valid current-version skeleton, never
            # empty stanzas (issue #643, req.pactdx.version-stamp): one
            # requirement + task + test_plan entry wired together, TODO
            # placeholders drawing warnings (not errors) until replaced.
            # Repos with declared defaults keep the existing empty
            # full-shape starter. (version_defaults resolved above.)
            if version_defaults.source == "builtin:current-language-version":
                stem = slugify(change_id)
                test_stem = stem.replace("-", "_")
                manifest.requirements = [
                    Requirement(
                        id=f"req.{stem}",
                        title=manifest.change.title,
                        statement=(
                            f"TODO: state the requirement — what must hold once "
                            f"{change_id} lands."
                        ),
                        evidence_policy=EvidencePolicy(required=["executed_test"]),
                    )
                ]
                manifest.tasks = [
                    Task(
                        id=f"task.{stem}",
                        title=manifest.change.title,
                        verification=(
                            "TODO: the command that proves the requirement "
                            "(e.g. uv run pytest tests/... -q)"
                        ),
                        requirements=[f"req.{stem}"],
                    )
                ]
                manifest.test_plan = [
                    TestPlanEntry(
                        id=f"test.req-{stem}.todo",
                        requirement=f"req.{stem}",
                        test_id=(f"tests/TODO/test_{test_stem}.py::test_{test_stem}"),
                    )
                ]
            save_manifest(repo_root, manifest)
            return manifest


def create_fast_path_change(
    repo_root: Path,
    change_id: str,
    *,
    title: str,
    intent: str,
    acceptance: str,
    task_title: str | None = None,
    test_id: str | None = None,
    test_command: str | None = None,
) -> PactManifest:
    """Create a compact but regular PACT manifest for small work."""
    path = manifest_path(repo_root, change_id)
    with manifest_write_lock(repo_root, change_id, must_exist=False):
        if path.exists():
            raise _duplicate_change_error(repo_root, change_id, path)
        stem = slugify(change_id)
        req_id = f"req.{stem}"
        task_id = f"task.{stem}"
        test_entries = []
        if test_id:
            test_entries.append(
                TestPlanEntry(
                    id=f"test.{slugify(req_id)}.{slugify(test_id)}",
                    requirement=req_id,
                    test_id=test_id,
                    command=test_command,
                )
            )
        version_defaults = pact_version_defaults(repo_root)
        target_pact_version = version_defaults.target_pact_version
        if target_pact_version is None and version_defaults.compatibility_mode:
            target_pact_version = 3
        native_metadata = {}
        legacy_prose = {
            "pact_shape": "fast_path",
            "fast_path": (
                "Compact PACT for small nontrivial work. Upgrade when "
                "scope, risk, hierarchy, formal proof, graph evidence, "
                "or review complexity increases."
            ),
        }
        if version_defaults.artifact_format_version >= 4:
            native_metadata = {
                "pact_shape": "fast_path",
                "fast_path": legacy_prose["fast_path"],
            }
            legacy_prose = {}
        manifest = PactManifest(
            pact_version=version_defaults.artifact_format_version,
            target_pact_version=target_pact_version,
            change=Change(
                id=change_id,
                title=title,
                status="proposal",
                rationale=intent,
                root_cause=(
                    "Small nontrivial change whose intent, task, acceptance, "
                    "and proof fit the PACT fast path."
                ),
                tier=1,
                method_metadata=native_metadata,
            ),
            requirements=[
                Requirement(
                    id=req_id,
                    title=title,
                    statement=intent,
                    evidence_policy=EvidencePolicy(required=["executed_test"]),
                    scenarios=[
                        Scenario(
                            id=f"scn.{stem}.fast-path",
                            title="Fast-path work is verified",
                            given=["a small nontrivial change fits one requirement"],
                            when=["the declared verification runs"],
                            then=[
                                "the requirement has executed proof before completion"
                            ],
                        )
                    ],
                    acceptance=[
                        AcceptanceCriterion(
                            id=f"acc.{stem}",
                            description=acceptance,
                            evidence=["executed_test"],
                        )
                    ],
                )
            ],
            tasks=[
                Task(
                    id=task_id,
                    title=task_title or title,
                    verification=test_command or test_id or "run declared verification",
                    requirements=[req_id],
                )
            ],
            test_plan=test_entries,
            prose=legacy_prose,
        )
        report = validate_manifest(manifest, repo_root=repo_root)
        if not report.ok:
            raise SubstrateError(
                "fast-path PACT failed validation",
                data={"errors": [i.to_dict() for i in report.issues]},
            )
        save_manifest(repo_root, manifest)
        return manifest


def upgrade_fast_path_change(repo_root: Path, change_id: str) -> PactManifest:
    """Graduate a fast-path PACT to the full operating shape."""

    def mutate(manifest: PactManifest) -> None:
        pact_shape = manifest.change.method_metadata.get(
            "pact_shape"
        ) or manifest.prose.get("pact_shape")
        if pact_shape != "fast_path":
            raise SubstrateError(
                "PACT is not marked as fast_path",
                data={
                    "change": change_id,
                    "pact_shape": pact_shape,
                },
            )
        if manifest.pact_version >= 4:
            manifest.change.method_metadata["pact_shape"] = "full"
            manifest.change.method_metadata["fast_path_upgraded_at"] = _now()
        else:
            manifest.prose["pact_shape"] = "full"
            manifest.prose["fast_path_upgraded_at"] = _now()

    return apply_edit(
        repo_root,
        change_id,
        command="fast-path upgrade",
        affected_ids=[change_id],
        mutate=mutate,
        rationale="graduate fast-path PACT to full shape",
    )


def migrate_version(
    repo_root: Path,
    change_id: str,
    *,
    target_version: int,
    artifact_version: int | None = None,
) -> PactManifest:
    if target_version >= 4:
        # v4 strips the in-file event state (design.md §6): the edits audit
        # trail moves to the decision ledger, so the migration itself does
        # not append an edit entry — the file carries intent only.
        with manifest_write_lock(repo_root, change_id):
            manifest = load_manifest(repo_root, change_id)
            manifest.target_pact_version = target_version
            if artifact_version is not None:
                manifest.pact_version = artifact_version
            manifest.edits = []
            manifest.prose = {}
            save_manifest(repo_root, manifest)
            # Views are never stored under v4: drop the committed pact.md.
            stale_view = manifest_path(repo_root, change_id).with_name("pact.md")
            stale_view.unlink(missing_ok=True)
            return manifest

    def mutate(manifest: PactManifest) -> None:
        manifest.target_pact_version = target_version
        if artifact_version is not None:
            manifest.pact_version = artifact_version

    return apply_edit(
        repo_root,
        change_id,
        command=(
            f"migrate-version {target_version}"
            + (
                f" artifact-format {artifact_version}"
                if artifact_version is not None
                else ""
            )
        ),
        affected_ids=[change_id],
        mutate=mutate,
        rationale=(
            f"target PACT v{target_version}"
            + (
                f"; artifact format v{artifact_version}"
                if artifact_version is not None
                else ""
            )
        ),
    )


def apply_edit(
    repo_root: Path,
    change_id: str,
    *,
    command: str,
    affected_ids: list[str],
    mutate: Callable[[PactManifest], None],
    rationale: str | None = None,
) -> PactManifest:
    with manifest_write_lock(repo_root, change_id):
        manifest = load_manifest(repo_root, change_id)
        return _apply_loaded_edit(
            repo_root,
            manifest,
            command=command,
            affected_ids=affected_ids,
            mutate=mutate,
            rationale=rationale,
        )


def _rejected_edit_error(
    repo_root: Path,
    manifest: PactManifest,
    new_errors: list,
) -> SubstrateError:
    """The write gate's refusal, complete in one call.

    It used to carry the raw error rows and nothing else, so an `evidence
    attach` rejected for an unregistered `--kind` named the offending value and
    left the caller to discover the legal vocabulary somewhere else — the
    audited "grep pact.schema.yaml or copy a sibling" loop, one indirection
    over. The legal set for each rejected code, and the near misses for the
    offending value, are both in hand HERE (req.refusal.complete).
    """
    from okg.substrate.pact import repair as repair_mod

    vocabulary = repair_vocabulary(repo_root, manifest)
    rows: list[dict] = []
    candidates: list[str] = []
    candidate_source = repair_mod.CANDIDATE_NONE
    for issue in new_errors:
        row = issue.to_dict()
        legal = vocabulary.get(row.get("code")) or []
        if legal:
            row["legal_values"] = legal
        offending = row.get("offending_value")
        if legal and isinstance(offending, str) and offending:
            near, source = repair_mod.near_miss_candidates(offending, legal)
            row["candidates"] = near
            row["candidate_source"] = source
            if not candidates:
                candidates, candidate_source = near, source
        rows.append(row)
    return SubstrateError(
        "PACT edit introduced new validation errors",
        data={
            "errors": rows,
            "change": manifest.change.id,
            # Hoisted so a caller reading only the top level still gets the
            # correction; `candidate_source` says whether it IS one.
            "candidates": candidates,
            "candidate_source": candidate_source,
        },
    )


def _apply_loaded_edit(
    repo_root: Path,
    manifest: PactManifest,
    *,
    command: str,
    affected_ids: list[str],
    mutate: Callable[[PactManifest], None],
    rationale: str | None = None,
) -> PactManifest:
    before = state_digest(manifest)
    before_report = validate_manifest(manifest, repo_root=repo_root)
    mutate(manifest)
    report = validate_manifest(manifest, repo_root=repo_root)
    if not report.ok:
        # Reject only edits that introduce a NEW error (keyed by code + path).
        # An edit that leaves an already-invalid manifest no worse (its errors
        # are a subset of the pre-edit errors) is accepted, so an invalid PACT
        # can be repaired incrementally — e.g. attaching evidence to one
        # requirement at a time when a prior agent marked tasks done without it.
        # Validity is still enforced at the read/retire/gate layer (report.ok);
        # this only relaxes the per-edit write gate. Per
        # improve-pact-closure-ergonomics.
        before_errors = {
            (issue.code, issue.path)
            for issue in before_report.issues
            if issue.severity == "error"
        }
        new_errors = [
            issue
            for issue in report.issues
            if issue.severity == "error"
            and (issue.code, issue.path) not in before_errors
        ]
        if new_errors:
            raise _rejected_edit_error(repo_root, manifest, new_errors)
    after = state_digest(manifest)
    from okg.substrate.pact.validation import _target_language_version

    if _target_language_version(manifest) < 4:
        # v4 manifests carry no in-file edits ledger (design.md §6); the
        # decision ledger is the audit trail, so the append is v0-3 only.
        manifest.edits.append(
            PactEdit(
                id=_new_id("edit"),
                at=_now(),
                actor=_actor(),
                command=command,
                rationale=rationale,
                affected_ids=affected_ids,
                before_digest=before,
                after_digest=after,
                validation="accepted",
            )
        )
    save_manifest(repo_root, manifest)
    return manifest


def advance_change(
    repo_root: Path,
    change_id: str,
    *,
    status: str,
) -> PactManifest:
    gate = {
        "proposal": "proposal",
        "implementation": "implementation",
        "verification": "verification",
    }.get(status)
    with manifest_write_lock(repo_root, change_id):
        manifest = load_manifest(repo_root, change_id)
        before_report = (
            validate_manifest(
                manifest,
                lifecycle_gate=gate,  # type: ignore[arg-type]
                repo_root=repo_root,
            )
            if gate
            else validate_manifest(manifest, repo_root=repo_root)
        )
        if not before_report.ok:
            raise SubstrateError(
                "PACT lifecycle transition failed validation",
                data={"errors": [i.to_dict() for i in before_report.issues]},
            )

        def mutate(target: PactManifest) -> None:
            target.change.status = status  # type: ignore[assignment]

        return _apply_loaded_edit(
            repo_root,
            manifest,
            command=f"advance {status}",
            affected_ids=[change_id],
            mutate=mutate,
            rationale=f"advance to {status}",
        )


def update_change_metadata(
    repo_root: Path,
    change_id: str,
    *,
    title: str | None = None,
    rationale: str | None = None,
    root_cause: str | None = None,
    affected_areas: list[str] | None = None,
    affected_code: list[str] | None = None,
    migration_notes: str | None = None,
) -> PactManifest:
    requested = {
        "title": title,
        "rationale": rationale,
        "root_cause": root_cause,
        "affected_areas": affected_areas,
        "affected_code": affected_code,
        "migration_notes": migration_notes,
    }
    if all(value is None for value in requested.values()):
        raise SubstrateError(
            "change update requires at least one metadata field",
            fix_hint=(
                "edit pact.yaml directly (change.rationale / change.root_cause), "
                "then run `okg pact gate post-edit <change>`"
            ),
        )

    def mutate(manifest: PactManifest) -> None:
        if title is not None:
            manifest.change.title = title
        if rationale is not None:
            manifest.change.rationale = rationale
        if root_cause is not None:
            manifest.change.root_cause = root_cause
        if affected_areas is not None:
            manifest.change.affected_areas = _unique(affected_areas)
        if affected_code is not None:
            manifest.change.affected_code = _unique(affected_code)
        if migration_notes is not None:
            manifest.change.migration_notes = migration_notes

    return apply_edit(
        repo_root,
        change_id,
        command="change update",
        affected_ids=[change_id],
        mutate=mutate,
        rationale="update change metadata",
    )


def approve_intent(
    repo_root: Path,
    change_id: str,
    *,
    reviewer: str,
    decision: str = "approved",
    notes: str | None = None,
) -> PactManifest:
    from okg.substrate.pact.markdown_view import (
        check_markdown_view,
        view_path,
        write_markdown_view,
    )

    with manifest_write_lock(repo_root, change_id):
        manifest = load_manifest(repo_root, change_id)
        view = check_markdown_view(repo_root, change_id)
        if not view.get("ok"):
            raise SubstrateError(
                "PACT review view must be fresh before intent approval",
                data={"review_view": view},
                fix_hint=f"okg pact view {change_id} --format=markdown",
            )
        reviewed_digest = intent_digest(manifest)
        evidence_id = f"ev.intent-review.{slugify(change_id)}.{_stamp()}"

        def mutate(target: PactManifest) -> None:
            target.evidence.append(
                Evidence(
                    id=evidence_id,
                    kind="intent_review",
                    status="pass" if decision in {"approved", "approve"} else "fail",
                    refs=[target.change.id],
                    artifact=str(view_path(repo_root, change_id)),
                    summary=notes,
                    data={
                        "reviewer": reviewer,
                        "decision": decision,
                        "reviewed_digest": reviewed_digest,
                        "view_source_digest": view.get("frontmatter", {}).get(
                            "source_digest"
                        ),
                    },
                )
            )

        updated = _apply_loaded_edit(
            repo_root,
            manifest,
            command="review approve",
            affected_ids=[change_id, evidence_id],
            mutate=mutate,
            rationale=notes,
        )
        write_markdown_view(repo_root, change_id)
        return updated


def add_requirement(
    repo_root: Path,
    change_id: str,
    *,
    req_id: str | None,
    title: str,
    statement: str,
    evidence: list[str],
) -> PactManifest:
    new_id = req_id or f"req.{slugify(title)}"

    def mutate(manifest: PactManifest) -> None:
        _ensure_absent({r.id for r in manifest.requirements}, new_id)
        criterion = AcceptanceCriterion(
            id=f"acc.{slugify(new_id)}",
            description=statement,
            evidence=evidence or ["planned_test"],
        )
        manifest.requirements.append(
            Requirement(
                id=new_id,
                title=title,
                statement=statement,
                evidence_policy=EvidencePolicy(required=evidence or ["planned_test"]),
                scenarios=[
                    Scenario(
                        id=f"scn.{slugify(new_id)}.default",
                        title=f"{title} validates",
                        when=["okg pact validate runs"],
                        then=[f"requirement {new_id} is checked"],
                    )
                ],
                acceptance=[criterion],
            )
        )

    return apply_edit(
        repo_root,
        change_id,
        command="require add",
        affected_ids=[new_id],
        mutate=mutate,
        rationale=statement,
    )


def add_task(
    repo_root: Path,
    change_id: str,
    *,
    task_id: str | None,
    title: str,
    verification: str,
    requirements: list[str],
) -> PactManifest:
    new_id = task_id or f"task.{slugify(title)}"

    def mutate(manifest: PactManifest) -> None:
        _ensure_absent({t.id for t in manifest.tasks}, new_id)
        manifest.tasks.append(
            Task(
                id=new_id,
                title=title,
                verification=verification,
                requirements=requirements,
            )
        )

    return apply_edit(
        repo_root,
        change_id,
        command="task add",
        affected_ids=[new_id, *requirements],
        mutate=mutate,
        rationale=title,
    )


def require_test(
    repo_root: Path,
    change_id: str,
    requirement: str,
    test_id: str,
    *,
    command: str | None = None,
) -> PactManifest:
    entry_id = f"test.{slugify(requirement)}.{slugify(test_id)}"

    def mutate(manifest: PactManifest) -> None:
        _ensure_absent({t.id for t in manifest.test_plan}, entry_id)
        manifest.test_plan.append(
            TestPlanEntry(
                id=entry_id,
                requirement=requirement,
                test_id=test_id,
                command=command,
            )
        )

    return apply_edit(
        repo_root,
        change_id,
        command="require-test",
        affected_ids=[requirement, entry_id],
        mutate=mutate,
        rationale=test_id,
    )


def upsert_requirement_scenario(
    repo_root: Path,
    change_id: str,
    requirement_id: str,
    *,
    scenario_id: str,
    title: str,
    given: list[str] | None = None,
    when: list[str] | None = None,
    then: list[str] | None = None,
    and_: list[str] | None = None,
) -> PactManifest:
    scenario = Scenario(
        id=scenario_id,
        title=title,
        given=given or [],
        when=when or [],
        then=then or [],
        and_=and_ or [],
    )

    def mutate(manifest: PactManifest) -> None:
        req = _requirement(manifest, requirement_id, repo_root=repo_root)
        for index, existing in enumerate(req.scenarios):
            if existing.id == scenario_id:
                req.scenarios[index] = scenario
                return
        if len(req.scenarios) == 1 and _is_default_scenario(req, req.scenarios[0]):
            req.scenarios = [scenario]
        else:
            req.scenarios.append(scenario)

    return apply_edit(
        repo_root,
        change_id,
        command="require scenario",
        affected_ids=[requirement_id, scenario_id],
        mutate=mutate,
        rationale=title,
    )


def upsert_requirement_acceptance(
    repo_root: Path,
    change_id: str,
    requirement_id: str,
    *,
    acceptance_id: str,
    description: str,
    evidence: list[str],
) -> PactManifest:
    acceptance = AcceptanceCriterion(
        id=acceptance_id,
        description=description,
        evidence=evidence,
    )

    def mutate(manifest: PactManifest) -> None:
        req = _requirement(manifest, requirement_id, repo_root=repo_root)
        for index, existing in enumerate(req.acceptance):
            if existing.id == acceptance_id:
                req.acceptance[index] = acceptance
                return
        if len(req.acceptance) == 1 and _is_default_acceptance(req, req.acceptance[0]):
            req.acceptance = [acceptance]
        else:
            req.acceptance.append(acceptance)

    return apply_edit(
        repo_root,
        change_id,
        command="require acceptance",
        affected_ids=[requirement_id, acceptance_id],
        mutate=mutate,
        rationale=description,
    )


def add_dependency(
    repo_root: Path,
    change_id: str,
    from_task: str,
    to_task: str,
    *,
    kind: str = "blocks",
    rationale: str | None = None,
) -> PactManifest:
    def mutate(manifest: PactManifest) -> None:
        manifest.dependencies.append(
            TaskDependency(
                **{
                    "from": from_task,
                    "to": to_task,
                    "kind": kind,
                    "rationale": rationale,
                }
            )
        )

    return apply_edit(
        repo_root,
        change_id,
        command="dep add",
        affected_ids=[from_task, to_task],
        mutate=mutate,
        rationale=rationale,
    )


def update_task_status(
    repo_root: Path,
    change_id: str,
    task_id: str,
    *,
    status: str,
    evidence_ref: str | None = None,
    evidence_refs: list[str] | None = None,
    reason: str | None = None,
) -> PactManifest:
    refs = list(
        dict.fromkeys(([evidence_ref] if evidence_ref else []) + (evidence_refs or []))
    )

    def mutate(manifest: PactManifest) -> None:
        task = _task(manifest, task_id, repo_root=repo_root)
        task.status = status  # type: ignore[assignment]
        if status == "blocked":
            task.blocked_reason = reason
        for ref in refs:
            if ref not in task.evidence:
                task.evidence.append(ref)

    return apply_edit(
        repo_root,
        change_id,
        command=f"task {status}",
        affected_ids=[task_id] + refs,
        mutate=mutate,
        rationale=reason,
    )


def attach_evidence(
    repo_root: Path,
    change_id: str,
    target: str,
    *,
    kind: str,
    ref: str,
    status: str = "pass",
    command: str | None = None,
    summary: str | None = None,
    generation_id: int | None = None,
    data: dict | None = None,
) -> PactManifest:
    if load_registry(repo_root).canonicalize(kind) == "executed_test" and status in {
        "pass",
        "waived",
    }:
        current = load_manifest(repo_root, change_id)
        acceptance_owner = {
            criterion.id: requirement.id
            for requirement in current.requirements
            for criterion in requirement.acceptance
        }
        tasks_by_id = {task.id: task for task in current.tasks}
        target_task = tasks_by_id.get(target)
        scoped_requirements = {
            acceptance_owner.get(target, target),
            *(target_task.requirements if target_task is not None else []),
        }
        scoped_subjects = scoped_requirements | {
            criterion.id
            for requirement in current.requirements
            if requirement.id in scoped_requirements
            for criterion in requirement.acceptance
        }
        named = [
            entry.test_id
            for entry in current.test_plan
            if ".py::" in entry.test_id
            and (
                entry.requirement in scoped_subjects
                or target in {entry.id, entry.test_id}
            )
        ]
        if named:
            raise SubstrateError(
                "passing executed-test evidence for named tests must come from "
                "`okg pact evidence ingest --pytest-report <junit.xml>`; "
                f"manual attachment cannot attest execution of {named}"
            )
    evidence_id = f"ev.{slugify(kind)}.{slugify(target)}.{_stamp()}"
    # First-class timestamp for staleness comparison
    # (right-size-pact-authoring req.rightsize.consistency).
    data = {"attached_at": _now(), **(data or {})}

    def mutate(manifest: PactManifest) -> None:
        manifest.evidence.append(
            Evidence(
                id=evidence_id,
                kind=kind,
                status=status,  # type: ignore[arg-type]
                refs=[target],
                artifact=ref,
                command=command,
                summary=summary,
                generation_id=generation_id,
                data=data or {},
            )
        )
        # The acceptance-id form. `missing_proof` names the composite slot
        # `req.X:acceptance:acc.Y`, and the gate's own
        # `missing_acceptance_evidence` error demands a PROOF LINK on the
        # criterion — a link no documented command created, so it was
        # reverse-engineered from sibling manifests in a 28-call audited
        # episode. Attaching against the criterion id now writes the link, so
        # the slot the refusal names is the slot the command fills.
        for requirement in manifest.requirements:
            for criterion in requirement.acceptance:
                if criterion.id != target:
                    continue
                # A criterion that already declares a kind/test/formal proof
                # is linked: the new row's target ref lets that declaration
                # resolve it. Appending the row id as a second flat token
                # would turn one proof into two indistinguishable arms and
                # make the criterion impossible to satisfy. Only repair the
                # genuinely tokenless case that this command was added for.
                if not acceptance_proof_tokens(criterion):
                    criterion.evidence.append(evidence_id)

    manifest = apply_edit(
        repo_root,
        change_id,
        command="attach-evidence",
        affected_ids=[target, evidence_id],
        mutate=mutate,
        rationale=summary,
    )
    _record_coordination_event(
        change_id,
        "proof",
        payload={
            "command": "evidence attach",
            "evidence_id": evidence_id,
            "target": target,
            "kind": kind,
            "ref": ref,
            "status": status,
            "summary": summary,
            "generation_id": generation_id,
        },
        repo_root=repo_root,
    )
    return manifest


def request_waiver(
    repo_root: Path,
    change_id: str,
    *,
    waiver_id: str | None,
    scope: str,
    reason: str,
    owner: str | None,
    expires: str | None,
) -> PactManifest:
    new_id = waiver_id or f"waiver.{slugify(scope)}.{_stamp()}"
    # An owner-less waiver is silently ignored by validation (`_waiver_for`
    # requires an owner), so default to the acting principal rather than mint a
    # no-op waiver (per improve-pact-closure-ergonomics). The principal chain
    # is the one project.yaml owner_approval documents: OKG_PRINCIPAL, else
    # the OS user — the same identity `okg pact decide` records.
    from okg.substrate.pact.decisions import acting_principal

    resolved_owner = owner or acting_principal()

    def mutate(manifest: PactManifest) -> None:
        manifest.waivers.append(
            Waiver(
                id=new_id,
                scope=scope,
                reason=reason,
                owner=resolved_owner,
                expires=expires,
                requires_owner_approval=True,
            )
        )

    return apply_edit(
        repo_root,
        change_id,
        command="waiver request",
        affected_ids=[new_id, scope],
        mutate=mutate,
        rationale=reason,
    )


def bind_tla(
    repo_root: Path,
    change_id: str,
    *,
    requirement: str,
    model: str,
    property_id: str,
    config: str | None = None,
    checker_command: str | None = None,
    refinement_tests: list[str] | None = None,
) -> PactManifest:
    binding_id = f"formal.{slugify(property_id)}"

    def mutate(manifest: PactManifest) -> None:
        manifest.formal.append(
            FormalBinding(
                id=binding_id,
                requirement=requirement,
                property_id=property_id,
                module=model,
                config=config,
                checker_command=checker_command,
                refinement_tests=refinement_tests or [],
            )
        )

    return apply_edit(
        repo_root,
        change_id,
        command="bind-tla",
        affected_ids=[requirement, binding_id],
        mutate=mutate,
        rationale=property_id,
    )


def ingest_pytest_report(
    repo_root: Path,
    change_id: str,
    report_path: Path,
) -> PactManifest:
    if not report_path.is_file():
        raise SubstrateError(
            "pytest report not found",
            data={"path": str(report_path)},
        )
    report_root = ElementTree.fromstring(report_path.read_text(encoding="utf-8"))
    with manifest_write_lock(repo_root, change_id):
        manifest = load_manifest(repo_root, change_id)
        test_plan = list(manifest.test_plan)
        task_requirements = {
            requirement: [
                task.id for task in manifest.tasks if requirement in task.requirements
            ]
            for requirement in {entry.requirement for entry in test_plan}
        }
        # Per `polish-pact-agent-ux` req.polish.ingest-rollup: one evidence
        # row per report per requirement, not one per test case. Per-case
        # detail stays in the junit artifact the row points at. Rollups
        # keyed by (ref, report identity); groups collect the per-case
        # facts before the rows are written.
        # Report identity is the canonical path (repo-relative when inside
        # the repo, resolved-absolute otherwise), not the argv spelling —
        # ingesting `junit.xml` and `$PWD/junit.xml` must be one report.
        resolved_report = report_path.resolve()
        try:
            artifact_str = str(resolved_report.relative_to(repo_root.resolve()))
        except ValueError:
            artifact_str = str(resolved_report)
        report_key = hashlib.sha256(artifact_str.encode("utf-8")).hexdigest()[:8]
        # A plan entry that names ONE test (`path::name`) is a promise about
        # which assertion proves the requirement. Track what the report actually
        # says about each such promise, so a promise the report never kept
        # cannot be counted as evidence. `observed` holds the strongest outcome
        # seen for the entry; an entry absent from it never ran at all — the
        # shape a marker filter, a collect-ignore or an unmet fixture produces,
        # none of which leave a trace in the report to notice.
        named_plan: dict[str, list] = {}
        for entry in test_plan:
            if _named_pytest_target(entry.test_id) is not None:
                named_plan.setdefault(entry.requirement, []).append(entry)
        observed: dict[str, str] = {}
        # Which test files this report actually covered. A named test missing
        # from a report that DID run its file was dropped — deselected by a
        # marker filter, or not collected. A named test whose file the report
        # never touched simply was not in this run's scope: that report proves
        # nothing about it, but it is not a broken promise either, and treating
        # it as one would mean no focused run could ever prove anything.
        report_files: set[str] = set()
        # Named entries some case in this report was attributed to. If a case was
        # attributed to a named entry but is not that test under strict identity
        # — a same-named function in another file — the entry is not out of
        # scope, it is a promise this report failed to keep.
        claimed: set[str] = set()

        # How much of this report the plan actually accounts for. A report whose
        # cases match no plan entry at all is the vacuous shape: the rollup can
        # only say "0 passed" or say nothing, and neither reads as "the plan
        # bound nothing". Counting it puts that fact on every row this ingest
        # writes.
        cases_seen = 0
        cases_unmatched = 0

        groups: dict[str, dict] = {}
        for case in report_root.iter():
            if not case.tag.endswith("testcase"):
                continue
            name = case.attrib.get("name", "")
            classname = case.attrib.get("classname", "")
            test_id = f"{classname}::{name}" if classname else name
            matching_tests = [
                entry
                for entry in test_plan
                if _test_id_matches_junit(entry.test_id, test_id, name)
            ]
            cases_seen += 1
            if not matching_tests:
                cases_unmatched += 1
            refs = _case_refs(case) or [entry.requirement for entry in matching_tests]
            if not refs and not matching_tests:
                continue
            status = "pass"
            if any(child.tag.endswith(("failure", "error")) for child in list(case)):
                status = "fail"
            elif any(child.tag.endswith("skipped") for child in list(case)):
                status = "missing"
            if classname:
                report_files.add(_file_key(classname))
            for entry in matching_tests:
                if _named_pytest_target(entry.test_id) is not None:
                    claimed.add(entry.id)
            for entry in test_plan:
                if not _named_test_ran(entry.test_id, classname, name):
                    continue
                # A test may appear more than once (parametrised cases, reruns).
                # Any failure or skip is the outcome that matters.
                if observed.get(entry.id) in (None, "pass"):
                    observed[entry.id] = status
            for ref in refs:
                group = groups.setdefault(
                    ref,
                    {
                        "counts": {"pass": 0, "fail": 0, "skip": 0},
                        "failed_test_ids": [],
                        "skipped_test_ids": [],
                        "junit_test_ids": [],
                        "planned_tests": [],
                        "commands": [],
                        "task_ids": [],
                    },
                )
                bucket = {"pass": "pass", "fail": "fail", "missing": "skip"}[status]
                group["counts"][bucket] += 1
                group["junit_test_ids"].append(test_id)
                if status == "fail":
                    group["failed_test_ids"].append(test_id)
                elif status == "missing":
                    group["skipped_test_ids"].append(test_id)
                group["planned_tests"].extend(
                    entry.id
                    for entry in matching_tests
                    if entry.requirement == ref or ref in {entry.id, entry.test_id}
                )
                group["commands"].extend(
                    entry.command for entry in matching_tests if entry.command
                )
                group["task_ids"].extend(
                    task_id
                    for requirement in refs
                    for task_id in task_requirements.get(requirement, [])
                )
        evidence: list[Evidence] = []
        for ref, group in groups.items():
            counts = group["counts"]
            # Fail-dominant: a report in which any of this requirement's
            # cases failed must not prove the requirement (the per-case
            # shape could satisfy on a partial pass; the rollup is honest).
            # A named plan entry the report did not pass is a promise it did not
            # keep. `absent` is the dangerous one: the test never reached the
            # report, so nothing in the rollup can notice it — which is how a
            # requirement came to be proven by a run that never executed the
            # assertion the plan names.
            # Does this report speak to this requirement at all? It does if any
            # of the requirement's named test files produced a case. If none
            # did, the report is about something else and says nothing here.
            #
            # Scoping cannot be per-FILE: a report may contain no case from a
            # file precisely BECAUSE every test in it was dropped — a module
            # where all tests are slow-marked, a collect-ignore, an
            # importorskip at module scope. Excusing an uncovered file would
            # therefore excuse the whole-file version of the very defect this
            # check exists to catch. So once a report speaks to a requirement,
            # it is answerable for every test that requirement names.
            #
            # The cost, stated rather than hidden: a requirement whose named
            # tests span several files cannot be proven by a report covering
            # only some of them. Run the tests the plan names, together.
            speaks_to_requirement = any(
                key == target[0] or key.startswith(f"{target[0]}.")
                for entry in named_plan.get(ref, [])
                if (target := _named_pytest_target(entry.test_id)) is not None
                for key in report_files
            )
            unrun = []
            for entry in named_plan.get(ref, []):
                outcome = observed.get(entry.id)
                if outcome == "pass":
                    continue
                if (
                    outcome is None
                    and not speaks_to_requirement
                    and entry.id not in claimed
                ):
                    continue
                unrun.append(
                    {
                        "test": entry.test_id,
                        "planned_test": entry.id,
                        "reason": {
                            None: "absent",
                            "missing": "skipped",
                            "fail": "failed",
                        }[outcome],
                    }
                )
            if counts["fail"]:
                status = "fail"
            elif counts["pass"] and not unrun:
                status = "pass"
            else:
                # Other tests passing does not stand in for the one promised.
                status = "missing"
            planned_test_ids = [
                entry_id
                for entry_id in dict.fromkeys(group["planned_tests"])
                if observed.get(entry_id, "pass") == "pass"
            ]
            task_ids = sorted(dict.fromkeys(group["task_ids"]))
            summary = (
                f"{counts['pass']} passed, {counts['fail']} failed, "
                f"{counts['skip']} skipped ({report_path.name})"
            )
            if group["failed_test_ids"]:
                summary += "; failed: " + ", ".join(group["failed_test_ids"][:10])
            if unrun:
                summary += "; named tests that did not run: " + ", ".join(
                    f"{item['test']} ({item['reason']})" for item in unrun[:10]
                )
            if test_plan and cases_unmatched:
                # Scoped to the REPORT, and said so: this counts every case in
                # the junit file, not this requirement's cases. A report whose
                # every case is unmatched is the vacuous shape, and it is
                # invisible in a per-requirement count.
                summary += (
                    f"; whole report: {cases_unmatched} of {cases_seen} cases "
                    "matched no plan entry"
                )
            evidence.append(
                Evidence(
                    id=f"ev.pytest.{slugify(ref)}.report-{report_key}",
                    kind="pytest",
                    status=status,  # type: ignore[arg-type]
                    refs=list(dict.fromkeys([ref, *planned_test_ids, *task_ids])),
                    artifact=artifact_str,
                    command=group["commands"][0] if group["commands"] else "pytest",
                    summary=summary,
                    data={
                        "ingested_at": _now(),
                        "report": artifact_str,
                        "counts": counts,
                        "failed_test_ids": group["failed_test_ids"],
                        "skipped_test_ids": group["skipped_test_ids"],
                        "unrun_planned_tests": unrun,
                        "report_cases": cases_seen,
                        "report_cases_unmatched_by_plan": cases_unmatched,
                        "junit_test_ids": group["junit_test_ids"],
                        "planned_tests": planned_test_ids,
                        "requirements": [ref],
                        "tasks": task_ids,
                    },
                )
            )

        def _same_report(artifact: str | None) -> bool:
            if not artifact:
                return False
            if artifact in {artifact_str, str(report_path)}:
                return True
            candidate = Path(artifact)
            if not candidate.is_absolute():
                candidate = repo_root / candidate
            try:
                return candidate.resolve() == resolved_report
            except OSError:
                return False

        def _is_ingested_row(ev: Evidence) -> bool:
            # Only rows THIS command family wrote: new rollups carry
            # data.report, legacy per-case rows carry data.junit_test_id.
            # Manual `evidence attach --kind pytest` rows carry neither and
            # must survive a re-ingest even when they cite the same report.
            data = ev.data or {}
            return ev.id.startswith("ev.pytest.") and (
                "report" in data or "junit_test_id" in data
            )

        def mutate(target_manifest: PactManifest) -> None:
            # A re-ingest of the same report refreshes it: drop the rows a
            # prior ingest of this artifact wrote (per-case legacy rows and
            # stale rollups alike), then upsert the fresh rollups. Rows from
            # other artifacts and manual attaches are untouched.
            #
            # A row an author downgraded to `partial` keeps that status
            # across the refresh. The rollup id is deterministic in
            # (ref, report), so without this the next ingest of the SAME
            # junit file would silently restore `pass` on a row whose author
            # had recorded "the pytest arm only" -- exactly the silent
            # upgrade req.pact-acceptance-prose-is-enforceable.partial-is-recordable
            # forbids. The status the ingest computed is kept in `data` so
            # the preservation is visible rather than a quiet overwrite.
            partial_ids = {
                ev.id for ev in target_manifest.evidence if ev.status == "partial"
            }
            target_manifest.evidence = [
                ev
                for ev in target_manifest.evidence
                if not (
                    ev.kind == "pytest"
                    and _same_report(ev.artifact)
                    and _is_ingested_row(ev)
                )
            ]
            for index, ev in enumerate(evidence):
                if ev.id in partial_ids and ev.status != "partial":
                    # Written back into `evidence` so the coordination event
                    # below reports the status that landed, not the one the
                    # rollup computed.
                    evidence[index] = ev.model_copy(
                        update={
                            "status": "partial",
                            "data": {
                                **(ev.data or {}),
                                "partial_preserved": True,
                                "ingest_computed_status": ev.status,
                            },
                        }
                    )
            existing = {ev.id: i for i, ev in enumerate(target_manifest.evidence)}
            for ev in evidence:
                if ev.id in existing:
                    target_manifest.evidence[existing[ev.id]] = ev
                else:
                    target_manifest.evidence.append(ev)

        updated = _apply_loaded_edit(
            repo_root,
            manifest,
            command="evidence ingest pytest",
            affected_ids=[ref for ev in evidence for ref in ev.refs],
            mutate=mutate,
            rationale=str(report_path),
        )
    _record_coordination_event(
        change_id,
        "proof",
        payload={
            "command": "evidence ingest pytest",
            "report_path": str(report_path),
            "evidence_count": len(evidence),
            "evidence_ids": [ev.id for ev in evidence],
            "requirements": sorted(
                {ref for ev in evidence for ref in ev.refs if ref.startswith("req.")}
            ),
            "tasks": sorted(
                {ref for ev in evidence for ref in ev.refs if ref.startswith("task.")}
            ),
            "statuses": sorted({ev.status for ev in evidence}),
        },
        repo_root=repo_root,
    )
    return updated


def ingest_graph_report(
    repo_root: Path,
    change_id: str,
    report_path: Path,
) -> PactManifest:
    if not report_path.is_file():
        raise SubstrateError(
            "graph evidence report not found",
            data={"path": str(report_path)},
        )
    raw = fastyaml.safe_load(report_path.read_text(encoding="utf-8")) or {}
    rows = raw if isinstance(raw, list) else raw.get("evidence", [raw])
    if not isinstance(rows, list):
        raise SubstrateError("graph report must be a mapping or list")
    evidence: list[Evidence] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        refs = row.get("refs") or row.get("targets")
        if isinstance(refs, str):
            refs = [refs]
        refs = refs or [row.get("ref") or row.get("target") or change_id]
        refs = [str(ref) for ref in refs if ref]
        kind = str(row.get("kind") or "published_generation")
        status = _normalize_evidence_status(str(row.get("status") or "pass"))
        generation_id = row.get("generation_id")
        data = {
            "ingested_at": _now(),
            **{
                key: value
                for key, value in row.items()
                if key
                not in {
                    "artifact",
                    "command",
                    "evidence",
                    "generation_id",
                    "kind",
                    "ref",
                    "refs",
                    "status",
                    "summary",
                    "target",
                    "targets",
                }
            },
        }
        if isinstance(row.get("data"), dict):
            data.update(row["data"])
        evidence.append(
            Evidence(
                id=f"ev.{slugify(kind)}.{slugify('-'.join(refs))}.{_stamp()}",
                kind=kind,
                status=status,  # type: ignore[arg-type]
                refs=refs,
                artifact=str(row.get("artifact") or report_path),
                command=row.get("command"),
                summary=row.get("summary"),
                generation_id=int(generation_id) if generation_id is not None else None,
                data=data,
            )
        )

    def mutate(target_manifest: PactManifest) -> None:
        target_manifest.evidence.extend(evidence)

    manifest = apply_edit(
        repo_root,
        change_id,
        command="evidence ingest graph",
        affected_ids=[ref for ev in evidence for ref in ev.refs],
        mutate=mutate,
        rationale=str(report_path),
    )
    _record_coordination_event(
        change_id,
        "proof",
        payload={
            "command": "evidence ingest graph",
            "report_path": str(report_path),
            "evidence_count": len(evidence),
            "evidence_ids": [ev.id for ev in evidence],
            "statuses": sorted({ev.status for ev in evidence}),
        },
        repo_root=repo_root,
    )
    return manifest


def ingest_formal_report(
    repo_root: Path,
    change_id: str,
    report_path: Path,
) -> PactManifest:
    if not report_path.is_file():
        raise SubstrateError(
            "formal checker report not found",
            data={"path": str(report_path)},
        )
    raw = fastyaml.safe_load(report_path.read_text(encoding="utf-8")) or {}
    results = _formal_results(raw)
    if not results:
        raise SubstrateError(
            "formal report has no result rows",
            data={
                "path": str(report_path),
                "errors": [
                    {
                        "code": "formal_report_missing_results",
                        "path": "results",
                        "message": "formal report must include at least one result row",
                    }
                ],
            },
        )
    with manifest_write_lock(repo_root, change_id):
        manifest = load_manifest(repo_root, change_id)
        formal_by_property = {
            binding.property_id: binding for binding in manifest.formal
        }
        formal_by_id = {binding.id: binding for binding in manifest.formal}
        validation_errors: list[dict[str, str]] = []
        seen_by_checker: dict[str, set[str]] = {}
        evidence: list[Evidence] = []
        for result_index, result in enumerate(results):
            property_ref = str(
                result.get("property_id")
                or result.get("property")
                or result.get("id")
                or ""
            )
            if not property_ref:
                validation_errors.append(
                    _formal_report_error(
                        "formal_report_missing_property",
                        f"results[{result_index}].property_id",
                        "formal report result has no property_id",
                    )
                )
                continue
            binding = formal_by_property.get(property_ref) or formal_by_id.get(
                property_ref
            )
            if binding is None:
                validation_errors.append(
                    _formal_report_error(
                        "formal_report_unknown_property",
                        f"results[{result_index}].property_id",
                        f"formal report references unknown property {property_ref}",
                    )
                )
                continue
            row_errors = _formal_report_result_errors(
                repo_root,
                report_path,
                result,
                result_index=result_index,
                binding=binding,
            )
            if row_errors:
                validation_errors.extend(row_errors)
                continue
            status = _normalize_formal_report_status(str(result["status"]))
            checker_command = result.get("checker_command")
            if binding.checker_command:
                seen_by_checker.setdefault(binding.checker_command, set()).add(
                    binding.property_id
                )
            artifact_path = _formal_report_artifact_path(
                report_path,
                str(result["artifact"]),
            )
            try:
                artifact_record = str(
                    artifact_path.resolve().relative_to(repo_root.resolve())
                )
            except ValueError:
                artifact_record = str(artifact_path)
            checker_script_digests = _formal_checker_script_digests(
                repo_root,
                binding.checker_command,
            )
            data = {
                "ingested_at": _now(),
                "property_id": binding.property_id,
                "checker_command": checker_command,
                "module": binding.module,
                "module_digest": result["module_digest"],
                "config": binding.config,
                "config_digest": result.get("config_digest"),
                "artifact_path": artifact_record,
                "artifact_digest": _path_digest(artifact_path),
                "checker_script_digests": checker_script_digests,
                "refinement_tests": result["refinement_tests"],
                "fresh": result["fresh"],
                **(
                    {"checker_output": result["checker_output"]}
                    if "checker_output" in result
                    else {}
                ),
                **{
                    key: result[key]
                    for key in (
                        "execution_profile",
                        "profile_timeout_seconds",
                        "proof_status",
                        "timed_out",
                        "failure_class",
                        "exit_code",
                    )
                    if key in result
                },
                "executed_command": result["command"],
            }
            data = {key: value for key, value in data.items() if value is not None}
            evidence.append(
                Evidence(
                    id=f"ev.formal.{slugify(binding.id)}.{_stamp()}",
                    kind=str(result.get("kind") or "formal"),
                    status=status,  # type: ignore[arg-type]
                    refs=[binding.id],
                    artifact=str(result["artifact"]),
                    command=result.get("command"),
                    summary=result.get("summary") or property_ref,
                    data=data,
                )
            )
        validation_errors.extend(
            _formal_report_missing_bound_property_errors(
                formal_by_property.values(),
                seen_by_checker,
            )
        )
        if validation_errors:
            raise SubstrateError(
                "formal report failed validation",
                data={"path": str(report_path), "errors": validation_errors},
            )

        def mutate(target_manifest: PactManifest) -> None:
            target_manifest.evidence.extend(evidence)

        updated = _apply_loaded_edit(
            repo_root,
            manifest,
            command="evidence ingest formal",
            affected_ids=[ref for ev in evidence for ref in ev.refs],
            mutate=mutate,
            rationale=str(report_path),
        )
    _record_coordination_event(
        change_id,
        "proof",
        payload={
            "command": "evidence ingest formal",
            "report_path": str(report_path),
            "evidence_count": len(evidence),
            "evidence_ids": [ev.id for ev in evidence],
            "statuses": sorted({ev.status for ev in evidence}),
        },
        repo_root=repo_root,
    )
    return updated


def evidence_status(repo_root: Path, change_id: str) -> dict:
    resolved = load_manifest_for_read(repo_root, change_id)
    manifest = resolved.manifest
    alignment = validate_manifest(manifest, repo_root=repo_root).alignment
    reqs = alignment.get("requirements", {})
    tests = alignment.get("test_plan", {})
    acceptance = alignment.get("acceptance", {})
    missing_requirements = [
        {"id": req_id, "missing": state.get("satisfaction_missing", [])}
        for req_id, state in reqs.items()
        if state.get("satisfaction_status") == "unsatisfied"
    ]
    # A read is `recorded` only when it ANSWERS the question. `evidence status`
    # answers "is the proof complete?", so a report whose every requirement is
    # unsatisfied is a refusal, not a success — it used to return ok: true with
    # nothing proven, which is the audited lie class.
    from okg.substrate.pact import repair as repair_mod

    # The old recommendation was a TEMPLATE, not a command: `--kind <kind>
    # --ref <ref>` carries shell metacharacters and names no kind, so pasting
    # it fails and reading it costs another call to learn the policy. The kind
    # now comes from the requirement's own evidence policy and the ref from
    # its test plan.
    missing_slots = [
        f"{row['id']}:{kind}"
        for row in missing_requirements
        for kind in (row["missing"] or ["executed_test"])
    ]
    evidence_next = (
        [
            f"{repair_mod.repo_command(repo_root)} pact decide "
            f"{shlex.quote(change_id)} --verdict reopen --json"
        ]
        if resolved.archived and missing_requirements
        else repair_mod.proof_slot_commands(
            manifest,
            change_id,
            missing_slots,
            prefix=repair_mod.repo_command(repo_root),
            # The subject of THIS refusal is the unsatisfied requirements it
            # just named — never every criterion in the manifest.
            subject_requirements=[row["id"] for row in missing_requirements],
        )
    )
    return envelope.from_ok(
        not missing_requirements,
        text=(
            f"evidence status {change_id}: "
            f"{len(reqs) - len(missing_requirements)}/{len(reqs)} requirements satisfied"
        ),
        blocked_by=[
            f"unsatisfied_requirement:{row['id']}" for row in missing_requirements
        ],
        next_commands=evidence_next,
        change=change_id,
        archived=resolved.archived,
        **(
            {"why": guidance}
            if missing_requirements
            and (guidance := repair_mod.v5_unbacked_guidance(manifest, evidence_next))
            else {}
        ),
        # The offending value, in the ONE place a reader looks for it: which
        # slot of which requirement is unproven. Without it the refusal named
        # a count and left the subject to a second call.
        missing_proof=missing_slots,
        requirements=[
            {
                "id": req.id,
                "required": req.evidence_policy.required,
                "status": reqs.get(req.id, {}).get("satisfaction_status"),
                "missing": reqs.get(req.id, {}).get("satisfaction_missing", []),
                "evidence": _matching_evidence_ids(manifest, {req.id}),
            }
            for req in manifest.requirements
        ],
        acceptance=[
            {
                "id": criterion.id,
                "requirement": req.id,
                "status": acceptance.get(criterion.id, {}).get("satisfaction_status"),
                "missing": acceptance.get(criterion.id, {}).get(
                    "satisfaction_missing",
                    [],
                ),
                "proofs": acceptance.get(criterion.id, {}).get("proofs", []),
            }
            for req in manifest.requirements
            for criterion in req.acceptance
        ],
        planned_tests=[
            {
                "id": entry.id,
                "requirement": entry.requirement,
                "test_id": entry.test_id,
                "status": tests.get(entry.id, {}).get("satisfaction_status"),
                "traceability": tests.get(entry.id, {}).get("traceability_status"),
                "stale": tests.get(entry.id, {}).get("traceability_status") == "stale",
                "evidence": _matching_evidence_ids(
                    manifest,
                    {entry.id, entry.test_id, entry.requirement},
                ),
            }
            for entry in manifest.test_plan
        ],
        missing_requirements=missing_requirements,
        stale_planned_tests=[
            {"id": test_id, "requirement": state.get("requirement")}
            for test_id, state in tests.items()
            if state.get("traceability_status") == "stale"
        ],
        waived=[
            {"id": req_id, "waiver": state.get("waiver")}
            for req_id, state in reqs.items()
            if state.get("satisfaction_status") == "waived"
        ],
        # Rows recorded `partial`: work that RAN and proved part of what its
        # task asked. They satisfy nothing (PARTIAL_EVIDENCE is deliberately
        # outside PASSING_EVIDENCE), so they never appear in the per-target
        # `evidence` lists above -- but they are not `missing` either, and an
        # operator reading this surface has to be able to tell those apart.
        # Per req.pact-acceptance-prose-is-enforceable.partial-is-recordable.
        partial=[
            {
                "id": evidence.id,
                "kind": evidence.kind,
                "refs": list(evidence.refs),
                "summary": evidence.summary,
            }
            for evidence in manifest.evidence
            if evidence.status == PARTIAL_EVIDENCE
        ],
    )


PACT_HOOK_MARKER = "# okg-pact-managed"


def install_gate_hooks(
    repo_root: Path,
    change_id: str,
    *,
    target: str = "all",
    dry_run: bool = False,
    force: bool = False,
) -> dict:
    if target not in {"local", "ci", "all"}:
        raise SubstrateError(
            "hook target must be local, ci, or all",
            data={"target": target},
        )
    manifest = load_manifest(repo_root, change_id)
    entries = _hook_entries(change_id, target=target)
    materialized: list[dict[str, str]] = []
    blocked: list[dict[str, str]] = []
    for rel_path, content, mode in entries:
        path = repo_root / rel_path
        action = "write"
        if path.exists():
            existing = path.read_text(encoding="utf-8", errors="ignore")
            if PACT_HOOK_MARKER not in existing and not force:
                blocked.append(
                    {
                        "path": str(path),
                        "reason": "existing file is not PACT-managed",
                    }
                )
                continue
            action = "update"
        materialized.append(
            {
                "path": str(path),
                "relative_path": rel_path,
                "action": action,
                "failure_mode": mode,
            }
        )
        if dry_run:
            continue
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(content, encoding="utf-8")
        if rel_path.startswith(".git/hooks/") or rel_path.endswith(".sh"):
            path.chmod(path.stat().st_mode | stat.S_IXUSR)
    ok = not blocked
    commands = (
        []
        if ok
        else [f"okg pact hooks install {change_id} --target {target} --force --json"]
    )
    return envelope.from_ok(
        ok,
        text=(
            f"hooks install {manifest.change.id}: "
            f"{len(materialized)} planned, {len(blocked)} blocked"
        ),
        blocked_by=[f"hook_not_pact_managed:{row['path']}" for row in blocked],
        next_commands=commands,
        change=manifest.change.id,
        target=target,
        dry_run=dry_run,
        installed=[] if dry_run else materialized,
        planned=materialized,
        blocked=blocked,
        commands=_hook_commands(change_id),
        recommended_commands=commands,
    )


def _hook_entries(change_id: str, *, target: str) -> list[tuple[str, str, str]]:
    entries: list[tuple[str, str, str]] = []
    if target in {"local", "all"}:
        entries.extend(
            [
                (
                    ".pact/hooks/post-edit.sh",
                    _hook_script(change_id, "post-edit", hard_fail=False),
                    "warn",
                ),
                (
                    ".pact/hooks/task-done.sh",
                    _hook_script(change_id, "task-done <task-id>", hard_fail=True),
                    "fail",
                ),
                (
                    ".git/hooks/pre-commit",
                    _hook_script(change_id, "pre-commit", hard_fail=True),
                    "fail",
                ),
                (
                    ".pact/hooks/pre-merge.sh",
                    _hook_script(change_id, "pre-merge", hard_fail=True),
                    "fail",
                ),
                (
                    ".pact/hooks/graph-freshness.sh",
                    _hook_script(change_id, "graph-freshness", hard_fail=True),
                    "fail",
                ),
                (
                    ".pact/hooks/reviewer-brief.sh",
                    _hook_script(change_id, "reviewer-brief", hard_fail=False),
                    "warn",
                ),
            ]
        )
    if target in {"ci", "all"}:
        entries.append(
            (
                ".github/workflows/pact-gates.yml",
                _ci_workflow(change_id),
                "fail",
            )
        )
    return entries


def _hook_script(change_id: str, gate: str, *, hard_fail: bool) -> str:
    if gate.startswith("task-done"):
        command = f'uv run okg pact gate task-done {change_id} "${{1:?task id required}}" --json'
    else:
        command = f"uv run okg pact gate {gate} {change_id} --json"
    if not hard_fail:
        # Warn mode still exits 0, but `|| true` swallowed the exit code, so a
        # refusal and a pass were indistinguishable in the hook output. Capture
        # the status and NAME the verdict on stderr instead.
        return (
            "#!/usr/bin/env sh\n"
            f"{PACT_HOOK_MARKER}\n"
            "set -u\n"
            "status=0\n"
            f"{command} || status=$?\n"
            'if [ "$status" -eq 3 ]; then\n'
            f'  echo "pact gate {gate}: deferred (ledger write queued) '
            '— warn only" >&2\n'
            'elif [ "$status" -ne 0 ]; then\n'
            f'  echo "pact gate {gate}: refused (exit $status) '
            '— warn only, not blocking this commit" >&2\n'
            "fi\n"
            "exit 0\n"
        )
    # Exit 3 is `deferred`: the gate subject HOLDS, and something the gate
    # could not write or could not read is named in the payload instead - a
    # queued decision-ledger write, or a graph projection it could not read
    # (issue #1476). Blocking a commit on either would punish the operator for
    # an absent database or projector, so a blocking hook passes deferred WITH
    # A WARNING and fails on refused (1) and crash (2). The warning must not
    # name one cause as if it were the only one: the payload's `blocked_by`
    # and `projection_deferrals` say which it was. Encoded here rather than
    # left to `set -e`, because a bare command makes deferred
    # indistinguishable from a failed gate.
    return (
        "#!/usr/bin/env sh\n"
        f"{PACT_HOOK_MARKER}\n"
        "set -u\n"
        "status=0\n"
        f"{command} || status=$?\n"
        'if [ "$status" -eq 3 ]; then\n'
        f'  echo "pact gate {gate}: deferred — the gate holds and its payload '
        "names what could not be recorded or read (a queued ledger write, or "
        "an unread graph projection); allowed\" >&2\n"
        "  exit 0\n"
        "fi\n"
        'exit "$status"\n'
    )


def _ci_workflow(change_id: str) -> str:
    # Same deferred contract as the local hooks: exit 3 means the gate holds
    # and its payload names what could not be recorded or read - a queued
    # ledger write, or an unread graph projection - so it is a warning
    # annotation, not a red lane. A bare `run:` step would fail CI on an
    # unreachable database or an absent projector.
    def _step(gate: str) -> str:
        return (
            f"      - name: pact gate {gate}\n"
            "        run: |\n"
            "          status=0\n"
            f"          uv run okg pact gate {gate} {change_id} --json || status=$?\n"
            '          if [ "$status" -eq 3 ]; then\n'
            f'            echo "::warning::pact gate {gate} deferred — '
            'the gate holds; see blocked_by and projection_deferrals in the '
            'payload for what could not be recorded or read"\n'
            "            exit 0\n"
            "          fi\n"
            '          exit "$status"\n'
        )

    return (
        f"{PACT_HOOK_MARKER}\n"
        "name: PACT Gates\n"
        "on:\n"
        "  pull_request:\n"
        "  push:\n"
        "    branches: [dev, main]\n"
        "jobs:\n"
        "  pact-gates:\n"
        "    runs-on: ubuntu-latest\n"
        "    steps:\n"
        "      - uses: actions/checkout@v4\n"
        "      - uses: astral-sh/setup-uv@v5\n"
        + _step("pre-commit")
        + _step("pre-merge")
        + _step("reviewer-brief")
    )


def _hook_commands(change_id: str) -> list[str]:
    return [
        f"uv run okg pact gate post-edit {change_id} --json",
        f"uv run okg pact gate task-done {change_id} <task-id> --json",
        f"uv run okg pact gate pre-commit {change_id} --json",
        f"uv run okg pact gate pre-merge {change_id} --json",
        f"uv run okg pact gate graph-freshness {change_id} --json",
        f"uv run okg pact gate reviewer-brief {change_id} --json",
    ]


def _register_intent_event(
    manifest: PactManifest,
    change_id: str,
    digest: str,
    *,
    valid: bool,
    repo_root: Path,
) -> dict:
    """Best-effort Tier E intent registration at gate post-edit.

    Never fails the gate (authoring works offline); unavailability is
    reported as a first-class status, not swallowed.
    """
    from okg.substrate.pact import decisions as ledger
    from okg.substrate.pact.review import derive_change_state

    worktree = ledger.current_worktree_identity()
    if worktree is None:
        return {"status": "skipped", "reason": "no git checkout detected"}
    try:
        conn = ledger.require_ledger_connection(repo_root=repo_root)
    except Exception as exc:  # noqa: BLE001 - degraded mode is legal here.
        return {"status": "unavailable", "reason": str(exc)[:160]}
    try:
        if valid:
            state = derive_change_state(
                manifest,
                ledger.load_decisions(conn, change_id),
                repo_root=repo_root,
            ).state
        else:
            state = "draft"
        event = ledger.register_intent(
            conn,
            change_id=change_id,
            intent_digest=digest,
            derived_state=state,
            worktree=worktree,
        )
    except Exception as exc:  # noqa: BLE001
        return {"status": "unavailable", "reason": str(exc)[:160]}
    finally:
        conn.close()
    return {
        "status": "registered",
        "worktree": worktree["suffix"],
        "branch": worktree["branch"],
        "derived_state": state,
        "coordination_event": event,
    }


def _load_manifest_for_post_edit(repo_root: Path, change_id: str) -> PactManifest:
    return load_manifest_for_read(repo_root, change_id).manifest


def _authored_raw_document(repo_root: Path, change_id: str) -> dict | None:
    """The authored pact.yaml as a raw mapping, before normalization backfills
    schema defaults. Feeds the PACT-hygiene lint's missing-status rule (#699).
    Best-effort: a read/parse failure returns None (the parsed manifest still
    carries the other two hygiene rules)."""
    from okg.substrate.pact.store import load_yaml_mapping

    for candidate in (
        manifest_path(repo_root, change_id),
        repo_root / "pact" / "archive" / change_id / "pact.yaml",
    ):
        if candidate.is_file():
            try:
                return load_yaml_mapping(candidate)
            except Exception:
                return None
    return None


def _gate_validation_report(
    manifest: PactManifest,
    repo_root: Path,
    raw_document: dict | None,
):
    """validate_manifest folded with the PACT-hygiene lint.

    The hygiene lint (task.audit-2026-07.enforce-pact-hygiene, #699) is enforced
    at the gate boundary only — NOT inside the universal `validate_manifest`,
    whose `.ok` many internal callers (edit-guard, review, retire, completion
    gate) depend on. Marking the final task done legitimately produces the
    all-done state; the operator advances change.status (or archives) next, and
    the gate is where that omission is refused, so status keeps discriminating
    in committed PACTs without blocking the in-flight closure edit itself.
    """
    from okg.substrate.pact.validation import pact_hygiene_issues

    report = validate_manifest(manifest, repo_root=repo_root)
    hygiene = pact_hygiene_issues(manifest, raw_document=raw_document)
    if hygiene:
        report.issues.extend(hygiene)
        report.ok = report.ok and not any(
            issue.severity == "error" for issue in hygiene
        )
    return report


def repair_vocabulary(repo_root: Path, manifest: PactManifest) -> dict[str, list[str]]:
    """The LEGAL set for each edit-repairable validation code.

    Sourced from the registry and the manifest itself, never hand-maintained:
    a refusal that names the offending value without the legal vocabulary is
    what sent every audited session to `pact.schema.yaml` or a sibling PACT.
    """
    from okg.substrate.pact.evidence_kinds import load_registry
    from okg.substrate.pact.validation import evidence_ref_vocabulary

    registry = load_registry(repo_root)
    kinds = sorted(registry.canonical | set(registry.aliases))
    requirement_ids = sorted(req.id for req in manifest.requirements)
    acceptance_ids = sorted(
        criterion.id for req in manifest.requirements for criterion in req.acceptance
    )
    return {
        "missing_evidence_policy": kinds,
        "unknown_evidence_kind": kinds,
        "unknown_acceptance_evidence": kinds,
        # THE check's own vocabulary, imported rather than restated: this list
        # was narrower than `_evidence_ref_issues` accepts (it omitted task,
        # test-plan, formal and change ids), so a refusal told an author to
        # change a ref the validator already allows.
        "unknown_evidence_ref": sorted(evidence_ref_vocabulary(manifest)),
        "unknown_requirement_ref": requirement_ids,
        "task_requirements_link": requirement_ids,
        "task_done_missing_evidence": requirement_ids,
        "stale_test_reference": sorted({*requirement_ids, *acceptance_ids}),
    }


def _policy_kinds_in_use(
    repo_root: Path,
    manifest: PactManifest,
    requirement_id: str | None,
) -> list[str]:
    """The kinds THIS manifest already says prove this requirement.

    Two sources, in order, both authored by whoever wrote the manifest:
    the evidence rows that reference it, and the test-plan entries that name
    it (whose `evidence` tokens are canonicalized through the registry, so a
    plan that says `pytest` narrows to `executed_test`).

    This is what makes an evidence-policy repair DETERMINATE without guessing:
    the answer is read off the manifest's own declarations rather than picked
    out of a twenty-member enum.
    """
    if requirement_id is None:
        return []
    from okg.substrate.pact.evidence_kinds import load_registry

    registry = load_registry(repo_root)
    owned = {requirement_id} | {
        criterion.id
        for req in manifest.requirements
        if req.id == requirement_id
        for criterion in req.acceptance
    }
    attached = sorted(
        {
            registry.canonicalize(row.kind)
            for row in manifest.evidence
            if row.kind and registry.is_known(row.kind) and owned & set(row.refs or ())
        }
    )
    if attached:
        return attached
    return sorted(
        {
            registry.canonicalize(token)
            for entry in manifest.test_plan
            if entry.requirement in owned
            for token in entry.evidence or ()
            if token and registry.is_known(token)
        }
    )


def _default_policy_kinds(repo_root: Path) -> list[str]:
    """The repo's declared default evidence policy (`pact/project.yaml`)."""
    from okg.substrate.pact.project_settings import load_project_settings

    try:
        settings = load_project_settings(Path(repo_root))
    except Exception:  # noqa: BLE001 - unreadable settings must not break a refusal
        return []
    policy = settings.get("default_evidence_policy")
    if not isinstance(policy, dict):
        return []
    required = policy.get("required")
    return [str(kind) for kind in required or [] if kind]


def _requirement_index(
    manifest: PactManifest, requirement_id: str | None
) -> int | None:
    for index, req in enumerate(manifest.requirements):
        if req.id == requirement_id:
            return index
    return None


def _task_index(manifest: PactManifest, task_id: str) -> int | None:
    for index, task in enumerate(manifest.tasks):
        if task.id == task_id:
            return index
    return None


def edit_resolution(
    repo_root: Path,
    manifest: PactManifest,
    row: dict,
) -> dict:
    """Narrow one validation row into a descriptor a BLIND applier can apply.

    Two jobs, both of which need the manifest and therefore cannot live in
    `repair.py`:

    * POINTER — the alignment codes address a task by id (``tasks.task.one``),
      which no mechanical walker can parse (ids contain dots). Rewritten to the
      index form the rest of the paths already use.
    * LEGAL SET — the code-keyed vocabulary is the full enum (every registered
      kind). A blind applier handed 20 legal kinds has to guess, and guessing
      is the defect. Where the manifest determines the answer — the kinds this
      requirement is already proven with, or the repo's declared default policy
      — the set is narrowed to it. Where it does not, the descriptor stays wide
      and the applier REFUSES rather than picking `legal_values[0]`.
    """
    code = str(row.get("code") or "")
    path = str(row.get("path") or "")
    offending = row.get("offending_value")
    resolution: dict = {}

    if code == "task_done_missing_evidence":
        # Only the LINK arm is an edit; the alignment arm is repaired by an
        # `evidence attach`, which is a command.
        if not path.endswith(".requirements"):
            return {"skip": True}
        task_id = path[len("tasks.") : -len(".requirements")]
        index = _task_index(manifest, task_id)
        if index is None:
            return {"skip": True}
        resolution["pointer"] = f"tasks[{index}].requirements"
        resolution["action"] = "link the task to at least one requirement"
        resolution["legal_values"] = sorted(req.id for req in manifest.requirements)
        return resolution

    if code == "unknown_evidence_kind" and path.endswith(".evidence_policy"):
        # The path names the policy mapping; the applier needs the LIST that
        # holds the offending token.
        index = int(path.split("[", 1)[1].split("]", 1)[0])
        req = manifest.requirements[index]
        slot = (
            "required" if offending in req.evidence_policy.required else "required_any"
        )
        resolution["pointer"] = f"requirements[{index}].evidence_policy.{slot}"
        resolution["legal_values"] = (
            _policy_kinds_in_use(repo_root, manifest, req.id)
            or _default_policy_kinds(repo_root)
        ) or None
        return {k: v for k, v in resolution.items() if v is not None}

    if code == "unknown_evidence_kind" and path.endswith(".kind"):
        index = int(path.split("[", 1)[1].split("]", 1)[0])
        row_refs = set(manifest.evidence[index].refs or ())
        demanded = sorted(
            {
                kind
                for req in manifest.requirements
                if row_refs & ({req.id} | {c.id for c in req.acceptance})
                for kind in (
                    *req.evidence_policy.required,
                    *req.evidence_policy.required_any,
                )
                if kind
            }
        )
        if demanded:
            resolution["legal_values"] = demanded
        return resolution

    if code == "missing_evidence_policy" and path.endswith(".evidence_policy.required"):
        index = int(path.split("[", 1)[1].split("]", 1)[0])
        narrowed = _policy_kinds_in_use(
            repo_root, manifest, manifest.requirements[index].id
        ) or _default_policy_kinds(repo_root)
        if narrowed:
            resolution["legal_values"] = narrowed
        return resolution

    if code == "missing_evidence_policy" and path == "requirements":
        # "This change declares tasks but no requirements." There is no legal
        # VOCABULARY for that: authoring a requirement is a judgment about what
        # the change must guarantee, and no enum determines it. The descriptor
        # says so — an empty legal set makes the applier refuse rather than
        # fill the slot with the first evidence kind it was handed.
        resolution["legal_values"] = []
        resolution["action"] = (
            "declare at least one requirement (with an evidence policy) for "
            "these tasks to prove"
        )
        return resolution

    return resolution


def manifest_edit_descriptors(
    repo_root: Path,
    change_id: str,
    manifest: PactManifest,
    issues: Iterable,
) -> list[dict]:
    """The complete, mechanically-applicable edit descriptors for a refusal."""
    from okg.substrate.pact import repair as repair_mod

    return repair_mod.manifest_edits(
        repo_root,
        change_id,
        issues,
        legal_values=repair_vocabulary(repo_root, manifest),
        resolve=lambda row: edit_resolution(repo_root, manifest, row),
    )


def _refutation_tokens(missing: Iterable[str]) -> list[str]:
    """The refusal's refuting rows, pulled back out of the proof tokens.

    A refutation is `{requirement}:refuted_by:{evidence id}:{kind}:{status}`
    (validation.REFUTATION_TOKEN). It is separated from the rest of
    `missing_proof` because the two need OPPOSITE advice: an empty proof slot
    is filled by attaching a row, and a refutation is not — the row is already
    there and it says the requirement does not hold.
    """
    from okg.substrate.pact.validation import REFUTATION_TOKEN

    marker = f":{REFUTATION_TOKEN}:"
    return [token for token in missing if marker in str(token)]


def _task_done_why(
    *,
    proof_commands: list[str],
    edits: list[dict],
    operator_action: dict | None,
    refutations: list[str] | None = None,
    v5_guidance: str | None = None,
) -> str:
    """Where the repair lives — command, canonical file, or another human."""
    parts: list[str] = []
    if v5_guidance:
        parts.append(v5_guidance)
    if refutations:
        # Named first: it is the only refusal cause that no `next_commands`
        # entry can clear, so a reader who scans the commands and stops would
        # otherwise see a refusal with nothing to run and no reason given.
        rows = ", ".join(sorted(refutations))
        parts.append(
            f"{len(refutations)} recorded measurement(s) REFUTE a linked "
            f"requirement and no attach clears them ({rows}); the repair is to "
            "make the recorded outcome true — fix the thing the row measured "
            "and re-run it, or correct the row's status with a summary saying "
            "why. A requirement-scoped waiver clears the requirement's gap in "
            "`pact view` but NOT this gate, which reads the task rollup "
            "(validation.py:2819-2827) and never consults waivers"
        )
    if proof_commands:
        parts.append(
            f"{len(proof_commands)} proof slot(s) are fillable in-band; run the "
            "attach command(s) in next_commands"
        )
    if edits:
        codes = ", ".join(sorted({str(edit["code"]) for edit in edits}))
        parts.append(
            "the canonical file is the repair surface for "
            f"{codes} — see repair.manifest_edits for the exact pointer, "
            "offending value and legal set (no okg pact verb writes those fields)"
        )
    if operator_action:
        parts.append(
            "the decision ledger is unreachable and no okg pact command clears "
            f"it: {operator_action['remedy']} (owner: {operator_action['owner']})"
        )
    return "; ".join(parts) or "the gate subject does not hold"


def _post_edit_repair(
    repo_root: Path,
    change_id: str,
    manifest: PactManifest,
    report,
) -> dict:
    """The repair block for `gate post-edit`.

    A manifest-content defect has no in-band verb — `pact.yaml` is AUTHORED,
    not generated — so this emits a command ONLY for the proof-shaped errors
    an `evidence attach` genuinely clears, and names the file, pointer,
    offending value and legal set for the rest. The pre-change list opened
    with `view --format=brief`, which runs cleanly and repairs nothing.
    """
    from okg.substrate.pact import repair as repair_mod

    prefix = repair_mod.repo_command(repo_root)
    error_issues = [issue for issue in report.issues if issue.severity == "error"]
    alignment = getattr(report, "alignment", None) or {}
    task_alignment = alignment.get("tasks") or {}
    slots: list[str] = []
    # The refusal's NAMED SUBJECT: the requirements of the tasks this gate is
    # refusing. A proof command may not reach outside it (req.refusal.executable
    # — a repair that attaches evidence to another requirement is fabricated
    # closure, not a repair).
    subject_requirements: list[str] = []
    for issue in error_issues:
        # Only the alignment arm of `task_done_missing_evidence` is
        # attach-repairable: its sibling ("resolves to no proof obligation at
        # all") is a LINK defect, and it is distinguishable by path.
        if issue.code != "task_done_missing_evidence":
            continue
        if str(issue.path).endswith(".requirements"):
            continue
        task_id = str(issue.path).split(".", 1)[-1]
        slots.extend(
            (task_alignment.get(task_id) or {}).get("satisfaction_missing") or []
        )
        for task in manifest.tasks:
            if task.id == task_id:
                subject_requirements.extend(task.requirements)
    commands = repair_mod.proof_slot_commands(
        manifest,
        change_id,
        dict.fromkeys(slots),
        prefix=prefix,
        subject_requirements=dict.fromkeys(subject_requirements),
    )
    v5_guidance = (
        repair_mod.v5_unbacked_guidance(manifest, commands) if slots else None
    )
    if commands:
        commands = repair_mod.executable_commands(
            [
                *commands,
                f"{prefix} pact gate post-edit {change_id} --json",
            ]
        )
    edits = manifest_edit_descriptors(repo_root, change_id, manifest, error_issues)
    why_parts: list[str] = []
    if commands:
        why_parts.append(
            "a done task is missing the evidence its requirement policy "
            "demands; the attach command in next_commands fills the slot"
        )
    if v5_guidance:
        why_parts.append(v5_guidance)
    if edits:
        codes = ", ".join(sorted({str(edit["code"]) for edit in edits}))
        why_parts.append(
            f"edit pact.yaml to clear {codes} — no okg pact verb writes those "
            "fields; repair.manifest_edits names the pointer and the legal set"
        )
    return {
        "commands": commands,
        "manifest_edits": edits,
        "operator_action": None,
        "why": "; ".join(why_parts) or "the manifest does not validate",
    }


def _post_edit_text(change_id: str, report, registration: dict) -> str:
    errors = sum(1 for issue in report.issues if issue.severity == "error")
    status = registration.get("status")
    return f"post-edit {change_id}: errors={errors} intent={status}"


def _lifecycle_successor(
    repo_root: Path,
    change_id: str,
    *,
    state: str | None,
    pending_decision: str | None = None,
) -> list[str]:
    """Return one command that advances the state reached by a gate."""
    from okg.substrate.pact import repair

    prefix = repair.repo_command(repo_root)
    quoted = shlex.quote(change_id)
    if state in {"proposed", "awaiting_review"} or pending_decision in {
        "go-ahead",
        "review",
    }:
        return [f"{prefix} pact decide {quoted} --verdict approve --json"]
    if state in {"draft", "in_progress"}:
        return [f"{prefix} pact view {quoted} --format=bootstrap --json"]
    if state == "retired":
        return [f"{prefix} pact gate pre-merge {quoted} --json"]
    if state in {"rejected", "superseded"}:
        return [f"{prefix} pact view {quoted} --format=status --json"]
    return []


def post_edit_check(repo_root: Path, change_id: str) -> dict:
    from okg.substrate.pact.validation import _target_language_version

    probe = _load_manifest_for_post_edit(repo_root, change_id)
    raw_document = _authored_raw_document(repo_root, change_id)
    if _target_language_version(probe) >= 4:
        # v4 (standardize-pact-review-interface): the gate validates and
        # ADMITS the authored file — it never rewrites it (no normalize)
        # and never materializes pact.md (views render on demand).
        from okg.substrate.pact.store import intent_digest

        report = _gate_validation_report(probe, repo_root, raw_document)
        digest = intent_digest(probe)
        registration = _register_intent_event(
            probe,
            change_id,
            digest,
            valid=report.ok,
            repo_root=repo_root,
        )
        # The gate's own record is the intent event. "unavailable" means the
        # ledger could not be reached, so nothing was registered — the gate is
        # DEFERRED, not green. "skipped" means there was no event to write in
        # the first place (no git checkout), which is not a queued write.
        ledger_deferred = registration.get("status") == "unavailable"
        post_edit_repair = _post_edit_repair(repo_root, change_id, probe, report)
        commands = [] if report.ok else post_edit_repair["commands"]
        lifecycle_state = registration.get("derived_state")
        if not report.ok:
            lifecycle_state = "draft"
        elif lifecycle_state is None:
            # A checkout-less local validation has no intent event.  Derive
            # from the manifest only; a recorded registration already carries
            # the ledger-aware authoritative state and must not be overwritten
            # with an empty decision history.
            from okg.substrate.pact.review import derive_change_state

            lifecycle_state = derive_change_state(probe, [], repo_root=repo_root).state
        if report.ok and not ledger_deferred:
            commands = _lifecycle_successor(repo_root, change_id, state=lifecycle_state)
        next_command = commands[0] if commands else None
        intent_edit_required = (
            report.ok
            and not ledger_deferred
            and lifecycle_state == "draft"
            and registration.get("status") == "registered"
        )
        return envelope.envelope(
            envelope.gate_verdict(holds=report.ok, deferred_write=ledger_deferred),
            text=_post_edit_text(change_id, report, registration),
            why=None if report.ok else post_edit_repair["why"],
            blocked_by=envelope.tokens(
                (issue.code for issue in report.issues if issue.severity == "error"),
                # Only when the DEFERRAL is the verdict: on a refusal the
                # validation errors are the cause, and folding the ledger in
                # would name something the manifest fix cannot clear.
                ["ledger_not_fresh"] if (ledger_deferred and report.ok) else [],
                ["intent_edit_required"] if intent_edit_required else [],
            ),
            next_commands=commands,
            change=change_id,
            normalized=False,
            validation=report.to_dict(),
            review_view=envelope.component(ok=True, status="on-demand"),
            intent_digest=digest,
            semantic_diff={"changed": None, "command": "v4 digest admission"},
            intent_registration=registration,
            repair=post_edit_repair,
            recommended_commands=commands,
            state=lifecycle_state,
            next_action=(
                {
                    "command": next_command,
                    "owner": (
                        "operator"
                        if lifecycle_state in {"proposed", "awaiting_review"}
                        else "agent"
                    ),
                    "why": (
                        "edit pact.yaml to address the recorded revision; rerun only "
                        "after the intent digest changes"
                        if intent_edit_required
                        else "continue the derived lifecycle"
                    ),
                    "wait_condition": (
                        "intent_digest_changes_after_direct_pact_edit"
                        if intent_edit_required
                        else None
                    ),
                }
                if next_command
                else None
            ),
        )
    manifest = normalize_manifest(repo_root, change_id)
    report = _gate_validation_report(manifest, repo_root, raw_document)
    view_payload: dict | None = None
    if report.ok:
        from okg.substrate.pact.markdown_view import (
            check_markdown_view,
            write_markdown_view,
        )

        write_markdown_view(repo_root, change_id)
        view_payload = check_markdown_view(repo_root, change_id)
    latest = manifest.edits[-1] if manifest.edits else None
    semantic_diff = {
        "changed": bool(latest and latest.before_digest != latest.after_digest),
        "command": latest.command if latest else None,
        "before_digest": latest.before_digest if latest else None,
        "after_digest": latest.after_digest if latest else None,
        "affected_ids": latest.affected_ids if latest else [],
    }
    ok = report.ok and bool(view_payload and view_payload.get("ok"))
    post_edit_repair = _post_edit_repair(repo_root, change_id, manifest, report)
    commands = [] if ok else post_edit_repair["commands"]
    lifecycle_state = "in_progress" if ok else "draft"
    if ok:
        try:
            from okg.substrate.pact.review import derive_change_state

            lifecycle_state = derive_change_state(
                manifest, [], repo_root=repo_root
            ).state
        except Exception:
            # The legacy normalization arm has no decision-ledger authority;
            # retain its historical in-progress fallback if derivation fails.
            pass
        commands = _lifecycle_successor(repo_root, change_id, state=lifecycle_state)
    next_command = commands[0] if commands else None
    return envelope.from_ok(
        ok,
        text=f"post-edit {change_id}: errors={len(report.issues)}",
        why=None if ok else post_edit_repair["why"],
        blocked_by=envelope.tokens(
            (issue.code for issue in report.issues if issue.severity == "error"),
            []
            if (view_payload is None or view_payload.get("ok"))
            else [f"review_view_{view_payload.get('status') or 'stale'}"],
        ),
        next_commands=commands,
        change=change_id,
        normalized=True,
        validation=report.to_dict(),
        review_view=view_payload,
        semantic_diff=semantic_diff,
        repair=post_edit_repair,
        recommended_commands=commands,
        state=lifecycle_state,
        next_action=(
            {
                "command": next_command,
                "owner": "agent",
                "why": "continue the derived lifecycle",
            }
            if next_command
            else None
        ),
    )


def _graph_evidence_requirements(manifest: PactManifest, task) -> list[str]:
    """The task's linked requirements that demand graph_report proof.

    Resolved through the evidence-kind registry, so a policy spelled `graph`
    counts exactly as `graph_report` does.
    """
    from okg.substrate.pact.evidence_kinds import DEFAULT_REGISTRY

    req_by_id = {req.id: req for req in manifest.requirements}
    demanding: list[str] = []
    for req_id in task.requirements:
        req = req_by_id.get(req_id)
        if req is None:
            continue
        tokens = (
            *req.evidence_policy.required,
            *req.evidence_policy.required_any,
        )
        if any(
            DEFAULT_REGISTRY.canonicalize(token) == "graph_report" for token in tokens
        ):
            demanding.append(req_id)
    return demanding


def _published_generation_authority_requirements(
    manifest: PactManifest,
    task,
) -> list[str]:
    """Graph-shaped requirements eligible for the explicit private authority.

    This is intentionally separate from ``_graph_evidence_requirements``.
    An absent opt-in must retain the pre-existing shared-projection behavior,
    while an opted-in task may prove either graph-report or exact
    published-generation policy.
    """
    from okg.substrate.pact.evidence_kinds import DEFAULT_REGISTRY

    req_by_id = {req.id: req for req in manifest.requirements}
    demanding: list[str] = []
    for req_id in task.requirements:
        req = req_by_id.get(req_id)
        if req is None:
            continue
        tokens = (
            *req.evidence_policy.required,
            *req.evidence_policy.required_any,
        )
        if any(
            DEFAULT_REGISTRY.canonicalize(token)
            in {"graph_report", "published_generation"}
            for token in tokens
        ):
            demanding.append(req_id)
    return demanding


def _generation_pin_matches(task, graph_authority: dict) -> bool:
    """Does the task's pinned generation agree with the projected one?

    Absence is not disagreement, in either direction: a task with no pin makes
    no claim to refute, and a projection carrying no generation id has not
    contradicted the pin (that case is already blocked by `graph_not_ready` /
    `graph_not_fresh`). Only two present-and-different numbers are a mismatch.
    """
    pinned = getattr(task, "coordination_generation_id", None)
    projected = graph_authority.get("projected_generation_id")
    if pinned is None or projected is None:
        return True
    try:
        return int(pinned) == int(projected)
    except (TypeError, ValueError):
        return False


def task_completion_gate(
    repo_root: Path,
    change_id: str,
    task_id: str,
) -> dict:
    from okg.substrate.pact.validation import task_proof_obligation

    manifest = load_manifest(repo_root, change_id)
    report = validate_manifest(manifest, repo_root=repo_root)
    alignment = report.alignment
    task = _task(manifest, task_id, repo_root=repo_root)
    state = alignment.get("tasks", {}).get(task_id, {})
    blockers = list(state.get("blockers", []))
    missing = list(state.get("satisfaction_missing", []))
    # Closing a task that resolves to no proof obligation checks nothing. The
    # refusal names the task, instead of the pre-change `ok`/`missing_proof: []`
    # pair that read as "proven" (pact-vacuous-gate-close req.vacuous.presence).
    if not task_proof_obligation(manifest, task):
        blockers.append(f"task_proof_obligation_vacuous:{task_id}")
    # Per `right-size-pact-authoring` req.rightsize.consistency: satisfying
    # evidence recorded BEFORE a later change to the declared scope no longer
    # proves the task — satisfaction is revoked loudly, never a silent pass.
    from okg.substrate.pact.scope_drift import (
        code_proof_freshness_boundary,
        staleness_report,
    )

    stale_evidence: dict[str, dict] = {}
    proof_freshness = code_proof_freshness_boundary(repo_root, manifest)
    for requirement_id in task.requirements:
        staleness = staleness_report(
            repo_root,
            manifest,
            requirement_id,
            code_proof_boundary=proof_freshness,
        )
        if staleness.get("revoked"):
            stale_evidence[requirement_id] = staleness
            missing.append(f"{requirement_id}:stale_evidence")
    coordination = resolve_coordination_gates(repo_root, change_id, task)
    missing.extend(coordination.get("missing", []))
    coordination_generation_matches = not any(
        detail.get("status") == "stale_generation"
        for detail in coordination.get("details", [])
        if isinstance(detail, dict)
    )
    target_version = manifest.target_pact_version or manifest.pact_version
    file_authority = manifest_file_authority(repo_root, change_id, manifest)
    # A task demands graph authority two ways, and only one of them used to
    # count: the explicit pin, and a linked requirement whose evidence policy
    # demands graph_report proof. Reading the pin alone let the demand be
    # deleted with the pin — `graph_required` collapsed to False and the clause
    # had nothing to quantify over (pact-vacuous-gate-close
    # req.vacuous.presence).
    if target_version >= 4:
        from okg.substrate.pact.published_generation_authority import (
            task_declares_published_generation_authority,
            verify_published_generation_authority,
        )

        published_generation_opt_in = task_declares_published_generation_authority(task)
    else:
        published_generation_opt_in = False
    graph_demanding_requirements = (
        _published_generation_authority_requirements(manifest, task)
        if published_generation_opt_in
        else _graph_evidence_requirements(manifest, task)
    )
    graph_required = target_version >= 4 and (
        task.coordination_generation_id is not None
        or bool(graph_demanding_requirements)
    )
    if target_version >= 4:
        if published_generation_opt_in:
            # Presence is the authority selector even when malformed. Never
            # consult or fall back to the shared projection on this arm.
            graph_authority = verify_published_generation_authority(
                repo_root,
                manifest,
                task,
                demanding_requirements=graph_demanding_requirements,
            )
        else:
            graph_authority = _workflow_graph_projection_status(repo_root, manifest)
        ledger_authority = _workflow_ledger_authority(repo_root)
        # The PIN is the subject of `generation_pin_mismatch`, so the check
        # compares the pin to the generation the projection actually carries.
        # Deriving it from `status == "fresh"` instead read ABSENCE (no
        # projection at all) as DISAGREEMENT, so an unpinned task collected a
        # pin-mismatch blocker it could not act on, and a genuinely mismatched
        # pin on a FRESH projection went unnoticed. `graph_not_fresh` /
        # `graph_not_ready` still carry the absent-projection case, so nothing
        # that refused before is accepted now.
        graph_generation_matches = _generation_pin_matches(task, graph_authority)
    else:
        graph_authority = envelope.component(
            ok=True,
            status="fresh",
            authority_scope="legacy_not_required",
            stale=False,
            projected_generation_id=None,
            recommended_commands=[],
        )
        ledger_authority = {"fresh": True, "status": "legacy_not_required"}
        graph_generation_matches = True
    # The external-repository receipt arm (issue #1424), read off the SAME
    # component the merge boundary reads. `allowed` is the declaration
    # parsing, `bound` is the whole verdict; the three identity answers come
    # from the receipt's own blockers rather than being passed as an
    # unexamined True.
    external_arm = (
        str(graph_authority.get("authority_scope") or "")
        == "external_repository_receipt"
    )
    external_blockers = (
        set(graph_authority.get("blockers") or ()) if external_arm else set()
    )
    code_claims = tuple(manifest.change.affected_code or ())
    ledger_fresh = ledger_authority.get("fresh") is True
    # "No DSN configured in this shell" is not a coordination
    # disagreement: coordination gates that pass locally stay agreed, and
    # the ledger blocker names the configuration fix instead
    # (issue #643, req.pactdx.ledger-hints).
    ledger_no_dsn = ledger_authority.get("cause") == "no_dsn_configured"
    authority_decision = evaluate_workflow_authority(
        WorkflowAuthorityInput(
            file_fresh=file_authority.get("fresh") is True,
            graph_required=graph_required,
            graph_ready=graph_authority.get("ok") is True,
            graph_fresh=graph_authority.get("status") == "fresh",
            ledger_fresh=ledger_fresh,
            ledger_no_dsn=ledger_no_dsn,
            # Stated, not defaulted (issue #1443): task completion is not the
            # merge boundary, and a recorded merge approval is no part of its
            # bar. A task is proven by its evidence; asking the ledger for an
            # approval here would refuse every task before review.
            ledger_approval=LEDGER_APPROVAL_NOT_JUDGED,
            generation_pin_matches=(
                coordination_generation_matches and graph_generation_matches
            ),
            evidence_artifacts_fresh=not missing,
            pact_coordination_agree=(
                bool(coordination.get("ok", False)) and (ledger_fresh or ledger_no_dsn)
            ),
            projection_authority=str(
                graph_authority.get("authority_scope") or "default_graph"
            ),
            external_authority_allowed=(
                (graph_authority.get("provenance") or {}).get("declaration")
                is not None
                if external_arm
                else False
            ),
            external_receipt_bound=(
                graph_authority.get("ok") is True if external_arm else True
            ),
            intent_digest_matches=(
                "external_receipt_intent_digest_mismatch" not in external_blockers
                if external_arm
                else True
            ),
            worktree_head_matches=(
                "external_receipt_head_mismatch" not in external_blockers
                if external_arm
                else True
            ),
            worktree_clean=(
                "external_receipt_worktree_not_clean" not in external_blockers
                if external_arm
                else True
            ),
            code_claims=code_claims,
            git_evidence=git_evidence_for_manifest(repo_root, manifest),
        )
    )
    authority_specific_blockers = (
        list(graph_authority.get("blockers") or [])
        if published_generation_opt_in or external_arm
        else []
    )
    ok = (
        report.ok
        and not blockers
        and not missing
        and not authority_specific_blockers
        and authority_decision.accepted
    )
    workflow_authority = {
        **workflow_authority_payload(authority_decision),
        "file_authority": file_authority,
        # Reported, not re-derivable: a reader (the friction benchmark, an
        # agent) that inferred "was graph authority demanded?" from the
        # blockers it can see would call every quiet gate not-required and
        # every noisy one required, which is the vacuity this clause closes.
        "graph_required": graph_required,
        "graph_demanding_requirements": graph_demanding_requirements,
        "graph_generation_pin": task.coordination_generation_id,
        "graph_projection": {
            "status": (
                "not_consulted"
                if published_generation_opt_in
                else graph_authority.get("status")
            ),
            "stale": None
            if published_generation_opt_in
            else graph_authority.get("stale"),
            "projected_generation_id": (
                None
                if published_generation_opt_in
                else graph_authority.get("projected_generation_id")
            ),
        },
        **(
            {"published_generation_authority": graph_authority}
            if published_generation_opt_in
            else {}
        ),
        # Same rule as the merge boundary: on this arm the surface reports
        # itself, whether the gate accepts or refuses.
        **(
            {
                "external_authority": envelope.component(
                    ok=graph_authority.get("ok") is True,
                    authority_scope="external_repository_receipt",
                    blockers=sorted(external_blockers),
                    provenance=graph_authority.get("provenance"),
                    attested=True,
                    verified_against_graph=False,
                    unverified=list(graph_authority.get("unverified") or ()),
                )
            }
            if external_arm
            else {}
        ),
        "ledger_authority": ledger_authority,
        "code_claims": list(code_claims),
        **(
            {"code_proof_freshness": proof_freshness}
            if proof_freshness.get("applies")
            else {}
        ),
    }
    # `next_commands` is built by one funnel (`repair.executable_commands`), so
    # nothing that fails to parse or trips the repo command guards can reach
    # it. Before this, the list opened with `task.verification` — the
    # requirement's PROSE verification clause — followed by generic strings
    # ("git status", "focused tests") and "configure PACT decision ledger
    # DSN". None of those is a command, and the first one is what an agent
    # runs (pact-self-repairing-refusals req.refusal.executable).
    from okg.substrate.pact import repair as repair_mod

    prefix = repair_mod.repo_command(repo_root)
    # Scoped to the REFUSED TASK's own links. Without this the trailing
    # acceptance sweep emitted attach commands for criteria belonging to
    # requirements this task does not prove — proof fabricated through the
    # repair path (req.refusal.executable).
    proof_commands = repair_mod.proof_slot_commands(
        manifest,
        change_id,
        missing,
        prefix=prefix,
        subject_requirements=task.requirements,
    )
    # Only when a GRAPH blocker is actually named. Emitting the projection
    # refresh unconditionally put an unrelated `okg deployment env ...` at the
    # head of `next_commands` on changes with no graph demand at all — a
    # recommendation that runs cleanly and moves nothing.
    graph_blocked = any(
        blocker.startswith("graph_") or blocker == "generation_pin_mismatch"
        for blocker in authority_decision.blockers
    ) or bool(authority_specific_blockers)
    graph_commands = (
        repair_mod.executable_commands(graph_authority.get("recommended_commands", []))
        if graph_blocked
        else []
    )
    error_issues = [issue for issue in report.issues if issue.severity == "error"]
    edits = manifest_edit_descriptors(repo_root, change_id, manifest, error_issues)
    # No `okg pact` command clears an unconfigured DSN, so the remedy is
    # carried as an owner + out-of-band instruction, never re-emitted as a
    # repair — re-emitting it is the measured infinite loop.
    operator_action = (
        repair_mod.ledger_remedy(ledger_authority) if not ledger_fresh else None
    )
    repairs = [*proof_commands, *graph_commands]
    commands = (
        []
        if ok
        else repair_mod.executable_commands(
            [
                *repairs,
                # The re-check only makes sense once something above has been run;
                # emitting it alone would recommend re-running the call that just
                # refused.
                *(
                    [f"{prefix} pact gate task-done {change_id} {task_id} --json"]
                    if repairs
                    else []
                ),
            ]
        )
    )
    repair_report = {
        "commands": commands,
        "manifest_edits": edits,
        "operator_action": operator_action,
        # The prose that used to lead `next_commands`. It is guidance, not an
        # invocation, so it lives here.
        "verification_clause": task.verification,
        "live_checks": list(authority_decision.required_live_checks),
        "coordination_refresh_required": bool(coordination.get("missing")),
    }
    why = (
        None
        if ok
        else _task_done_why(
            proof_commands=proof_commands,
            edits=edits,
            operator_action=operator_action,
            refutations=_refutation_tokens(missing),
            v5_guidance=(
                repair_mod.v5_unbacked_guidance(manifest, proof_commands)
                if missing
                else None
            ),
        )
    )
    # `blocked_by` is the SINGLE cause location. Before this, a refusal's
    # reason rotated across `blockers`, `missing_proof`, `closure_intent`, and
    # `workflow_authority.blockers`, so a reader that checked one of them saw
    # ok: false beside an empty diagnostic.
    result = envelope.from_ok(
        ok,
        text=(
            f"task-done {change_id} {task_id}: "
            f"{'holds' if ok else 'refused'} "
            f"missing_proof={len(missing)} blockers={len(blockers)}"
        ),
        blocked_by=envelope.tokens(
            blockers,
            (f"missing_proof:{token}" for token in missing),
            workflow_authority.get("blockers") or [],
            authority_specific_blockers,
            (issue.code for issue in report.issues if issue.severity == "error"),
        ),
        next_commands=commands,
        why=why,
        change=change_id,
        task=task_id,
        task_status=task.status,
        blockers=blockers,
        missing_proof=missing,
        **({"stale_evidence": stale_evidence} if stale_evidence else {}),
        coordination_gates=coordination,
        workflow_authority=workflow_authority,
        validation_errors=[i.to_dict() for i in report.issues],
        repair=repair_report,
        # Read by any consumer classifying a refusal tri-valued: an
        # operator-action row must name its owner.
        **({"owner": operator_action["owner"]} if operator_action else {}),
        **({"remedy": operator_action["remedy"]} if operator_action else {}),
        recommended_commands=commands,
    )
    result["coordination_event"] = _record_coordination_event(
        change_id,
        "progress",
        task_id=task_id,
        intent_digest_value=intent_digest(manifest),
        payload=envelope.component(
            ok=ok,
            command="gate task-done",
            task_status=task.status,
            blockers=blockers,
            missing_proof=missing,
            coordination_gates=coordination,
            workflow_authority=workflow_authority,
            validation_error_count=len(report.issues),
        ),
        repo_root=repo_root,
    )
    return result


def _workflow_graph_projection_status(
    repo_root: Path,
    manifest: PactManifest,
) -> dict:
    try:
        from okg.substrate.pact import cli as pact_cli
        from okg.substrate.pact.store import intent_digest

        # The SAME seam the merge boundary uses, so a manifest owned by an
        # external repository (issue #1424) is judged by its receipt here too.
        # Reading the sync-root projection on this arm would refuse a task for
        # a projection that cannot exist, which is the refusal the whole
        # surface was added to remove.
        return pact_cli._merge_authority_status(
            repo_root, manifest, intent_digest(manifest)
        )
    except Exception as exc:  # noqa: BLE001 - authority refusal is the contract.
        return envelope.component(
            ok=False,
            status="unavailable",
            stale=None,
            reason=str(exc),
            projected_generation_id=None,
            recommended_commands=[
                f"uv run okg pact gate graph-freshness {manifest.change.id} --json"
            ],
        )


def _workflow_ledger_authority(repo_root: Path) -> dict:
    from okg.substrate.pact.decisions import require_ledger_connection

    try:
        conn = require_ledger_connection(repo_root=repo_root)
    except SubstrateError as exc:
        payload: dict = {
            "fresh": False,
            "status": "unavailable",
            "reason": str(exc),
        }
        # Carry the classified cause + fix upstream so blockers can name
        # "no DSN configured" instead of coordination disagreement
        # (issue #643, req.pactdx.ledger-hints).
        cause = (exc.data or {}).get("cause")
        if cause:
            payload["cause"] = cause
        deployment = (exc.data or {}).get("deployment")
        if deployment:
            payload["deployment"] = deployment
        if exc.fix_hint:
            payload["fix_hint"] = exc.fix_hint
        return payload
    try:
        return {"fresh": True, "status": "available"}
    finally:
        conn.close()


def normalize_manifest(repo_root: Path, change_id: str) -> PactManifest:
    with manifest_write_lock(repo_root, change_id):
        manifest = load_manifest(repo_root, change_id)
        loaded_digest = state_digest(manifest)
        last_recorded_digest = (
            manifest.edits[-1].after_digest if manifest.edits else "sha256:unrecorded"
        )
        manifest.requirements.sort(key=lambda item: item.id)
        manifest.tasks.sort(key=lambda item: item.id)
        manifest.dependencies.sort(
            key=lambda item: (item.to_task, item.from_task, item.kind)
        )
        manifest.test_plan.sort(key=lambda item: item.id)
        manifest.evidence.sort(key=lambda item: item.id)
        normalized_digest = state_digest(manifest)
        if (
            not manifest.edits
            or loaded_digest != last_recorded_digest
            or normalized_digest != loaded_digest
        ):
            manifest.edits.append(
                PactEdit(
                    id=_new_id("edit"),
                    at=_now(),
                    actor=_actor(),
                    command="normalize direct-yaml",
                    rationale="normalize or audit direct YAML mutation",
                    affected_ids=[],
                    before_digest=last_recorded_digest,
                    after_digest=normalized_digest,
                    validation="accepted",
                )
            )
        save_manifest(repo_root, manifest)
        return manifest


def save_edit_plan(
    repo_root: Path,
    change_id: str,
    *,
    operations: list[dict],
) -> dict:
    manifest = load_manifest(repo_root, change_id)
    plan_id = f"plan.{_stamp()}"
    payload = {
        "id": plan_id,
        "change": change_id,
        "base_digest": state_digest(manifest),
        "operations": operations,
    }
    plan_dir = repo_root / ".pact" / "plans"
    plan_dir.mkdir(parents=True, exist_ok=True)
    plan_path = plan_dir / f"{plan_id}.yaml"
    plan_path.write_text(yaml.safe_dump(payload, sort_keys=False), encoding="utf-8")
    return {
        "plan_id": plan_id,
        "path": str(plan_path),
        "base_digest": payload["base_digest"],
    }


def apply_edit_plan(repo_root: Path, plan_id: str) -> PactManifest:
    plan_path = repo_root / ".pact" / "plans" / f"{plan_id}.yaml"
    if not plan_path.is_file():
        raise SubstrateError("edit plan not found", data={"plan_id": plan_id})
    payload = fastyaml.safe_load(plan_path.read_text(encoding="utf-8")) or {}
    change_id = str(payload.get("change"))
    with manifest_write_lock(repo_root, change_id):
        manifest = load_manifest(repo_root, change_id)
        if state_digest(manifest) != payload.get("base_digest"):
            if any(edit.command == f"edit apply {plan_id}" for edit in manifest.edits):
                return manifest
            operations = payload.get("operations") or []
            if all(
                _edit_plan_operation_is_satisfied(manifest, op) for op in operations
            ):
                return manifest
            # v4 deliberately has no in-file edits ledger. Preserve exact-plan
            # replay idempotence by proving the saved operations are already a
            # no-op against the current semantic state. A genuinely divergent
            # plan remains stale and is rejected below.
            replay = manifest.model_copy(deep=True)
            try:
                for op in payload.get("operations") or []:
                    _apply_operation(replay, op)
            except SubstrateError:
                replay = None
            if replay is not None and state_digest(replay) == state_digest(manifest):
                return manifest
            raise SubstrateError(
                "edit plan is stale",
                data={
                    "plan_id": plan_id,
                    "expected": payload.get("base_digest"),
                    "actual": state_digest(manifest),
                },
            )

        def mutate(target: PactManifest) -> None:
            for op in payload.get("operations") or []:
                _apply_operation(target, op)

        return _apply_loaded_edit(
            repo_root,
            manifest,
            command=f"edit apply {plan_id}",
            affected_ids=[],
            mutate=mutate,
            rationale="edit plan",
        )


def migrate_openspec(repo_root: Path, change_id: str) -> PactManifest:
    source = repo_root / "openspec" / "changes" / change_id
    if not source.is_dir():
        raise SubstrateError(
            "Legacy OpenSpec change not found",
            data={"path": str(source)},
            # PACT layout-history-openspec: the okg repository keeps its own
            # frozen tree at history/openspec/, which this reader does not
            # look in. Say so rather than leave only the missing path.
            fix_hint=(
                "migrate-openspec reads <repo>/openspec/changes/<id>/ only; "
                "the okg repository keeps its frozen OpenSpec tree at "
                "history/openspec/changes/"
            ),
        )
    proposal = _read_optional(source / "proposal.md")
    title = _first_heading(proposal) or change_id.replace("-", " ").title()
    # migrate_openspec creates a fresh PACT manifest from the legacy OpenSpec
    # source, so the change isn't expected to exist yet.
    with manifest_write_lock(repo_root, change_id, must_exist=False):
        manifest = PactManifest(
            change=Change(
                id=change_id,
                title=title.replace("Change:", "").strip(),
                status="proposal",
                root_cause=_section(proposal, "Why"),
                migration_notes=f"Migrated from openspec/changes/{change_id}",
            )
        )
        manifest.requirements.extend(_parse_openspec_requirements(source))
        manifest.tasks.extend(_parse_openspec_tasks(source / "tasks.md"))
        # OpenSpec's tasks.md carries no per-requirement association, so a
        # migrated task would arrive linked to nothing — a manifest the
        # migrating CLI's own validator refuses (`task_requirements_link`,
        # pact-vacuous-gate-close). Link every migrated task to every migrated
        # requirement: the conservative reading, since the change's tasks
        # exist to serve its requirements, and it can only ENLARGE the proof
        # obligation. Narrowing the links is authoring work for whoever picks
        # the migrated change up.
        requirement_ids = [req.id for req in manifest.requirements]
        for task in manifest.tasks:
            if not task.requirements:
                task.requirements = list(requirement_ids)
        save_manifest(repo_root, manifest)
        return manifest


def _apply_operation(manifest: PactManifest, op: dict) -> None:
    kind = op.get("op")
    if kind == "dep_add":
        manifest.dependencies.append(
            TaskDependency(
                **{
                    "from": op["from"],
                    "to": op["to"],
                    "kind": op.get("kind", "blocks"),
                    "rationale": op.get("rationale"),
                }
            )
        )
    elif kind == "task_done":
        _task(manifest, op["task"]).status = "done"
    else:
        raise SubstrateError("unknown edit-plan operation", data={"op": op})


def _edit_plan_operation_is_satisfied(manifest: PactManifest, op: dict) -> bool:
    kind = op.get("op")
    if kind == "dep_add":
        expected_kind = op.get("kind", "blocks")
        expected_rationale = op.get("rationale")
        return any(
            dependency.from_task == op.get("from")
            and dependency.to_task == op.get("to")
            and dependency.kind == expected_kind
            and dependency.rationale == expected_rationale
            for dependency in manifest.dependencies
        )
    if kind == "task_done":
        return any(
            task.id == op.get("task") and task.status == "done"
            for task in manifest.tasks
        )
    return False


def _case_refs(case: ElementTree.Element) -> list[str]:
    refs: list[str] = []
    for prop in case.iter():
        if not prop.tag.endswith("property"):
            continue
        name = prop.attrib.get("name", "")
        value = prop.attrib.get("value", "")
        if name in {
            "pact_requirement",
            "pact_requirements",
            "requirement",
            "requirements",
            "acceptance",
            "acceptance_id",
            "formal_property",
        }:
            refs.extend(v.strip() for v in re.split(r"[, ]+", value) if v.strip())
    return refs


def _file_key(value: str) -> str:
    """Normalize a test file path or a junit classname to one comparable key."""

    key = value.replace("/", ".")
    if key.endswith(".py"):
        key = key[: -len(".py")]
    return key


def _plan_node_target(test_id: str) -> tuple[str, tuple[str, ...]] | None:
    """Split a pytest plan entry into a (file key, node segment) pair.

    `tests/x.py` becomes `("tests.x", ())`; `tests/x.py::test_y` becomes
    `("tests.x", ("test_y",))`; `tests/x.py::TestThing` becomes
    `("tests.x", ("TestThing",))` and `tests/x.py::TestThing::test_y` becomes
    `("tests.x", ("TestThing", "test_y"))`.

    Returns None for anything that is not a pytest node id. Plan entries also
    use `design::`, `graph::`, `browser::`, `ci::` and similar schemes to name
    non-pytest evidence; those can never appear in a junit report, so treating
    them as tests a report failed to run would make their requirements
    permanently unprovable.

    Whether a single `::` segment names a class or a function is not decidable
    from the plan entry alone, and it does not have to be: the segments are
    matched against the junit case's own segments, which carry that structure.
    """

    if ".py::" in test_id:
        path, _, rest = test_id.partition("::")
        # Drop the parameter suffix before splitting: a parametrised id can
        # itself contain `::` inside the brackets, and splitting first would
        # invent segments out of the parameter value.
        rest = rest.split("[", 1)[0]
        segments = tuple(segment for segment in rest.split("::") if segment)
        return _file_key(path), segments
    if test_id.endswith(".py") and "::" not in test_id:
        return _file_key(test_id), ()
    return None


def _named_pytest_target(test_id: str) -> tuple[str, tuple[str, ...]] | None:
    """A plan entry that names something narrower than a whole file.

    File-level entries are a scope, not a promise about one assertion, so they
    are not subject to the must-run check; everything below a file is.
    """

    target = _plan_node_target(test_id)
    if target is None or not target[1]:
        return None
    return target


def _junit_node_segments(
    file_key: str,
    junit_classname: str,
    junit_name: str,
) -> tuple[str, ...] | None:
    """Node segments of a junit case relative to `file_key`.

    junit spells a class-based case as `classname="tests.x.TestThing"`,
    `name="test_y"`, so the case's segments below the file are
    `("TestThing", "test_y")`. Returns None when the case does not belong to
    the file at all.

    Known limit: a junit classname is a dotted string with no marker for where
    the module ends and the class begins, so if a repository ever held BOTH
    `tests/x.py` and a package `tests/x/`, a plan entry naming the file would
    also cover `tests/x/y.py`'s cases. Python cannot import both at one name,
    and this repository has no such pair; a stricter split would need the
    filesystem, which this pure matcher deliberately does not read.
    """

    if not junit_classname:
        return None
    key = _file_key(junit_classname)
    if key == file_key:
        class_path: tuple[str, ...] = ()
    elif key.startswith(f"{file_key}."):
        class_path = tuple(key[len(file_key) + 1 :].split("."))
    else:
        return None
    return (*class_path, junit_name.split("[")[0])


def _plan_entry_covers_junit(
    planned: str,
    junit_classname: str,
    junit_name: str,
) -> bool:
    """Does this plan entry's scope contain this junit case?

    A file entry covers every case in the file, including class-based and
    nested-class ones; a class entry covers every case in that class; a method
    entry covers just that method. Matching is by node segments, so an entry is
    never satisfied by a same-named test in another file or another class.
    """

    target = _plan_node_target(planned)
    if target is None:
        return False
    file_key, segments = target
    actual = _junit_node_segments(file_key, junit_classname, junit_name)
    if actual is None:
        return False
    return actual[: len(segments)] == segments


def _named_test_ran(planned: str, junit_classname: str, junit_name: str) -> bool:
    """Strict identity for deciding a named plan entry actually ran.

    Deliberately stricter than `_test_id_matches_junit`, which accepts a match on
    the bare function name. Same-named test functions in different files are
    routine, and accepting one for the other would let a test that never ran be
    recorded as the proof — the defect this check exists to close.
    """

    if _named_pytest_target(planned) is None:
        return False
    return _plan_entry_covers_junit(planned, junit_classname, junit_name)


def _test_id_matches_junit(planned: str, junit_test_id: str, junit_name: str) -> bool:
    if planned == junit_test_id:
        return True
    # Split off the classname before the parameter suffix: a parametrised id
    # can carry `::` inside its brackets, and `rpartition` would then take the
    # separator from the parameter value.
    junit_classname = junit_test_id.split("[", 1)[0].rpartition("::")[0]
    if _plan_entry_covers_junit(planned, junit_classname, junit_name):
        return True
    if planned.endswith(".py") and "::" not in planned:
        return False
    if planned.endswith(f"::{junit_name}"):
        return True
    # junit spells a parametrised case `test_p[a]`; a plan entry naming the test
    # without its parameters still refers to it.
    base_name = junit_name.split("[")[0]
    if base_name != junit_name and planned.endswith(f"::{base_name}"):
        return True
    normalized_planned = planned.replace("/", ".").replace(".py::", "::")
    return normalized_planned == junit_test_id


def _normalize_evidence_status(raw: str) -> str:
    value = raw.lower()
    if value in {"ok", "passed", "success", "published"}:
        return "pass"
    if value in {"error", "failed", "blocked"}:
        return "fail"
    if value in {"skipped", "blocked_missing_prereq"}:
        return "missing"
    return value


def _parse_openspec_requirements(change_dir_path: Path) -> list[Requirement]:
    out: list[Requirement] = []
    specs_dir = change_dir_path / "specs"
    for spec_path in sorted(specs_dir.glob("*/spec.md")):
        capability = spec_path.parent.name
        text = spec_path.read_text(encoding="utf-8")
        parts = re.split(r"^### Requirement:\s*", text, flags=re.MULTILINE)
        for part in parts[1:]:
            lines = part.splitlines()
            title = lines[0].strip()
            body = "\n".join(lines[1:]).strip()
            req_id = f"req.{slugify(capability)}.{slugify(title)}"
            scenarios = []
            for scenario_title in re.findall(
                r"^#### Scenario:\s*(.+)$", body, re.MULTILINE
            ):
                scenarios.append(
                    Scenario(
                        id=f"scn.{slugify(req_id)}.{slugify(scenario_title)}",
                        title=scenario_title.strip(),
                        when=["migrated legacy OpenSpec scenario runs"],
                        then=["expected behavior is preserved"],
                    )
                )
            if not scenarios:
                scenarios.append(
                    Scenario(
                        id=f"scn.{slugify(req_id)}.migrated",
                        title="migrated scenario",
                        when=["PACT validation runs"],
                        then=["migrated requirement is represented"],
                    )
                )
            out.append(
                Requirement(
                    id=req_id,
                    title=title,
                    statement=_statement_from_body(body),
                    evidence_policy=EvidencePolicy(required=["planned_test"]),
                    scenarios=scenarios,
                    acceptance=[
                        AcceptanceCriterion(
                            id=f"acc.{slugify(req_id)}",
                            description=title,
                            evidence=["planned_test"],
                        )
                    ],
                )
            )
    return out


def _parse_openspec_tasks(path: Path) -> list[Task]:
    if not path.is_file():
        return []
    tasks: list[Task] = []
    current: list[str] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        if re.match(r"^\s*-\s+\[[ xX]\]\s+", line):
            if current:
                tasks.append(_task_from_lines(current))
            current = [line]
        elif current and (line.startswith("      ") or line.startswith("  ")):
            current.append(line.strip())
    if current:
        tasks.append(_task_from_lines(current))
    return tasks


def _task_from_lines(lines: list[str]) -> Task:
    status = "done" if re.match(r"^\s*-\s+\[[xX]\]\s+", lines[0]) else "todo"
    first = re.sub(r"^\s*-\s+\[[ xX]\]\s+", "", lines[0]).strip()
    m = re.match(r"(?P<num>\d+(?:\.\d+)*)\s+(?P<title>.+)", first)
    if m:
        task_id = f"task.{m.group('num')}"
        title = m.group("title")
    else:
        task_id = f"task.{slugify(first)}"
        title = first
    body = " ".join(lines)
    verification = "manual verification required"
    vm = re.search(r"Verification:\s*(.+)", body)
    if vm:
        verification = vm.group(1).strip()
    return Task(
        id=task_id,
        title=title,
        status=status,  # type: ignore[arg-type]
        verification=verification,
    )


def _formal_results(raw: object) -> list[dict]:
    if isinstance(raw, list):
        return [item for item in raw if isinstance(item, dict)]
    if not isinstance(raw, dict):
        raise SubstrateError("formal report must be a mapping or list")
    for key in ("results", "properties", "checks"):
        value = raw.get(key)
        if isinstance(value, list):
            return [item for item in value if isinstance(item, dict)]
    return [raw]


def _formal_report_result_errors(
    repo_root: Path,
    report_path: Path,
    result: dict,
    *,
    result_index: int,
    binding: FormalBinding,
) -> list[dict[str, str]]:
    errors: list[dict[str, str]] = []
    prefix = f"results[{result_index}]"
    status = result.get("status")
    if status is None:
        errors.append(
            _formal_report_error(
                "formal_report_missing_status",
                f"{prefix}.status",
                f"formal report result for {binding.property_id} has no status",
            )
        )
    elif _normalize_formal_report_status(str(status)) != "pass":
        errors.append(
            _formal_report_error(
                "formal_report_nonpassing_status",
                f"{prefix}.status",
                f"formal report result for {binding.property_id} is not passing",
            )
        )
    command = result.get("command")
    if not isinstance(command, str) or not command.strip():
        errors.append(
            _formal_report_error(
                "formal_report_missing_command",
                f"{prefix}.command",
                f"formal report result for {binding.property_id} has no executed command",
            )
        )
    checker_command = result.get("checker_command")
    if binding.checker_command:
        if not isinstance(checker_command, str) or not checker_command.strip():
            errors.append(
                _formal_report_error(
                    "formal_report_missing_checker_command",
                    f"{prefix}.checker_command",
                    f"formal report result for {binding.property_id} has no checker command",
                )
            )
        elif checker_command != binding.checker_command:
            errors.append(
                _formal_report_error(
                    "formal_report_checker_command_mismatch",
                    f"{prefix}.checker_command",
                    f"formal report checker command does not match binding {binding.id}",
                )
            )
    artifact = result.get("artifact")
    if not isinstance(artifact, str) or not artifact.strip():
        errors.append(
            _formal_report_error(
                "formal_report_missing_artifact",
                f"{prefix}.artifact",
                f"formal report result for {binding.property_id} has no checker artifact",
            )
        )
    elif not _formal_report_artifact_path(report_path, artifact).is_file():
        errors.append(
            _formal_report_error(
                "formal_report_missing_artifact_file",
                f"{prefix}.artifact",
                f"formal report artifact does not exist: {artifact}",
            )
        )
    else:
        artifact_path = _formal_report_artifact_path(report_path, artifact)
        if not _path_within(artifact_path, report_path.parent.resolve()):
            errors.append(
                _formal_report_error(
                    "formal_report_artifact_outside_report_dir",
                    f"{prefix}.artifact",
                    "formal report artifact must be in the report directory",
                )
            )
    if "fresh" not in result:
        errors.append(
            _formal_report_error(
                "formal_report_missing_freshness",
                f"{prefix}.fresh",
                f"formal report result for {binding.property_id} has no freshness flag",
            )
        )
    elif result.get("fresh") is not True:
        errors.append(
            _formal_report_error(
                "formal_report_stale",
                f"{prefix}.fresh",
                f"formal report result for {binding.property_id} is not fresh",
            )
        )
    refinement_tests = result.get("refinement_tests")
    if not isinstance(refinement_tests, list) or not all(
        isinstance(ref, str) for ref in refinement_tests
    ):
        errors.append(
            _formal_report_error(
                "formal_report_missing_refinement_tests",
                f"{prefix}.refinement_tests",
                f"formal report result for {binding.property_id} has no refinement test list",
            )
        )
    elif archived_refs := sorted(
        ref for ref in refinement_tests if _is_archived_refinement_ref(ref)
    ):
        errors.append(
            _formal_report_error(
                "formal_report_archived_refinement_test",
                f"{prefix}.refinement_tests",
                "formal report refinement tests must point at active tests, not "
                f"archive-only paths: {', '.join(archived_refs)}",
            )
        )
    elif not set(binding.refinement_tests).issubset(set(refinement_tests)):
        errors.append(
            _formal_report_error(
                "formal_report_incomplete_refinement_tests",
                f"{prefix}.refinement_tests",
                f"formal report result for {binding.property_id} does not cover all binding refinement tests",
            )
        )
    module_digest = _path_digest(repo_root / binding.module)
    if module_digest is None:
        errors.append(
            _formal_report_error(
                "formal_report_missing_module",
                f"{prefix}.module",
                f"formal binding module does not exist: {binding.module}",
            )
        )
    elif not result.get("module_digest"):
        errors.append(
            _formal_report_error(
                "formal_report_missing_module_digest",
                f"{prefix}.module_digest",
                f"formal report result for {binding.property_id} has no module digest",
            )
        )
    elif result.get("module_digest") != module_digest:
        errors.append(
            _formal_report_error(
                "formal_report_module_digest_mismatch",
                f"{prefix}.module_digest",
                f"formal report module digest does not match {binding.module}",
            )
        )
    if binding.config:
        config_digest = _path_digest(repo_root / binding.config)
        if config_digest is None:
            errors.append(
                _formal_report_error(
                    "formal_report_missing_config",
                    f"{prefix}.config",
                    f"formal binding config does not exist: {binding.config}",
                )
            )
        elif not result.get("config_digest"):
            errors.append(
                _formal_report_error(
                    "formal_report_missing_config_digest",
                    f"{prefix}.config_digest",
                    f"formal report result for {binding.property_id} has no config digest",
                )
            )
        elif result.get("config_digest") != config_digest:
            errors.append(
                _formal_report_error(
                    "formal_report_config_digest_mismatch",
                    f"{prefix}.config_digest",
                    f"formal report config digest does not match {binding.config}",
                )
            )
    for script_ref, digest in _formal_checker_script_digests(
        repo_root,
        binding.checker_command,
    ).items():
        if digest is None:
            errors.append(
                _formal_report_error(
                    "formal_report_missing_checker_script",
                    f"{prefix}.checker_command",
                    f"formal checker script does not exist: {script_ref}",
                )
            )
    return errors


def _is_archived_refinement_ref(ref: str) -> bool:
    normalized = ref.replace("\\", "/")
    while normalized.startswith("./"):
        normalized = normalized[2:]
    return normalized == "archive" or normalized.startswith("archive/")


def _normalize_formal_report_status(status: str) -> str:
    normalized = status.strip().lower()
    if normalized in {"ok", "passed", "success"}:
        return "pass"
    return normalized


def _formal_report_artifact_path(report_path: Path, artifact: str) -> Path:
    path = Path(artifact).expanduser()
    if path.is_absolute():
        return path.resolve()
    return (report_path.parent / path).resolve()


def _path_within(path: Path, parent: Path) -> bool:
    try:
        path.resolve().relative_to(parent.resolve())
    except ValueError:
        return False
    return True


def _formal_report_missing_bound_property_errors(
    bindings: Iterable[FormalBinding],
    seen_by_checker: dict[str, set[str]],
) -> list[dict[str, str]]:
    errors: list[dict[str, str]] = []
    bindings_by_checker: dict[str, set[str]] = {}
    for binding in bindings:
        if not binding.checker_command:
            continue
        bindings_by_checker.setdefault(binding.checker_command, set()).add(
            binding.property_id
        )
    for checker_command, seen_properties in seen_by_checker.items():
        missing = sorted(
            bindings_by_checker.get(checker_command, set()) - seen_properties
        )
        for property_id in missing:
            errors.append(
                _formal_report_error(
                    "formal_report_missing_bound_property",
                    "results",
                    f"formal report for checker {checker_command} omitted bound property {property_id}",
                )
            )
    return errors


def _formal_report_error(code: str, path: str, message: str) -> dict[str, str]:
    return {"code": code, "path": path, "message": message}


def _path_digest(path: Path) -> str | None:
    if not path.is_file():
        return None
    return "sha256:" + hashlib.sha256(path.read_bytes()).hexdigest()


def _formal_checker_script_digests(
    repo_root: Path,
    checker_command: str | None,
) -> dict[str, str | None]:
    if not checker_command:
        return {}
    return {
        script_ref: _path_digest(repo_root / script_ref)
        for script_ref in _checker_script_refs(checker_command)
    }


# How many legal ids a refusal prints before it stops. An unbounded `known`
# put every id in the manifest on the wire on every unknown-id refusal; the
# candidates are the actionable part, and the count says what the list omits.
KNOWN_ID_LISTING_CAP = 20


def _unknown_id_error(
    subject: str,
    value: str,
    known: list[str],
    *,
    repo_root: Path | None = None,
    change_id: str | None = None,
    key: str | None = None,
) -> SubstrateError:
    """An unknown-id refusal that lists the near misses from THIS manifest.

    "task not found" with only the offending id cost one `view` call per
    occurrence to learn the legal ids; the candidates are already in hand at
    the raise site (pact-self-repairing-refusals req.refusal.complete).

    ``candidate_source`` travels with the list: a difflib hit is a CORRECTION
    (safe to re-invoke with), a truncated listing is a MENU. Collapsing the two
    is how "did you mean" becomes a guess.
    """
    from okg.substrate.pact import repair as repair_mod

    candidates, source = repair_mod.near_miss_candidates(value, known)
    suffix = f"; did you mean: {', '.join(candidates)}?" if candidates else ""
    prefix = repair_mod.repo_command(repo_root)
    return SubstrateError(
        f"{subject} not found: {value}{suffix}",
        data={
            (key or f"{subject}_id"): value,
            "candidates": candidates,
            "candidate_source": source,
            "known": known[:KNOWN_ID_LISTING_CAP],
            "known_total": len(known),
            **({"change": change_id} if change_id else {}),
        },
        fix_hint=(
            f"{prefix} pact view {shlex.quote(change_id)} --format=next --json"
            if change_id
            else None
        ),
    )


def _task(
    manifest: PactManifest,
    task_id: str,
    *,
    repo_root: Path | None = None,
) -> Task:
    for task in manifest.tasks:
        if task.id == task_id:
            return task
    raise _unknown_id_error(
        "task",
        task_id,
        [task.id for task in manifest.tasks],
        repo_root=repo_root,
        change_id=manifest.change.id,
        key="task_id",
    )


def _requirement(
    manifest: PactManifest,
    requirement_id: str,
    *,
    repo_root: Path | None = None,
) -> Requirement:
    for requirement in manifest.requirements:
        if requirement.id == requirement_id:
            return requirement
    raise _unknown_id_error(
        "requirement",
        requirement_id,
        [req.id for req in manifest.requirements],
        repo_root=repo_root,
        change_id=manifest.change.id,
        key="requirement_id",
    )


def _is_default_scenario(req: Requirement, scenario: Scenario) -> bool:
    return (
        scenario.id == f"scn.{slugify(req.id)}.default"
        and scenario.when == ["okg pact validate runs"]
        and scenario.then == [f"requirement {req.id} is checked"]
    )


def _is_default_acceptance(
    req: Requirement,
    acceptance: AcceptanceCriterion,
) -> bool:
    return (
        acceptance.id == f"acc.{slugify(req.id)}"
        and acceptance.description == req.statement
    )


def _ensure_absent(values: set[str], value: str) -> None:
    if value in values:
        raise SubstrateError("PACT id already exists", data={"id": value})


def _matching_evidence_ids(
    manifest: PactManifest,
    refs: set[str],
) -> list[str]:
    return [
        evidence.id
        for evidence in manifest.evidence
        if evidence.status in {"pass", "waived"} and bool(set(evidence.refs) & refs)
    ]


def _unique(values: list[str]) -> list[str]:
    return list(dict.fromkeys(values))


def _new_id(prefix: str) -> str:
    return f"{prefix}.{_stamp()}"


def _stamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _actor() -> str:
    return os.environ.get("PACT_ACTOR") or os.environ.get("USER") or "agent"


def _read_optional(path: Path) -> str:
    return path.read_text(encoding="utf-8") if path.is_file() else ""


def _first_heading(text: str) -> str | None:
    for line in text.splitlines():
        if line.startswith("# "):
            return line[2:].strip()
    return None


def _section(text: str, heading: str) -> str | None:
    pattern = rf"^## {re.escape(heading)}\s*$"
    match = re.search(pattern, text, re.MULTILINE)
    if not match:
        return None
    rest = text[match.end() :]
    next_heading = re.search(r"^##\s+", rest, re.MULTILINE)
    body = rest[: next_heading.start()] if next_heading else rest
    return body.strip() or None


def _statement_from_body(body: str) -> str:
    lines = []
    for line in body.splitlines():
        if line.startswith("#### Scenario:"):
            break
        if line.strip():
            lines.append(line.strip())
    return " ".join(lines) or "Migrated legacy OpenSpec requirement"


def operations_from_json(raw: str | None) -> list[dict]:
    if not raw:
        return []
    data = json.loads(raw)
    if isinstance(data, dict):
        return [data]
    if isinstance(data, list):
        return data
    raise SubstrateError("edit operations JSON must be object or list")
