"""Fail-closed recovery receipts for incomplete ingest checkpoints.

Source repair runs against the mutable Chronos branch and therefore cannot
hold the branch session fence used by :class:`SourceRunner`.  Recovery instead
records the exact source authority, freezes the repaired branch in a blocked
checkpoint, and promotes only that checkpoint through the publisher's guarded
path.
"""

from __future__ import annotations

import logging
from collections.abc import Callable, Mapping, Sequence
from pathlib import Path
from typing import Any

from okg.substrate.errors import SubstrateError

log = logging.getLogger(__name__)


BLOCKED_INGEST_REASON = "ingest_source_incomplete"
RECOVERY_SCHEMA = "okg.ingest_incomplete_recovery.v1"
REPAIR_REQUIRED = "repair_required"
FROZEN = "frozen"
#: An operator has retired this checkpoint's recovery authority with a
#: recorded reason. The checkpoint itself stays `blocked` — nothing about it
#: is rewritten as published — but it no longer demands an exact repair
#: contract, so the deployment's other workflows can run again.
SUPERSEDED = "superseded"

#: How one source of a recovery contract stands against today's registry.
CLASS_REMOVED = "removed"
CLASS_RENAMED = "renamed"
CLASS_REBOUND = "rebound"
CLASS_PRESENT = "present"
#: A source in one of these classes can never be repaired by a retry.
RETIRABLE_CLASSES = frozenset({CLASS_REMOVED, CLASS_RENAMED})
#: The contract fields that make a registered source ``rebound`` when they
#: differ from today's registry entry. ``run_mode`` is not one: it is a
#: choice of the run (poll or event scope against a full run), not registry
#: state, so a retry with the recorded mode can succeed and that difference
#: keeps the ordinary mismatch refusal.
REBOUND_FIELDS = (
    "catalog_fingerprint",
    "binding_fingerprint",
    "admission_contract",
)
#: The reason and actors recorded when a contract is retired because its
#: required sources are gone (PACT recovery-contract-removed-sources).
CONTRACT_RETIRED_REASON = "contract_sources_removed"
MIGRATE_ACTOR = "okg migrate"
WORKER_ACTOR = "okg runtime worker"


def staged_source_redrive_state(
    *,
    dsn: str,
    deployment: str,
) -> dict[str, Any]:
    """Atomically observe staged source work and its checkpoint predecessor."""
    from okg.substrate.db.pool import GENERAL, pooled_connection
    from okg.substrate.ingest.run_base import EXCLUDE_RUN_BASE_SQL
    from okg.substrate.publisher import Publisher

    with pooled_connection(dsn, purpose=GENERAL) as conn, conn.transaction():
        with conn.cursor() as cur:
            cur.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY")
            # `latest` filters on the deployment only, not the generation
            # kind, so it leaves out run-base checkpoints explicitly: one is
            # cut at every source run's start and is never a generation
            # (issue #2993).
            cur.execute(
                f"""
            WITH staged AS (
              SELECT EXISTS (
                SELECT 1
                  FROM okg.admission_envelopes e
                 WHERE e.deployment_name = %(deployment)s
                   AND e.producer_kind = 'source'
                   AND e.status IN ('pending', 'accepted')
                   AND COALESCE(e.target_branch_name, 'default') = 'default'
                   AND (
                     e.status = 'accepted'
                     OR e.admission_mode IN ('direct', 'fast_track')
                   )
                   AND EXISTS (
                     SELECT 1
                       FROM okg.admission_record_refs r
                      WHERE r.admission_id = e.admission_id
                        AND r.ref_kind IN (
                          'node_fact_range', 'edge_fact_range', 'payload'
                        )
                        AND (
                          r.ref_kind != 'payload'
                          OR r.payload->>'kind' = 'chronos_graph_run'
                        )
                   )
              ) AS present
            ), latest AS (
              SELECT checkpoint_id, metadata::jsonb AS metadata
                FROM public._chronos_branch_interval_checkpoints
               WHERE metadata::jsonb->>'deployment_name' = %(deployment)s
                 AND COALESCE(metadata::jsonb->>'branch', 'default') = 'default'
                 AND {EXCLUDE_RUN_BASE_SQL}
               ORDER BY created_at DESC, checkpoint_id DESC
               LIMIT 1
            ), latest_published AS (
              SELECT checkpoint_id
                FROM public._chronos_branch_interval_checkpoints
               WHERE metadata::jsonb->>'deployment_name' = %(deployment)s
                 AND COALESCE(metadata::jsonb->>'branch', 'default') = 'default'
                 AND metadata::jsonb->>'status' = 'published'
               ORDER BY created_at DESC, checkpoint_id DESC
               LIMIT 1
            )
            SELECT staged.present,
                   latest.checkpoint_id,
                   latest.metadata,
                   latest_published.checkpoint_id
              FROM staged
              LEFT JOIN latest ON TRUE
              LEFT JOIN latest_published ON TRUE
                """,
                {"deployment": deployment},
            )
            row = cur.fetchone()
        try:
            branch_mutation_token = Publisher.chronos_graph_branch_mutation_token(
                conn,
                branch_name="default",
            )
        except SubstrateError as exc:
            if exc.data.get("reason") != "branch_missing":
                raise
            branch_mutation_token = None
    metadata = dict(row[2] or {})
    blocked_reason = metadata.get("blocked_reason") or {}
    return {
        "staged_work_present": bool(row[0]),
        "latest_checkpoint_id": (str(row[1]) if row[1] is not None else None),
        "latest_checkpoint_status": metadata.get("status"),
        "latest_blocked_reason": (
            blocked_reason.get("reason")
            if isinstance(blocked_reason, Mapping)
            else None
        ),
        "latest_blocked_receipt": (
            dict(blocked_reason) if isinstance(blocked_reason, Mapping) else {}
        ),
        "latest_published_checkpoint_id": (str(row[3]) if row[3] is not None else None),
        "branch_mutation_token": branch_mutation_token,
    }


def build_source_contract(
    *,
    ordered: Sequence[str],
    runnable: Mapping[str, Any],
    deps: Mapping[str, set[str]],
    source_events: Mapping[str, Sequence[Any]],
    source_binding_plans: Mapping[str, Any],
    run_mode_for_source: Callable[
        [Any, str, tuple[Any, ...], Mapping[str, Any] | None],
        str,
    ],
) -> list[dict[str, Any]]:
    """Describe the exact selected source authority before any source runs."""
    from okg.substrate.admission import stable_idempotency_key
    from okg.substrate.ingest.adapter_factory import (
        source_adapter_binding_fingerprint,
        source_adapter_graph_branch,
    )
    from okg.substrate.source_sync import source_sync_scope_payload

    contract: list[dict[str, Any]] = []
    for name in ordered:
        entry = runnable[name]
        events = tuple(source_events.get(name, ()))
        sync_scope = source_sync_scope_payload(name, events)
        binding = source_binding_plans.get(name)
        fingerprint = (
            binding.registry_binding_fingerprint
            if binding is not None
            else source_adapter_binding_fingerprint(entry)
        )
        contract.append(
            {
                "source": name,
                "ownership_id": entry.ownership_id,
                "admission_contract": entry.admission_contract,
                "catalog_fingerprint": stable_idempotency_key(entry.raw),
                "binding_fingerprint": fingerprint,
                "dependencies": sorted(deps.get(name, set())),
                "graph_branch": source_adapter_graph_branch(entry),
                "run_mode": run_mode_for_source(
                    entry,
                    name,
                    events,
                    sync_scope,
                ),
                "sync_scope": sync_scope or {},
            }
        )
    return contract


def recovery_admission_ids(
    *,
    dsn: str,
    deployment: str,
    predecessor_checkpoint_id: str,
) -> list[str]:
    """Read the exact staged admissions eligible for recovery."""
    from okg.substrate.admission import recovery_candidate_admission_ids
    from okg.substrate.db.pool import GENERAL, pooled_connection

    with pooled_connection(dsn, purpose=GENERAL) as conn, conn.transaction():
        return recovery_candidate_admission_ids(
            conn,
            deployment_name=deployment,
            branch_name="default",
            predecessor_generation_id=predecessor_checkpoint_id,
        )


def ensure_frozen_feedback(
    *,
    dsn: str,
    deployment: str,
    checkpoint_id: str,
    recovery: Mapping[str, Any],
) -> dict[str, Any]:
    """Finish exact freeze feedback after a crash between its two writes.

    The publisher and branch locks of the crashed publish are gone here, so
    when the checkpoint's receipt carries the locked admission set, any
    non-source admission made since (by any writer) is tolerated, left
    accepted and unbound, and listed under ``tolerated_unbound_admissions``
    in the returned feedback. Source admissions still need exact equality.
    """
    from okg.substrate.admission import (
        apply_generation_feedback,
        workflow_ref_from_dbos,
    )
    from okg.substrate.db.pool import GENERAL, pooled_connection
    from okg.substrate.runtime.source_sync_workflow import (
        SOURCE_SYNC_WORKFLOW_NAME,
    )

    with pooled_connection(dsn, purpose=GENERAL) as conn:
        return apply_generation_feedback(
            conn,
            deployment_name=deployment,
            generation_id=checkpoint_id,
            expected_admission_ids=recovery.get("admission_ids") or (),
            recovery_from_generation_id=recovery["predecessor_checkpoint_id"],
            workflow_ref=workflow_ref_from_dbos(SOURCE_SYNC_WORKFLOW_NAME),
        )


def promotion_publish_kwargs(
    *,
    redrive_state: Mapping[str, Any],
    recovery: Mapping[str, Any],
    deployment: str,
) -> dict[str, Any]:
    """Build the guarded publisher arguments for one frozen checkpoint."""
    checkpoint_id = str(redrive_state["latest_checkpoint_id"])
    mutation_token = redrive_state.get("branch_mutation_token")
    if not mutation_token:
        raise SubstrateError(
            "cannot prove the frozen recovery branch mutation token",
            data={
                "reason": "incomplete_recovery_branch_token_missing",
                "deployment": deployment,
                "latest_checkpoint_id": checkpoint_id,
            },
        )
    return {
        "expected_latest_checkpoint_id": checkpoint_id,
        "expected_branch_mutation_token": str(mutation_token),
        "require_expected_latest_checkpoint": True,
        "expected_admission_ids": recovery.get("admission_ids") or (),
        "recovery_from_generation_id": checkpoint_id,
        "admission_feedback_expected_status": "published",
    }


def operator_fix_hint(deployment: str) -> str:
    """Return the explicit remedy for receipts too old to recover safely."""
    return (
        "inspect and repair the incomplete sources, then explicitly supersede "
        "the legacy checkpoint with: okg run --once --inline "
        f"--force-enrichers --deployment {deployment} --apply"
    )


def recovery_from_redrive_state(
    redrive_state: Mapping[str, Any],
    *,
    deployment: str,
) -> dict[str, Any] | None:
    """Parse the latest incomplete receipt or refuse a legacy receipt.

    The latest checkpoint is the only recovery authority.  A source-name-only
    v1 completeness receipt cannot prove run mode, observed scope, or adapter
    binding and therefore must never authorize automatic source execution.
    """
    if (
        redrive_state.get("latest_checkpoint_status") != "blocked"
        or redrive_state.get("latest_blocked_reason") != BLOCKED_INGEST_REASON
    ):
        return None
    receipt = redrive_state.get("latest_blocked_receipt")
    recovery = receipt.get("recovery") if isinstance(receipt, Mapping) else None
    if (
        isinstance(recovery, Mapping)
        and recovery.get("schema") == RECOVERY_SCHEMA
        and recovery.get("state") == SUPERSEDED
    ):
        # Retired by an operator. Not an authority any more, and not a legacy
        # receipt either — the distinction matters, because the legacy branch
        # below refuses the whole run.
        return None
    if (
        not isinstance(recovery, Mapping)
        or recovery.get("schema") != RECOVERY_SCHEMA
        or recovery.get("state") not in {REPAIR_REQUIRED, FROZEN}
        or not isinstance(recovery.get("source_contract"), list)
        or not isinstance(recovery.get("admission_ids"), list)
        or not recovery.get("predecessor_checkpoint_id")
    ):
        raise SubstrateError(
            "legacy incomplete checkpoint lacks exact recovery authority",
            data={
                "reason": "incomplete_recovery_authority_missing",
                "deployment": deployment,
                "latest_checkpoint_id": redrive_state.get("latest_checkpoint_id"),
                "blocked_reason": BLOCKED_INGEST_REASON,
            },
            fix_hint=operator_fix_hint(deployment),
        )
    return dict(recovery)


def _with_recorded_whole_scope_request(
    want: Mapping[str, Any],
    got: Mapping[str, Any],
    source_profiles: Mapping[str, str | None],
) -> dict[str, Any]:
    """``got`` with its run mode put back to a recorded whole-scope request.

    Only when ``want`` recorded the generic request and ``got`` holds exactly
    what that request resolves to for the source's profile today.
    """
    from okg.substrate.source_run_modes import (
        WHOLE_SCOPE_REQUEST,
        resolve_whole_scope_request,
    )

    got = dict(got)
    if (
        isinstance(want, Mapping)
        and want.get("source") == got.get("source")
        and want.get("run_mode") == WHOLE_SCOPE_REQUEST
        and got.get("run_mode") != WHOLE_SCOPE_REQUEST
        and got.get("run_mode") == resolve_whole_scope_request(
            source_profiles.get(str(got.get("source"))), WHOLE_SCOPE_REQUEST,
        )
    ):
        got["run_mode"] = WHOLE_SCOPE_REQUEST
    return got


def require_exact_repair_contract(
    *,
    expected: Mapping[str, Any],
    actual_source_contract: Sequence[Mapping[str, Any]],
    deployment: str,
    checkpoint_id: str,
    source_profiles: Mapping[str, str | None] | None = None,
    registry: Mapping[str, Any] | None = None,
    active_producer_ids: Callable[[Sequence[str]], set[str]] | None = None,
) -> None:
    """Refuse repair before source execution unless authority is identical.

    One difference is not a change of authority: a checkpoint recorded
    before whole-scope requests were resolved per profile holds
    `scope_complete` for a source whose profile now runs that request in
    its own whole-scope mode (`reconcile`, `release_new`, `append`).
    ``source_profiles`` names each source's declared profile so that
    recorded request matches the resolved mode; everything else must still
    be identical.

    The refusal names both ways out: re-run with the recorded selection, or
    retire the checkpoint's repair requirement with
    `okg runtime supersede-checkpoint` (see
    :func:`supersede_incomplete_checkpoint`) when a source cannot be made to
    pass. Without the second, one unfixable source wedges every workflow.

    ``registry`` is the source registry the caller resolved (name to entry).
    Given it, a contract that names a source the registry no longer holds
    (removed or renamed), or holds with a changed binding (rebound), can
    never be matched by a retry. That is refused
    ``incomplete_recovery_contract_unsatisfiable`` and says what changed,
    instead of the mismatch refusal that invites a retry.
    """
    from okg.substrate.publish_recovery import _supersede_command

    expected_contract = list(expected.get("source_contract") or ())
    actual_contract = [dict(item) for item in actual_source_contract]
    if expected_contract == actual_contract:
        return
    if (
        source_profiles
        and len(expected_contract) == len(actual_contract)
        and expected_contract == [
            _with_recorded_whole_scope_request(want, got, source_profiles)
            for want, got in zip(expected_contract, actual_contract)
        ]
    ):
        return
    expected_sources = _contract_sources(expected_contract)
    actual_sources = _contract_sources(actual_contract)
    if registry is not None:
        classes = classify_contract_sources(
            contract=expected_contract,
            registry=registry,
            actual_contract=actual_contract,
            active_producer_ids=_safe_active_ids(
                active_producer_ids, expected_contract,
            ),
        )
        if any(item["class"] != CLASS_PRESENT for item in classes):
            raise _unsatisfiable_refusal(
                expected=expected,
                classes=classes,
                actual_sources=actual_sources,
                deployment=deployment,
                checkpoint_id=checkpoint_id,
            )
    if expected_sources != actual_sources:
        what_changed = (
            f"the blocked checkpoint {checkpoint_id} requires the next run to "
            f"repeat exactly the sources {_name_list(expected_sources)}, but "
            f"this run selected {_name_list(actual_sources)}"
        )
        changed_sources = sorted(
            set(expected_sources).symmetric_difference(actual_sources)
        )
    else:
        changed_sources = sorted(
            {
                str(want.get("source"))
                for want, got in zip(expected_contract, actual_contract)
                if want != got
            }
        )
        what_changed = (
            f"this run selected the same sources as the blocked checkpoint "
            f"{checkpoint_id}, but the run mode, event scope, or registry "
            f"binding recorded for {_name_list(changed_sources)} is different"
        )
    # The dry run, not the write: the reason in the command is a placeholder,
    # and an applied supersede cannot be undone.
    escape_hatch = _supersede_command(deployment, apply=False)
    raise SubstrateError(
        "incomplete checkpoint recovery scope or binding changed: "
        + what_changed,
        data={
            "reason": "incomplete_recovery_contract_mismatch",
            "deployment": deployment,
            "latest_checkpoint_id": checkpoint_id,
            "expected_source_contract": expected_contract,
            "actual_source_contract": actual_contract,
            "expected_sources": expected_sources,
            "actual_sources": actual_sources,
            "changed_sources": changed_sources,
            "escape_hatch_command": escape_hatch,
        },
        fix_hint=(
            "retry with the exact source selection, event scope, run mode, "
            "and registry bindings recorded by the blocked checkpoint "
            f"(sources: {_name_list(expected_sources)}). If one of those "
            "sources cannot be made to pass, retire the checkpoint's repair "
            f"requirement instead. First run the dry run: {escape_hatch}. "
            "It changes nothing and shows which repair requirement it would "
            "retire. Then run the same command again with --apply, and with "
            "--reason replaced by what was actually done about those "
            "sources; the reason is recorded, and an applied supersede "
            "cannot be undone. Superseding deletes nothing and does not "
            "count the unfinished sources as ingested: the checkpoint stays "
            "blocked and is never published, and the next ordinary run "
            "resumes each source from its own saved progress. What the "
            "sources that did finish in the stranded run already wrote "
            "becomes readable with the next publish; their admission records "
            "stay marked blocked on that checkpoint. What is given up is the "
            "guarantee that the unfinished sources finish before anything "
            "else publishes"
        ),
    )


def _safe_active_ids(
    lookup: Callable[[Sequence[str]], set[str]] | None,
    contract: Sequence[Mapping[str, Any]],
) -> set[str] | None:
    """Ownership ids with an active producer profile, or None if unread."""
    if lookup is None:
        return None
    ids = sorted({
        str(item["ownership_id"]) for item in contract if item.get("ownership_id")
    })
    try:
        return set(lookup(ids))
    except Exception as exc:  # noqa: BLE001 - evidence only; never a verdict
        log.warning(
            "could not read okg.producer_profiles for the recovery contract "
            "classification: %s: %s", type(exc).__name__, exc,
        )
        return None


def active_producer_ids(
    *,
    dsn: str,
    deployment: str,
    ids: Sequence[str],
    connect_timeout: int = 10,
) -> set[str]:
    """The given ids that hold an active ``okg.producer_profiles`` row."""
    from okg.substrate.db.connection import connect as db_connect

    if not ids:
        return set()
    with db_connect(dsn, connect_timeout=connect_timeout, autocommit=True) as conn:
        with conn.cursor() as cur:
            return _active_producer_ids_on(cur, deployment, ids)


def _active_producer_ids_on(cur: Any, deployment: str, ids: Sequence[str]) -> set[str]:
    cur.execute(
        """
        SELECT DISTINCT producer_id
          FROM okg.producer_profiles
         WHERE deployment_name = %s
           AND status = 'active'
           AND producer_id = ANY(%s)
        """,
        (deployment, list(ids)),
    )
    return {str(producer_id) for (producer_id,) in cur.fetchall()}


def _registry_facts(entry: Any) -> dict[str, Any]:
    """What a registry entry alone says about a contract source."""
    from okg.substrate.admission import stable_idempotency_key

    return {
        "admission_contract": entry.admission_contract,
        "catalog_fingerprint": stable_idempotency_key(entry.raw),
    }


def classify_contract_sources(
    *,
    contract: Sequence[Mapping[str, Any]],
    registry: Mapping[str, Any],
    actual_contract: Sequence[Mapping[str, Any]] = (),
    active_producer_ids: set[str] | None = None,
) -> list[dict[str, Any]]:
    """Put each source of a recovery contract in one class (issue: removed sources).

    ``registry`` is the source registry the caller resolved (name to entry);
    okg stores none in the database. ``actual_contract`` is the contract the
    current run would record (its selected sources), which carries the
    binding fingerprint only a run can resolve; a source the run did not
    select is compared on the two fields the registry alone gives
    (``catalog_fingerprint`` and ``admission_contract``). The run mode is
    never compared: it is the run's choice, not registry state.
    ``active_producer_ids`` are the ownership ids that hold an active
    ``okg.producer_profiles`` row; each row records whether its id does, as
    evidence for the reader (``None`` when it was not read).

    Classes: ``removed`` (the name is gone, and its ownership id is held by
    no current source or by more than one), ``renamed`` (the name is gone and
    exactly one current source, not itself in the contract, holds the
    ownership id), ``rebound`` (the name is present but its registry entry
    or binding changed; ``changed_fields`` names each field), and
    ``present``.
    """
    contract_names = {str(item.get("source")) for item in contract}
    holders: dict[str, list[str]] = {}
    for name, entry in registry.items():
        holders.setdefault(str(entry.ownership_id), []).append(str(name))
    actual = {str(item.get("source")): dict(item) for item in actual_contract}
    rows: list[dict[str, Any]] = []
    for want in contract:
        name = str(want.get("source"))
        ownership_id = want.get("ownership_id")
        row: dict[str, Any] = {"source": name, "ownership_id": ownership_id}
        if active_producer_ids is not None:
            row["active_producer_profile"] = (
                str(ownership_id) in active_producer_ids if ownership_id else False
            )
        if name not in registry:
            held = holders.get(str(ownership_id), []) if ownership_id else []
            if len(held) == 1 and held[0] not in contract_names:
                row.update({"class": CLASS_RENAMED, "renamed_to": held[0]})
            else:
                row["class"] = CLASS_REMOVED
            rows.append(row)
            continue
        if name in actual:
            current = dict(actual[name])
        else:
            current = _registry_facts(registry[name])
        changed = [
            field for field in REBOUND_FIELDS
            if field in want and field in current and want[field] != current[field]
        ]
        if changed:
            row.update({"class": CLASS_REBOUND, "changed_fields": changed})
        else:
            row["class"] = CLASS_PRESENT
        rows.append(row)
    return rows


def retirement_class_condition(
    classes: Sequence[Mapping[str, Any]],
    required_sources: Sequence[str],
) -> tuple[bool, str | None]:
    """Whether every required source is removed or renamed (R1, condition 1).

    Returns ``(ok, reason)``; ``reason`` is ``no_required_sources``,
    ``required_source_rebound`` or ``required_source_present`` when not ok.
    A contract with no required source is never retired by class: nothing
    says which source it was holding.
    """
    required = [str(name) for name in required_sources]
    if not required:
        return False, "no_required_sources"
    by_name = {str(item["source"]): item for item in classes}
    kinds = {
        by_name[name]["class"] if name in by_name else CLASS_PRESENT
        for name in required
    }
    if kinds <= RETIRABLE_CLASSES:
        return True, None
    if CLASS_PRESENT in kinds:
        return False, "required_source_present"
    return False, "required_source_rebound"


def _describe_class(item: Mapping[str, Any]) -> str:
    name = item["source"]
    if item["class"] == CLASS_REMOVED:
        return f"{name} was removed from the source registry"
    if item["class"] == CLASS_RENAMED:
        return (
            f"{name} was renamed to {item.get('renamed_to')} "
            "(same ownership id)"
        )
    if item["class"] == CLASS_REBOUND:
        return (
            f"{name} is still registered but its "
            f"{', '.join(item.get('changed_fields') or ())} changed"
        )
    return f"{name} is unchanged"


def _migrate_command(deployment: str) -> str:
    from okg.substrate.support.user_hints import okg_prefix

    return f"{okg_prefix()} migrate --deployment {deployment} --apply"


def _unsatisfiable_refusal(
    *,
    expected: Mapping[str, Any],
    classes: Sequence[Mapping[str, Any]],
    actual_sources: Sequence[str],
    deployment: str,
    checkpoint_id: str,
) -> SubstrateError:
    from okg.substrate.publish_recovery import _supersede_command

    expected_contract = list(expected.get("source_contract") or ())
    expected_sources = _contract_sources(expected_contract)
    broken = [item for item in classes if item["class"] != CLASS_PRESENT]
    required = list(expected.get("required_sources") or ())
    retirable, _why = retirement_class_condition(classes, required)
    escape_hatch = _supersede_command(deployment, apply=False)
    migrate_command = _migrate_command(deployment)
    changes = "; ".join(_describe_class(item) for item in broken)
    hint = (
        "retrying with the same selection cannot succeed: the blocked "
        f"checkpoint {checkpoint_id} requires sources that no longer match "
        "the registry. "
    )
    if retirable:
        hint += (
            "Every source it is holding back is gone, so its repair "
            "requirement can be retired automatically: restart "
            f"`okg runtime worker`, or run {migrate_command}. Both retire it "
            "only when the deployment and okg repositories are committed, the checkpoint "
            "belongs to this deployment, and no unfinished undo or kept "
            "rerun blocks publishing; otherwise they report which. "
        )
    hint += (
        "To retire it yourself, run the dry run first: "
        f"{escape_hatch}. It changes nothing and shows which repair "
        "requirement it would retire. Then run the same command again with "
        "--apply, and with --reason replaced by what was actually done; the "
        "reason is recorded, and an applied supersede cannot be undone. "
        "Superseding deletes nothing: the checkpoint stays blocked and is "
        "never published"
    )
    return SubstrateError(
        "incomplete checkpoint recovery can never match the current source "
        f"registry: the blocked checkpoint {checkpoint_id} requires "
        f"{_name_list(expected_sources)}, but {changes}",
        data={
            "reason": "incomplete_recovery_contract_unsatisfiable",
            "deployment": deployment,
            "latest_checkpoint_id": checkpoint_id,
            "source_classes": [dict(item) for item in classes],
            "unsatisfiable_sources": [item["source"] for item in broken],
            "required_sources": required,
            "expected_sources": expected_sources,
            "actual_sources": list(actual_sources),
            "automatic_retirement_applies": retirable,
            "escape_hatch_command": escape_hatch,
            **({"migrate_command": migrate_command} if retirable else {}),
        },
        fix_hint=hint,
    )


def source_sync_hold(
    *,
    dsn: str,
    deployment: str,
    sources: Mapping[str, str],
    connect_timeout: int = 10,
    registry: Mapping[str, Any] | None = None,
) -> dict[str, Any] | None:
    """What a `repair_required` checkpoint is holding back, or None (#3342).

    ``registry`` (name to entry, the caller's resolved source registry) adds
    ``source_classes`` -- each contract source as removed, renamed, rebound
    or present -- and ``automatic_retirement_applies``: whether every source
    the checkpoint holds back is gone, so ``okg migrate --deployment`` or a
    worker start retires it (subject to the checks they run). No ingest runs.

    While the latest checkpoint is blocked with a repair requirement,
    `run_ingest` refuses every publishing source-sync run that does not
    repeat the recorded contract (selection, run mode, scope, binding),
    before any source starts, so no admission envelope records the refusal.
    Under `okg runtime worker` each source's poll is its own run, so every
    other source stops updating. This reads that state live and writes
    nothing: the checkpoint, the selection it demands, the failed sources,
    and which registry sources (`sources`: registry name -> ownership id)
    have been admitted since the block.

    It reads only the latest checkpoint's metadata and the admissions after
    it -- not the branch mutation token the publish path computes, which
    hashes the whole mutable segment -- because `okg status --watch` calls it
    every few seconds. A legacy receipt with no recovery authority refuses
    every run; it is reported with `legacy_receipt: true` and its own fix.
    """
    from datetime import datetime

    from okg.substrate.db.connection import connect as db_connect
    from okg.substrate.ingest.run_base import EXCLUDE_RUN_BASE_SQL
    from okg.substrate.publish_recovery import _supersede_command

    with db_connect(dsn, connect_timeout=connect_timeout, autocommit=True) as conn:
        with conn.cursor() as cur:
            # The same `latest` the refusal path reads
            # (`staged_source_redrive_state`).
            cur.execute(
                f"""
                SELECT checkpoint_id, metadata::jsonb, created_at
                  FROM public._chronos_branch_interval_checkpoints
                 WHERE metadata::jsonb->>'deployment_name' = %(deployment)s
                   AND COALESCE(metadata::jsonb->>'branch', 'default') = 'default'
                   AND {EXCLUDE_RUN_BASE_SQL}
                 ORDER BY created_at DESC, checkpoint_id DESC
                 LIMIT 1
                """,
                {"deployment": deployment},
            )
            row = cur.fetchone()
            if row is None:
                return None
            checkpoint_id, metadata, created_at = row
            metadata = dict(metadata or {})
            blocked_reason = metadata.get("blocked_reason") or {}
            state = {
                "latest_checkpoint_id": str(checkpoint_id),
                "latest_checkpoint_status": metadata.get("status"),
                "latest_blocked_reason": (
                    blocked_reason.get("reason")
                    if isinstance(blocked_reason, Mapping) else None
                ),
                "latest_blocked_receipt": (
                    dict(blocked_reason) if isinstance(blocked_reason, Mapping) else {}
                ),
            }
            legacy = False
            try:
                recovery = recovery_from_redrive_state(state, deployment=deployment)
            except SubstrateError as exc:
                if exc.data.get("reason") != "incomplete_recovery_authority_missing":
                    raise
                recovery, legacy = None, True
            if not legacy and (
                recovery is None or recovery.get("state") != REPAIR_REQUIRED
            ):
                return None
            # Chronos stores checkpoint `created_at` as ISO text.
            blocked_at = (
                datetime.fromisoformat(created_at)
                if isinstance(created_at, str) else created_at
            )
            admitted: set[str] = set()
            if blocked_at is not None:
                cur.execute(
                    """
                    SELECT DISTINCT source
                      FROM okg.admission_envelopes
                     WHERE deployment_name = %s
                       AND producer_kind = 'source'
                       AND status IN ('accepted', 'published', 'blocked')
                       AND created_at > %s
                       AND source = ANY(%s)
                    """,
                    (deployment, blocked_at, list(sources.values())),
                )
                admitted = {str(source) for (source,) in cur.fetchall()}
    contract_sources = (
        _contract_sources(recovery.get("source_contract") or ()) if recovery else []
    )
    since = {name: sources[name] in admitted for name in sorted(sources)}
    classification: dict[str, Any] = {}
    if registry is not None and recovery:
        contract = list(recovery.get("source_contract") or ())
        classes = classify_contract_sources(
            contract=contract,
            registry=registry,
            active_producer_ids=_safe_active_ids(
                lambda ids: active_producer_ids(
                    dsn=dsn,
                    deployment=deployment,
                    ids=ids,
                    connect_timeout=connect_timeout,
                ),
                contract,
            ),
        )
        applies, _why = retirement_class_condition(
            classes, recovery.get("required_sources") or (),
        )
        classification = {
            "source_classes": classes,
            "automatic_retirement_applies": applies,
        }
    return {
        **classification,
        "deployment": deployment,
        "checkpoint_id": str(checkpoint_id),
        "blocked_at": blocked_at.isoformat() if blocked_at is not None else None,
        "legacy_receipt": legacy,
        "required_selection": contract_sources,
        "failed_sources": sorted((recovery or {}).get("required_sources") or ()),
        "sources": [
            {"source": name, "admitted_since_block": since[name]}
            for name in sorted(sources)
        ],
        # Every source counts, the repair selection's own included: a poll
        # that selects a subset of it, or another scope, is refused too, and
        # a repair run that fails again cuts a newer checkpoint.
        "not_admitted_since_block": [
            name for name in sorted(sources) if not since[name]
        ],
        "escape_hatch_command": (
            operator_fix_hint(deployment)
            if legacy else _supersede_command(deployment, apply=False)
        ),
    }


def source_sync_hold_line(hold: Mapping[str, Any]) -> str:
    """One operator sentence for a `source_sync_hold` report."""
    if hold.get("legacy_receipt"):
        refused = "every publishing source-sync run is refused"
        way_out = hold["escape_hatch_command"]
    else:
        refused = (
            "every publishing source-sync run that does not repeat exactly "
            f"{_name_list(hold['required_selection'])} (same run mode and "
            "scope) is refused"
        )
        way_out = (
            "or retire the requirement (dry run first): "
            + hold["escape_hatch_command"]
        )
    stalled = hold.get("not_admitted_since_block") or []
    since = f" (since {hold['blocked_at']})" if hold.get("blocked_at") else ""
    changed = [
        item for item in hold.get("source_classes") or ()
        if item.get("class") != CLASS_PRESENT
    ]
    what_changed = ""
    if changed:
        what_changed = (
            " A retry cannot succeed: "
            + "; ".join(_describe_class(item) for item in changed)
            + "."
        )
        if hold.get("automatic_retirement_applies"):
            what_changed += (
                " Every source it holds back is gone, so restarting `okg "
                "runtime worker` or running "
                + _migrate_command(str(hold.get("deployment") or "<deployment>"))
                + " retires it (when the repositories are committed and no "
                "unfinished undo blocks publishing). A worker that started "
                "before the source was removed or renamed does not see the "
                "change: restart it."
            )
    line = (
        f"source sync is held by blocked checkpoint {hold['checkpoint_id']}"
        f"{since}: {refused} before it starts, and records no admission"
        + (
            f"; {_name_list(stalled)} {'has' if len(stalled) == 1 else 'have'} "
            "not been admitted since"
            if stalled else ""
        )
        + (
            f". Repair {_name_list(hold['failed_sources'])}, "
            if hold.get("failed_sources") else ". "
        )
        + way_out
    )
    return line + what_changed


def _contract_sources(contract: Sequence[Mapping[str, Any]]) -> list[str]:
    return [str(item.get("source")) for item in contract]


def _name_list(names: Sequence[str]) -> str:
    return ", ".join(names) if names else "(none)"


def blocker_receipt(
    *,
    completeness: Mapping[str, Any],
    state: str,
    predecessor_checkpoint_id: str,
    source_contract: Sequence[Mapping[str, Any]],
    admission_ids: Sequence[str],
) -> dict[str, Any]:
    """Build the blocker stamped into one exact frozen checkpoint."""
    if state not in {REPAIR_REQUIRED, FROZEN}:
        raise ValueError(f"unknown incomplete recovery state {state!r}")
    receipt = dict(completeness)
    receipt["status"] = "incomplete"
    receipt["reason"] = BLOCKED_INGEST_REASON
    required = {
        str(item["source"])
        for key in ("failed_sources", "dependency_blocked_sources")
        for item in completeness.get(key) or ()
        if isinstance(item, Mapping) and item.get("source")
    }
    receipt["recovery"] = {
        "schema": RECOVERY_SCHEMA,
        "state": state,
        "predecessor_checkpoint_id": predecessor_checkpoint_id,
        "source_contract": [dict(item) for item in source_contract],
        "required_sources": sorted(required),
        "admission_ids": sorted({str(item) for item in admission_ids}),
    }
    return receipt


def redrive_probe_report(
    redrive_state: Mapping[str, Any],
) -> dict[str, Any]:
    """Render the authoritative staged-work observation for the run report."""
    return {
        "status": "available",
        "staged_work_present": bool(redrive_state["staged_work_present"]),
        "latest_checkpoint_id": redrive_state["latest_checkpoint_id"],
        "latest_checkpoint_status": redrive_state["latest_checkpoint_status"],
        "latest_published_checkpoint_id": redrive_state[
            "latest_published_checkpoint_id"
        ],
        "branch_mutation_token": redrive_state["branch_mutation_token"],
    }


def resume_before_source_writes(
    *,
    redrive_state: Mapping[str, Any],
    deployment: str,
    source_contract: Sequence[Mapping[str, Any]],
    ensure_feedback: Callable[[str, Mapping[str, Any]], Any],
    load_state: Callable[[], dict[str, Any]],
    promote: Callable[
        [Mapping[str, Any], Mapping[str, Any]],
        dict[str, Any],
    ],
    source_profiles: Mapping[str, str | None] | None = None,
    registry: Mapping[str, Any] | None = None,
    active_producer_ids: Callable[[Sequence[str]], set[str]] | None = None,
) -> dict[str, Any]:
    """Resolve or promote an incomplete receipt before ordinary source work."""
    authority = recovery_from_redrive_state(
        redrive_state,
        deployment=deployment,
    )
    if authority is None:
        return {
            "authority": None,
            "redrive_state": dict(redrive_state),
            "staged_redrive_needed": bool(redrive_state["staged_work_present"]),
            "report": None,
            "promotion": None,
        }

    recovery_report = {
        "status": authority["state"],
        "checkpoint_id": redrive_state["latest_checkpoint_id"],
    }
    if authority["state"] == REPAIR_REQUIRED:
        require_exact_repair_contract(
            expected=authority,
            actual_source_contract=source_contract,
            deployment=deployment,
            checkpoint_id=str(redrive_state["latest_checkpoint_id"]),
            source_profiles=source_profiles,
            registry=registry,
            active_producer_ids=active_producer_ids,
        )
        return {
            "authority": authority,
            "redrive_state": dict(redrive_state),
            "staged_redrive_needed": False,
            "report": recovery_report,
            "promotion": None,
        }

    frozen_checkpoint_id = str(redrive_state["latest_checkpoint_id"])
    ensure_feedback(frozen_checkpoint_id, authority)
    promotion = promote(load_state(), authority)
    if promotion.get("status") != "published":
        raise SubstrateError(
            "frozen incomplete recovery was not promoted; refusing to start "
            "ordinary source writes",
            data={
                "deployment": deployment,
                "frozen_checkpoint_id": frozen_checkpoint_id,
                "publish_status": promotion.get("status"),
                "publish_code": promotion.get("code"),
                "publish_error": promotion.get("error"),
            },
            fix_hint=(
                "retry after resolving the competing branch or checkpoint mutation"
            ),
        )
    recovery_report.update(
        {
            "status": "promoted",
            "published_checkpoint_id": promotion.get("generation_id"),
        }
    )
    next_state = load_state()
    return {
        "authority": None,
        "redrive_state": next_state,
        "staged_redrive_needed": bool(next_state["staged_work_present"]),
        "report": recovery_report,
        "promotion": promotion,
    }


def prepare_freeze(
    *,
    enabled: bool,
    deployment: str,
    completeness: Mapping[str, Any],
    state: str,
    source_contract: Sequence[Mapping[str, Any]],
    load_state: Callable[[], dict[str, Any]],
    admission_ids_for: Callable[[str], Sequence[str]],
) -> tuple[dict[str, Any] | None, dict[str, Any] | None]:
    """Capture the exact branch fence and build its recovery receipt."""
    if not enabled:
        return None, None
    freeze_state = load_state()
    predecessor = str(freeze_state.get("latest_checkpoint_id") or "none")
    if not freeze_state.get("branch_mutation_token"):
        raise SubstrateError(
            "cannot freeze incomplete recovery without an exact branch mutation token",
            data={
                "reason": "incomplete_recovery_branch_token_missing",
                "deployment": deployment,
                "latest_checkpoint_id": freeze_state.get("latest_checkpoint_id"),
            },
        )
    blocker = blocker_receipt(
        completeness=completeness,
        state=state,
        predecessor_checkpoint_id=predecessor,
        source_contract=source_contract,
        admission_ids=admission_ids_for(predecessor),
    )
    return freeze_state, blocker


def freeze_publish_kwargs(
    *,
    freeze_state: Mapping[str, Any] | None,
    blocker: Mapping[str, Any] | None,
) -> dict[str, Any]:
    """Return guarded publisher arguments, or no arguments off recovery."""
    if freeze_state is None or blocker is None:
        return {}
    recovery = blocker["recovery"]
    return {
        "publish_blocker": blocker,
        "expected_latest_checkpoint_id": freeze_state.get("latest_checkpoint_id"),
        "expected_branch_mutation_token": freeze_state["branch_mutation_token"],
        "require_expected_latest_checkpoint": True,
        "expected_admission_ids": recovery["admission_ids"],
        "recovery_from_generation_id": recovery["predecessor_checkpoint_id"],
        "admission_feedback_expected_status": "blocked",
    }


def promote_blocked_freeze(
    *,
    published: dict[str, Any],
    blocker: Mapping[str, Any] | None,
    load_state: Callable[[], dict[str, Any]],
    promote: Callable[
        [Mapping[str, Any], Mapping[str, Any]],
        dict[str, Any],
    ],
) -> tuple[dict[str, Any], dict[str, Any] | None, dict[str, Any] | None]:
    """Promote a newly frozen checkpoint and return its report fragments."""
    if (
        blocker is None
        or blocker["recovery"]["state"] != FROZEN
        or published.get("status") != "blocked"
    ):
        return published, None, None
    frozen_publish = published
    promoted = promote(load_state(), blocker["recovery"])
    return (
        promoted,
        frozen_publish,
        {
            "status": (
                "promoted"
                if promoted.get("status") == "published"
                else "promotion_refused"
            ),
            "frozen_checkpoint_id": frozen_publish.get("generation_id"),
            "published_checkpoint_id": promoted.get("generation_id"),
        },
    )


def supersede_incomplete_checkpoint(
    *,
    dsn: str,
    deployment: str,
    reason: str,
    actor: str = "operator",
    checkpoint_id: str | None = None,
    apply: bool = False,
    record: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Retire an incomplete checkpoint's recovery authority, with a reason.

    An incomplete checkpoint demands that the NEXT run reproduce its exact
    repair contract. When one of the named sources cannot be made to pass,
    that contract is unsatisfiable and every queued workflow refuses with
    `incomplete_recovery_contract_mismatch` — one failing source wedges the
    whole deployment, and the only documented remedy targets legacy receipts
    (see :func:`operator_fix_hint`), which this is not.

    This is the deliberate exit. It marks the recovery authority
    :data:`SUPERSEDED` and stamps who did it, when, and why. It does NOT
    rewrite the checkpoint's `blocked` status, does not delete anything, and
    does not claim the incomplete sources ingested: the work those sources
    did not finish stays unfinished, and the next ordinary run picks them up
    from their own watermarks.

    Dry run by default. `apply=True` performs the write under an exact
    precondition on the current recovery state, so a concurrent change
    refuses rather than being overwritten.

    Returns a report with `applied`, the checkpoint id, the prior state, and
    the contract being retired. Raises :class:`SubstrateError` when there is
    nothing to supersede, when the named checkpoint is not the latest one, or
    when the state moved under the write.
    """
    from okg.substrate.db.pool import GENERAL, pooled_connection

    reason = (reason or "").strip()
    if not reason:
        raise SubstrateError(
            "superseding an incomplete checkpoint requires a reason",
            data={
                "reason": "incomplete_recovery_supersede_reason_required",
                "deployment": deployment,
            },
            fix_hint=(
                "pass --reason with what was done about the incomplete "
                "sources; the reason is recorded on the checkpoint"
            ),
        )

    state = staged_source_redrive_state(dsn=dsn, deployment=deployment)
    latest_id = state.get("latest_checkpoint_id")
    if (
        state.get("latest_checkpoint_status") != "blocked"
        or state.get("latest_blocked_reason") != BLOCKED_INGEST_REASON
    ):
        raise SubstrateError(
            "no incomplete checkpoint to supersede",
            data={
                "reason": "incomplete_recovery_nothing_to_supersede",
                "deployment": deployment,
                "latest_checkpoint_id": latest_id,
                "latest_checkpoint_status": state.get(
                    "latest_checkpoint_status"
                ),
            },
            fix_hint=(
                "the latest checkpoint is not an incomplete-ingest blocker; "
                "nothing is wedged by a repair contract"
            ),
        )
    if checkpoint_id is not None and str(checkpoint_id) != str(latest_id):
        raise SubstrateError(
            "the named checkpoint is not the deployment's latest",
            data={
                "reason": "incomplete_recovery_supersede_checkpoint_drift",
                "deployment": deployment,
                "requested_checkpoint_id": str(checkpoint_id),
                "latest_checkpoint_id": latest_id,
            },
            fix_hint=(
                "re-read the current state and supersede the latest "
                "checkpoint, or omit --checkpoint-id"
            ),
        )

    receipt = state.get("latest_blocked_receipt") or {}
    recovery = receipt.get("recovery") if isinstance(receipt, Mapping) else None
    if (
        not isinstance(recovery, Mapping)
        or recovery.get("schema") != RECOVERY_SCHEMA
    ):
        raise SubstrateError(
            "this checkpoint carries no recovery authority to supersede",
            data={
                "reason": "incomplete_recovery_authority_missing",
                "deployment": deployment,
                "latest_checkpoint_id": latest_id,
            },
            fix_hint=operator_fix_hint(deployment),
        )
    prior_state = recovery.get("state")
    if prior_state == SUPERSEDED:
        raise SubstrateError(
            "this checkpoint's recovery authority is already superseded",
            data={
                "reason": "incomplete_recovery_already_superseded",
                "deployment": deployment,
                "latest_checkpoint_id": latest_id,
                "superseded_at": recovery.get("superseded_at"),
                "superseded_by": recovery.get("superseded_by"),
                "supersede_reason": recovery.get("supersede_reason"),
            },
            fix_hint=(
                "the deployment is no longer wedged by this checkpoint; "
                "run the ordinary source sync"
            ),
        )

    report: dict[str, Any] = {
        "schema": "okg.ingest_incomplete_recovery.supersede.v1",
        "deployment": deployment,
        "checkpoint_id": latest_id,
        "prior_state": prior_state,
        "required_sources": list(recovery.get("required_sources") or ()),
        "reason": reason,
        "actor": actor,
        "applied": False,
    }
    if not apply:
        report["next_command"] = (
            f"okg runtime supersede-checkpoint --deployment {deployment} "
            f"--reason {reason!r} --apply"
        )
        return report

    superseded_at = _utcnow_iso()
    with pooled_connection(dsn, purpose=GENERAL) as conn:
        with conn.transaction(), conn.cursor() as cur:
            # The WHERE clause pins the exact checkpoint AND its exact prior
            # recovery state. If a concurrent repair or a second operator
            # moved it, the UPDATE matches nothing and this refuses rather
            # than overwriting a state it never read.
            cur.execute(
                """
                UPDATE public._chronos_branch_interval_checkpoints
                   SET metadata = jsonb_set(
                         metadata::jsonb,
                         '{blocked_reason,recovery}',
                         (metadata::jsonb #> '{blocked_reason,recovery}')
                           || %(patch)s::jsonb,
                         false
                       )::text
                 WHERE checkpoint_id = %(checkpoint_id)s
                   AND metadata::jsonb #> '{blocked_reason,recovery}'
                       = %(prior_recovery)s::jsonb
                """,
                {
                    "checkpoint_id": latest_id,
                    # Compare the WHOLE recovery object, not just its state.
                    # The source contract and required sources could change
                    # while the state stayed put, and this report would then
                    # name a contract that is not the one being retired.
                    # jsonb equality ignores key order, so this is an exact
                    # compare-and-swap against what was read.
                    "prior_recovery": _json_dumps(recovery),
                    "patch": _json_dumps({
                        "state": SUPERSEDED,
                        "superseded_state": prior_state,
                        "superseded_at": superseded_at,
                        "superseded_by": actor,
                        "supersede_reason": reason,
                        # What an automatic retirement read, so an auditor
                        # can see the decision's inputs (absent for an
                        # operator supersede).
                        **(
                            {"contract_retirement": dict(record)}
                            if record else {}
                        ),
                    }),
                },
            )
            updated = cur.rowcount
    if updated != 1:
        raise SubstrateError(
            "incomplete checkpoint changed while it was being superseded",
            data={
                "reason": "incomplete_recovery_supersede_state_drift",
                "deployment": deployment,
                "latest_checkpoint_id": latest_id,
                "expected_state": prior_state,
                "rows_updated": updated,
            },
            fix_hint=(
                "re-read the checkpoint state and retry; something else "
                "moved this checkpoint's recovery authority"
            ),
        )
    report["applied"] = True
    report["superseded_at"] = superseded_at
    return report


def _utcnow_iso() -> str:
    from datetime import datetime, timezone

    return datetime.now(timezone.utc).isoformat()


def _json_dumps(payload: Mapping[str, Any]) -> str:
    import json

    return json.dumps(dict(payload), sort_keys=True, default=str)


# --- Retiring a contract whose required sources are gone -------------------
#
# PACT recovery-contract-removed-sources. A `repair_required` checkpoint
# holds the deployment until its failed sources repair. When every one of
# those sources has been removed or renamed, nothing can repair them and the
# hold only wedges the deployment, so `okg migrate --deployment` and `okg
# runtime worker` start retire it through the operator supersede, under the
# conditions in :func:`retire_removed_source_contract`.

_UNSET: Any = object()
_RACE_REASONS = frozenset({
    "incomplete_recovery_already_superseded",
    "incomplete_recovery_nothing_to_supersede",
    "incomplete_recovery_supersede_checkpoint_drift",
    "incomplete_recovery_supersede_state_drift",
})
REPORT_KIND = "removed_source_contract"


def registry_fingerprint(registry: Mapping[str, Any]) -> str:
    """sha256 over the resolved registry's names, ownership ids and bindings."""
    import hashlib
    import json

    from okg.substrate.ingest.adapter_factory import (
        source_adapter_binding_fingerprint,
    )

    rows = sorted(
        [
            str(name),
            str(entry.ownership_id),
            source_adapter_binding_fingerprint(entry),
        ]
        for name, entry in registry.items()
    )
    return hashlib.sha256(
        json.dumps(rows, separators=(",", ":")).encode("utf-8")
    ).hexdigest()


def _committed_deployment_revision(
    deployment_dir: Path | str,
) -> tuple[str | None, str | None]:
    """``(revision, None)`` for a committed manifest, else ``(None, why)``."""
    from okg.substrate.publisher.repository_provenance import (
        tracking_repository_provenance,
    )

    try:
        provenance = tracking_repository_provenance(Path(deployment_dir))
    except Exception as exc:  # noqa: BLE001 - reported as the reason, not hidden
        repository = (getattr(exc, "data", None) or {}).get("repository")
        where = f" (repository {repository})" if repository else ""
        return None, f"{type(exc).__name__}: {exc}{where}"
    if provenance.status in {"exact", "distribution"}:
        return str(provenance.deployment_revision), None
    return None, f"repository provenance is {provenance.status}"


def _read_latest_checkpoints(
    dsn: str,
    deployment: str,
) -> tuple[
    tuple[str, dict[str, Any]] | None,
    tuple[str, dict[str, Any]] | None,
]:
    """``(own, foreign)`` latest default-branch checkpoints, from direct reads.

    ``own`` is the deployment's latest (the one every run reads). ``foreign``
    is read only when the deployment has none: the latest checkpoint
    recorded under another deployment name, as a copied database holds.
    """
    from okg.substrate.db.connection import connect as db_connect
    from okg.substrate.ingest.run_base import EXCLUDE_RUN_BASE_SQL

    with db_connect(dsn, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT to_regclass('public._chronos_branch_interval_checkpoints')"
        )
        if cur.fetchone()[0] is None:
            return None, None
        cur.execute(
            f"""
            SELECT checkpoint_id, metadata::jsonb
              FROM public._chronos_branch_interval_checkpoints
             WHERE metadata::jsonb->>'deployment_name' = %(deployment)s
               AND COALESCE(metadata::jsonb->>'branch', 'default') = 'default'
               AND {EXCLUDE_RUN_BASE_SQL}
             ORDER BY created_at DESC, checkpoint_id DESC
             LIMIT 1
            """,
            {"deployment": deployment},
        )
        row = cur.fetchone()
        if row is not None:
            return (str(row[0]), dict(row[1] or {})), None
        cur.execute(
            f"""
            SELECT checkpoint_id, metadata::jsonb
              FROM public._chronos_branch_interval_checkpoints
             WHERE metadata::jsonb->>'deployment_name' IS NOT NULL
               AND metadata::jsonb->>'deployment_name' <> %(deployment)s
               AND COALESCE(metadata::jsonb->>'branch', 'default') = 'default'
               AND {EXCLUDE_RUN_BASE_SQL}
             ORDER BY created_at DESC, checkpoint_id DESC
             LIMIT 1
            """,
            {"deployment": deployment},
        )
        row = cur.fetchone()
        return None, ((str(row[0]), dict(row[1] or {})) if row else None)


def _repair_required_recovery(
    checkpoint_id: str,
    metadata: Mapping[str, Any],
    *,
    deployment: str,
) -> dict[str, Any] | None:
    """The checkpoint's recovery object if it is `repair_required`, else None."""
    blocked_reason = metadata.get("blocked_reason") or {}
    state = {
        "latest_checkpoint_id": checkpoint_id,
        "latest_checkpoint_status": metadata.get("status"),
        "latest_blocked_reason": (
            blocked_reason.get("reason")
            if isinstance(blocked_reason, Mapping) else None
        ),
        "latest_blocked_receipt": (
            dict(blocked_reason) if isinstance(blocked_reason, Mapping) else {}
        ),
    }
    try:
        recovery = recovery_from_redrive_state(state, deployment=deployment)
    except SubstrateError as exc:
        if exc.data.get("reason") != "incomplete_recovery_authority_missing":
            raise
        return None  # a legacy receipt has its own fix; not this retirement
    if recovery is None or recovery.get("state") != REPAIR_REQUIRED:
        return None
    return recovery


def _publish_block_now(dsn: str, deployment: str) -> dict[str, Any] | None:
    """The publisher's own guard, read now; runs in progress are transient."""
    from okg.substrate.db.connection import connect as db_connect
    from okg.substrate.ingest.run_base import publish_block

    with db_connect(dsn, autocommit=True) as conn:
        return publish_block(conn, deployment=deployment, include_in_progress=False)


def _step_that_clears(
    block: Mapping[str, Any],
    classes: Sequence[Mapping[str, Any]],
    deployment: str,
) -> list[str]:
    """The commands that clear a publish block, per blocked source."""
    from okg.substrate.support.user_hints import okg_prefix

    prefix = okg_prefix()
    code = block.get("code")
    if code == "source_run_undo_unfinished":
        return [f"{prefix} runtime finish-undo --deployment {deployment} --apply"]
    # A publish block names a source by its ownership id (the runner's
    # source), a contract by its registry name; a removed source matches by
    # either. A renamed source's ownership id now belongs to the live source
    # under its new name, so `retire-source` would delete that source's
    # records: it reruns under the new name instead.
    removed = {
        str(value)
        for item in classes
        if item["class"] == CLASS_REMOVED
        for value in (item["source"], item.get("ownership_id"))
        if value
    }
    renamed_to = {
        str(value): str(item["renamed_to"])
        for item in classes
        if item["class"] == CLASS_RENAMED
        for value in (item["source"], item.get("ownership_id"))
        if value
    }
    steps: list[str] = []
    for source in block.get("sources") or ():
        if source in renamed_to:
            steps.append(
                f"{prefix} ingest --deployment {deployment} "
                f"--include {renamed_to[source]}"
            )
        elif source in removed:
            steps.append(
                f"{prefix} remediate retire-source --source {source} "
                f"--deployment-name {deployment}"
            )
        else:
            steps.append(
                f"{prefix} ingest --deployment {deployment} --include {source}"
            )
    return steps


def retire_removed_source_contract(
    *,
    dsn: str,
    deployment: str,
    load_registry: Callable[[], tuple[Mapping[str, Any], Path | str]],
    actor: str,
    apply: bool = True,
) -> dict[str, Any] | None:
    """Retire a `repair_required` contract whose required sources are gone.

    Returns None when the deployment holds no such contract. Otherwise one
    report (``kind: removed_source_contract``) whose ``status`` is
    ``retired``, ``would_retire`` (``apply=False``), ``retired_by_other`` or
    ``not_retired`` with a ``reason``. Retires once, through
    :func:`supersede_incomplete_checkpoint`, only when:

    1. every source in the contract's ``required_sources`` is ``removed`` or
       ``renamed`` against the resolved registry, and that registry's
       deployment manifest is committed;
    2. the checkpoint's recorded deployment name is ``deployment`` (else
       ``incomplete_recovery_deployment_renamed``: the unfinished-undo guard
       filters by that name, so a copy under another name would hide the
       original's unfinished undo);
    3. the publisher's own guard (``publish_block``) returns nothing (else
       ``incomplete_recovery_publish_blocked``: a source kept with
       ``finish-undo --accept-current`` stays ``source_rerun_required`` and a
       removed source never reruns).

    ``actor`` names the caller (``okg migrate`` or ``okg runtime worker``).
    The supersede's compare-and-swap decides a race: the loser re-reads and
    reports ``retired_by_other`` instead of failing.
    """
    own, foreign = _read_latest_checkpoints(dsn, deployment)
    base: dict[str, Any] = {
        "kind": REPORT_KIND,
        "deployment": deployment,
        "actor": actor,
        "applied": False,
    }
    if own is None:
        if foreign is None:
            return None
        checkpoint_id, metadata = foreign
        recorded = metadata.get("deployment_name")
        if _repair_required_recovery(
            checkpoint_id, metadata, deployment=str(recorded),
        ) is None:
            return None
        return _renamed_report(base, checkpoint_id, deployment, recorded)
    checkpoint_id, metadata = own
    recovery = _repair_required_recovery(
        checkpoint_id, metadata, deployment=deployment,
    )
    if recovery is None:
        return None
    # `own` was selected by `metadata.deployment_name == deployment`, so the
    # name needs no second check here. What catches a copy under another name
    # is the `foreign` branch above, and at publish time the publisher guard's
    # name scope (`pending_undos` and `rerun_required` filter by name).
    base["checkpoint_id"] = checkpoint_id
    required = [str(name) for name in recovery.get("required_sources") or ()]
    base["required_sources"] = required
    try:
        registry, deployment_dir = load_registry()
    except Exception as exc:  # noqa: BLE001 - reported with its type and text
        unloaded = (
            "the source registry could not be loaded, so the contract "
            "was not classified"
        )
        return {
            **base,
            "status": "not_retired",
            "reason": "registry_unavailable",
            "error": f"{type(exc).__name__}: {exc}",
            "message": unloaded,
        }
    contract = list(recovery.get("source_contract") or ())
    classes = classify_contract_sources(
        contract=contract,
        registry=registry,
        active_producer_ids=_safe_active_ids(
            lambda ids: active_producer_ids(
                dsn=dsn, deployment=deployment, ids=ids,
            ),
            contract,
        ),
    )
    base["source_classes"] = classes
    ok, why = retirement_class_condition(classes, required)
    if not ok:
        kept = (
            "a source the checkpoint holds back still exists (or is only "
            "rebound), so its repair requirement is kept; repair it, or "
            "supersede with a reason"
        )
        nothing_held = "the checkpoint names no required source"
        return {
            **base,
            "status": "not_retired",
            "reason": why,
            "message": nothing_held if why == "no_required_sources" else kept,
        }
    revision, problem = _committed_deployment_revision(deployment_dir)
    if revision is None:
        uncommitted = (
            "a repository the registry is read from (the deployment's, or "
            "the okg checkout's) has uncommitted changes, so the registry "
            "does not authorize a retirement; the detail names the "
            "repository. Commit it and run again"
        )
        return {
            **base,
            "status": "not_retired",
            "reason": "manifest_uncommitted",
            "detail": problem,
            "message": uncommitted,
        }
    block = _publish_block_now(dsn, deployment)
    if block is not None:
        steps = _step_that_clears(block, classes, deployment)
        blocked_text = (
            f"publishing is blocked ({block['code']}), so retiring the "
            "repair requirement would only move the wedge into the "
            "publisher; clear it first with: " + "; ".join(steps)
        )
        return {
            **base,
            "status": "not_retired",
            "reason": "incomplete_recovery_publish_blocked",
            "publish_block": block["code"],
            "sources": list(block.get("sources") or ()),
            "clears_with": steps,
            "message": blocked_text,
        }
    names = sorted(required)
    reason = f"{CONTRACT_RETIRED_REASON}: {', '.join(names)}"
    record = {
        "reason": CONTRACT_RETIRED_REASON,
        "sources": names,
        "source_classes": [dict(item) for item in classes],
        "registry_fingerprint": registry_fingerprint(registry),
        "deployment_revision": revision,
    }
    if not apply:
        return {**base, "status": "would_retire", "retire_reason": reason}
    try:
        superseded = supersede_incomplete_checkpoint(
            dsn=dsn,
            deployment=deployment,
            reason=reason,
            actor=actor,
            checkpoint_id=checkpoint_id,
            apply=True,
            record=record,
        )
    except SubstrateError as exc:
        code = exc.data.get("reason")
        if code not in _RACE_REASONS:
            raise
        return _lost_race_report(dsn, deployment, base, str(code))
    log.warning(
        "retired the repair contract of blocked checkpoint %s (deployment %s, "
        "actor %s): its required sources %s were removed or renamed",
        checkpoint_id, deployment, actor, ", ".join(names),
    )
    return {
        **base,
        "status": "retired",
        "applied": True,
        "retire_reason": reason,
        "superseded_at": superseded.get("superseded_at"),
        "registry_fingerprint": record["registry_fingerprint"],
        "deployment_revision": revision,
    }


def _renamed_report(
    base: Mapping[str, Any],
    checkpoint_id: str,
    deployment: str,
    recorded: Any,
) -> dict[str, Any]:
    text = (
        f"checkpoint {checkpoint_id} was recorded for deployment "
        f"{recorded!r}, not {deployment!r}. The unfinished-undo guard "
        "filters by deployment name, so this deployment cannot see that "
        "deployment's unfinished undo and nothing is retired. Run the "
        f"command against {recorded!r}, or supersede by hand after "
        "checking its unfinished undos"
    )
    return {
        **base,
        "checkpoint_id": checkpoint_id,
        "status": "not_retired",
        "reason": "incomplete_recovery_deployment_renamed",
        "recorded_deployment": recorded,
        "message": text,
    }


def _lost_race_report(
    dsn: str,
    deployment: str,
    base: Mapping[str, Any],
    code: str,
) -> dict[str, Any]:
    """Re-read after a lost compare-and-swap and say who won."""
    own, _foreign = _read_latest_checkpoints(dsn, deployment)
    blocked = (own[1].get("blocked_reason") or {}) if own else {}
    recovery = blocked.get("recovery") if isinstance(blocked, Mapping) else None
    if isinstance(recovery, Mapping) and recovery.get("state") == SUPERSEDED:
        log.info(
            "repair contract of checkpoint %s was already retired by %s at %s",
            base.get("checkpoint_id"),
            recovery.get("superseded_by"),
            recovery.get("superseded_at"),
        )
        won = (
            f"retired by {recovery.get('superseded_by')} at "
            f"{recovery.get('superseded_at')}"
        )
        return {
            **base,
            "status": "retired_by_other",
            "retired_by": recovery.get("superseded_by"),
            "retired_at": recovery.get("superseded_at"),
            "retire_reason": recovery.get("supersede_reason"),
            "message": won,
        }
    moved = (
        "the checkpoint changed while it was being retired; the next "
        "migrate or worker start tries again"
    )
    return {**base, "status": "not_retired", "reason": code, "message": moved}


def retire_removed_source_contract_for_deployment(
    *,
    dsn: str,
    deployment: str,
    actor: str,
    manifest: Any = None,
    repo_root_override: Any = _UNSET,
    apply: bool = True,
) -> dict[str, Any] | None:
    """:func:`retire_removed_source_contract` with the deployment's own registry.

    ``manifest`` and ``repo_root_override`` are what a caller that already
    resolved the deployment holds (worker start); without them they are
    resolved here, as ``okg ingest`` does.
    """

    def load() -> tuple[Mapping[str, Any], Path | str]:
        from okg.substrate.deployments import resolve_deployment
        from okg.substrate.deployments.sync_root import (
            require_usable_source_configuration_authority,
        )
        from okg.substrate.sources.registry import load_source_registry_admission

        resolved = manifest if manifest is not None else resolve_deployment(deployment)
        override = repo_root_override
        if override is _UNSET:
            override = require_usable_source_configuration_authority(
                resolved.sync_root, command=f"{actor} source contract retirement",
            )
        admission = load_source_registry_admission(
            resolved.source_registry, repo_root_override=override,
        )
        return admission.entries, resolved.deployment_dir

    return retire_removed_source_contract(
        dsn=dsn, deployment=deployment, load_registry=load, actor=actor, apply=apply,
    )


def removed_source_contract_candidate(
    *,
    dsn: str,
    deployment: str,
) -> dict[str, Any] | None:
    """A `repair_required` contract found without a deployment, or None.

    `okg migrate` without ``--deployment`` loads no source registry, so it
    cannot classify; it names the deployment to pass and retires nothing.
    """
    own, _foreign = _read_latest_checkpoints(dsn, deployment)
    if own is None:
        return None
    recovery = _repair_required_recovery(own[0], own[1], deployment=deployment)
    if recovery is None:
        return None
    held = (
        "a repair requirement is held; migrate without --deployment loads "
        "no source registry and retires nothing. Run: "
        + _migrate_command(deployment)
    )
    return {
        "kind": REPORT_KIND,
        "deployment": deployment,
        "checkpoint_id": own[0],
        "status": "candidate",
        "applied": False,
        "required_sources": list(recovery.get("required_sources") or ()),
        "message": held,
    }
