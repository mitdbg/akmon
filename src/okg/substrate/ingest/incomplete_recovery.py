"""Fail-closed recovery receipts for incomplete ingest checkpoints.

Source repair runs against the mutable Chronos branch and therefore cannot
hold the branch session fence used by :class:`SourceRunner`.  Recovery instead
records the exact source authority, freezes the repaired branch in a blocked
checkpoint, and promotes only that checkpoint through the publisher's guarded
path.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from typing import Any

from okg.substrate.errors import SubstrateError


BLOCKED_INGEST_REASON = "ingest_source_incomplete"
RECOVERY_SCHEMA = "okg.ingest_incomplete_recovery.v1"
REPAIR_REQUIRED = "repair_required"
FROZEN = "frozen"
#: An operator has retired this checkpoint's recovery authority with a
#: recorded reason. The checkpoint itself stays `blocked` — nothing about it
#: is rewritten as published — but it no longer demands an exact repair
#: contract, so the deployment's other workflows can run again.
SUPERSEDED = "superseded"


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


def source_sync_hold(
    *,
    dsn: str,
    deployment: str,
    sources: Mapping[str, str],
    connect_timeout: int = 10,
) -> dict[str, Any] | None:
    """What a `repair_required` checkpoint is holding back, or None (#3342).

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
    return {
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
    return (
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
