"""Shared real-publish verification gate.

Every live-claiming path (``okg deployment bootstrap verify``, ``okg up``'s
smoke-publish phase, and ``okg provision``) must end at the same bar: the
publish produced a *published* (not blocked) generation, the required positive
``nodes_live``/``edges_live`` row delta, and a dead-letter queue within bounds.
Fresh graphs require both row families. A caller that first observes an already
populated predecessor may explicitly accept a positive one-sided incremental
delta; zero-total change still requires the separate validated no-op contract.

The bootstrap path (``deployment_bootstrap.workflow._run_real_publish_proof``)
owns the staged-catalog/fixture orchestration and maps failures onto pipeline
stages; this module factors out the *assertions* it makes about a publish so the
upgrade smoke-publish phase and provision can apply the identical gate instead
of trusting an exit code or a non-null generation id.

The functions here return a structured verdict ``dict`` rather than raising, so
they slot into the upgrade phase-result dict pattern and provision's ``ok``
computation without inventing a new wire error code. Callers that want an
exception (the bootstrap path) inspect ``ok`` and raise their own.
"""
from __future__ import annotations

import sys
from typing import Any, Mapping

from okg.substrate.db.connection import connect as db_connect

from okg.substrate.publish_lock_headroom import lock_headroom_from_payload
from okg.substrate.runtime_queue_health import (
    queue_status_counts_for_viz,
    summarize_runtime_queue_health,
)


def publish_status(payload: Any) -> str | None:
    """Best-effort extract the publish ``status`` from a publish/ingest payload.

    Accepts either a single publish summary (``{"status": ...}``) or an ingest
    report carrying ``publishes``; returns the status of the last real publish.
    """
    if not isinstance(payload, Mapping):
        return None
    if "status" in payload:
        return _coerce_status(payload.get("status"))
    publish = payload.get("publish")
    if isinstance(publish, Mapping) and "status" in publish:
        return _coerce_status(publish.get("status"))
    publishes = payload.get("publishes")
    if isinstance(publishes, list):
        for item in reversed(publishes):
            if isinstance(item, Mapping) and item.get("status") not in (
                None,
                "skipped",
            ):
                return _coerce_status(item.get("status"))
    return None


def _coerce_status(value: Any) -> str | None:
    return str(value) if value is not None else None


def publish_generation_id(payload: Any) -> str | int | None:
    """Best-effort extract the published generation id from a publish payload."""
    if not isinstance(payload, Mapping):
        return None
    for key in ("generation_id", "target_generation_id"):
        if payload.get(key) is not None:
            return payload[key]
    publish = payload.get("publish")
    if isinstance(publish, Mapping):
        generation = publish_generation_id(publish)
        if generation is not None:
            return generation
    publishes = payload.get("publishes")
    if isinstance(publishes, list):
        for item in reversed(publishes):
            if isinstance(item, Mapping):
                generation = item.get("generation_id") or item.get(
                    "target_generation_id"
                )
                if generation is not None:
                    return generation
    return None


def _publish_row_delta(payload: Any) -> tuple[int | None, int | None]:
    """Extract the (nodes_added, edges_added) row delta from a publish payload.

    Returns ``(None, None)`` when the payload carries no delta counts (so the
    caller fails closed rather than treating an absent delta as zero).
    """
    if not isinstance(payload, Mapping):
        return (None, None)
    if "nodes_added" in payload or "edges_added" in payload:
        return (
            _present_int(payload, "nodes_added"),
            _present_int(payload, "edges_added"),
        )
    publish = payload.get("publish")
    if isinstance(publish, Mapping) and (
        "nodes_added" in publish or "edges_added" in publish
    ):
        return _publish_row_delta(publish)
    publishes = payload.get("publishes")
    nodes = 0
    edges = 0
    seen = False
    if isinstance(publishes, list):
        for item in publishes:
            if not isinstance(item, Mapping):
                continue
            if "nodes_added" in item or "edges_added" in item:
                seen = True
                node_value = _present_int(item, "nodes_added")
                edge_value = _present_int(item, "edges_added")
                if node_value is None or edge_value is None:
                    return (None, None)
                nodes += node_value
                edges += edge_value
    if seen:
        return (nodes, edges)
    return (None, None)


def _publish_row_mutation_delta(payload: Any) -> tuple[int | None, int | None]:
    """Return total node/edge mutations for an existing-graph smoke proof."""
    if not isinstance(payload, Mapping):
        return (None, None)
    node_keys = ("nodes_added", "nodes_updated", "nodes_retracted")
    edge_keys = ("edges_added", "edges_updated", "edges_retracted")
    if any(key in payload for key in (*node_keys, *edge_keys)):
        node_seen = any(key in payload for key in node_keys)
        edge_seen = any(key in payload for key in edge_keys)
        node_values = [_present_int(payload, key) for key in node_keys if key in payload]
        edge_values = [_present_int(payload, key) for key in edge_keys if key in payload]
        if any(value is None for value in (*node_values, *edge_values)):
            return (None, None)
        nodes = sum(value for value in node_values if value is not None)
        edges = sum(value for value in edge_values if value is not None)
        return (nodes if node_seen else None, edges if edge_seen else None)
    publish = payload.get("publish")
    if isinstance(publish, Mapping):
        return _publish_row_mutation_delta(publish)
    publishes = payload.get("publishes")
    if isinstance(publishes, list):
        node_total = 0
        edge_total = 0
        node_seen = False
        edge_seen = False
        for item in publishes:
            nodes, edges = _publish_row_mutation_delta(item)
            if nodes is not None:
                node_seen = True
                node_total += nodes
            if edges is not None:
                edge_seen = True
                edge_total += edges
        return (
            node_total if node_seen else None,
            edge_total if edge_seen else None,
        )
    return (None, None)


def _coerce_int(value: Any) -> int | None:
    """Parse an integer counter without truncation or bool coercion."""
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, str):
        text = value.strip()
        digits = text[1:] if text[:1] in {"+", "-"} else text
        if digits.isascii() and digits.isdigit():
            return int(text)
    return None


def _present_int(payload: Mapping[str, Any], key: str) -> int | None:
    """Parse a present, non-negative counter; absent or malformed stays unknown."""
    if key not in payload:
        return None
    value = _coerce_int(payload.get(key))
    return value if value is not None and value >= 0 else None


def dlq_count(
    dsn: str,
    *,
    ignored_workflow_names: set[str] | None = None,
) -> tuple[int, dict[str, int]]:
    """Return current DLQ count and live DBOS queue status for ``dsn``.

    The strict default includes every workflow. A caller that has already
    validated a named workflow family through a stronger dedicated authority
    may explicitly delegate that family here; deployment-up smoke uses this to
    share deployment readiness's source-convergence partition.
    """
    with db_connect(dsn, autocommit=True) as conn, conn.cursor() as cur:
        summary = summarize_runtime_queue_health(
            cur,
            ignored_workflow_names=ignored_workflow_names,
        )
        current_dlq = summary["current_runtime_dlq"]
        dbos_provisioned = _dbos_workflow_status_exists(cur)
    queue_depth = queue_status_counts_for_viz(summary)
    queue_depth["terminal_failure_history"] = int(
        summary.get("terminal_history_dlq_count") or 0
    )
    queue_depth["current_dlq"] = int(current_dlq["current_count"])
    status = str(summary.get("status") or "unavailable")
    if status == "unavailable" and not dbos_provisioned:
        # Provable ABSENCE of the DBOS runtime is not "unreadable": a
        # cold-start CLI publish (okg provision / init / run --once) on a
        # fresh database has never launched DBOS, so `dbos.workflow_status`
        # does not exist, no queue can hold work, and the DLQ is provably
        # zero. Since #634 the health view reports this state as a sentinel
        # `unavailable` row, which made the fail-closed gate refuse EVERY
        # cold-start provision. Annotate loudly (dbos_not_provisioned) but
        # do not refuse; a runtime that EXISTS and cannot be read still
        # refuses below.
        queue_depth["dbos_not_provisioned"] = 0
        return int(current_dlq["current_count"]), queue_depth
    # "dead_letter" is a modeled, actionable state surfaced through the
    # current-DLQ count and handled by ``assert_publish_proof``'s dedicated
    # dead-letter branch. Historical/superseded failures do not make the
    # current deployment unhealthy, but unreadable runtime state still does.
    if status not in {"available", "dead_letter"} and not (
        status in {"failed", "cancelled", "dlq"}
        and int(current_dlq["current_count"]) == 0
    ):
        queue_depth[f"runtime_{status}"] = 1
    return int(current_dlq["current_count"]), queue_depth


def _dbos_workflow_status_exists(cur: Any) -> bool:
    """True iff the DBOS system schema was ever provisioned here."""
    cur.execute("SELECT to_regclass('dbos.workflow_status') IS NOT NULL")
    row = cur.fetchone()
    value = row[0] if not isinstance(row, dict) else next(iter(row.values()))
    return bool(value)


def _is_validated_no_source_work(payload: Any) -> bool:
    """True iff ``payload`` is the exact validated no-work smoke shape.

    The accepted no-op is the triple emitted at ingest/service.py:391-396
    (``status=skipped, publish_skipped=true, reason=no_source_work``) as the
    FINAL publish entry, with at least one requested source and every requested
    source OK. Any other shape — a skip with a different reason, a non-final
    skip, a source failure, or a missing status — is not a validated no-op and
    falls through to the strict gate (make-migrate-idempotent, design ruling 4).
    """
    if not isinstance(payload, Mapping):
        return False
    publishes = payload.get("publishes")
    if not isinstance(publishes, list) or not publishes:
        return False
    final = publishes[-1]
    if not isinstance(final, Mapping):
        return False
    if (
        _coerce_status(final.get("status")) != "skipped"
        or final.get("publish_skipped") is not True
        or final.get("reason") != "no_source_work"
    ):
        return False
    ordered = payload.get("ordered")
    if not isinstance(ordered, list) or not ordered:
        return False
    requested = [item for item in ordered if isinstance(item, str) and item]
    if len(requested) != len(ordered) or len(set(requested)) != len(requested):
        return False
    results = payload.get("results")
    if not isinstance(results, list) or not results:
        # A genuine no-op ran its requested sources (they produced no new
        # work). No results => nothing was proven; fail closed.
        return False
    completed: list[str] = []
    for item in results:
        if (
            not isinstance(item, Mapping)
            or _coerce_status(item.get("status")) != "OK"
            or not isinstance(item.get("name"), str)
            or not item["name"]
        ):
            return False
        completed.append(item["name"])
    return (
        len(set(completed)) == len(completed)
        and set(completed) == set(requested)
    )


def _caught_up_authorized_sources(payload: Any) -> set[str] | None:
    """Requested sources proven caught-up, or None when the shape is unreadable.

    A source counts as caught-up only when its ``OK`` result row carries an
    ACCEPTED terminal admission — ``admission_status == "accepted"`` and
    ``producer_policy.terminal_state_authorized`` exactly True.

    Both are deliberately chosen because they are NOT clobberable by the
    producer-authority evidence merge. In ingest/service.py the surfaced verdict
    is ``merge_policy_evidence(structural_verdict, authority_evidence)``, which
    overwrites shared ``producer_policy`` keys with the authority map's, and only
    THEN re-stamps ``terminal_state_authorized`` to
    ``structural_allowed and authority_allowed``. ``terminal_state_authorized``
    being True is therefore the admission layer's authoritative "authorized
    caught-up zero-output" decision, and it structurally implies a completed
    scope, a healthy source, and an authoritative-currency reason (authoritative
    replay / empty snapshot / unchanged). Re-checking those mergeable sub-fields
    here would add no safety (they are already implied) while risking a false
    REJECT of a healthy caught-up deploy if a future authority map overwrote one
    of them. ``admission_status`` is surfaced OUTSIDE ``producer_policy`` (so it
    is not merged) and excludes a review-mode source that is authorized yet only
    "pending" (submitted for review, not published) — an unpublished source is
    not "current".

    A source that produced real appended output carries no zero-output verdict
    (an empty ``admission_verdict``) and is simply not proven here. Returns
    ``None`` (fail closed) when ``results`` is missing or any row is malformed,
    so the caller keeps the strict zero-delta failure rather than assume currency.
    """
    if not isinstance(payload, Mapping):
        return None
    results = payload.get("results")
    if not isinstance(results, list):
        return None
    proven: set[str] = set()
    for row in results:
        if not isinstance(row, Mapping):
            return None
        name = row.get("name")
        if not isinstance(name, str) or not name:
            return None
        # A non-OK source is a wedge signal; leave it unproven so the requested
        # set can never be fully satisfied when any source failed.
        if row.get("status") != "OK":
            continue
        # An accepted terminal state, not merely an authorized one: a review-mode
        # source can be authorized while its envelope is only "pending".
        if row.get("admission_status") != "accepted":
            continue
        verdict = row.get("admission_verdict")
        policy = (
            verdict.get("producer_policy") if isinstance(verdict, Mapping) else None
        )
        if not isinstance(policy, Mapping):
            continue
        if policy.get("terminal_state_authorized") is True:
            proven.add(name)
    return proven


def _is_caught_up_authoritative_replay(payload: Any) -> bool:
    """True iff EVERY requested source is a proven authorized caught-up replay.

    Fail-closed: requires a well-formed non-empty requested-source list
    (``ordered``) and every requested source present in ``results`` with an
    authorized zero-output terminal verdict. Any missing or unreadable verdict,
    any source that produced real work, any partial / facts_rejected /
    unpublished-basis / degraded reason, or any requested source absent from or
    failed in ``results`` yields False. This is the committed-generation analogue
    of `_is_validated_no_source_work` (which accepts the publish-*skip* no-op):
    here a real head generation was published but its live-row differential was
    empty because the continuously running worker already made the graph current.
    """
    if not isinstance(payload, Mapping):
        return False
    ordered = payload.get("ordered")
    if not isinstance(ordered, list) or not ordered:
        return False
    requested = [item for item in ordered if isinstance(item, str) and item]
    if len(requested) != len(ordered) or len(set(requested)) != len(requested):
        return False
    proven = _caught_up_authorized_sources(payload)
    if proven is None:
        return False
    return set(requested).issubset(proven)


def is_caught_up_noop(payload: Any) -> bool:
    """True iff a smoke publish settled as a validated caught-up no-op.

    Either the commit publish was SKIPPED under the validated ``no_source_work``
    contract, or it committed a head generation whose live-row differential was
    empty because every requested source authoritatively replayed already-current
    rows. In both cases the routine deployment minted no new distinct content: the
    smoke proved the current published head is already current. Callers (the
    upgrade readiness/finalize join) use this to bind readiness / MCP / budget
    alignment to that head instead of demanding a freshly minted generation. It
    re-derives the decision from the publish payload alone (not a transient proof
    dict) so it is stable across a resume. Fail-closed: an unreadable, wedged, or
    partial payload is False.
    """
    return _is_validated_no_source_work(payload) or _is_caught_up_authoritative_replay(
        payload
    )


def _publish_refusal_fields(payload: Any) -> dict[str, Any]:
    """The refusing publish's own error, code, data and hint, when present."""
    if not isinstance(payload, Mapping):
        return {}
    fields: dict[str, Any] = {}
    for key, out in (
        ("error", "publish_error"),
        ("code", "publish_code"),
        ("error_data", "publish_error_data"),
        ("fix_hint", "publish_fix_hint"),
    ):
        value = payload.get(key)
        if value is None:
            continue
        fields[out] = dict(value) if isinstance(value, Mapping) else value
    return fields


def assert_publish_proof(
    payload: Any,
    *,
    dsn: str,
    require_row_delta: bool = True,
    allow_partial_row_delta: bool = False,
    allow_updated_rows: bool = False,
    allow_no_source_work: bool = False,
    allow_caught_up_replay: bool = False,
    ignored_runtime_workflow_names: set[str] | None = None,
    lock_headroom_not_applicable: str | None = None,
) -> dict[str, Any]:
    """Apply the uniform real-publish gate to a publish/ingest ``payload``.

    Returns a verdict dict ``{"ok": bool, ...}``. On failure ``ok`` is False and
    ``reason`` carries a human-readable 'could not read ...' / 'was not
    published' / 'did not add rows' / 'dead-letter' explanation. The verdict
    always echoes the observed ``status``, ``generation_id``, ``nodes_added``,
    ``edges_added``, and ``dlq_count`` so callers can surface them.

    The gate fails *closed*: an unreadable payload (no parseable status) is a
    failure, not an empty-safe pass. First-provision callers keep the default
    insert-strict proof. A caller that has independently proven a populated
    predecessor generation may opt into ``allow_partial_row_delta`` so a
    positive node-only or edge-only incremental publish is accepted; a
    zero-total delta still fails. Existing-graph deployment smoke may opt into
    ``allow_updated_rows`` so added, updated, or retracted live-row mutations
    satisfy the positive-delta requirement while a true zero-change publish
    remains a failure.

    ``allow_no_source_work`` is the upgrade smoke phase's opt-in for the one
    valid no-op: an unchanged-source smoke over a populated deployment produces
    no new facts, so the commit publish emits the validated
    ``no_source_work`` triple. Only the smoke phase passes this flag;
    bootstrap-verify and provision stay insert-strict. Even on the accepted
    no-op the DLQ / runtime-queue checks still run — a no-op over a poisoned
    queue fails.

    ``allow_caught_up_replay`` is the upgrade smoke phase's opt-in for the
    *committed* no-op: on a continuously published deployment whose runtime
    worker already advanced the graph to its head, a routine smoke publish that
    is NOT skipped still commits a published head generation whose live-row
    differential is empty because every requested source authoritatively
    replayed already-current rows. When set, a zero live-row mutation delta is
    accepted — instead of failing as "did not change live node and edge rows" —
    only if every requested source (``ordered``) reached an authorized
    zero-output terminal state in ``results`` (``terminal_state_authorized`` and
    ``completed_scope`` True, an authoritative-currency ``reason``, healthy
    source). A wedged, partial-scope, rejected-fact, unpublished-basis,
    degraded, or verdict-missing source keeps the strict failure. status,
    generation-id, DLQ, and runtime-queue checks are unchanged, so a blocked
    generation or poisoned queue still fails. The smoke phase passes this
    flag, and so does provision's packaged-installer repeat arm
    (``cli.provision._packaged_replay_proof``) after it has already bound the
    republish to the predecessor observed before refresh with equal live
    counts; bootstrap-verify, first installs and ordinary provision stay
    insert-strict.

    Issue #1291 adds the publish *capacity* bar: the receipt's measured
    advisory-lock hold must clear the configured safety factor against the hard
    statement timeout (default 2x, i.e. at most half the bound). Since
    decision 23a, a measured hold over that bar no longer fails the proof: the
    publish committed, so the verdict stays ``ok`` and carries a
    ``capacity_warning`` block (hold, bar, and how the bar was derived) that
    reporting commands turn into their own exit code. A payload that carries
    no lock receipt at all, or a disabled statement timeout, is *unproven*,
    not healthy — it still fails the gate rather than passing by omission. A caller that knows no
    publish work ran under the lock (an observed already-current generation,
    for example) must say so explicitly through
    ``lock_headroom_not_applicable=<reason>``; the exemption is never inferred
    from a missing field.

    ``ignored_runtime_workflow_names`` is a second smoke-only ownership input:
    deployment-up has already validated doctor and source-sync state through
    dedicated readiness authorities, so it passes their shared named
    partition. The default remains empty and strict for provision, bootstrap,
    promotion, and direct publish proof. Failures outside an explicitly named
    partition always remain blocking.
    """
    if allow_no_source_work and _is_validated_no_source_work(payload):
        dead_letter, queue_depth = _publish_dlq_count(
            dsn,
            ignored_runtime_workflow_names=ignored_runtime_workflow_names,
        )
        runtime_queue_error = next(
            (
                key
                for key, count in queue_depth.items()
                if key.startswith("runtime_") and count
            ),
            None,
        )
        if runtime_queue_error is not None:
            return {
                "ok": False,
                "reason": (
                    "no-op smoke could not establish healthy DBOS queue state "
                    f"({runtime_queue_error.removeprefix('runtime_')})"
                ),
                "status": "skipped",
                "no_source_work": True,
                "dlq_count": dead_letter,
                "queue_depth": queue_depth,
                "fix_hint": (
                    "run `okg jobs status --deployment <name> --json` and repair "
                    "DBOS runtime availability or stale active work"
                ),
            }
        if dead_letter:
            return {
                "ok": False,
                "reason": "no-op smoke found dead-letter runtime work",
                "status": "skipped",
                "no_source_work": True,
                "dlq_count": dead_letter,
                "queue_depth": queue_depth,
                "fix_hint": (
                    "inspect DBOS workflow failures and clear the dead-letter "
                    "queue before rerunning the upgrade"
                ),
            }
        return {
            "ok": True,
            "status": "skipped",
            "no_source_work": True,
            "generation_id": None,
            "dlq_count": dead_letter,
            "queue_depth": queue_depth,
            "lock_headroom": lock_headroom_from_payload(
                payload,
                not_applicable_reason=(
                    "the validated no-source-work cycle published nothing "
                    "under the publisher lock"
                ),
            ),
        }

    status = publish_status(payload)
    generation_id = publish_generation_id(payload)
    nodes_added, edges_added = _publish_row_delta(payload)
    nodes_changed, edges_changed = _publish_row_mutation_delta(payload)

    if status is None:
        return {
            "ok": False,
            "reason": "could not read publish status from the publish payload",
            "status": None,
            "generation_id": generation_id,
            "fix_hint": (
                "the publish command returned a payload without a 'status'; "
                "rerun the publish and inspect its JSON output"
            ),
        }
    if status != "published":
        refusal = _publish_refusal_fields(payload)
        if generation_id is None:
            # #1791: the publish failed before a generation existed, so
            # "describe <g>" named nothing. Carry the publish's own cause and
            # point at the command that reports it and republishes the
            # staged records once it is fixed.
            republish = (
                "then run `okg ingest --deployment <name> --json` -- it "
                "reports the refusing cause and republishes the staged "
                "records once it is fixed (no teardown needed)"
            )
            own_hint = refusal.get("publish_fix_hint")
            fix_hint = (
                f"{own_hint}; {republish}"
                if own_hint
                else "the publish was refused before a generation existed; "
                "read publish_error / publish_error_data above, fix that "
                f"cause, {republish}"
            )
        else:
            from okg.substrate.consistency.missing_patterns import (
                blocked_reason_fix_lines,
            )

            # #3624: a block on a missing narrowing or subtype names the
            # pattern and the module to add, ahead of the generic hint.
            pattern_fixes = blocked_reason_fix_lines(
                payload.get("blocked_reason")
                if isinstance(payload, Mapping) else None
            )
            # Each fix line can hold "; " itself, so " | " keeps lines apart.
            fix_hint = " | ".join([
                *pattern_fixes,
                "run `okg generation describe <g>` and fix the blocking invariant",
            ])
        return {
            "ok": False,
            "reason": f"publish generation {generation_id} was not published",
            "status": status,
            "generation_id": generation_id,
            **refusal,
            "fix_hint": fix_hint,
        }
    if generation_id is None:
        return {
            "ok": False,
            "reason": "published payload did not identify its generation",
            "status": status,
            "generation_id": None,
            "fix_hint": "rerun the publish and require a generation_id in its JSON output",
        }

    dead_letter, queue_depth = _publish_dlq_count(
        dsn,
        ignored_runtime_workflow_names=ignored_runtime_workflow_names,
    )
    runtime_queue_error = next(
        (key for key, count in queue_depth.items() if key.startswith("runtime_") and count),
        None,
    )
    if runtime_queue_error is not None:
        return {
            "ok": False,
            "reason": (
                "publish verification could not establish healthy DBOS queue state "
                f"({runtime_queue_error.removeprefix('runtime_')})"
            ),
            "status": status,
            "generation_id": generation_id,
            "nodes_added": nodes_added,
            "edges_added": edges_added,
            "dlq_count": dead_letter,
            "queue_depth": queue_depth,
            "fix_hint": (
                "run `okg jobs status --deployment <name> --json` and repair "
                "DBOS runtime availability or stale active work"
            ),
        }

    caught_up_replay = False
    if require_row_delta:
        if allow_updated_rows:
            if nodes_changed is None or edges_changed is None:
                return {
                    "ok": False,
                    "reason": "could not read publish live-row mutations",
                    "status": status,
                    "generation_id": generation_id,
                    "nodes_changed": nodes_changed,
                    "edges_changed": edges_changed,
                    "dlq_count": dead_letter,
                    "queue_depth": queue_depth,
                }
            if nodes_changed <= 0 or edges_changed <= 0:
                if not (
                    allow_caught_up_replay
                    and _is_caught_up_authoritative_replay(payload)
                ):
                    return {
                        "ok": False,
                        "reason": (
                            "publish did not change live node and edge rows "
                            f"(nodes_changed={nodes_changed}, "
                            f"edges_changed={edges_changed})"
                        ),
                        "status": status,
                        "generation_id": generation_id,
                        "nodes_added": nodes_added,
                        "edges_added": edges_added,
                        "nodes_changed": nodes_changed,
                        "edges_changed": edges_changed,
                        "dlq_count": dead_letter,
                        "queue_depth": queue_depth,
                    }
                # Caught-up authoritative replay over an already-current graph:
                # every requested smoke source reached an authorized zero-output
                # terminal state, so a zero live-row delta proves the pipeline is
                # caught up, not wedged. The dead-letter / runtime-queue checks
                # below (and the blocked-generation check above) still apply.
                caught_up_replay = True
        else:
            nodes_changed = nodes_added
            edges_changed = edges_added
            if nodes_added is None or edges_added is None:
                return {
                    "ok": False,
                    "reason": (
                        "could not read publish row delta "
                        "(nodes_added/edges_added) from the publish payload"
                    ),
                    "status": status,
                    "generation_id": generation_id,
                    "nodes_added": nodes_added,
                    "edges_added": edges_added,
                    "dlq_count": dead_letter,
                    "queue_depth": queue_depth,
                    "fix_hint": (
                        "the publish payload carried no row-delta counts; a "
                        "live-claiming publish must report nodes_added/edges_added"
                    ),
                }
            insufficient_delta = (
                nodes_added <= 0 and edges_added <= 0
                if allow_partial_row_delta
                else nodes_added <= 0 or edges_added <= 0
            )
            if insufficient_delta:
                return {
                    "ok": False,
                    "reason": (
                        (
                            "publish did not add any live node or edge rows "
                            if allow_partial_row_delta
                            else "publish did not add live node and edge rows "
                        )
                        + (
                            f"(nodes_added={nodes_added}, "
                            f"edges_added={edges_added})"
                        )
                    ),
                    "status": status,
                    "generation_id": generation_id,
                    "nodes_added": nodes_added,
                    "edges_added": edges_added,
                    "dlq_count": dead_letter,
                    "queue_depth": queue_depth,
                    "fix_hint": (
                        "check that source facts use subtypes and edge types "
                        "accepted by the active catalog"
                    ),
                }

    if dead_letter:
        return {
            "ok": False,
            "reason": "publish verification found dead-letter runtime work",
            "status": status,
            "generation_id": generation_id,
            "nodes_added": nodes_added,
            "edges_added": edges_added,
            "dlq_count": dead_letter,
            "queue_depth": queue_depth,
            "fix_hint": (
                "inspect DBOS workflow failures and fix the source or runtime "
                "configuration"
            ),
        }

    # Capacity gate (issue #1291): a publish that finished 18s short of its
    # hard statement timeout is one corpus increment away from a wedged
    # publisher, so it is not release proof. Absent evidence fails closed.
    lock_headroom = lock_headroom_from_payload(
        payload,
        not_applicable_reason=lock_headroom_not_applicable,
    )
    # Decision 23a: a measured hold over the bar is a CAPACITY WARNING, not a
    # failure. The publish committed, so the proof passes and carries the
    # measurement. An unmeasured or unbounded hold still fails below: nothing
    # was measured against a bound, so there is no warning to report.
    capacity_warning = (
        lock_hold_capacity_warning(lock_headroom)
        if lock_headroom.get("status") == "blocked"
        else None
    )
    if not lock_headroom["ok"] and capacity_warning is None:
        return {
            "ok": False,
            "reason": lock_headroom["reason"],
            "status": status,
            "generation_id": generation_id,
            "nodes_added": nodes_added,
            "edges_added": edges_added,
            "nodes_changed": nodes_changed,
            "edges_changed": edges_changed,
            "caught_up_replay": caught_up_replay,
            "dlq_count": dead_letter,
            "queue_depth": queue_depth,
            "lock_headroom": lock_headroom,
            "fix_hint": lock_headroom.get("fix_hint"),
        }

    return {
        "ok": True,
        "status": status,
        "generation_id": generation_id,
        "nodes_added": nodes_added,
        "edges_added": edges_added,
        "nodes_changed": nodes_changed,
        "edges_changed": edges_changed,
        "caught_up_replay": caught_up_replay,
        "dlq_count": dead_letter,
        "queue_depth": queue_depth,
        "lock_headroom": lock_headroom,
        **(
            {"capacity_warning": capacity_warning}
            if capacity_warning is not None
            else {}
        ),
    }


# Decision 23a: the proof's status when every check passed but the publish
# held the lock past the capacity bar. Commands that report this proof exit
# with ``EXIT_CAPACITY_WARNING`` (cli/run_summary.py) instead of 0 or 1.
CAPACITY_WARNING_STATUS = "capacity_warning"


def lock_hold_capacity_warning(lock_headroom: Mapping[str, Any]) -> dict[str, Any]:
    """The capacity warning for a measured lock hold over the bar.

    Keeps the hold, the bar, and how the bar was derived, so the operator sees
    the same numbers the old failure showed.
    """
    policy = lock_headroom.get("policy")
    policy = policy if isinstance(policy, Mapping) else {}
    hold_ms = lock_headroom.get("hold_ms")
    max_hold_ms = lock_headroom.get("max_hold_ms")
    running_ms = policy.get("hard_timeout_ms")
    bound_ms = policy.get("policy_timeout_ms")
    factor = policy.get("required_factor")
    source = policy.get("policy_timeout_source")

    def seconds(value: Any) -> str:
        # Three decimals below 10 s, so a lowered bar (90 ms) never reads as
        # a rounded 0.1s beside a 0.4s hold.
        if value is None:
            return "unknown"
        secs = float(value) / 1000.0
        return f"{secs:.1f}s" if secs >= 10.0 else f"{secs:.3f}s"

    if source == "clamped_to_default":
        bound_text = (
            f"the built-in {seconds(bound_ms)} default (the running statement "
            f"timeout {seconds(running_ms)} is longer and was not authorized "
            "to widen the bar)"
        )
    elif source == "raised_timeout_override":
        bound_text = (
            f"the running statement timeout {seconds(bound_ms)} (raised past "
            "the built-in 900.0s default by the operator override)"
        )
    else:
        bound_text = f"the running statement timeout {seconds(bound_ms)}"
    derivation = (
        f"bar = {bound_text} / headroom factor {factor} "
        f"({policy.get('factor_source') or 'unknown source'}) = "
        f"{seconds(max_hold_ms)}"
    )
    return {
        "status": CAPACITY_WARNING_STATUS,
        "check": "publisher.lock_headroom",
        "hold_ms": hold_ms,
        "max_hold_ms": max_hold_ms,
        "hard_timeout_ms": running_ms,
        "policy_timeout_ms": bound_ms,
        "policy_timeout_source": source,
        "required_factor": factor,
        "factor_source": policy.get("factor_source"),
        "bar_derivation": derivation,
        "message": (
            f"{lock_headroom.get('reason')}. The publish "
            f"committed; it held the lock {seconds(hold_ms)} against a "
            f"{seconds(max_hold_ms)} bar; {derivation}"
        ),
        "fix_hint": lock_headroom.get("fix_hint"),
    }


def surface_capacity_warning(
    proof: Any,
    *,
    surface: str,
    stream: Any = None,
) -> dict[str, Any] | None:
    """Print a passed proof's capacity warning and return it for the JSON.

    Callers put the returned block at the top level of their own output as
    ``capacity_warning``. The line goes to stderr so it never corrupts a
    ``--json`` payload on stdout. Exit codes are the caller's business.
    """
    if not isinstance(proof, Mapping) or proof.get("ok") is not True:
        return None
    warning = proof.get("capacity_warning")
    if not isinstance(warning, Mapping):
        return None
    print(
        f"capacity warning ({surface}): {warning.get('message')}",
        file=stream if stream is not None else sys.stderr,
    )
    return dict(warning)


def _publish_dlq_count(
    dsn: str,
    *,
    ignored_runtime_workflow_names: set[str] | None,
) -> tuple[int, dict[str, int]]:
    """Preserve the strict no-keyword call for the default gate."""
    if not ignored_runtime_workflow_names:
        return dlq_count(dsn)
    return dlq_count(
        dsn,
        ignored_workflow_names=set(ignored_runtime_workflow_names),
    )
