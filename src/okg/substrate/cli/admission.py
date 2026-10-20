"""CLI handlers for `okg admission`.

Extracted from ``cli/main.py`` by ``extract-cli-admission``
(2026-05). The admission subcommand family lives here:
``_cmd_admission`` dispatches between submit / decide-promote /
decide; ``_json_arg_or_file`` is the admission-local helper for
loading JSON args from either the CLI or a file path.

The cli.main helpers (``_emit_error``, ``_emit_ok``,
``_resolve_dsn``, etc.) are imported lazily via the shared
``_load_main_helpers()`` pattern (one module-level cache,
populated on first handler invocation) — same shape as Wave D-1
introduced for ``cli/doctor.py``.
"""
from __future__ import annotations

import argparse
import json
import os
import uuid
from pathlib import Path
from typing import Any, Mapping
from okg.substrate.support.user_hints import okg_prefix


# Module-level helper bindings, populated by _load_main_helpers().
_emit_error: Any = None
_emit_ok: Any = None
_json_safe: Any = None
_resolve_dsn: Any = None
SubstrateError: Any = None


def _load_main_helpers() -> None:
    """Lazily import ``cli.main`` helpers + ``SubstrateError``.

    Called from the first line of every ``_cmd_admission*``
    handler. Subsequent calls are a no-op. The names resolve as
    module-level globals at handler-body call sites.
    """
    global _emit_error, _emit_ok, _json_safe
    global _resolve_dsn, SubstrateError
    from okg.substrate.cli.main import (
        _emit_error as _e,
        _emit_ok as _o,
        _json_safe as _j,
        _resolve_dsn as _r,
    )
    from okg.substrate.errors import SubstrateError as _se
    _emit_error = _e
    _emit_ok = _o
    _json_safe = _j
    _resolve_dsn = _r
    SubstrateError = _se


def _admission_row(row: Mapping[str, Any]) -> dict[str, Any]:
    _load_main_helpers()
    return {str(k): _json_safe(v) for k, v in row.items()}


def _print_admission_list(rows: list[dict[str, Any]]) -> None:
    if not rows:
        print("no admission envelopes found")
        return

    print(
        f"{'admission_id':36}  {'status':11}  {'mode':10}  "
        f"{'producer':24}  {'source':20}  {'target_gen':>10}  created_at"
    )
    for row in rows:
        producer = str(row.get("producer_id") or "")
        source = str(row.get("source") or "")
        target_gen = (
            row.get("canonical_generation_ref")
            or row.get("target_generation_ref")
        )
        print(
            f"{row['admission_id']:36}  "
            f"{str(row.get('status') or '')[:11]:11}  "
            f"{str(row.get('admission_mode') or '')[:10]:10}  "
            f"{producer[:24]:24}  "
            f"{source[:20]:20}  "
            f"{str(target_gen or '-'):>10}  "
            f"{row.get('created_at') or ''}"
        )


def _print_admission_show(payload: dict[str, Any]) -> None:
    admission = payload["admission"]
    print(f"admission_id: {admission['admission_id']}")
    for key in (
        "deployment_name",
        "status",
        "admission_mode",
        "producer_kind",
        "producer_id",
        "intent",
        "source",
        "run_id",
        "target_generation_ref",
        "canonical_generation_ref",
        "idempotency_key",
        "created_at",
        "updated_at",
        "decided_at",
        "published_at",
    ):
        print(f"{key}: {admission.get(key)}")

    print("events:")
    for event in payload["events"]:
        actor = event.get("actor") or "-"
        reason = event.get("reason") or "-"
        print(
            f"  {event['event_id']}: {event['event_type']} "
            f"actor={actor} reason={reason} at={event.get('created_at')}"
        )

    print("record_refs:")
    for ref in payload["record_refs"]:
        count = ref.get("record_count")
        fact_min = ref.get("fact_id_min")
        fact_max = ref.get("fact_id_max")
        print(
            f"  {ref['ref_id']}: {ref['ref_kind']} key={ref['ref_key']} "
            f"records={count} facts={fact_min}..{fact_max}"
        )


def _cmd_admission(args: argparse.Namespace, *, as_json: bool) -> int:
    """Operator surface for admission envelopes."""

    _load_main_helpers()
    subcmd = getattr(args, "subcmd", None)
    if subcmd not in {"list", "show", "submit", "decide"}:
        return _emit_error(
            SubstrateError(
                "okg admission: expected `list`, `show`, `submit`, "
                "or `decide`",
                fix_hint="okg admission list --deployment <name>",
            ),
            as_json=as_json,
        )

    try:
        dsn = _resolve_dsn(args)
    except SubstrateError as err:
        return _emit_error(err, as_json=as_json)

    if subcmd == "submit":
        return _cmd_admission_submit(args, dsn=dsn, as_json=as_json)
    if subcmd == "decide":
        return _cmd_admission_decide(args, dsn=dsn, as_json=as_json)

    from okg.substrate.db.connection import connect as db_connect
    from psycopg.rows import dict_row

    with db_connect(dsn, autocommit=True, row_factory=dict_row) as conn:
        if subcmd == "list":
            clauses: list[str] = []
            params: list[Any] = []
            for attr, column in (
                ("deployment", "deployment_name"),
                ("status", "status"),
                ("mode", "admission_mode"),
                ("source", "source"),
            ):
                value = getattr(args, attr, None)
                if value:
                    clauses.append(f"{column} = %s")
                    params.append(value)
            if getattr(args, "held", False):
                # Every mode whose runs finalize `pending` for a decision,
                # from the same set that routes them to the held branch
                # (issue #1752).
                from okg.substrate.admission import HELD_ADMISSION_MODES

                clauses.append("admission_mode = ANY(%s)")
                params.append(sorted(HELD_ADMISSION_MODES))
            where_sql = (
                " WHERE " + " AND ".join(clauses)
                if clauses
                else ""
            )
            limit = max(1, min(int(args.limit), 500))
            params.append(limit)
            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    SELECT admission_id, deployment_name, status,
                           admission_mode, producer_kind, producer_id,
                           intent, source, run_id,
                           target_generation_ref, canonical_generation_ref,
                           created_at, updated_at, decided_at, published_at
                      FROM okg.v_admission_envelopes
                      {where_sql}
                     ORDER BY created_at DESC, admission_id DESC
                     LIMIT %s
                    """,
                    params,
                )
                rows = [_admission_row(row) for row in cur.fetchall()]
            payload = {"admissions": rows, "count": len(rows)}
            if as_json:
                _emit_ok(payload, as_json=True)
            else:
                _print_admission_list(rows)
            return 0

        admission_id = str(args.admission_id)
        clauses = ["admission_id = %s"]
        params = [admission_id]
        if getattr(args, "deployment", None):
            clauses.append("deployment_name = %s")
            params.append(args.deployment)
        where_sql = " AND ".join(clauses)

        with conn.cursor() as cur:
            cur.execute(
                f"""
                SELECT *
                  FROM okg.v_admission_envelopes
                 WHERE {where_sql}
                """,
                params,
            )
            row = cur.fetchone()
            if row is None:
                return _emit_error(
                    SubstrateError(
                        f"admission {admission_id} not found",
                        data={"admission_id": admission_id},
                        fix_hint=(
                            "list recent admissions with "
                            "`okg admission list --deployment <name>`"
                        ),
                    ),
                    as_json=as_json,
                )
            admission = _admission_row(row)

            cur.execute(
                """
                SELECT *
                  FROM okg.v_admission_events
                 WHERE admission_id = %s
                 ORDER BY created_at ASC, event_id ASC
                """,
                (admission_id,),
            )
            events = [_admission_row(event) for event in cur.fetchall()]

            cur.execute(
                """
                SELECT *
                  FROM okg.v_admission_record_refs
                 WHERE admission_id = %s
                 ORDER BY created_at ASC, ref_id ASC
                """,
                (admission_id,),
            )
            refs = [_admission_row(ref) for ref in cur.fetchall()]

    payload = {
        "admission": admission,
        "events": events,
        "record_refs": refs,
    }
    if as_json:
        _emit_ok(payload, as_json=True)
    else:
        _print_admission_show(payload)
    return 0


def _cmd_admission_submit(
    args: argparse.Namespace,
    *,
    dsn: str,
    as_json: bool,
) -> int:
    _load_main_helpers()
    deployment = getattr(args, "deployment", None) or os.environ.get(
        "OKG_DEPLOYMENT",
    )
    if not deployment:
        return _emit_error(
            SubstrateError(
                "okg admission submit requires --deployment",
                fix_hint=(
                    "okg admission submit --deployment <name> "
                    "--producer-id <id> --payload-json '{...}'"
                ),
            ),
            as_json=as_json,
        )
    try:
        payload = _json_arg_or_file(
            args,
            json_attr="payload_json",
            file_attr="payload_file",
            default={},
        )
        authority = _json_arg_or_file(
            args,
            json_attr="authority_json",
            file_attr=None,
            default={},
        )
        check_summary = _json_arg_or_file(
            args,
            json_attr="check_summary_json",
            file_attr=None,
            default={},
        )
    except SubstrateError as err:
        return _emit_error(err, as_json=as_json)

    from okg.substrate.runtime.config import validate_admission_intent

    try:
        validate_admission_intent(args.intent)
    except ValueError as exc:
        return _emit_error(
            SubstrateError(
                str(exc),
                data={"intent": args.intent},
                fix_hint=(
                    "use a bounded machine identifier such as "
                    "review_proposal"
                ),
            ),
            as_json=as_json,
        )

    # Inline path, mirroring `okg admission decide`: a deployment with
    # runtime.enabled false has no DBOS worker to consume the queue, so
    # enqueue-and-poll would wait until its timeout. The submit write is
    # one transaction, so run it synchronously against the resolved DSN.
    # A runtime-enabled deployment keeps the DBOS path below unchanged.
    runtime_enabled = _deployment_runtime_enabled(deployment)
    if isinstance(runtime_enabled, SubstrateError):
        return _emit_error(runtime_enabled, as_json=as_json)
    if not runtime_enabled:
        return _cmd_admission_submit_inline(
            args,
            dsn=dsn,
            deployment=deployment,
            payload=payload,
            authority=authority,
            check_summary=check_summary,
            as_json=as_json,
        )

    from okg.substrate.runtime.admin import client_enqueue_admission_submit

    try:
        result = client_enqueue_admission_submit(
            deployment=deployment,
            dsn=dsn,
            payload=payload,
            producer_id=args.producer_id,
            producer_kind=args.producer_kind,
            intent=args.intent,
            idempotency_key=args.idempotency_key,
            authority=authority,
            check_summary=check_summary,
            timeout_seconds=float(args.max_wait_seconds),
            poll_interval_seconds=float(args.poll_interval_seconds),
        )
    except Exception as exc:  # noqa: BLE001
        return _emit_error(
            SubstrateError(
                f"DBOS-path admission submit enqueue failed: {exc}",
                data={
                    "deployment": deployment,
                    "producer_id": args.producer_id,
                    "producer_kind": args.producer_kind,
                    "intent": args.intent,
                },
                fix_hint=(
                    f"check the DBOS runtime worker with `{okg_prefix()} "
                    f"dogfood status --deployment {deployment} --json`; "
                    "admission submit does not fall back to an OKG job runner"
                ),
            ),
            as_json=as_json,
        )
    out = {
        "workflow_id": result.workflow_id,
        "workflow_status": result.status,
        "output": result.output,
        "error": result.error,
        "elapsed_seconds": result.elapsed_seconds,
    }
    if as_json:
        _emit_ok(out, as_json=True)
    else:
        print(
            f"admission submit: workflow={result.workflow_id} "
            f"status={result.status}"
        )
        if result.output:
            print(f"admission_id: {result.output.get('admission_id')}")
    return 0 if result.status == "SUCCESS" else 1


def _deployment_runtime_enabled(deployment: str) -> "bool | Any":
    """Return the deployment's runtime.enabled, or a SubstrateError.

    Inline admission runs only when the manifest resolves and its runtime
    is not enabled (runtime.enabled false, or no runtime block, whose
    default is off); an unresolvable manifest refuses rather than
    guessing a path.
    """
    _load_main_helpers()
    try:
        from okg.substrate.deployments import resolve_deployment

        return bool(resolve_deployment(deployment).runtime.enabled)
    except Exception as exc:  # noqa: BLE001 - surface deployment policy failures.
        return SubstrateError(
            f"could not resolve admission runtime policy: {exc}",
            data={"deployment": deployment},
            fix_hint=(
                "fix or select the deployment manifest; inline admission "
                "runs only for a resolvable manifest whose runtime is not "
                "enabled"
            ),
        )


def _cmd_admission_submit_inline(
    args: argparse.Namespace,
    *,
    dsn: str,
    deployment: str,
    payload: dict[str, Any],
    authority: dict[str, Any],
    check_summary: dict[str, Any],
    as_json: bool,
) -> int:
    """Run the admission submit write in-process (runtime disabled)."""
    _load_main_helpers()
    from okg.substrate.producer_authority import ProducerPolicyViolation
    from okg.substrate.runtime.admission_workflows import (
        _run_submit_with_runtime_approver,
    )

    # The same body the DBOS submit step runs: it takes the idempotency
    # advisory lock and returns an existing envelope on replay before any
    # profile or payload write. A runtime-disabled deployment has no runtime
    # approver, so none is passed.
    try:
        output = _run_submit_with_runtime_approver(
            deployment=deployment,
            payload=payload,
            producer_id=args.producer_id,
            producer_kind=args.producer_kind,
            intent=args.intent,
            idempotency_key=args.idempotency_key,
            authority=authority or None,
            check_summary=check_summary or None,
            approver_config=None,
            dsn=dsn,
        )
    except SubstrateError as err:
        return _emit_error(err, as_json=as_json)
    except ProducerPolicyViolation as exc:
        return _emit_error(
            SubstrateError(
                exc.message,
                data={
                    "deployment": deployment,
                    "code": exc.code,
                    "verdict": exc.verdict,
                },
                fix_hint="fix the producer policy in --authority-json",
            ),
            as_json=as_json,
        )
    out = {
        "workflow_id": None,
        "workflow_status": "INLINE",
        "output": _json_safe(output),
        "error": None,
        "elapsed_seconds": None,
    }
    if as_json:
        _emit_ok(out, as_json=True)
    else:
        print("admission submit: inline (runtime disabled)")
        print(f"admission_id: {output.get('admission_id')}")
    return 0


def _cmd_admission_decide_promote(
    args: argparse.Namespace,
    *,
    dsn: str,
    deployment: str,
    as_json: bool,
) -> int:
    """``okg admission decide --action promote --request-id <uuid>``.

    Opens a lens-promotion admission envelope from a committed
    overlay request and prints the new envelope's ``admission_id``
    and ``status`` (typically ``'pending'``).
    """

    _load_main_helpers()
    request_id_raw = getattr(args, "request_id", None)
    if not request_id_raw:
        return _emit_error(
            SubstrateError(
                "okg admission decide --action promote requires "
                "--request-id <uuid>",
                fix_hint=(
                    "okg admission decide --deployment <name> "
                    "--action promote --request-id <uuid>"
                ),
            ),
            as_json=as_json,
        )
    try:
        request_id = uuid.UUID(str(request_id_raw))
    except ValueError as exc:
        return _emit_error(
            SubstrateError(
                f"--request-id is not a valid UUID: {exc}",
                data={"request_id": request_id_raw},
            ),
            as_json=as_json,
        )

    from okg.substrate.db.connection import connect as db_connect

    from okg.substrate.lenses import (
        LensPromotionError,
        LensPromotionMappingError,
        LensPromotionNotSupported,
        promote_lens_overlay,
    )

    target_branch = getattr(args, "target_branch", None)
    try:
        with db_connect(dsn) as conn:
            result = promote_lens_overlay(
                conn,
                request_id=request_id,
                actor_id=str(args.actor),
                deployment_name=deployment,
                target_branch_name=target_branch,
            )
    except LensPromotionNotSupported as exc:
        return _emit_error(
            SubstrateError(
                str(exc),
                code="lens_promotion_not_supported",
                fix_hint=exc.fix_hint,
                data={"definition_id": exc.definition_id},
            ),
            as_json=as_json,
        )
    except LensPromotionMappingError as exc:
        return _emit_error(
            SubstrateError(
                str(exc),
                code="lens_promotion_mapping_error",
            ),
            as_json=as_json,
        )
    except LensPromotionError as exc:
        return _emit_error(
            SubstrateError(
                str(exc),
                code="lens_promotion_error",
            ),
            as_json=as_json,
        )

    payload = {
        "admission_id": result.admission_id,
        "status": result.status,
        "inserted": result.inserted,
        "request_id": str(result.request_id),
        "definition_id": result.definition_id,
        "producer_id": result.producer_id,
        "target_branch_name": target_branch,
    }
    if as_json:
        _emit_ok(payload, as_json=True)
    else:
        action = "opened" if result.inserted else "coalesced"
        print(
            f"admission decide --action promote: {action} "
            f"admission_id={result.admission_id} "
            f"status={result.status}"
        )
    return 0


def _cmd_admission_decide(
    args: argparse.Namespace,
    *,
    dsn: str,
    as_json: bool,
) -> int:
    _load_main_helpers()
    deployment = getattr(args, "deployment", None) or os.environ.get(
        "OKG_DEPLOYMENT",
    )
    if not deployment:
        return _emit_error(
            SubstrateError(
                "okg admission decide requires --deployment",
                fix_hint=(
                    "okg admission decide --deployment <name> "
                    "<admission_id> --decision accept"
                ),
            ),
            as_json=as_json,
        )

    if getattr(args, "action", None) == "promote":
        return _cmd_admission_decide_promote(
            args, dsn=dsn, deployment=deployment, as_json=as_json
        )

    if not args.admission_id:
        return _emit_error(
            SubstrateError(
                "okg admission decide requires <admission_id>",
                fix_hint=(
                    "okg admission decide --deployment <name> "
                    "<admission_id> --decision accept"
                ),
            ),
            as_json=as_json,
        )
    if not args.decision:
        return _emit_error(
            SubstrateError(
                "okg admission decide requires --decision",
                fix_hint=(
                    "okg admission decide --deployment <name> "
                    "<admission_id> --decision accept"
                ),
            ),
            as_json=as_json,
        )
    try:
        verdict = _json_arg_or_file(
            args,
            json_attr="verdict_json",
            file_attr=None,
            default={},
        )
    except SubstrateError as err:
        return _emit_error(err, as_json=as_json)

    # Inline fallback (make-admission-review-load-bearing): on
    # deployments without the DBOS runtime (academic-researcher installs
    # default to runtime.enabled: false) there is no worker to
    # consume the queue, so enqueue-and-poll would hang. The decide
    # path is a plain transactional write, so run it synchronously
    # against the resolved DSN instead.
    runtime_enabled = _deployment_runtime_enabled(deployment)
    if isinstance(runtime_enabled, SubstrateError):
        return _emit_error(runtime_enabled, as_json=as_json)
    if not runtime_enabled:
        from okg.substrate.runtime.admission_workflows import _run_decide

        try:
            output = _run_decide(
                deployment=deployment,
                admission_id=str(args.admission_id),
                decision=str(args.decision),
                actor=str(args.actor),
                reason=args.reason,
                verdict=verdict,
                dsn=dsn,
            )
        except SubstrateError as err:
            return _emit_error(err, as_json=as_json)
        out = {
            "workflow_id": None,
            "workflow_status": "INLINE",
            "output": _json_safe(output),
            "error": None,
            "elapsed_seconds": None,
        }
        if as_json:
            _emit_ok(out, as_json=True)
        else:
            print("admission decide: inline (runtime disabled)")
            _print_decide_outcome(output)
        return 0

    from okg.substrate.runtime.admin import client_enqueue_admission_decide

    try:
        result = client_enqueue_admission_decide(
            deployment=deployment,
            dsn=dsn,
            admission_id=args.admission_id,
            decision=args.decision,
            actor=args.actor,
            reason=args.reason,
            verdict=verdict,
            timeout_seconds=float(args.max_wait_seconds),
            poll_interval_seconds=float(args.poll_interval_seconds),
        )
    except Exception as exc:  # noqa: BLE001
        return _emit_error(
            SubstrateError(
                f"DBOS-path admission decide enqueue failed: {exc}",
                data={
                    "deployment": deployment,
                    "admission_id": args.admission_id,
                    "decision": args.decision,
                    "actor": args.actor,
                },
                fix_hint=(
                    f"check the DBOS runtime worker with `{okg_prefix()} "
                    f"dogfood status --deployment {deployment} --json`; "
                    "runtime-enabled admission decide does not fall back "
                    "to an OKG job runner"
                ),
            ),
            as_json=as_json,
        )
    out = {
        "workflow_id": result.workflow_id,
        "workflow_status": result.status,
        "output": result.output,
        "error": result.error,
        "elapsed_seconds": result.elapsed_seconds,
    }
    if as_json:
        _emit_ok(out, as_json=True)
    else:
        print(
            f"admission decide: workflow={result.workflow_id} "
            f"status={result.status}"
        )
        if result.output:
            _print_decide_outcome(result.output)
    return 0 if result.status == "SUCCESS" else 1


def _print_decide_outcome(output: dict[str, Any]) -> None:
    """Print a decide result for a person: its status and, when the decide
    was refused, the reason and the operator's next step."""
    print(f"admission_status: {output.get('status')}")
    if output.get("status") != "refused":
        return
    if output.get("reason"):
        print(f"reason: {output['reason']}")
    fix_hint = output.get("fix_hint") or (output.get("error") or {}).get(
        "fix_hint",
    )
    if fix_hint:
        print(f"fix_hint: {fix_hint}")


def _json_arg_or_file(
    args: argparse.Namespace,
    *,
    json_attr: str,
    file_attr: str | None,
    default: dict[str, Any],
) -> dict[str, Any]:
    _load_main_helpers()
    raw = getattr(args, json_attr, None)
    if raw:
        source = f"--{json_attr.replace('_', '-')}"
        text = raw
    elif file_attr and getattr(args, file_attr, None):
        source = f"--{file_attr.replace('_', '-')}"
        text = Path(getattr(args, file_attr)).read_text(encoding="utf-8")
    else:
        return dict(default)
    try:
        parsed = json.loads(text)
    except json.JSONDecodeError as exc:
        raise SubstrateError(
            f"{source} must be valid JSON: {exc}",
            data={"source": source},
        ) from exc
    if not isinstance(parsed, dict):
        raise SubstrateError(
            f"{source} must decode to a JSON object",
            data={"source": source, "type": type(parsed).__name__},
        )
    return parsed
