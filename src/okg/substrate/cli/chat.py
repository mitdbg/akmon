"""Chat-surface operator verbs (PACT ``chat-source-admin``).

Two verbs, both thin:

``okg chat source-admin``
    Read the deployment's write-enabled MCP endpoint back over an
    authenticated ``tools/list``, decide the transport precondition, and
    report the admin preset that WOULD be projected. It renders nothing
    into an Open WebUI instance. This verb is the readback and the
    refusal.

    The WRITE belongs to ``okg chat sync`` (PACT
    ``chat-source-admin-writer``): when the deployment declares
    ``chat.sso.admin_group``, sync projects an admin preset through the same
    ``project_admin_preset``, registers its connection, writes the row with
    the admin group's grant, and reads both back. The ids differ: this verb
    reports ``<deployment>-admin`` (or ``--preset-id``) and binds no
    connection id, while sync writes ``<chat preset id>-admin`` (for example
    ``okg-<deployment>-admin``) bound to ``<mcp server id>-admin``, and
    decides transport on the instance's MCP URL rather than ``--host``. Its gating claim is
    SPECIFIED, not enforced: the grant is visibility, which Open WebUI
    bypasses for every instance admin, and the per-caller refusal is
    blocked on ``mcp-principal-mapping``.

``okg chat sso-check``
    Decide the deployment's identity model (PACT
    ``chat-sso-integration``) and refuse rather than guess: a
    half-declared identity provider, a mode that disagrees with
    ``nomos.principal_mapping``, or a console published under a path
    prefix with no evidence that its bundle is base-aware. That last one
    is a gate on the BUILD, not a ban on the topology — pass
    ``--console-bundle-index`` to have it measured, or
    ``--console-base-aware`` to declare it. With ``--render`` it prints
    the production front's nginx or oauth2-proxy configuration; the
    upstreams are command-line inputs because reverse-proxy topology is
    operator state, never manifest content.

``okg chat sync``
    The deployment lifecycle verb for the chat surface (PACT
    ``chat-sync-projection``). It renders the deployment's declared
    ``chat:`` block into its Open WebUI instance across five written
    surfaces plus one probe, and proves every write by reading it back
    through the instance's own HTTP API. A value that cannot be read
    back, a value that disagrees with the effective desired state, and a
    surface that could not be written each fail the run with the surface,
    the expected value and the observed value named. The instance URL and
    its admin credential are operator environment
    (``OKG_CHAT_INSTANCE_URL`` / ``OKG_CHAT_ADMIN_TOKEN``) and the run
    refuses by name when either is unset.

``okg chat source-retry``
    Retries a document whose ingest failed. It reads the DLQ
    authority, builds the ONLY command chain that can clear a DLQ row
    (``okg jobs enqueue-source --supersedes-workflow-id <failed>``), runs
    it under ``--apply``, and re-reads the DLQ to judge the outcome. It
    holds no retry queue and no failure record of its own. Under
    ``--apply`` it exits NON-ZERO whenever the retry is not proven to
    have succeeded; without ``--apply`` it prints the chain it WOULD run
    and exits zero with ``ok: false``, because a plan is not a retry.

Neither verb adds a source-administration capability: the nine MCP
source-admin tools and the existing recovery commands are the whole
mechanism.
"""
from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
from typing import Any, Callable, Mapping, Sequence  # noqa: F401 - Mapping used in _load_chat_and_mapping

from okg.substrate.errors import SubstrateError
from okg.substrate.cli.child_argv import running_okg_argv


CommandRunner = Callable[[Sequence[str]], "subprocess.CompletedProcess[str]"]


def _emit(payload: Mapping[str, Any], *, as_json: bool) -> None:
    if as_json or os.environ.get("OKG_AGENT") == "1":
        print(json.dumps(payload, sort_keys=True, default=str))
        return
    print(f"chat {payload.get('surface')}: {payload.get('status')}")


def _emit_error(err: SubstrateError, *, as_json: bool) -> int:
    if as_json or os.environ.get("OKG_AGENT") == "1":
        print(json.dumps(err.to_json(), sort_keys=True), file=sys.stderr)
        return 1
    fix = f"\n  fix_hint: {err.fix_hint}" if err.fix_hint else ""
    print(f"error [{err.code}]: {err.message}{fix}", file=sys.stderr)
    if err.data:
        print(f"  data: {err.data}", file=sys.stderr)
    return 1


# ---------------------------------------------------------------------------
# okg chat source-admin
# ---------------------------------------------------------------------------

def _manifest_inputs(args: argparse.Namespace) -> Any:
    """The deployment's typed ``chat:`` block, or None when it has none.

    Consuming ``chat-manifest-block`` rather than making the operator
    restate the endpoint port, the admin group, and the credential
    variable on the command line: a hand-restated fact can disagree with
    the manifest, and a disagreement nobody reads is not detectable.
    An explicit flag still wins, so an operator can point the readback at
    a different endpoint without editing the manifest.
    """
    from okg.substrate import fastyaml
    from okg.substrate.chat.source_admin import admin_binding_inputs
    from okg.substrate.deployments import find_deployment_dir

    try:
        dep_dir = find_deployment_dir(args.deployment)
    except Exception:  # noqa: BLE001 — an unresolvable deployment has no block
        return None
    if dep_dir is None:
        return None
    manifest = dep_dir / "deployment.yaml"
    if not manifest.is_file():
        return None
    raw = fastyaml.safe_load(manifest.read_text()) or {}
    from okg.substrate.deployments.chat_block import parse_chat_block

    block = parse_chat_block(raw, manifest_path=manifest)
    if block is None or not block.enabled:
        return None
    return admin_binding_inputs(block, host=args.host)


def _cmd_source_admin(args: argparse.Namespace, *, as_json: bool) -> int:
    from okg.substrate.chat.source_admin import (
        ChatSourceAdminError,
        project_admin_preset,
        read_live_tools,
        require_admin_transport,
        user_preset_tools,
    )

    preset_id = args.preset_id or f"{args.deployment}-admin"
    try:
        declared = _manifest_inputs(args)
        endpoint = args.endpoint or (
            declared.endpoint if declared is not None else None
        )
        if not endpoint:
            raise ChatSourceAdminError(
                "no MCP endpoint: neither --endpoint nor the deployment's "
                "chat.mcp.port is declared. The endpoint is not guessed.",
                data={"deployment": args.deployment, "reason": "no_endpoint"},
                fix_hint="pass --endpoint, or declare chat.mcp.port",
            )
        groups = tuple(args.group or ()) or (
            declared.access_group_names if declared is not None else ()
        )
        auth_token_env = args.auth_token_env or (
            declared.auth_token_env if declared is not None else None
        )
        # BEFORE the readback. A non-loopback unauthenticated endpoint is
        # refused for the reason it is refused for, not for whatever the
        # connection attempt happened to return. `auth_observed` is False
        # here on purpose: nothing has been observed yet.
        require_admin_transport(
            deployment=args.deployment,
            preset_id=preset_id,
            endpoint=endpoint,
            auth_token_env=auth_token_env,
        )
        live = read_live_tools(endpoint, auth_token_env=auth_token_env)
        user_tools = user_preset_tools(tuple(args.user_tool or ()), live=live)
        projection = project_admin_preset(
            deployment=args.deployment,
            preset_id=preset_id,
            live=live,
            access_group_names=groups,
            auth_token_env=auth_token_env,
            requested_tools=tuple(args.tool or ()),
        )
    except ChatSourceAdminError as err:
        return _emit_error(err, as_json=as_json)
    except SubstrateError as err:
        return _emit_error(err, as_json=as_json)

    payload = {
        "schema": "okg.chat.source_admin.preset.v1",
        "surface": "source-admin",
        "status": "ok",
        "ok": True,
        "live_tools": list(live.tool_names),
        "auth_posture": live.auth_posture,
        "user_preset_tool_ids": list(user_tools),
        "inputs_from_manifest": declared is not None,
        **projection.to_dict(),
    }
    _emit(payload, as_json=as_json)
    # The server's own loopback warning is CARRIED, not suppressed. The
    # guard's warn tuple otherwise reaches only run_server's log; on the
    # loopback interim this is the readback the evidence can cite.
    if projection.transport.warning:
        print(
            f"warning: {projection.transport.warning}",
            file=sys.stderr,
        )
    return 0


# ---------------------------------------------------------------------------
# okg chat source-retry
# ---------------------------------------------------------------------------

def _default_runner(argv: Sequence[str]) -> "subprocess.CompletedProcess[str]":
    return subprocess.run(
        list(argv), capture_output=True, text=True, check=False,
    )


def _read_dlq(dsn: str) -> dict[str, Any]:
    from okg.substrate.db.connection import connect as db_connect

    from okg.substrate.runtime_dlq import current_runtime_dlq

    with db_connect(dsn) as conn, conn.cursor() as cur:
        return current_runtime_dlq(cur)


def _observe_source(
    dsn: str, deployment: str, source: str,
) -> tuple[Any, int | None, Any]:
    """Observe ``(watermark, row_count, last_run_id)`` for one source.

    Read through the SAME authorities ``list_sources`` and ``add_source``
    use — ``_watermark_for`` over ``okg.source_watermarks``,
    ``_last_run_counts`` over the runner's own per-run payload — rather
    than a query written here. A private query drifts from the tables the
    rest of the substrate reads, and a drifted query returns "no rows",
    which is indistinguishable from "nothing changed".

    ``_last_run_counts`` rather than the legacy fact-log diff because the
    fact log is BLIND on Chronos-native deployments: it answers 0/0 for
    every source, which would make every retry look effect-free. The
    run id is carried alongside the counts so a new run with an
    identical row delta still reads as a change.

    ``None`` means NOT OBSERVED. The retry verdict fails closed on an
    unobserved signal, so a broken read here is reported as an unproven
    effect, never as a successful retry.
    """
    from okg.substrate.db.connection import connect as db_connect

    from okg.substrate.mcp.source_admin import (
        _counts_for,
        _last_run_counts,
        _load_registry_yaml,
        _watermark_for,
    )

    try:
        _path, raw = _load_registry_yaml(deployment)
        entry = (raw.get("sources") or {}).get(source) or {}
        ownership_id = entry.get("ownership_id") or source
    except FileNotFoundError:
        ownership_id = source
    try:
        with db_connect(dsn) as conn:
            watermark = _watermark_for(conn, ownership_id).get(
                "last_ingest_at"
            )
            last_run = _last_run_counts(conn, deployment, ownership_id)
            counts = _counts_for(conn, ownership_id)
    except Exception as exc:  # noqa: BLE001 — an unreadable signal is UNKNOWN
        print(
            f"warning: could not observe {source!r} watermark/row count "
            f"({exc}); the retry verdict will report the effect as "
            "unproven rather than assume it",
            file=sys.stderr,
        )
        return None, None, None
    if last_run is not None:
        row_count = int(last_run.get("nodes_appended") or 0) + int(
            last_run.get("edges_appended") or 0
        )
        return watermark, row_count, last_run.get("run_id")
    nodes = int(counts.get("node_count", 0) or 0)
    edges = int(counts.get("edge_count", 0) or 0)
    if not counts.get("count_available", True) or (nodes == 0 and edges == 0):
        # 0/0 from the legacy fact log is "unavailable", never "zero
        # rows" — the same rule add_source applies to its own envelope.
        return watermark, None, None
    return watermark, nodes + edges, None


def _cmd_source_retry(
    args: argparse.Namespace,
    *,
    as_json: bool,
    runner: CommandRunner | None = None,
) -> int:
    from okg.substrate.chat.source_admin import (
        RetryObservation,
        retry_plan,
        retry_verdict,
    )
    from okg.substrate.cli.dsn_authority import resolve_read_dsn

    run = runner or _default_runner
    resolution = resolve_read_dsn(args, surface="chat.source-retry")
    deployment = resolution.deployment or args.deployment
    before_dlq = _read_dlq(resolution.dsn)
    plan = retry_plan(
        deployment=deployment,
        source=args.source,
        dlq_payload=before_dlq,
        # The RUNNING interpreter, not a bare `okg` from PATH: a
        # different install on PATH would enqueue against a different
        # code revision than the one judging the result. `-P` keeps the
        # operator's folder off the child's sys.path (issue #2301).
        okg_argv_prefix=tuple(running_okg_argv()),
    )
    if not plan.applicable:
        payload = {
            "schema": "okg.chat.source_retry.v1",
            "surface": "source-retry",
            "status": "not_applicable",
            "ok": False,
            "deployment": deployment,
            "source": args.source,
            "refusal": plan.refusal,
        }
        _emit(payload, as_json=as_json)
        return 1

    if not args.apply:
        payload = {
            "schema": "okg.chat.source_retry.v1",
            "surface": "source-retry",
            "status": "planned",
            # A plan is not a retry. `ok` stays False so a caller that
            # forgot --apply cannot read the exit code as a cleared DLQ.
            "ok": False,
            "deployment": deployment,
            "source": args.source,
            "failed_workflow_ids": list(plan.failed_workflow_ids),
            "commands": list(plan.rendered),
        }
        _emit(payload, as_json=as_json)
        return 0

    wm_before, rows_before, run_before = _observe_source(
        resolution.dsn, deployment, args.source,
    )
    before = RetryObservation(
        dlq_payload=before_dlq,
        watermark=wm_before,
        queue_state={
            "current_count": before_dlq.get("current_count"),
            "last_run_id": run_before,
        },
        row_count=rows_before,
    )
    results: list[dict[str, Any]] = []
    for argv in plan.commands:
        completed = run(argv)
        results.append({
            "command": " ".join(argv),
            "returncode": completed.returncode,
            "stdout": (completed.stdout or "").strip()[-2000:],
            "stderr": (completed.stderr or "").strip()[-2000:],
        })
    after_dlq = _read_dlq(resolution.dsn)
    wm_after, rows_after, run_after = _observe_source(
        resolution.dsn, deployment, args.source,
    )
    after = RetryObservation(
        dlq_payload=after_dlq,
        watermark=wm_after,
        queue_state={
            "current_count": after_dlq.get("current_count"),
            "last_run_id": run_after,
        },
        row_count=rows_after,
    )
    verdict = retry_verdict(
        plan=plan, before=before, after=after, command_results=results,
    )
    payload = {
        "schema": "okg.chat.source_retry.v1",
        "surface": "source-retry",
        "status": "succeeded" if verdict.succeeded else "not_succeeded",
        "ok": verdict.succeeded,
        "deployment": deployment,
        "source": args.source,
        "failed_workflow_ids": list(plan.failed_workflow_ids),
        "commands": list(plan.rendered),
        "command_results": results,
        "dlq_row_still_current": verdict.dlq_row_still_current,
        "still_current_workflow_ids": list(
            verdict.still_current_workflow_ids
        ),
        "effect_free": verdict.effect_free,
        "unobserved_signals": list(verdict.unobserved_signals),
        "failed_commands": list(verdict.failed_commands),
        "reasons": list(verdict.reasons),
        "dlq_before": before_dlq.get("current_count"),
        "dlq_after": after_dlq.get("current_count"),
    }
    _emit(payload, as_json=as_json)
    return 0 if verdict.succeeded else 1


# ---------------------------------------------------------------------------
# okg chat sso-check
# ---------------------------------------------------------------------------

def _load_chat_and_mapping(deployment: str) -> tuple[Any, Any]:
    """The two halves of the identity model, read from one manifest.

    Read together rather than separately because the whole point of the
    check is that they can disagree: fetching them from one load means a
    reported skew is a skew in what the deployment declares, not a skew
    between two moments of reading it.
    """
    from okg.substrate import fastyaml
    from okg.substrate.deployments import find_deployment_dir
    from okg.substrate.deployments.chat_block import parse_chat_block
    from okg.substrate.mcp.principal_mapping import parse_principal_mapping

    dep_dir = find_deployment_dir(deployment)
    if dep_dir is None or not (dep_dir / "deployment.yaml").is_file():
        raise SubstrateError(
            f"deployment {deployment!r} has no deployment.yaml to read an "
            "identity model from",
            data={"deployment": deployment, "reason": "manifest_absent"},
            fix_hint="check the deployment name, or run `okg install`",
        )
    manifest = dep_dir / "deployment.yaml"
    raw = fastyaml.safe_load(manifest.read_text()) or {}
    chat = parse_chat_block(raw, manifest_path=manifest)
    nomos = raw.get("nomos")
    mapping_raw = nomos.get("principal_mapping") if isinstance(nomos, Mapping) else None
    mapping = parse_principal_mapping(mapping_raw) if mapping_raw else None
    return chat, mapping


def _cmd_sso_check(args: argparse.Namespace, *, as_json: bool) -> int:
    from okg.substrate.chat.sso import (
        CHAT_SURFACE,
        CONSOLE_SURFACE,
        ConsoleBuild,
        Upstream,
        render_nginx_site,
        render_oauth2_proxy_cfg,
        require_sso_startup_contract,
    )

    try:
        chat, mapping = _load_chat_and_mapping(args.deployment)
        decision = require_sso_startup_contract(
            deployment=args.deployment,
            chat=chat,
            principal_mapping=mapping,
            console_build=ConsoleBuild(
                declared_base_aware=bool(
                    getattr(args, "console_base_aware", False)
                ),
                index_path=getattr(args, "console_bundle_index", None),
            ),
        )
        if args.render == "oauth2-proxy":
            print(render_oauth2_proxy_cfg(decision), end="")
            return 0
        if args.render == "nginx":
            missing = [
                name for name, value in (
                    ("--chat-upstream", args.chat_upstream),
                    ("--console-upstream", args.console_upstream),
                )
                if not value
            ]
            if missing:
                raise SubstrateError(
                    "rendering the nginx front needs both upstreams: "
                    + ", ".join(missing)
                    + ". They are operator-state topology and are never read "
                    "from the manifest",
                    data={"deployment": args.deployment, "missing": missing},
                    fix_hint=(
                        "pass --chat-upstream http://127.0.0.1:8080 and "
                        "--console-upstream http://127.0.0.1:5199"
                    ),
                )
            print(
                render_nginx_site(
                    decision,
                    upstreams=[
                        Upstream(
                            surface=CHAT_SURFACE, base_url=args.chat_upstream,
                        ),
                        Upstream(
                            surface=CONSOLE_SURFACE,
                            base_url=args.console_upstream,
                        ),
                    ],
                ),
                end="",
            )
            return 0
    except SubstrateError as err:
        return _emit_error(err, as_json=as_json)

    payload = {
        "schema": "okg.chat.sso.decision.v1",
        "surface": "sso-check",
        "status": "ok",
        "ok": True,
        "principal_mapping_declared": mapping is not None,
        **decision.to_dict(),
    }
    _emit(payload, as_json=as_json)
    return 0


# ---------------------------------------------------------------------------
# okg chat sync
# ---------------------------------------------------------------------------

def _print_human(report: Any) -> None:
    """The operator-readable rendering of a sync run.

    Everything a reader has to act on is here, not only in the JSON: which
    surfaces were written, what deviated from the raw manifest AND WHY, what
    was excluded, what the /mcp probe observed, and which clauses this
    version cannot satisfy. A run whose only honest output is a JSON blob is
    a run nobody reads.
    """
    # `report.status`, not a re-derivation from `ok`: the third state
    # (verified_with_unsatisfiable_clauses) exists precisely so a reader who
    # never scrolls past the first line still learns that a clause the
    # requirement names is unproved.
    print(
        f"chat sync {report.deployment}: {report.status.upper()} "
        f"({report.changes} change(s), Open WebUI {report.openwebui_version})"
    )
    for name in report.surfaces:
        outcome = report.surfaces[name]
        line = f"  {name:<13} {outcome.status:<24} changes={outcome.changes}"
        print(line)
        if outcome.refusal:
            print(f"      refusal: {outcome.refusal}")
        for note in outcome.notes:
            print(f"      note: {note}")
    mcp = report.mcp or {}
    print(
        f"  {'mcp':<13} "
        f"{'reachable' if mcp.get('reachable') else 'UNREACHABLE':<24} "
        f"probe={mcp.get('checked') or mcp.get('reason')}"
    )
    css = report.css_bundle
    if css is not None:
        # Printed as BYTES on purpose. This surface can never report what the
        # site looks like — the instance exposes no such answer — so the line
        # says which digest is being served and nothing more.
        print(
            f"  {'css_bundle':<13} {css.state:<24} "
            f"byte-identical={css.byte_identical} "
            f"served={css.observed_sha256 or 'nothing'}"
        )
        if css.drift_detail:
            print(f"      drift: {css.drift_detail}")
        if css.refusal:
            print(f"      refusal: {css.refusal}")
        for note in css.notes:
            print(f"      note: {note}")
    # The CHECKABLE numbers. req.readback-no-partial-apply requires the count
    # of written surfaces verified to be printed rather than asserted (5 when
    # prompts is written, 4 when it is dropped), and req.idempotent requires
    # the perturbed-and-detected count alongside it — otherwise a run that
    # quietly verified one surface reads the same as one that verified five.
    dropped = report.written_surfaces_dropped
    total = len(getattr(report, "surfaces", {})) or 5
    print(
        f"  written surfaces verified: {report.written_surfaces_verified} "
        f"of {total}"
        + (
            " (dropped: "
            + ", ".join(f"{d['surface']} [{d['status']}]" for d in dropped)
            + ")"
            if dropped
            else ""
        )
    )
    # The arithmetic, printed. `verified` alone cannot show a surface that
    # fell out of BOTH counts — which is how deep_links went missing from a
    # live run that still exited 0.
    print(
        "  written surfaces accounted (verified + dropped): "
        f"{report.written_surfaces_accounted} of {total}"
    )
    for entry in dropped:
        refusal = entry.get("refusal") or "(no refusal recorded)"
        print(
            f"      dropped {entry['surface']} [{entry['status']}]: "
            + (refusal if len(refusal) <= 240 else refusal[:240] + "…")
        )
    # "detected and applied", not "drift": on a first run every surface
    # changes because nothing was configured yet, and calling that drift
    # would misdescribe the common case. On a second run the same number is
    # the drift count req.idempotent asks for, and it must equal the
    # written-surface count when every written surface was perturbed.
    print(
        "  surfaces with changes detected and applied: "
        f"{report.surfaces_with_detected_changes}"
    )
    print(
        f"  census-admitted knobs driven: {len(report.driven_knobs)} of "
        f"{len(report.census_admitted_knobs)} {report.driven_knobs}"
    )
    for excluded in report.census_excluded_knobs:
        reason = excluded["reason"]
        print(
            f"  census-excluded knob {excluded['knob']} "
            f"(mechanism {excluded['mechanism']}): "
            + (reason if len(reason) <= 200 else reason[:200] + "…")
        )
    if report.prompt is not None:
        print(
            f"  system prompt: {len(report.prompt.text)} chars from "
            f"{list(report.prompt.contributing)}; absent: "
            f"{list(report.prompt.absent)}"
        )
    if report.health:
        # Printed on every run including `not_probed` and `probe_failed`: a
        # health line that disappears when the check breaks is what makes a
        # broken probe look like a healthy deployment.
        banner = report.health.get("banner") or {}
        print(
            f"  deployment health: {report.health.get('state')} "
            f"(banner {banner.get('owned_action', 'absent')})"
        )
        failure = report.health.get("probe_failure")
        if failure:
            print(
                f"  health probe FAILED ({failure['reason']}): "
                f"{failure['detail']}"
            )
    for deviation in report.deviations:
        print(
            f"  deviation: {deviation['key']} = "
            f"{deviation['effective_value']!r} (manifest: "
            f"{deviation['manifest_value']!r}) — {deviation['cause']}"
        )
    for exclusion in report.exclusions:
        print(f"  excluded {exclusion['tool']!r}: {exclusion['reason']}")
    for clause in report.unsatisfiable:
        print(f"  UNSATISFIABLE {clause['clause']}: {clause['reason']}")


def _cmd_sync(args: argparse.Namespace, *, as_json: bool) -> int:
    from okg.substrate.chat.health_banner import cli_health_probe
    from okg.substrate.chat.sync import (
        ChatSyncError,
        InstanceClient,
        load_inputs,
        resolve_operator_environment,
        sync,
    )

    # Bound OUTSIDE the try so a failure mid-run can still print what was
    # already applied. `_demote_on_failure` preserves every surface's diff and
    # marks it `applied_but_run_failed`; printing only the error would throw
    # that away and leave the operator to guess how much of their instance was
    # rewritten before the run stopped.
    report = None
    try:
        base_url, token = resolve_operator_environment()
        inputs = load_inputs(
            args.deployment,
            mcp_probe_url=args.mcp_probe_url,
            mcp_instance_url=args.mcp_instance_url,
            mcp_host=args.mcp_host,
            install_plan_dir=getattr(args, "install_plan", None),
        )
        inputs = _with_recorded_instance_url(args, inputs, base_url)
        expected_plan = None
        if getattr(args, "plan", None):
            from okg.substrate.chat.plan_apply import load_chat_plan

            expected_plan = load_chat_plan(args.plan)
        client = InstanceClient(base_url, token)
        # THIS verb is the health projection's runner (PACT
        # chat-health-banner-projection cadence rule): the banner changes when
        # `okg chat sync` runs, and its as-of stamp is the staleness bound the
        # chat user reads.
        # The container runtime reaches the byte-delivery surface
        # (chat.css_bundle). It is constructed unconditionally and used
        # lazily: a deployment that declares no bundle and has no recorded
        # delivery never touches it, so an operator with no container runtime
        # on the machine is unaffected until they declare one.
        from okg.substrate.chat.instance import DockerRuntime, container_name_for

        report = sync(
            inputs,
            client,
            probe_health=cli_health_probe,
            container_runtime=DockerRuntime(),
            container=container_name_for(args.deployment),
            expected_plan=expected_plan,
        )
    except (ChatSyncError, SubstrateError) as err:
        code = _emit_error(err, as_json=as_json)
        partial = getattr(err, "partial_report", None)
        if partial is not None:
            if as_json or os.environ.get("OKG_AGENT") == "1":
                print(json.dumps(partial.to_dict(), sort_keys=True, default=str))
            else:
                _print_human(partial)
        return code

    if as_json or os.environ.get("OKG_AGENT") == "1":
        print(json.dumps(report.to_dict(), sort_keys=True, default=str))
    else:
        _print_human(report)
    return 0 if report.ok else 1


def _with_recorded_instance_url(
    args: argparse.Namespace, inputs: Any, instance_base_url: str | None,
) -> Any:
    """Without ``--mcp-instance-url``, register the graph URL ``okg chat
    setup`` recorded for the instance at ``instance_base_url``, or refuse
    before anything is written (issue #2876)."""
    import dataclasses

    from okg.substrate.chat.setup import resolve_sync_instance_url

    if args.mcp_instance_url:
        return inputs
    url, note = resolve_sync_instance_url(
        args.deployment, probe_url=inputs.mcp_probe_url,
        instance_base_url=instance_base_url,
    )
    if url is None:
        return inputs
    print(f"note: {note}", file=sys.stderr)
    return dataclasses.replace(inputs, mcp_instance_url=url)


def _chat_inputs(
    args: argparse.Namespace, instance_base_url: str | None = None,
) -> Any:
    """``instance_base_url`` is the instance the verb talks to; ``plan``
    talks to none and passes ``OKG_CHAT_INSTANCE_URL`` when it is set."""
    from okg.substrate.chat.sync import load_inputs

    inputs = load_inputs(
        args.deployment,
        mcp_probe_url=args.mcp_probe_url,
        mcp_instance_url=args.mcp_instance_url,
        mcp_host=args.mcp_host,
        install_plan_dir=getattr(args, "install_plan", None),
    )
    return _with_recorded_instance_url(args, inputs, instance_base_url)


def _cmd_chat_plan(args: argparse.Namespace, *, as_json: bool) -> int:
    """Thin wrapper. Every decision lives in ``chat.plan_apply``."""
    from okg.substrate.chat.plan_apply import write_chat_plan

    try:
        from okg.substrate.chat.sync import ENV_INSTANCE_URL

        instance_url = (os.environ.get(ENV_INSTANCE_URL) or "").strip() or None
        written = write_chat_plan(_chat_inputs(args, instance_url), args.out)
    except SubstrateError as err:
        return _emit_error(err, as_json=as_json)
    _emit(
        {
            "surface": "chat-plan",
            "status": "written",
            "plan_path": written["plan_path"],
            "plan_digest": written["plan_digest"],
        },
        as_json=as_json,
    )
    return 0


def _cmd_chat_readback(args: argparse.Namespace, *, as_json: bool) -> int:
    """Thin wrapper. Reads the instance; writes only the local document."""
    from okg.substrate.chat.plan_apply import (
        chat_readback,
        load_chat_plan,
        write_chat_readback,
    )
    from okg.substrate.chat.sync import InstanceClient, resolve_operator_environment

    try:
        base_url, token = resolve_operator_environment()
        inputs = _chat_inputs(args, base_url)
        document = chat_readback(
            inputs,
            InstanceClient(base_url, token),
            load_chat_plan(args.plan),
        )
        written = write_chat_readback(document, args.out)
    except SubstrateError as err:
        return _emit_error(err, as_json=as_json)
    if as_json or os.environ.get("OKG_AGENT") == "1":
        print(json.dumps(document, sort_keys=True, default=str))
    else:
        print(
            f"chat readback: {'ok' if document['ok'] else document['code']} "
            f"-> {written['readback_path']}"
        )
    # Non-zero on deviation, with the stable code already in the document.
    return 0 if document["ok"] else 1


def _cmd_chat_setup(args: argparse.Namespace, *, as_json: bool) -> int:
    """Thin wrapper. Every decision lives in ``chat.setup``."""
    from okg.substrate.chat.block_scaffold import ChatAnswers
    from okg.substrate.chat.setup import (
        SetupOptions,
        open_in_browser,
        render_human,
        run_setup,
    )

    answers = ChatAnswers(
        provider=args.provider,
        provider_base_url=args.provider_base_url,
        provider_key_env=args.provider_key_env,
        model=args.model,
        mcp_port=args.mcp_port,
        instance_port=args.instance_port,
        search_profile=args.search_profile,
        search_subtypes=tuple(args.search_subtype or ()),
    )
    options = SetupOptions(
        app_database_env=args.app_database_url_env,
        secret_env=args.admin_secret_env,
        instance_port=args.instance_port,
        image=args.image,
        host=args.host,
        host_gateway_alias=args.host_gateway_alias or None,
        container_runtime=args.container_runtime,
        container_network=args.container_network,
        ready_timeout=args.ready_timeout,
        mcp_probe_url=args.mcp_probe_url,
        mcp_instance_url=args.mcp_instance_url,
        mcp_host=args.mcp_host,
        install_plan=getattr(args, "install_plan", None),
        admin_email=args.admin_email,
        question=args.question,
        start_mcp_serve=not args.no_start_mcp_serve,
        mcp_ready_timeout=args.mcp_ready_timeout,
        chat_answers=answers,
        configure_only=args.configure_only,
    )
    report = run_setup(args.deployment, options=options)
    if as_json or os.environ.get("OKG_AGENT") == "1":
        print(json.dumps(report.to_dict(), sort_keys=True, default=str))
    else:
        print(render_human(report))
    if report.ok and args.open_browser and report.site_url:
        opened = open_in_browser(report.site_url)
        if not (as_json or os.environ.get("OKG_AGENT") == "1"):
            print(
                f"  browser: {'opened' if opened else 'could not be opened; open the URL above'}"
            )
    return 0 if report.ok else 1


def _cmd_chat_codex_shim(args: argparse.Namespace, *, as_json: bool) -> int:
    """Thin wrapper. Every decision lives in ``chat.codex_shim``."""
    from pathlib import Path

    from okg.substrate.chat.codex_shim import (
        BIND_HOST,
        CodexShimError,
        build_server,
        load_config,
    )

    try:
        config = load_config(
            codex=args.codex,
            token_env=args.token_env,
            workdir=Path(args.workdir) if args.workdir else None,
            codex_home=Path(args.codex_home) if args.codex_home else None,
            mcp_url=args.mcp_url,
            mcp_token_env=args.mcp_token_env,
            mcp_tools=(args.mcp_tools or "").split(",") if args.mcp_tools else None,
            run_timeout=args.run_timeout,
            max_concurrent=args.max_concurrent,
            accept_codex_version=args.accept_codex_version,
        )
        server = build_server(config, port=args.port)
    except SubstrateError as err:
        return _emit_error(err, as_json=as_json)
    except OSError as exc:
        return _emit_error(
            CodexShimError(
                f"cannot listen on {BIND_HOST}:{args.port}: {exc}.",
                data={"reason": "port_unavailable", "port": args.port},
                fix_hint="stop whatever holds the port or pass --port",
            ),
            as_json=as_json,
        )
    document = {"ok": True, "listening": f"http://{BIND_HOST}:{server.server_address[1]}/v1",
                **config.describe()}
    if as_json or os.environ.get("OKG_AGENT") == "1":
        print(json.dumps(document, sort_keys=True, default=str))
    else:
        mcp = document["mcp"]
        print(f"codex-shim listening on {document['listening']} ({document['codex_version']})")
        print(f"  bearer token: ${document['token_env']}")
        print(f"  sandbox: {document['sandbox']}; shell and other built-ins disabled; history: {document['history']}")
        print(
            "  graph: "
            + (
                f"{mcp['url']} (token ${mcp['bearer_token_env_var']}; tools {', '.join(mcp['tools'])})"
                if mcp else "none: answers are NOT grounded"
            )
        )
        print(f"  codex home (dedicated): {document['codex_home']}; prompt checked for operator context")
        print(f"  codex working directory: {document['workdir']}")
        print("  every chat user spends the operator's ChatGPT plan")
    sys.stdout.flush()
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()
    return 0


def _cmd_chat(args: argparse.Namespace, *, as_json: bool) -> int:
    if args.chat_subcmd == "setup":
        return _cmd_chat_setup(args, as_json=as_json)
    if args.chat_subcmd == "codex-shim":
        return _cmd_chat_codex_shim(args, as_json=as_json)
    if args.chat_subcmd == "source-admin":
        return _cmd_source_admin(args, as_json=as_json)
    if args.chat_subcmd == "source-retry":
        return _cmd_source_retry(args, as_json=as_json)
    if args.chat_subcmd == "sso-check":
        return _cmd_sso_check(args, as_json=as_json)
    if args.chat_subcmd == "sync":
        return _cmd_sync(args, as_json=as_json)
    if args.chat_subcmd == "plan":
        return _cmd_chat_plan(args, as_json=as_json)
    if args.chat_subcmd == "readback":
        return _cmd_chat_readback(args, as_json=as_json)
    raise SystemExit(f"unknown chat subcommand: {args.chat_subcmd!r}")


__all__ = [
    "_cmd_chat",
    "_cmd_chat_codex_shim",
    "_cmd_chat_plan",
    "_cmd_chat_readback",
    "_cmd_chat_setup",
    "_cmd_source_admin",
    "_cmd_source_retry",
    "_cmd_sso_check",
    "_cmd_sync",
]
