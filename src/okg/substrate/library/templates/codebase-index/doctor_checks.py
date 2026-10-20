"""Doctor checks owned by the {{ deployment_name }} deployment."""
from __future__ import annotations
import argparse
import json
from typing import Any

from okg.substrate.doctor import DoctorContext
from okg.substrate.errors import SubstrateError


AGENT_MEMORY_FIXTURE_SOURCE_RE = (
    r"(^|/)deployments/{{ deployment_name }}/fixtures/"
)

_OKG_WORKSPACE_DOCTOR_EDGE_PATTERNS = (
    "acceptance_criterion -> verified_by -> test_case",
    "tool_use_event -> affects -> code_symbol",
    "test_case -> references -> code_symbol",
    "interface_endpoint -> routes_to -> code_symbol",
    "security_finding -> affects -> code_symbol",
    "profile_hotspot -> affects -> code_symbol",
)


def doctor_okg_workspace(
    args: argparse.Namespace,
    *,
    context: DoctorContext,
    as_json: bool,
) -> int:
    """Deployment-owned coding-context quality checks."""
    from okg.substrate.db.connection import connect as db_connect

    try:
        dsn = context.resolve_dsn(args)
        with db_connect(dsn) as conn:
            snapshot = _okg_workspace_coding_context_snapshot(
                conn,
                deployment=getattr(args, "deployment", None)
                or "{{ deployment_name }}",
                generation_id=getattr(args, "generation", None),
            )
            # Chronos cutover: performance-failure probe reads the CURRENT
            # default-branch graph state through a checkout (no generation
            # pin).
            from okg.substrate.graph_store import ChronosGraphStore

            perf_graph = ChronosGraphStore.from_connection(
                conn, ensure_schema=False
            ).checkout()
            performance_failures = _okg_workspace_performance_failures(perf_graph)
    except Exception as exc:
        payload = {
            "ok": False,
            "status": "ERROR",
            "errors": [
                {
                    "code": "okg_workspace_probe_failed",
                    "message": str(exc),
                }
            ],
        }
        context.emit_ok(payload, as_json=as_json)
        return 1

    payload = _okg_workspace_doctor_payload(
        snapshot,
        performance_failures=performance_failures,
    )
    context.emit_ok(payload, as_json=as_json)
    return 1 if payload["status"] == "FAIL" else 0


def _okg_workspace_doctor_payload(
    snapshot: dict[str, Any],
    *,
    performance_failures: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    performance_failures = performance_failures or []
    checks: list[dict[str, Any]] = []

    subtypes = dict(snapshot.get("subtype_counts") or {})
    edge_patterns = dict(snapshot.get("edge_pattern_counts") or {})
    languages = dict(snapshot.get("language_symbol_counts") or {})
    language_files = dict(snapshot.get("language_source_file_counts") or {})
    source_status = dict(snapshot.get("optional_source_status") or {})
    evidence = dict(snapshot.get("evidence_status") or {})
    agent_memory = dict(snapshot.get("agent_memory_quality") or {})
    record_set_quality = dict(snapshot.get("record_set_quality") or {})
    summary = dict(snapshot.get("summary") or {})
    queues = dict(snapshot.get("queues") or {})
    freshness = dict(snapshot.get("freshness") or {})
    embedding = dict(snapshot.get("embedding") or {})
    embedding_enabled = embedding.get("enabled") is True

    _append_okg_workspace_check(
        checks,
        name="code_source_coverage",
        status="OK" if subtypes.get("code_symbol", 0) > 0 else "FAIL",
        code="code_symbols_missing",
        message="no code symbols are live for {{ deployment_name }}",
        actual=subtypes.get("code_symbol", 0),
        expected="> 0",
        fix_hint=(
            "run `okg ingest --deployment {{ deployment_name }} --include "
            "git-files,code-structure` and check tree-sitter parser setup"
        ),
    )
    for language in ("python", "javascript", "typescript", "go", "rust", "cpp"):
        symbol_count = int(languages.get(language, 0) or 0)
        file_count = int(language_files.get(language, 0) or 0)
        if symbol_count > 0:
            status = "OK"
            expected = "> 0 symbols"
        elif file_count > 0:
            status = "WARN"
            expected = "> 0 symbols for observed source files"
        else:
            status = "OK"
            expected = "no source files or > 0 symbols"
        _append_okg_workspace_check(
            checks,
            name=f"language_coverage:{language}",
            status=status,
            code="language_symbols_missing",
            message=f"no {language} symbols are live",
            actual={
                "symbols": symbol_count,
                "source_files": file_count,
            },
            expected=expected,
            fix_hint=(
                "confirm the repo has files for this language and that the "
                "code-structure source includes the extension"
            ),
        )

    symbol_link_patterns = {
        "tool_use_event -> affects -> code_symbol":
            ("tool_use_event_to_symbol_links_missing", None),
        "test_case -> references -> code_symbol":
            ("test_case_to_symbol_links_missing", None),
        "interface_endpoint -> routes_to -> code_symbol":
            ("interface_handler_links_missing", None),
        "security_finding -> affects -> code_symbol":
            ("security_finding_symbol_links_missing", "security_finding"),
        "profile_hotspot -> affects -> code_symbol":
            ("profile_hotspot_symbol_links_missing", "profile_hotspot"),
    }
    for pattern, (code, source_subtype) in symbol_link_patterns.items():
        edge_count = int(edge_patterns.get(pattern, 0) or 0)
        source_count = (
            int(subtypes.get(source_subtype, 0) or 0)
            if source_subtype
            else None
        )
        status = (
            "OK"
            if edge_count > 0 or (source_subtype and source_count == 0)
            else "WARN"
        )
        _append_okg_workspace_check(
            checks,
            name=f"symbol_links:{pattern}",
            status=status,
            code=code,
            message=f"missing live edge pattern {pattern}",
            actual=edge_count,
            expected="0 source nodes or > 0" if source_subtype else "> 0",
            fix_hint=(
                "run a publish cycle after the relevant source emits data; "
                "if facts exist, inspect the matching deterministic linker"
            ),
        )

    _append_okg_workspace_check(
        checks,
        name="interface_endpoint_count",
        status="OK" if subtypes.get("interface_endpoint", 0) > 0 else "WARN",
        code="interface_endpoints_missing",
        message="no deterministic CLI/MCP/API interface endpoints are live",
        actual=subtypes.get("interface_endpoint", 0),
        expected="> 0",
        fix_hint="run the interfaces source and publish the handler linker",
    )
    data_assets = subtypes.get("data_asset", 0) + subtypes.get("config_key", 0)
    _append_okg_workspace_check(
        checks,
        name="data_asset_count",
        status="OK" if data_assets > 0 else "WARN",
        code="data_assets_missing",
        message="no deterministic data/config touchpoints are live",
        actual=data_assets,
        expected="> 0",
        fix_hint="run the code-data-flow source and publish DataAssetLinker",
    )

    for source in ("semgrep-data-flow", "codeql-data-flow"):
        status = str(source_status.get(source) or "disabled")
        _append_okg_workspace_check(
            checks,
            name=f"third_party_data_flow:{source}",
            status="OK" if status in {"configured", "imported"} else "WARN",
            code=f"{source.replace('-', '_')}_not_imported",
            message=f"{source} has status {status!r}",
            actual=status,
            expected="configured or imported",
            fix_hint=(
                "drop Semgrep/CodeQL artifacts under reports/ or "
                ".artifacts/, or leave this warning as the explicit disabled "
                "state for repos without third-party data-flow analysis"
            ),
        )

    _append_okg_workspace_check(
        checks,
        name="test_report_evidence",
        status=(
            "OK"
            if evidence.get("test-reports") == "imported"
            else "WARN"
        ),
        code="test_reports_not_imported",
        message="test-report evidence is not imported",
        actual=evidence.get("test-reports"),
        expected="imported",
        fix_hint="write JUnit/coverage artifacts under reports/ or .artifacts/",
    )
    _append_okg_workspace_check(
        checks,
        name="acceptance_criteria_test_links",
        status=(
            "OK"
            if evidence.get("acceptance-criteria") == "linked"
            else "WARN"
        ),
        code="acceptance_criteria_without_test_links",
        message="OpenSpec acceptance criteria are not linked to test evidence",
        actual=evidence.get("acceptance-criteria"),
        expected="linked",
        fix_hint=(
            "add criterion ids to JUnit testcase properties, pytest markers, "
            "classnames, or test names"
        ),
    )
    _append_okg_workspace_check(
        checks,
        name="security_artifact_evidence",
        status=(
            "OK"
            if evidence.get("security-analysis") == "imported"
            else "WARN"
        ),
        code="security_artifacts_not_imported",
        message="security-analysis artifacts are not imported",
        actual=evidence.get("security-analysis"),
        expected="imported",
        fix_hint=(
            "drop SARIF or scanner JSON artifacts from Semgrep, CodeQL, "
            "Bandit, Gitleaks, pip-audit, OSV-Scanner, or Trivy under "
            "reports/ or .artifacts/"
        ),
    )
    _append_okg_workspace_check(
        checks,
        name="profile_measurement_evidence",
        status=(
            "OK"
            if evidence.get("profile-measurements") == "imported"
            else "WARN"
        ),
        code="profile_measurements_not_imported",
        message="profile/benchmark artifacts are not imported",
        actual=evidence.get("profile-measurements"),
        expected="imported",
        fix_hint=(
            "capture OKG profile bundles under profiles/{{ deployment_name }}/ or "
            "benchmark JSON under reports/ or .artifacts/"
        ),
    )

    _append_okg_workspace_check(
        checks,
        name="agent_memory_supported_claim_evidence",
        status=(
            "OK"
            if int(agent_memory.get("supported_without_evidence_count") or 0) == 0
            else "FAIL"
        ),
        code="agent_memory_supported_claim_without_evidence",
        message="supported agent-memory claims lack live supporting evidence",
        actual=int(agent_memory.get("supported_without_evidence_count") or 0),
        expected=0,
        fix_hint=(
            "link supported claims with supported_by edges to live "
            "observations, verification results, test reports, or other "
            "deterministic evidence"
        ),
        samples=list(agent_memory.get("supported_without_evidence_samples") or [])[:10],
    )
    _append_okg_workspace_check(
        checks,
        name="agent_memory_model_inferred_supported_claims",
        status=(
            "OK"
            if int(
                agent_memory.get(
                    "model_inferred_supported_without_deterministic_count",
                ) or 0
            ) == 0
            else "FAIL"
        ),
        code="agent_memory_supported_model_inferred_without_deterministic_evidence",
        message=(
            "model-inferred supported claims lack deterministic supporting "
            "evidence"
        ),
        actual=int(
            agent_memory.get(
                "model_inferred_supported_without_deterministic_count",
            ) or 0
        ),
        expected=0,
        fix_hint=(
            "demote the claim to hypothesis or attach deterministic evidence "
            "such as a verification result, test report, or source/code node"
        ),
        samples=list(
            agent_memory.get(
                "model_inferred_supported_without_deterministic_samples",
            ) or []
        )[:10],
    )
    _append_okg_workspace_check(
        checks,
        name="agent_memory_code_claim_subjects",
        status=(
            "OK"
            if int(agent_memory.get("code_claim_without_code_subject_count") or 0) == 0
            else "WARN"
        ),
        code="agent_memory_code_claim_without_code_subject",
        message="code-scoped agent-memory claims lack source_file or code_symbol subjects",
        actual=int(agent_memory.get("code_claim_without_code_subject_count") or 0),
        expected=0,
        fix_hint=(
            "set subject_id to a source_file/code_symbol node or add a "
            "references edge to the deterministic code target"
        ),
        samples=list(agent_memory.get("code_claim_without_code_subject_samples") or [])[:10],
    )
    _append_okg_workspace_check(
        checks,
        name="agent_memory_stale_supported_claims",
        status=(
            "OK"
            if int(agent_memory.get("stale_supported_claim_count") or 0) == 0
            else "WARN"
        ),
        code="agent_memory_stale_supported_claim",
        message="supported agent-memory claims are stale relative to their evidence",
        actual=int(agent_memory.get("stale_supported_claim_count") or 0),
        expected=0,
        fix_hint="re-run the verification packet against the latest generation",
        samples=list(agent_memory.get("stale_supported_claim_samples") or [])[:10],
    )
    _append_okg_workspace_check(
        checks,
        name="agent_memory_packet_warnings",
        status=(
            "OK"
            if int(agent_memory.get("packet_warning_count") or 0) == 0
            else "WARN"
        ),
        code="agent_memory_packet_validation_warnings",
        message="agent-memory packets imported validation warnings",
        actual=int(agent_memory.get("packet_warning_count") or 0),
        expected=0,
        fix_hint="fix the packet shape or remove unrecognized evidence ids",
        samples=list(agent_memory.get("packet_warning_samples") or [])[:10],
    )

    stale_node_count = int(record_set_quality.get("stale_node_count") or 0)
    _append_okg_workspace_check(
        checks,
        name="record_set_stale_source_nodes",
        status="OK" if stale_node_count == 0 else "FAIL",
        code="record_set_stale_source_nodes",
        message=(
            "record-set sources have latest inserted node facts outside "
            "their declared current record footprint"
        ),
        actual={
            "stale_node_count": stale_node_count,
            "sources_with_record_set": int(
                record_set_quality.get("sources_with_record_set") or 0,
            ),
            "by_source": record_set_quality.get("stale_nodes_by_source")
            or {},
        },
        expected=0,
        fix_hint=(
            "run a full-scope/reconcile ingest for the affected source; "
            "if stale rows remain, inspect the source's record_set "
            "declaration or emit an explicit one-time cleanup retraction"
        ),
        samples=list(record_set_quality.get("stale_node_samples") or [])[:10],
    )
    stale_edge_count = int(record_set_quality.get("stale_edge_count") or 0)
    _append_okg_workspace_check(
        checks,
        name="record_set_stale_source_edges",
        status="OK" if stale_edge_count == 0 else "FAIL",
        code="record_set_stale_source_edges",
        message=(
            "record-set sources have latest inserted edge facts outside "
            "their declared current record footprint"
        ),
        actual={
            "stale_edge_count": stale_edge_count,
            "sources_with_record_set": int(
                record_set_quality.get("sources_with_record_set") or 0,
            ),
            "by_source": record_set_quality.get("stale_edges_by_source")
            or {},
        },
        expected=0,
        fix_hint=(
            "run a full-scope/reconcile ingest for the affected source; "
            "if stale rows remain, inspect the source's record_set "
            "declaration or emit an explicit one-time cleanup retraction"
        ),
        samples=list(record_set_quality.get("stale_edge_samples") or [])[:10],
    )
    missing_node_count = int(
        record_set_quality.get("missing_node_count") or 0,
    )
    _append_okg_workspace_check(
        checks,
        name="record_set_missing_source_nodes",
        status="OK" if missing_node_count == 0 else "FAIL",
        code="record_set_missing_source_nodes",
        message=(
            "record-set sources declare node identities that are not "
            "latest inserted facts for that source"
        ),
        actual={
            "missing_node_count": missing_node_count,
            "sources_with_record_set": int(
                record_set_quality.get("sources_with_record_set") or 0,
            ),
            "by_source": record_set_quality.get("missing_nodes_by_source")
            or {},
        },
        expected=0,
        fix_hint=(
            "run a full-scope/reconcile ingest for the affected source; "
            "if missing rows remain, inspect persisted dedupe and the "
            "source's record_set declaration"
        ),
        samples=list(record_set_quality.get("missing_node_samples") or [])[:10],
    )
    missing_edge_count = int(
        record_set_quality.get("missing_edge_count") or 0,
    )
    _append_okg_workspace_check(
        checks,
        name="record_set_missing_source_edges",
        status="OK" if missing_edge_count == 0 else "FAIL",
        code="record_set_missing_source_edges",
        message=(
            "record-set sources declare edge identities that are not "
            "latest inserted facts for that source"
        ),
        actual={
            "missing_edge_count": missing_edge_count,
            "sources_with_record_set": int(
                record_set_quality.get("sources_with_record_set") or 0,
            ),
            "by_source": record_set_quality.get("missing_edges_by_source")
            or {},
        },
        expected=0,
        fix_hint=(
            "run a full-scope/reconcile ingest for the affected source; "
            "if missing rows remain, inspect persisted dedupe and the "
            "source's record_set declaration"
        ),
        samples=list(record_set_quality.get("missing_edge_samples") or [])[:10],
    )

    if performance_failures:
        _append_okg_workspace_check(
            checks,
            name="performance_budget_failures",
            status="FAIL",
            code="performance_budget_failed",
            message=(
                f"{len(performance_failures)} benchmark measurement(s) "
                "exceeded linked performance budgets"
            ),
            actual=len(performance_failures),
            expected=0,
            fix_hint=(
                "inspect the benchmark artifact, profile bundle, and linked "
                "performance_budget nodes; either improve the workload or "
                "update the declared budget with an OpenSpec rationale"
            ),
            samples=performance_failures[:10],
        )
    else:
        _append_okg_workspace_check(
            checks,
            name="performance_budget_failures",
            status="OK",
            code="performance_budget_failed",
            message="no benchmark measurement exceeds a linked budget",
            actual=0,
            expected=0,
            fix_hint="",
        )

    embed_lag = int(summary.get("embed_lag") or 0)
    _append_okg_workspace_check(
        checks,
        name="embed_lag",
        status=(
            "OK"
            if not embedding_enabled or embed_lag <= 5000
            else "WARN"
        ),
        code="embed_queue_lag_high",
        message=(
            "embedding backlog is above the {{ deployment_name }} target"
            if embedding_enabled
            else "embedding is disabled for {{ deployment_name }}"
        ),
        actual=(
            embed_lag
            if embedding_enabled
            else {"enabled": False, "backlog": embed_lag}
        ),
        expected=(
            "<= 5000" if embedding_enabled else "embedding disabled"
        ),
        fix_hint=(
            "run `okg runtime worker --deployment {{ deployment_name }}`"
            if embedding_enabled else ""
        ),
    )
    dead_letter = int(queues.get("dead_letter", 0) or 0)
    _append_okg_workspace_check(
        checks,
        name="job_dead_letter_count",
        status="OK" if dead_letter == 0 else "FAIL",
        code="job_dead_letter_nonzero",
        message="runtime dead-letter queue is non-empty",
        actual=dead_letter,
        expected=0,
        fix_hint="inspect DBOS workflow failures and retry after fixing root cause",
    )
    oldest_embed_age = _float_or_none(queues.get("oldest_embed_queued_seconds"))
    _append_okg_workspace_check(
        checks,
        name="embed_queue_age",
        status=(
            "OK"
            if (
                not embedding_enabled
                or oldest_embed_age is None
                or oldest_embed_age <= 3600
            )
            else "WARN"
        ),
        code="embed_queue_old",
        message=(
            "oldest queued embed job is older than one hour"
            if embedding_enabled
            else "embedding is disabled for {{ deployment_name }}"
        ),
        actual=(
            oldest_embed_age
            if embedding_enabled
            else {"enabled": False, "oldest_queued_seconds": oldest_embed_age}
        ),
        expected=(
            "<= 3600 seconds"
            if embedding_enabled else "embedding disabled"
        ),
        fix_hint=(
            "enable embedding, run `okg runtime worker --deployment "
            "{{ deployment_name }}`, and inspect DBOS workflow status"
            if embedding_enabled else ""
        ),
    )

    rate_resolved = (
        (summary.get("unresolved_mentions") or {}).get("rate_resolved")
        if isinstance(summary.get("unresolved_mentions"), dict)
        else None
    )
    _append_okg_workspace_check(
        checks,
        name="unresolved_mention_rate",
        status=(
            "OK"
            if _float_or_none(rate_resolved) is not None
            and float(rate_resolved) >= 0.75
            else "WARN"
        ),
        code="unresolved_mention_rate_low",
        message="entity mention resolution rate is below target",
        actual=rate_resolved,
        expected=">= 0.75",
        fix_hint="add aliases or source facts for frequent unresolved mentions",
    )

    for source, max_age_seconds in {
        "codex-sessions": 14 * 24 * 3600,
        "security-analysis": 7 * 24 * 3600,
        "profile-measurements": 14 * 24 * 3600,
    }.items():
        age = _float_or_none(freshness.get(f"{source}_age_seconds"))
        if age is None:
            status = "WARN"
        else:
            status = "OK" if age <= max_age_seconds else "WARN"
        _append_okg_workspace_check(
            checks,
            name=f"source_freshness:{source}",
            status=status,
            code=f"{source.replace('-', '_')}_stale",
            message=f"{source} has stale or missing source watermark",
            actual=age,
            expected=f"<= {max_age_seconds} seconds",
            fix_hint=f"run `okg ingest --deployment {{ deployment_name }} --include {source}`",
        )

    failures = [c for c in checks if c["status"] == "FAIL"]
    warnings = [c for c in checks if c["status"] == "WARN"]
    status = "FAIL" if failures else "WARN" if warnings else "OK"
    return {
        "ok": not failures,
        "status": status,
        "deployment": snapshot.get("deployment"),
        "generation_id": snapshot.get("generation_id"),
        "summary": summary,
        "counts": {
            "subtypes": subtypes,
            "edge_patterns": edge_patterns,
            "languages": languages,
            "agent_memory_quality": agent_memory,
            "scanner_findings_by_severity": snapshot.get(
                "scanner_findings_by_severity", {},
            ),
            "dependency_vulnerabilities_by_severity": snapshot.get(
                "dependency_vulnerabilities_by_severity", {},
            ),
            "top_profile_hotspot": snapshot.get("top_profile_hotspot"),
            "record_set_quality": record_set_quality,
        },
        "checks": checks,
        "failures": failures,
        "warnings": warnings,
    }


def _append_okg_workspace_check(
    checks: list[dict[str, Any]],
    *,
    name: str,
    status: str,
    code: str,
    message: str,
    actual: Any,
    expected: Any,
    fix_hint: str,
    samples: list[dict[str, Any]] | None = None,
) -> None:
    check = {
        "name": name,
        "status": status,
        "code": code,
        "message": message if status != "OK" else "",
        "actual": actual,
        "expected": expected,
    }
    if status != "OK" and fix_hint:
        check["fix_hint"] = fix_hint
    if samples:
        check["samples"] = samples
    checks.append(check)


def _okg_workspace_coding_context_snapshot(
    conn,
    *,
    deployment: str,
    generation_id: int | str | None = None,
) -> dict[str, Any]:
    gen_id, catalog_version_id = _resolve_published_generation_for_doctor(
        conn, deployment, generation_id,
    )
    # Chronos cutover: graph-content reads resolve through a Chronos checkout
    # pinned to the resolved published generation (okg.graph_nodes/graph_edges);
    # the published generation itself was resolved from the Chronos checkpoint
    # store above (okg.graph_generations is no longer written).
    from okg.substrate.graph_store import ChronosGraphStore

    graph = ChronosGraphStore.from_connection(
        conn, ensure_schema=False
    ).checkout_generation(gen_id)
    subtype_counts = _okg_workspace_subtype_counts(graph)
    edge_pattern_counts = _okg_workspace_edge_pattern_counts(graph)
    edge_summary_counts = _okg_workspace_edge_summary_counts(graph)
    language_counts = _okg_workspace_language_symbol_counts(graph)
    language_file_counts = _okg_workspace_language_source_file_counts(graph)
    empty_attr_metrics = _okg_workspace_empty_attr_metrics(graph)
    node_count = sum(subtype_counts.values())
    edge_count = edge_summary_counts["edge_count"]
    containment_edges = edge_summary_counts["containment_edges"]
    containment_rate = (
        round(containment_edges / edge_count, 4) if edge_count else 0.0
    )
    return {
        "deployment": deployment,
        "generation_id": gen_id,
        "catalog_version_id": catalog_version_id,
        "summary": {
            "node_count": node_count,
            "edge_count": edge_count,
            "containment_edge_rate": containment_rate,
            "embed_lag": _okg_workspace_embed_lag(graph),
            "unresolved_mentions": _okg_workspace_unresolved_mentions(graph),
        },
        "embedding": _okg_workspace_embedding_config(deployment),
        "subtype_counts": subtype_counts,
        "empty_attr_counts": {
            subtype: metrics["empty_attrs"]
            for subtype, metrics in empty_attr_metrics.items()
        },
        "empty_attr_rates": {
            subtype: metrics["empty_attr_rate"]
            for subtype, metrics in empty_attr_metrics.items()
        },
        "edge_pattern_counts": edge_pattern_counts,
        "language_symbol_counts": language_counts,
        "language_source_file_counts": language_file_counts,
        "optional_source_status": {
            "semgrep-data-flow": _okg_workspace_source_status(
                conn,
                "{{ deployment_name }}.semgrep-data-flow",
                imported_subtypes=("data_flow_observation",),
            ),
            "codeql-data-flow": _okg_workspace_source_status(
                conn,
                "{{ deployment_name }}.codeql-data-flow",
                imported_subtypes=("data_flow_observation",),
            ),
        },
        "evidence_status": _okg_workspace_evidence_status(
            conn,
            graph,
            subtype_counts=subtype_counts,
            edge_pattern_counts=edge_pattern_counts,
        ),
        # Chronos cutover: reads the agent-memory graph through a checkout
        # pinned to gen_id. The legacy stale-evidence branch that compared
        # the max evidence generation was dropped (the Chronos graph carries
        # no per-row generation column); see
        # _okg_workspace_agent_memory_quality.
        "agent_memory_quality": _okg_workspace_agent_memory_quality(
            conn, gen_id,
        ),
        "record_set_quality": _okg_workspace_record_set_quality(
            conn,
            graph=graph,
        ),
        "queues": _okg_workspace_queue_snapshot(conn),
        "freshness": _okg_workspace_source_freshness(conn),
        "scanner_findings_by_severity": _okg_workspace_severity_counts(
            graph,
            subtypes=("security_finding", "secret_finding"),
        ),
        "dependency_vulnerabilities_by_severity": (
            _okg_workspace_severity_counts(
                graph,
                subtypes=("dependency_vulnerability",),
            )
        ),
        "top_profile_hotspot": _okg_workspace_top_profile_hotspot(graph),
    }


def _okg_workspace_embedding_config(deployment: str) -> dict[str, Any]:
    from okg.substrate.deployments import resolve_deployment

    config = resolve_deployment(deployment).embedding_config
    return {
        "enabled": config.enabled,
        "backend": config.backend,
        "reason": config.reason,
    }


def _resolve_published_generation_for_doctor(
    conn,
    deployment: str,
    generation_id: int | str | None,
) -> tuple[str, int]:
    # Chronos cutover: the chronos-native publisher no longer writes
    # okg.graph_generations rows — a published generation is a Chronos
    # checkpoint. Resolve the published generation from the Chronos store
    # (a CheckpointInfo, whose metadata carries catalog_version_id).
    from okg.substrate.graph_store import ChronosGraphStore

    store = ChronosGraphStore.from_connection(conn, ensure_schema=False)
    if generation_id is None:
        cp = store.latest_generation(
            deployment_name=deployment, status="published",
        )
    else:
        try:
            cp = store.get_generation(generation_id)
        except Exception:  # noqa: BLE001 — missing checkpoint -> "no published generation" below.
            cp = None
        if cp is not None and (
            cp.metadata.get("status") != "published"
            or cp.metadata.get("deployment_name") != deployment
        ):
            cp = None
    if cp is None:
        raise SubstrateError(
            f"no published generation for deployment {deployment!r}",
            fix_hint="run `okg ingest --deployment {{ deployment_name }}` and "
                     "`okg run --once --deployment {{ deployment_name }} --apply`",
        )
    # gen_id is the STRING checkpoint id, usable with
    # store.checkout_generation(gen_id); catalog_version_id stays int.
    gen_id = cp.checkpoint_id
    catalog_version_id = int(cp.metadata["catalog_version_id"])
    return gen_id, catalog_version_id


def _okg_workspace_subtype_counts(graph) -> dict[str, int]:
    rows = graph.query(
        """
        SELECT subtype, count(*) AS n
          FROM okg.graph_nodes
         GROUP BY subtype
        """,
    )
    return {str(row["subtype"]): int(row["n"]) for row in rows}


def _okg_workspace_empty_attr_metrics(
    graph,
) -> dict[str, dict[str, float | int]]:
    rows = graph.query(
        """
        SELECT subtype,
               count(*)::int AS total,
               count(*) FILTER (WHERE attrs = '{}'::jsonb)::int
                 AS empty_attrs
          FROM okg.graph_nodes
         GROUP BY subtype
        """,
    )
    out: dict[str, dict[str, float | int]] = {}
    for row in rows:
        total_int = int(row["total"])
        empty_int = int(row["empty_attrs"])
        out[str(row["subtype"])] = {
            "total": total_int,
            "empty_attrs": empty_int,
            "empty_attr_rate": (
                round(empty_int / total_int, 4)
                if total_int
                else 0.0
            ),
        }
    return out


def _okg_workspace_edge_pattern_counts(
    graph,
    patterns: tuple[str, ...] | None = None,
) -> dict[str, int]:
    """Count the exact edge patterns the doctor evaluates.

    The older implementation grouped every live edge by joined endpoint
    subtype. On interval-backed checkout views that becomes a full graph join,
    which is diagnostic overkill for a readiness check that only tests a fixed
    set of patterns.
    """
    requested = patterns or _OKG_WORKSPACE_DOCTOR_EDGE_PATTERNS
    counts: dict[str, int] = {}
    for pattern in requested:
        try:
            src_subtype, edge_type, dst_subtype = pattern.split(" -> ")
        except ValueError as exc:
            raise ValueError(f"invalid edge pattern {pattern!r}") from exc
        rows = graph.query(
            """
            SELECT count(*) AS n
              FROM okg.graph_edges e
              JOIN okg.graph_nodes src
                ON src.node_id = e.src
               AND src.subtype = %(src_subtype)s
              JOIN okg.graph_nodes dst
                ON dst.node_id = e.dst
               AND dst.subtype = %(dst_subtype)s
             WHERE e.edge_type = %(edge_type)s
            """,
            {
                "src_subtype": src_subtype,
                "edge_type": edge_type,
                "dst_subtype": dst_subtype,
            },
        )
        counts[pattern] = int(rows[0]["n"] or 0) if rows else 0
    return counts


def _okg_workspace_edge_summary_counts(graph) -> dict[str, int]:
    rows = graph.query(
        """
        SELECT count(*)::int AS edge_count,
               count(*) FILTER (WHERE edge_type = 'contains')::int
                 AS containment_edges
          FROM okg.graph_edges
        """,
    )
    row = rows[0] if rows else {}
    return {
        "edge_count": int(row.get("edge_count") or 0),
        "containment_edges": int(row.get("containment_edges") or 0),
    }


def _okg_workspace_edge_count(graph) -> int:
    rows = graph.query("SELECT count(*) AS n FROM okg.graph_edges")
    return int(rows[0]["n"] or 0)


def _okg_workspace_containment_edge_count(graph) -> int:
    rows = graph.query(
        "SELECT count(*) AS n FROM okg.graph_edges WHERE edge_type = 'contains'",
    )
    return int(rows[0]["n"] or 0)


def _okg_workspace_language_symbol_counts(
    graph,
) -> dict[str, int]:
    aliases = {
        "c++": "cpp",
        "cc": "cpp",
        "cxx": "cpp",
        "tsx": "typescript",
        "jsx": "javascript",
    }
    counts: dict[str, int] = {}
    rows = graph.query(
        """
        SELECT lower(COALESCE(attrs->>'language', attrs->>'lang', '')) AS language,
               count(*) AS n
          FROM okg.graph_nodes
         WHERE subtype = 'code_symbol'
         GROUP BY 1
        """,
    )
    for row in rows:
        key = aliases.get(str(row["language"]), str(row["language"]))
        if not key:
            key = "unknown"
        counts[key] = counts.get(key, 0) + int(row["n"])
    return counts


def _okg_workspace_language_source_file_counts(
    graph,
) -> dict[str, int]:
    aliases = {
        "c++": "cpp",
        "cc": "cpp",
        "cxx": "cpp",
        "tsx": "typescript",
        "jsx": "javascript",
    }
    counts: dict[str, int] = {}
    rows = graph.query(
        """
        SELECT lower(COALESCE(attrs->>'language', attrs->>'lang', '')) AS language,
               count(*) AS n
          FROM okg.graph_nodes
         WHERE subtype = 'source_file'
         GROUP BY 1
        """,
    )
    for row in rows:
        key = aliases.get(str(row["language"]), str(row["language"]))
        if not key:
            key = "unknown"
        counts[key] = counts.get(key, 0) + int(row["n"])
    return counts


def _okg_workspace_embed_lag(graph) -> int:
    rows = graph.query(
        """
        SELECT count(*) AS n
          FROM okg.graph_nodes
         WHERE attrs ? 'text'
           AND embedding IS NULL
        """
    )
    return int(rows[0]["n"])


def _okg_workspace_unresolved_mentions(
    graph,
) -> dict[str, Any]:
    rows = graph.query(
        """
        WITH mentions AS (
          SELECT node_id
            FROM okg.graph_nodes
           WHERE subtype = 'entity_mention'
        ),
        resolved AS (
          SELECT DISTINCT e.src AS node_id
            FROM okg.graph_edges e
            JOIN mentions m ON m.node_id = e.src
           WHERE e.edge_type IN ('mentions', 'references')
        )
        SELECT (SELECT count(*) FROM mentions) AS total,
               (SELECT count(*) FROM resolved) AS resolved
        """,
    )
    row = rows[0]
    total = int(row["total"] or 0)
    resolved = int(row["resolved"] or 0)
    return {
        "total": total,
        "resolved": resolved,
        "unresolved": max(total - resolved, 0),
        "rate_resolved": round(resolved / total, 4) if total else 1.0,
    }


def _okg_workspace_source_status(
    conn,
    ownership_id: str,
    *,
    imported_subtypes: tuple[str, ...],
) -> str:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT count(*)
              FROM okg.node_facts
             WHERE source = %s
               AND subtype = ANY(%s)
               AND op = 'I'
            """,
            (ownership_id, list(imported_subtypes)),
        )
        imported = int(cur.fetchone()[0] or 0)
        cur.execute(
            "SELECT last_reconcile_at FROM okg.source_watermarks "
            "WHERE source = %s",
            (ownership_id,),
        )
        row = cur.fetchone()
    if imported:
        return "imported"
    if row and row[0] is not None:
        return "configured"
    return "disabled"


def _okg_workspace_evidence_status(
    conn,
    graph,
    *,
    subtype_counts: dict[str, int] | None = None,
    edge_pattern_counts: dict[str, int] | None = None,
) -> dict[str, str]:
    subtypes = (
        subtype_counts
        if subtype_counts is not None
        else _okg_workspace_subtype_counts(graph)
    )
    edge_patterns = (
        edge_pattern_counts
        if edge_pattern_counts is not None
        else _okg_workspace_edge_pattern_counts(graph)
    )
    return {
        "test-reports": (
            "imported"
            if subtypes.get("test_report", 0) > 0
            else _configured_or_missing(
                conn, "{{ deployment_name }}.test-reports", "configured",
            )
        ),
        "acceptance-criteria": (
            "linked"
            if edge_patterns.get(
                "acceptance_criterion -> verified_by -> test_case", 0,
            ) > 0
            else "unlinked"
        ),
        "security-analysis": (
            "imported"
            if (
                subtypes.get("security_finding", 0)
                + subtypes.get("secret_finding", 0)
                + subtypes.get("dependency_vulnerability", 0)
                + subtypes.get("sbom_artifact", 0)
            ) > 0
            else _configured_or_missing(
                conn, "{{ deployment_name }}.security-analysis",
                "no-configured-artifact",
            )
        ),
        "profile-measurements": (
            "imported"
            if (
                subtypes.get("profile_run", 0)
                + subtypes.get("benchmark_measurement", 0)
            ) > 0
            else _configured_or_missing(
                conn, "{{ deployment_name }}.profile-measurements",
                "no-profile-captured",
            )
        ),
    }


def _okg_workspace_record_set_quality(
    conn,
    *,
    graph=None,
    sample_limit: int = 10,
) -> dict[str, Any]:
    """Compare record-set cursors with the latest source-owned fact state."""
    limit = max(int(sample_limit), 0)
    if graph is not None:
        return _okg_workspace_record_set_quality_from_graph(
            conn,
            graph,
            sample_limit=limit,
        )

    node_sql = """
        WITH record_sources AS (
          SELECT authority.source,
                 COALESCE(
                   authority.record_set->'records',
                   '{}'::jsonb
                 ) AS records
            FROM (
              SELECT sw.source,
                     COALESCE(
                       sw.last_applied_rev->'record_set',
                       srb.record_set,
                       srs.record_set
                     ) AS record_set
                FROM okg.source_watermarks sw
                LEFT JOIN okg.source_record_set_blobs srb
                  ON srb.source = sw.source
                 AND srb.sha256 = sw.last_applied_rev
                   ->'record_set_ref'->>'sha256'
                LEFT JOIN okg.source_record_sets srs
                  ON srs.source = sw.source
               WHERE sw.last_applied_rev ? 'record_set'
                  OR sw.last_applied_rev ? 'record_set_ref'
            ) authority
           WHERE jsonb_typeof(authority.record_set->'records') = 'object'
        ),
        record_items AS (
          SELECT rs.source, rec.key AS record_key, rec.value AS emission
            FROM record_sources rs
            CROSS JOIN LATERAL jsonb_each(rs.records) AS rec(key, value)
        ),
        expected_node_objects AS (
          SELECT source, record_key, emission->'primary' AS node_obj
            FROM record_items
           WHERE jsonb_typeof(emission->'primary') = 'object'
          UNION ALL
          SELECT ri.source, ri.record_key, descendant.value AS node_obj
            FROM record_items ri
            CROSS JOIN LATERAL jsonb_array_elements(
              CASE
                WHEN jsonb_typeof(ri.emission->'descendants') = 'array'
                  THEN ri.emission->'descendants'
                ELSE '[]'::jsonb
              END
            ) AS descendant(value)
        ),
        expected_nodes AS (
          SELECT DISTINCT
                 source,
                 node_obj->>'node_id' AS node_id,
                 node_obj->>'subtype' AS subtype
            FROM expected_node_objects
           WHERE jsonb_typeof(node_obj) = 'object'
             AND COALESCE(node_obj->>'node_id', '') <> ''
        ),
        latest_source_nodes AS (
          SELECT DISTINCT ON (nf.source, nf.node_id)
                 nf.source, nf.node_id, nf.subtype, nf.op
            FROM okg.node_facts nf
            JOIN record_sources rs ON rs.source = nf.source
           ORDER BY nf.source, nf.node_id,
                    nf.observed_at DESC, nf.fact_id DESC
        ),
        live_source_nodes AS (
          SELECT source, node_id, subtype
            FROM latest_source_nodes
           WHERE op = 'I'
        ),
        stale AS (
          SELECT ln.source, ln.node_id, ln.subtype
            FROM live_source_nodes ln
            LEFT JOIN expected_nodes en
              ON en.source = ln.source
             AND en.node_id = ln.node_id
           WHERE en.node_id IS NULL
        ),
        missing AS (
          SELECT en.source, en.node_id, en.subtype
            FROM expected_nodes en
            LEFT JOIN live_source_nodes ln
              ON ln.source = en.source
             AND ln.node_id = en.node_id
           WHERE ln.node_id IS NULL
        )
        SELECT
          (SELECT count(*) FROM record_sources)::int
            AS sources_with_record_set,
          (SELECT count(*) FROM expected_nodes)::int
            AS expected_node_count,
          (SELECT count(*) FROM live_source_nodes)::int
            AS live_source_node_count,
          (SELECT count(*) FROM stale)::int
            AS stale_node_count,
          (SELECT count(*) FROM missing)::int
            AS missing_node_count,
          COALESCE((
            SELECT jsonb_object_agg(by_source.source, by_source.count)
              FROM (
                SELECT source, count(*)::int AS count
                  FROM stale
                 GROUP BY source
              ) by_source
          ), '{}'::jsonb)::text AS stale_nodes_by_source,
          COALESCE((
            SELECT jsonb_agg(to_jsonb(sample))
              FROM (
                SELECT source, node_id, subtype
                  FROM stale
                 ORDER BY source, node_id
                 LIMIT %(limit)s
              ) sample
          ), '[]'::jsonb)::text AS stale_node_samples
          ,
          COALESCE((
            SELECT jsonb_object_agg(by_source.source, by_source.count)
              FROM (
                SELECT source, count(*)::int AS count
                  FROM missing
                 GROUP BY source
              ) by_source
          ), '{}'::jsonb)::text AS missing_nodes_by_source,
          COALESCE((
            SELECT jsonb_agg(to_jsonb(sample))
              FROM (
                SELECT source, node_id, subtype
                  FROM missing
                 ORDER BY source, node_id
                 LIMIT %(limit)s
              ) sample
          ), '[]'::jsonb)::text AS missing_node_samples
        """
    edge_sql = """
        WITH record_sources AS (
          SELECT authority.source,
                 COALESCE(
                   authority.record_set->'records',
                   '{}'::jsonb
                 ) AS records
            FROM (
              SELECT sw.source,
                     COALESCE(
                       sw.last_applied_rev->'record_set',
                       srb.record_set,
                       srs.record_set
                     ) AS record_set
                FROM okg.source_watermarks sw
                LEFT JOIN okg.source_record_set_blobs srb
                  ON srb.source = sw.source
                 AND srb.sha256 = sw.last_applied_rev
                   ->'record_set_ref'->>'sha256'
                LEFT JOIN okg.source_record_sets srs
                  ON srs.source = sw.source
               WHERE sw.last_applied_rev ? 'record_set'
                  OR sw.last_applied_rev ? 'record_set_ref'
            ) authority
           WHERE jsonb_typeof(authority.record_set->'records') = 'object'
        ),
        record_items AS (
          SELECT rs.source, rec.key AS record_key, rec.value AS emission
            FROM record_sources rs
            CROSS JOIN LATERAL jsonb_each(rs.records) AS rec(key, value)
        ),
        expected_edge_objects AS (
          SELECT ri.source, ri.record_key, edge_obj.value AS edge_obj
            FROM record_items ri
            CROSS JOIN LATERAL jsonb_array_elements(
              CASE
                WHEN jsonb_typeof(ri.emission->'edges') = 'array'
                  THEN ri.emission->'edges'
                ELSE '[]'::jsonb
              END
            ) AS edge_obj(value)
        ),
        expected_edges AS (
          SELECT DISTINCT
                 source,
                 edge_obj->>'src' AS src,
                 edge_obj->>'edge_type' AS edge_type,
                 edge_obj->>'dst' AS dst
            FROM expected_edge_objects
           WHERE jsonb_typeof(edge_obj) = 'object'
             AND COALESCE(edge_obj->>'src', '') <> ''
             AND COALESCE(edge_obj->>'edge_type', '') <> ''
             AND COALESCE(edge_obj->>'dst', '') <> ''
        ),
        latest_source_edges AS (
          SELECT DISTINCT ON (ef.source, ef.src, ef.edge_type, ef.dst)
                 ef.source, ef.src, ef.edge_type, ef.dst, ef.op
            FROM okg.edge_facts ef
            JOIN record_sources rs ON rs.source = ef.source
           ORDER BY ef.source, ef.src, ef.edge_type, ef.dst,
                    ef.observed_at DESC, ef.fact_id DESC
        ),
        live_source_edges AS (
          SELECT source, src, edge_type, dst
            FROM latest_source_edges
           WHERE op = 'I'
        ),
        stale AS (
          SELECT le.source, le.src, le.edge_type, le.dst
            FROM live_source_edges le
            LEFT JOIN expected_edges ee
              ON ee.source = le.source
             AND ee.src = le.src
             AND ee.edge_type = le.edge_type
             AND ee.dst = le.dst
           WHERE ee.src IS NULL
        ),
        missing AS (
          SELECT ee.source, ee.src, ee.edge_type, ee.dst
            FROM expected_edges ee
            LEFT JOIN live_source_edges le
              ON le.source = ee.source
             AND le.src = ee.src
             AND le.edge_type = ee.edge_type
             AND le.dst = ee.dst
           WHERE le.src IS NULL
        )
        SELECT
          (SELECT count(*) FROM expected_edges)::int
            AS expected_edge_count,
          (SELECT count(*) FROM live_source_edges)::int
            AS live_source_edge_count,
          (SELECT count(*) FROM stale)::int
            AS stale_edge_count,
          (SELECT count(*) FROM missing)::int
            AS missing_edge_count,
          COALESCE((
            SELECT jsonb_object_agg(by_source.source, by_source.count)
              FROM (
                SELECT source, count(*)::int AS count
                  FROM stale
                 GROUP BY source
              ) by_source
          ), '{}'::jsonb)::text AS stale_edges_by_source,
          COALESCE((
            SELECT jsonb_agg(to_jsonb(sample))
              FROM (
                SELECT source, src, edge_type, dst
                  FROM stale
                 ORDER BY source, src, edge_type, dst
                LIMIT %(limit)s
              ) sample
          ), '[]'::jsonb)::text AS stale_edge_samples
          ,
          COALESCE((
            SELECT jsonb_object_agg(by_source.source, by_source.count)
              FROM (
                SELECT source, count(*)::int AS count
                  FROM missing
                 GROUP BY source
              ) by_source
          ), '{}'::jsonb)::text AS missing_edges_by_source,
          COALESCE((
            SELECT jsonb_agg(to_jsonb(sample))
              FROM (
                SELECT source, src, edge_type, dst
                  FROM missing
                 ORDER BY source, src, edge_type, dst
                 LIMIT %(limit)s
              ) sample
          ), '[]'::jsonb)::text AS missing_edge_samples
        """
    with conn.cursor() as cur:
        cur.execute(node_sql, {"limit": limit})
        (
            sources_with_record_set,
            expected_node_count,
            live_source_node_count,
            stale_node_count,
            missing_node_count,
            stale_nodes_by_source,
            stale_node_samples,
            missing_nodes_by_source,
            missing_node_samples,
        ) = cur.fetchone()
        cur.execute(edge_sql, {"limit": limit})
        (
            expected_edge_count,
            live_source_edge_count,
            stale_edge_count,
            missing_edge_count,
            stale_edges_by_source,
            stale_edge_samples,
            missing_edges_by_source,
            missing_edge_samples,
        ) = cur.fetchone()

    return {
        "sources_with_record_set": int(sources_with_record_set or 0),
        "expected_node_count": int(expected_node_count or 0),
        "live_source_node_count": int(live_source_node_count or 0),
        "stale_node_count": int(stale_node_count or 0),
        "missing_node_count": int(missing_node_count or 0),
        "stale_nodes_by_source": _json_from_db(
            stale_nodes_by_source, default={},
        ),
        "stale_node_samples": _json_from_db(
            stale_node_samples, default=[],
        ),
        "missing_nodes_by_source": _json_from_db(
            missing_nodes_by_source, default={},
        ),
        "missing_node_samples": _json_from_db(
            missing_node_samples, default=[],
        ),
        "expected_edge_count": int(expected_edge_count or 0),
        "live_source_edge_count": int(live_source_edge_count or 0),
        "stale_edge_count": int(stale_edge_count or 0),
        "missing_edge_count": int(missing_edge_count or 0),
        "stale_edges_by_source": _json_from_db(
            stale_edges_by_source, default={},
        ),
        "stale_edge_samples": _json_from_db(
            stale_edge_samples, default=[],
        ),
        "missing_edges_by_source": _json_from_db(
            missing_edges_by_source, default={},
        ),
        "missing_edge_samples": _json_from_db(
            missing_edge_samples, default=[],
        ),
    }


def _okg_workspace_record_set_quality_from_graph(
    conn,
    graph,
    *,
    sample_limit: int,
) -> dict[str, Any]:
    del conn
    limit = max(int(sample_limit), 0)
    node_rows = graph.query(
        """
        WITH record_sources AS (
          SELECT authority.source,
                 COALESCE(
                   authority.record_set->'records',
                   '{}'::jsonb
                 ) AS records
            FROM (
              SELECT sw.source,
                     COALESCE(
                       sw.last_applied_rev->'record_set',
                       srb.record_set,
                       srs.record_set
                     ) AS record_set
                FROM okg.source_watermarks sw
                LEFT JOIN okg.source_record_set_blobs srb
                  ON srb.source = sw.source
                 AND srb.sha256 = sw.last_applied_rev
                   ->'record_set_ref'->>'sha256'
                LEFT JOIN okg.source_record_sets srs
                  ON srs.source = sw.source
               WHERE sw.last_applied_rev ? 'record_set'
                  OR sw.last_applied_rev ? 'record_set_ref'
            ) authority
           WHERE jsonb_typeof(authority.record_set->'records') = 'object'
        ),
        record_items AS (
          SELECT rs.source, rec.key AS record_key, rec.value AS emission
            FROM record_sources rs
            CROSS JOIN LATERAL jsonb_each(rs.records) AS rec(key, value)
        ),
        expected_node_objects AS (
          SELECT source, record_key, emission->'primary' AS node_obj
            FROM record_items
           WHERE jsonb_typeof(emission->'primary') = 'object'
          UNION ALL
          SELECT ri.source, ri.record_key, descendant.value AS node_obj
            FROM record_items ri
            CROSS JOIN LATERAL jsonb_array_elements(
              CASE
                WHEN jsonb_typeof(ri.emission->'descendants') = 'array'
                  THEN ri.emission->'descendants'
                ELSE '[]'::jsonb
              END
            ) AS descendant(value)
        ),
        expected_nodes AS (
          SELECT DISTINCT
                 source,
                 node_obj->>'node_id' AS node_id,
                 node_obj->>'subtype' AS subtype
            FROM expected_node_objects
           WHERE jsonb_typeof(node_obj) = 'object'
             AND COALESCE(node_obj->>'node_id', '') <> ''
        ),
        live_source_nodes AS (
          SELECT n.source, n.node_id, n.subtype
            FROM okg.graph_nodes n
            JOIN record_sources rs ON rs.source = n.source
        ),
        stale AS (
          SELECT ln.source, ln.node_id, ln.subtype
            FROM live_source_nodes ln
            LEFT JOIN expected_nodes en
              ON en.source = ln.source
             AND en.node_id = ln.node_id
           WHERE en.node_id IS NULL
        ),
        missing AS (
          SELECT en.source, en.node_id, en.subtype
            FROM expected_nodes en
            LEFT JOIN live_source_nodes ln
              ON ln.source = en.source
             AND ln.node_id = en.node_id
           WHERE ln.node_id IS NULL
        )
        SELECT
          (SELECT count(*) FROM record_sources)::int
            AS sources_with_record_set,
          (SELECT count(*) FROM expected_nodes)::int
            AS expected_node_count,
          (SELECT count(*) FROM live_source_nodes)::int
            AS live_source_node_count,
          (SELECT count(*) FROM stale)::int
            AS stale_node_count,
          (SELECT count(*) FROM missing)::int
            AS missing_node_count,
          COALESCE((
            SELECT jsonb_object_agg(by_source.source, by_source.count)
              FROM (
                SELECT source, count(*)::int AS count
                  FROM stale
                 GROUP BY source
              ) by_source
          ), '{}'::jsonb)::text AS stale_nodes_by_source,
          COALESCE((
            SELECT jsonb_agg(to_jsonb(sample))
              FROM (
                SELECT source, node_id, subtype
                  FROM stale
                 ORDER BY source, node_id
                 LIMIT :limit
              ) sample
          ), '[]'::jsonb)::text AS stale_node_samples,
          COALESCE((
            SELECT jsonb_object_agg(by_source.source, by_source.count)
              FROM (
                SELECT source, count(*)::int AS count
                  FROM missing
                 GROUP BY source
              ) by_source
          ), '{}'::jsonb)::text AS missing_nodes_by_source,
          COALESCE((
            SELECT jsonb_agg(to_jsonb(sample))
              FROM (
                SELECT source, node_id, subtype
                  FROM missing
                 ORDER BY source, node_id
                 LIMIT :limit
              ) sample
          ), '[]'::jsonb)::text AS missing_node_samples
        """,
        {"limit": limit},
    )
    edge_rows = graph.query(
        """
        WITH record_sources AS (
          SELECT authority.source,
                 COALESCE(
                   authority.record_set->'records',
                   '{}'::jsonb
                 ) AS records
            FROM (
              SELECT sw.source,
                     COALESCE(
                       sw.last_applied_rev->'record_set',
                       srb.record_set,
                       srs.record_set
                     ) AS record_set
                FROM okg.source_watermarks sw
                LEFT JOIN okg.source_record_set_blobs srb
                  ON srb.source = sw.source
                 AND srb.sha256 = sw.last_applied_rev
                   ->'record_set_ref'->>'sha256'
                LEFT JOIN okg.source_record_sets srs
                  ON srs.source = sw.source
               WHERE sw.last_applied_rev ? 'record_set'
                  OR sw.last_applied_rev ? 'record_set_ref'
            ) authority
           WHERE jsonb_typeof(authority.record_set->'records') = 'object'
        ),
        record_items AS (
          SELECT rs.source, rec.key AS record_key, rec.value AS emission
            FROM record_sources rs
            CROSS JOIN LATERAL jsonb_each(rs.records) AS rec(key, value)
        ),
        expected_edge_objects AS (
          SELECT ri.source, ri.record_key, edge_obj.value AS edge_obj
            FROM record_items ri
            CROSS JOIN LATERAL jsonb_array_elements(
              CASE
                WHEN jsonb_typeof(ri.emission->'edges') = 'array'
                  THEN ri.emission->'edges'
                ELSE '[]'::jsonb
              END
            ) AS edge_obj(value)
        ),
        expected_edges AS (
          SELECT DISTINCT
                 source,
                 edge_obj->>'src' AS src,
                 edge_obj->>'edge_type' AS edge_type,
                 edge_obj->>'dst' AS dst
            FROM expected_edge_objects
           WHERE jsonb_typeof(edge_obj) = 'object'
             AND COALESCE(edge_obj->>'src', '') <> ''
             AND COALESCE(edge_obj->>'edge_type', '') <> ''
             AND COALESCE(edge_obj->>'dst', '') <> ''
        ),
        live_source_edges AS (
          SELECT e.source, e.src, e.edge_type, e.dst
            FROM okg.graph_edges e
            JOIN record_sources rs ON rs.source = e.source
        ),
        stale AS (
          SELECT le.source, le.src, le.edge_type, le.dst
            FROM live_source_edges le
            LEFT JOIN expected_edges ee
              ON ee.source = le.source
             AND ee.src = le.src
             AND ee.edge_type = le.edge_type
             AND ee.dst = le.dst
           WHERE ee.src IS NULL
        ),
        source_missing AS (
          SELECT ee.source, ee.src, ee.edge_type, ee.dst
            FROM expected_edges ee
            LEFT JOIN live_source_edges le
              ON le.source = ee.source
             AND le.src = ee.src
             AND le.edge_type = ee.edge_type
             AND le.dst = ee.dst
           WHERE le.src IS NULL
        ),
        missing AS (
          SELECT sm.source, sm.src, sm.edge_type, sm.dst
            FROM source_missing sm
           WHERE NOT EXISTS (
             SELECT 1
               FROM okg.graph_edges ge
              WHERE ge.src = sm.src
                AND ge.edge_type = sm.edge_type
                AND ge.dst = sm.dst
           )
        )
        SELECT
          (SELECT count(*) FROM expected_edges)::int
            AS expected_edge_count,
          (SELECT count(*) FROM live_source_edges)::int
            AS live_source_edge_count,
          (SELECT count(*) FROM stale)::int
            AS stale_edge_count,
          (SELECT count(*) FROM missing)::int
            AS missing_edge_count,
          COALESCE((
            SELECT jsonb_object_agg(by_source.source, by_source.count)
              FROM (
                SELECT source, count(*)::int AS count
                  FROM stale
                 GROUP BY source
              ) by_source
          ), '{}'::jsonb)::text AS stale_edges_by_source,
          COALESCE((
            SELECT jsonb_agg(to_jsonb(sample))
              FROM (
                SELECT source, src, edge_type, dst
                  FROM stale
                 ORDER BY source, src, edge_type, dst
                 LIMIT :limit
              ) sample
          ), '[]'::jsonb)::text AS stale_edge_samples,
          COALESCE((
            SELECT jsonb_object_agg(by_source.source, by_source.count)
              FROM (
                SELECT source, count(*)::int AS count
                  FROM missing
                 GROUP BY source
              ) by_source
          ), '{}'::jsonb)::text AS missing_edges_by_source,
          COALESCE((
            SELECT jsonb_agg(to_jsonb(sample))
              FROM (
                SELECT source, src, edge_type, dst
                  FROM missing
                 ORDER BY source, src, edge_type, dst
                 LIMIT :limit
              ) sample
          ), '[]'::jsonb)::text AS missing_edge_samples
        """,
        {"limit": limit},
    )
    node_row = node_rows[0] if node_rows else {}
    edge_row = edge_rows[0] if edge_rows else {}
    return {
        "sources_with_record_set": int(
            node_row.get("sources_with_record_set") or 0,
        ),
        "expected_node_count": int(node_row.get("expected_node_count") or 0),
        "live_source_node_count": int(
            node_row.get("live_source_node_count") or 0,
        ),
        "stale_node_count": int(node_row.get("stale_node_count") or 0),
        "missing_node_count": int(node_row.get("missing_node_count") or 0),
        "stale_nodes_by_source": _json_from_db(
            node_row.get("stale_nodes_by_source"), default={},
        ),
        "stale_node_samples": _json_from_db(
            node_row.get("stale_node_samples"), default=[],
        ),
        "missing_nodes_by_source": _json_from_db(
            node_row.get("missing_nodes_by_source"), default={},
        ),
        "missing_node_samples": _json_from_db(
            node_row.get("missing_node_samples"), default=[],
        ),
        "expected_edge_count": int(edge_row.get("expected_edge_count") or 0),
        "live_source_edge_count": int(
            edge_row.get("live_source_edge_count") or 0,
        ),
        "stale_edge_count": int(edge_row.get("stale_edge_count") or 0),
        "missing_edge_count": int(edge_row.get("missing_edge_count") or 0),
        "stale_edges_by_source": _json_from_db(
            edge_row.get("stale_edges_by_source"), default={},
        ),
        "stale_edge_samples": _json_from_db(
            edge_row.get("stale_edge_samples"), default=[],
        ),
        "missing_edges_by_source": _json_from_db(
            edge_row.get("missing_edges_by_source"), default={},
        ),
        "missing_edge_samples": _json_from_db(
            edge_row.get("missing_edge_samples"), default=[],
        ),
    }


def _okg_workspace_record_sets(conn) -> dict[str, dict[str, Any]]:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT sw.source,
                   sw.last_applied_rev,
                   srb.record_set,
                   srs.record_set
              FROM okg.source_watermarks sw
              LEFT JOIN okg.source_record_set_blobs srb
                ON srb.source = sw.source
               AND srb.sha256 = sw.last_applied_rev
                 ->'record_set_ref'->>'sha256'
              LEFT JOIN okg.source_record_sets srs
                ON srs.source = sw.source
             WHERE sw.last_applied_rev ? 'record_set'
                OR sw.last_applied_rev ? 'record_set_ref'
            """
        )
        rows = cur.fetchall()

    out: dict[str, dict[str, Any]] = {}
    for source, revision, blob_record_set, legacy_record_set in rows:
        if isinstance(revision, str):
            revision = json.loads(revision)
        if not isinstance(revision, dict):
            continue
        record_set = (
            revision.get("record_set")
            or blob_record_set
            or legacy_record_set
        )
        if not isinstance(record_set, dict):
            continue
        records = record_set.get("records")
        if isinstance(records, dict):
            out[str(source)] = records
    return out


def _okg_workspace_agent_memory_quality(
    conn,
    generation_id: int | str,
) -> dict[str, Any]:
    deterministic_subtypes = [
        "agent_observation",
        "verification_result",
        "test_report",
        "coverage_report",
        "runtime_event",
        "stack_frame",
        "test_run",
        "test_case",
        "security_finding",
        "secret_finding",
        "dependency_vulnerability",
        "profile_run",
        "profile_stage",
        "profile_query",
        "profile_hotspot",
        "benchmark_measurement",
        "performance_budget",
        "data_flow_observation",
        "data_flow_step",
        "source_file",
        "code_symbol",
        "acceptance_criterion",
        "acceptance_scenario",
    ]
    from okg.substrate.graph_store import ChronosGraphStore

    # Chronos cutover: graph-content reads resolve through a checkout
    # pinned to the generation (okg.graph_nodes/graph_edges); the
    # checkout injects visibility, so no per-row generation predicate is
    # needed and binds use the `:name` style.
    graph = ChronosGraphStore.from_connection(
        conn, ensure_schema=False,
    ).checkout_generation(generation_id)
    memory_subtypes = [
        "agent_claim",
        "agent_observation",
        "agent_experiment",
        "verification_result",
        "agent_decision",
    ]

    supported_without_evidence = _agent_memory_rows(
        graph.query(
            """
            WITH claims AS (
              SELECT node_id, attrs
                FROM okg.graph_nodes
               WHERE subtype = 'agent_claim'
                 AND COALESCE(attrs->>'source_artifact', '')
                     !~ :fixture_source_re
            ),
            problem AS (
              SELECT c.node_id,
                     c.attrs->>'statement' AS statement,
                     c.attrs->>'status' AS status
                FROM claims c
               WHERE lower(COALESCE(c.attrs->>'status', '')) = 'supported'
                 AND NOT EXISTS (
                   SELECT 1
                     FROM okg.graph_edges e
                     JOIN okg.graph_nodes d
                       ON d.node_id = e.dst
                    WHERE e.src = c.node_id
                      AND e.edge_type = 'supported_by'
                 )
            )
            SELECT node_id, statement, status, count(*) OVER() AS total
              FROM problem
             ORDER BY node_id
             LIMIT 10
            """,
            {"fixture_source_re": AGENT_MEMORY_FIXTURE_SOURCE_RE},
        )
    )

    model_inferred_without_deterministic = _agent_memory_rows(
        graph.query(
            """
            WITH claims AS (
              SELECT node_id, attrs
                FROM okg.graph_nodes
               WHERE subtype = 'agent_claim'
                 AND COALESCE(attrs->>'source_artifact', '')
                     !~ :fixture_source_re
            ),
            problem AS (
              SELECT c.node_id,
                     c.attrs->>'statement' AS statement,
                     c.attrs->>'confidence_kind' AS confidence_kind
                FROM claims c
               WHERE lower(COALESCE(c.attrs->>'status', '')) = 'supported'
                 AND lower(COALESCE(c.attrs->>'confidence_kind', '')) =
                     'model_inferred'
                 AND NOT EXISTS (
                   SELECT 1
                     FROM okg.graph_edges e
                     JOIN okg.graph_nodes d
                       ON d.node_id = e.dst
                    WHERE e.src = c.node_id
                      AND e.edge_type = 'supported_by'
                      AND (
                        e.attrs->>'confidence_kind' = 'deterministic'
                        OR d.attrs->>'confidence_kind' = 'deterministic'
                        OR d.subtype = ANY(:deterministic)
                      )
                 )
            )
            SELECT node_id, statement, confidence_kind,
                   count(*) OVER() AS total
              FROM problem
             ORDER BY node_id
             LIMIT 10
            """,
            {
                "deterministic": deterministic_subtypes,
                "fixture_source_re": AGENT_MEMORY_FIXTURE_SOURCE_RE,
            },
        )
    )

    code_claim_without_code_subject = _agent_memory_rows(
        graph.query(
            """
            WITH claims AS (
              SELECT node_id, attrs
                FROM okg.graph_nodes
               WHERE subtype = 'agent_claim'
                 AND COALESCE(attrs->>'source_artifact', '')
                     !~ :fixture_source_re
            ),
            problem AS (
              SELECT c.node_id,
                     c.attrs->>'statement' AS statement,
                     c.attrs->>'claim_kind' AS claim_kind
                FROM claims c
               WHERE (
                   lower(COALESCE(c.attrs->>'claim_kind', '')) LIKE '%code%'
                   OR lower(COALESCE(c.attrs->>'claim_kind', ''))
                      LIKE '%implementation%'
                 )
                 AND NOT EXISTS (
                   SELECT 1
                     FROM okg.graph_edges e
                     JOIN okg.graph_nodes d
                       ON d.node_id = e.dst
                    WHERE e.src = c.node_id
                      AND e.edge_type = 'references'
                      AND d.subtype IN ('source_file', 'code_symbol')
                 )
            )
            SELECT node_id, statement, claim_kind, count(*) OVER() AS total
              FROM problem
             ORDER BY node_id
             LIMIT 10
            """,
            {"fixture_source_re": AGENT_MEMORY_FIXTURE_SOURCE_RE},
        )
    )

    # Chronos cutover (class-A reduction): the prior query also flagged
    # supported claims whose evidence was published at a generation later
    # than the claim's last_verified_generation_id. That branch read the
    # max evidence generation over evidence nodes — a per-row generation
    # column the Chronos graph does not carry — so it is dropped. The
    # surviving check reports claims explicitly marked status='stale'.
    stale_supported = _agent_memory_rows(
        graph.query(
            """
            SELECT node_id,
                   attrs->>'statement' AS statement,
                   attrs->>'status' AS status,
                   count(*) OVER() AS total
              FROM okg.graph_nodes
             WHERE subtype = 'agent_claim'
               AND COALESCE(attrs->>'source_artifact', '')
                   !~ :fixture_source_re
               AND lower(COALESCE(attrs->>'status', '')) = 'stale'
             ORDER BY node_id
             LIMIT 10
            """,
            {"fixture_source_re": AGENT_MEMORY_FIXTURE_SOURCE_RE},
        )
    )

    packet_warnings = _agent_memory_rows(
        graph.query(
            """
            WITH memory_nodes AS (
              SELECT node_id, subtype, attrs
                FROM okg.graph_nodes
               WHERE subtype = ANY(:subtypes)
                 AND COALESCE(attrs->>'source_artifact', '')
                     !~ :fixture_source_re
            ),
            problem AS (
              SELECT node_id, subtype,
                     attrs->'validation_warnings' AS validation_warnings
                FROM memory_nodes
               WHERE jsonb_typeof(attrs->'validation_warnings') = 'array'
                 AND jsonb_array_length(attrs->'validation_warnings') > 0
            )
            SELECT node_id, subtype, validation_warnings::text AS validation_warnings,
                   count(*) OVER() AS total
              FROM problem
             ORDER BY node_id
             LIMIT 10
            """,
            {
                "subtypes": memory_subtypes,
                "fixture_source_re": AGENT_MEMORY_FIXTURE_SOURCE_RE,
            },
        )
    )

    subtype_counts = {
        str(row["subtype"]): int(row["n"])
        for row in graph.query(
            """
            SELECT subtype, count(*) AS n
              FROM okg.graph_nodes
             WHERE subtype = ANY(:subtypes)
             GROUP BY subtype
            """,
            {"subtypes": memory_subtypes},
        )
    }

    return {
        "subtype_counts": subtype_counts,
        "supported_without_evidence_count": supported_without_evidence["count"],
        "supported_without_evidence_samples": supported_without_evidence["samples"],
        "model_inferred_supported_without_deterministic_count": (
            model_inferred_without_deterministic["count"]
        ),
        "model_inferred_supported_without_deterministic_samples": (
            model_inferred_without_deterministic["samples"]
        ),
        "code_claim_without_code_subject_count": (
            code_claim_without_code_subject["count"]
        ),
        "code_claim_without_code_subject_samples": (
            code_claim_without_code_subject["samples"]
        ),
        "stale_supported_claim_count": stale_supported["count"],
        "stale_supported_claim_samples": stale_supported["samples"],
        "packet_warning_count": packet_warnings["count"],
        "packet_warning_samples": packet_warnings["samples"],
    }


def _agent_memory_rows(rows: list[dict[str, Any]]) -> dict[str, Any]:
    """Shape Chronos dict rows into {count, samples}.

    Each query selects projection columns followed by a trailing
    ``total`` window count; the first column is the node id. Rows are
    dicts (Chronos checkout ``query`` output) whose insertion order
    matches the SELECT column order.
    """
    if not rows:
        return {"count": 0, "samples": []}
    values = [list(row.values()) for row in rows]
    total = int(values[0][-1] or 0)
    samples = []
    for value_row in values:
        sample = {
            f"field_{idx}": value
            for idx, value in enumerate(value_row[:-1])
        }
        if value_row:
            sample["node_id"] = str(value_row[0])
        samples.append(sample)
    return {"count": total, "samples": samples}


def _json_from_db(value: Any, *, default: Any) -> Any:
    if value is None:
        return default
    if isinstance(value, str):
        try:
            return json.loads(value)
        except json.JSONDecodeError:
            return default
    return value


def _configured_or_missing(
    conn, ownership_id: str, missing_status: str,
) -> str:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT 1 FROM okg.source_watermarks WHERE source = %s",
            (ownership_id,),
        )
        return "configured" if cur.fetchone() else missing_status


def _okg_workspace_queue_snapshot(conn) -> dict[str, Any]:
    from okg.substrate.runtime_queue_health import (
        queue_status_counts_for_viz,
        summarize_runtime_queue_health,
    )

    with conn.cursor() as cur:
        summary = summarize_runtime_queue_health(cur)
    counts = queue_status_counts_for_viz(summary)
    embed_ages = [
        float(row["oldest_active_age_seconds"])
        for row in summary.get("runtime_queue_health", [])
        if row.get("workflow_name") in {"okg_embed_batch", "okg_embed_batch_enqueue"}
        and row.get("oldest_active_age_seconds") is not None
    ]
    counts.update(
        {
            "oldest_embed_queued_seconds": max(embed_ages) if embed_ages else None,
            "runtime_status": summary.get("status"),
            "terminal_failure_history": summary.get(
                "terminal_history_dlq_count",
                0,
            ),
        }
    )
    return counts


def _okg_workspace_source_freshness(conn) -> dict[str, Any]:
    freshness: dict[str, Any] = {}
    with conn.cursor() as cur:
        for name in (
            "codex-sessions",
            "security-analysis",
            "profile-measurements",
        ):
            cur.execute(
                """
                SELECT EXTRACT(EPOCH FROM now() - last_reconcile_at)
                  FROM okg.source_watermarks
                 WHERE source = %s
                   AND last_reconcile_at IS NOT NULL
                """,
                (f"{{ deployment_name }}.{name}",),
            )
            row = cur.fetchone()
            freshness[f"{name}_age_seconds"] = (
                float(row[0]) if row and row[0] is not None else None
            )
    return freshness


def _okg_workspace_severity_counts(
    graph,
    *,
    subtypes: tuple[str, ...],
) -> dict[str, int]:
    rows = graph.query(
        """
        SELECT lower(COALESCE(attrs->>'severity', 'unknown')) AS severity,
               count(*) AS n
          FROM okg.graph_nodes
         WHERE subtype = ANY(:subtypes)
         GROUP BY 1
        """,
        {"subtypes": list(subtypes)},
    )
    return {str(row["severity"]): int(row["n"]) for row in rows}


def _okg_workspace_top_profile_hotspot(
    graph,
) -> dict[str, Any] | None:
    rows = graph.query(
        """
        SELECT node_id, attrs
          FROM okg.graph_nodes
         WHERE subtype = 'profile_hotspot'
         ORDER BY COALESCE(attrs->>'self_time_ms', '') DESC,
                  COALESCE(attrs->>'total_time_ms', '') DESC
         LIMIT 1
        """,
    )
    if not rows:
        return None
    row = rows[0]
    return {"node_id": str(row["node_id"]), "attrs": dict(row["attrs"] or {})}


def _okg_workspace_performance_failures(graph) -> list[dict[str, Any]]:
    rows = graph.query(
        """
        SELECT bm.node_id AS bm_node_id, bm.attrs AS bm_attrs,
               pb.node_id AS pb_node_id, pb.attrs AS pb_attrs
          FROM okg.graph_nodes bm
          JOIN okg.graph_edges e
            ON e.src = bm.node_id
           AND e.edge_type = 'references'
          JOIN okg.graph_nodes pb
            ON pb.node_id = e.dst
           AND pb.subtype = 'performance_budget'
         WHERE bm.subtype = 'benchmark_measurement'
        """
    )

    failures: list[dict[str, Any]] = []
    for row in rows:
        measurement_id = row["bm_node_id"]
        budget_id = row["pb_node_id"]
        measurement = dict(row["bm_attrs"] or {})
        budget = dict(row["pb_attrs"] or {})
        value = _float_or_none(measurement.get("value"))
        threshold = _float_or_none(budget.get("threshold"))
        status = str(measurement.get("status") or "").lower()
        operator = str(budget.get("operator") or "<=")
        failed = status == "failed"
        if value is not None and threshold is not None:
            if operator in {"<=", "max", "under"} and value > threshold:
                failed = True
            elif operator in {">=", "min", "over"} and value < threshold:
                failed = True
        if not failed:
            continue
        failures.append({
            "measurement_id": str(measurement_id),
            "budget_id": str(budget_id),
            "scenario": str(measurement.get("scenario") or ""),
            "metric": str(measurement.get("metric") or budget.get("metric") or ""),
            "value": value,
            "threshold": threshold,
            "unit": str(measurement.get("unit") or budget.get("unit") or ""),
            "operator": operator,
            "status": status or "failed",
        })
    return failures


def _float_or_none(value: Any) -> float | None:
    try:
        if value is None or value == "":
            return None
        return float(value)
    except (TypeError, ValueError):
        return None
