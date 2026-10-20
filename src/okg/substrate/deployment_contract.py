"""Deployment portfolio lifecycle contract audit.

This module is intentionally DB-free. It validates the portfolio-level
``contract:`` block carried by deployment manifests and composes the existing
deployment lint report instead of duplicating manifest/source rules.
"""
from __future__ import annotations

import os
import re
from collections.abc import Iterable, Mapping
from datetime import datetime, timezone
from pathlib import Path
from types import MappingProxyType
from typing import Any

from okg.substrate import fastyaml
from okg.substrate.deployment_lint import lint_deployment
from okg.substrate.mcp.operators import CURRENT_TOOL_MIGRATIONS, OPERATOR_TOOLS


CONTRACT_AUDIT_SCHEMA = "okg.deployment_contract.audit.v1"
CONTRACT_VERSION = 1
CONTRACT_LEVELS = ("current", "next")
LIFECYCLE_STATES = (
    "production",
    "staging",
    "prototype",
    "demo",
    "blocked",
    "archived",
)

_PROD_LIKE = frozenset({"production", "staging"})
_NON_PROD_ACTIVE = frozenset({"prototype", "demo"})
_READY_GOOD = frozenset({"clean", "pass"})
_READY_NON_PROD = frozenset({
    "blocked",
    "command_failed",
    "historical",
    "not_required",
    "stale",
    "unavailable",
})
_READY_ALLOWED = _READY_GOOD | _READY_NON_PROD
_BLOCKED_READY_STATUSES = frozenset({
    "blocked",
    "command_failed",
    "stale",
    "unavailable",
})
READY_STATUS_VOCABULARY = _READY_ALLOWED
# Every status word a readiness producer can emit, mapped onto the closed
# contract vocabulary above (issue #1572). The readiness slot builder in
# ``okg.substrate.cli.deployment`` is the only consumer; it must not invent a
# status the auditor cannot read. ``summarize_runtime_queue_health`` emits
# "available", "dead_letter" and "unknown", none of which used to appear here,
# so a HEALTHY production queue reported "available" and tripped
# ``deployment_contract.readiness.slot_not_clean``. The words come from the
# readiness facet producers named in
# ``tests/substrate/cli/test_deployment_ready_evidence.py``, which reads them
# out of those functions and fails when one is missing from this table.
READY_STATUS_ALIASES: Mapping[str, str] = MappingProxyType({
    # healthy
    "available": "clean",
    "clean": "clean",
    "fresh": "clean",
    "ok": "clean",
    "pass": "clean",
    "published": "clean",
    "ready": "clean",
    # degraded but not a blocker on its own
    "stale": "stale",
    "warning": "stale",
    # deliberately not measured
    "not_required": "not_required",
    "skipped": "not_required",
    # measured nothing: the probe did not produce a verdict
    "missing": "unavailable",
    "unavailable": "unavailable",
    "unknown": "unavailable",
    # a run that could not complete
    "check_load_error": "command_failed",
    "command_failed": "command_failed",
    # a measured failure
    "blocked": "blocked",
    "blocker": "blocked",
    "dead_letter": "blocked",
    "fail": "blocked",
    "failed": "blocked",
    "mismatch": "blocked",
    # audit-only classification the auditor already accepts verbatim
    "historical": "historical",
})


def normalize_ready_slot_status(status: object) -> tuple[str, str | None]:
    """Map a producer status word onto the readiness contract vocabulary.

    Returns ``(contract_status, unrecognized_word)``. This is FAIL-CLOSED: a
    word :data:`READY_STATUS_ALIASES` does not define reports "unavailable"
    and is returned as the second element so the caller records the raw word
    in evidence. Passing an unmapped word through would put a status in the
    evidence block that the auditor cannot classify -- neither allowed (so a
    live deployment fails ``slot_not_clean``) nor blocking (so a blocked
    deployment cannot anchor its blocker scope). Issue #1572.
    """

    raw = "" if status is None else str(status)
    mapped = READY_STATUS_ALIASES.get(raw.strip().lower())
    if mapped is None:
        return "unavailable", raw
    return mapped, None


_LIVE_READINESS_SLOTS = (
    "environment",
    "migrations",
    "latest_published_generation",
    "source_freshness",
    "queues_runtime",
    "dlq",
    "doctor",
    "mcp_read_alignment",
)
_SEVERITY_ORDER = {"blocker": 0, "warning": 1, "info": 2, "skipped": 3}
_LEGACY_TOOL_TERMS = tuple(sorted({
    *CURRENT_TOOL_MIGRATIONS.keys(),
    "tool_surface",
}))
_MAX_INSTRUCTION_BYTES = 256 * 1024


def audit_deployment_portfolio(
    root: str | Path,
    *,
    contract_level: str = "current",
    include_archived: bool = False,
    env: Mapping[str, str] | None = None,
) -> dict[str, Any]:
    """Audit every deployment manifest under ``root``.

    ``contract_level="current"`` gates the contract that is expected to pass
    today. Source-probe enforcement is fail-closed in both levels.
    ``contract_level="next"`` enables forward-looking checks such as retired
    AKMON surface references.
    """

    if contract_level not in CONTRACT_LEVELS:
        raise ValueError(
            "contract_level must be one of "
            f"{', '.join(CONTRACT_LEVELS)}"
        )
    root_path = Path(root).resolve()
    base_env = dict(os.environ if env is None else env)

    manifests = _discover_manifest_paths(root_path)
    deployments: list[dict[str, Any]] = []
    portfolio_findings: list[dict[str, Any]] = []
    if not manifests:
        portfolio_findings.append(_finding(
            code="deployment_portfolio.manifests.none_found",
            severity="blocker",
            stage="discovery",
            message="no deployment.yaml files were found",
            why=(
                "The portfolio audit needs at least one deployment manifest "
                "to evaluate the lifecycle contract."
            ),
            location=_loc(root_path),
            expected="one or more deployment.yaml files",
            next_action="run from okg-deployments or pass --root <repo>",
        ))

    for manifest_path in manifests:
        deployment = _audit_one_deployment(
            manifest_path,
            root_path=root_path,
            contract_level=contract_level,
            env=base_env,
        )
        if (
            deployment.get("lifecycle") == "archived"
            and not include_archived
            and not deployment.get("finding_count", 0)
        ):
            continue
        deployments.append(deployment)

    all_findings = [
        *portfolio_findings,
        *[
            finding
            for deployment in deployments
            for finding in deployment.get("findings", [])
        ],
    ]
    summary = _summary(all_findings)
    readiness_summary = _portfolio_readiness_summary(deployments, all_findings)
    return {
        "schema": CONTRACT_AUDIT_SCHEMA,
        "ok": summary["blockers"] == 0,
        "root": str(root_path),
        "contract_level": contract_level,
        "include_archived": include_archived,
        "deployment_count": len(deployments),
        "finding_count": len(all_findings),
        "summary": summary,
        "readiness_summary": readiness_summary,
        "findings": _sorted_findings(portfolio_findings),
        "deployments": deployments,
    }


def format_portfolio_audit_summary(report: Mapping[str, Any]) -> str:
    """Concise human-readable rendering for ``okg deployments audit``."""

    summary = report.get("summary") or {}
    lines = [
        (
            "deployments audit: "
            f"{report.get('deployment_count', 0)} deployment(s), "
            f"ok={str(report.get('ok')).lower()}, "
            f"blockers={summary.get('blockers', 0)}, "
            f"warnings={summary.get('warnings', 0)}"
        )
    ]
    readiness_summary = report.get("readiness_summary") or {}
    if isinstance(readiness_summary, Mapping):
        lines.append(
            "readiness summary: "
            f"env_warnings={readiness_summary.get('env_var_warning_count', 0)}, "
            f"governance_missing={readiness_summary.get('missing_governance_count', 0)}, "
            f"blocked_scopes={readiness_summary.get('blocked_readiness_scope_count', 0)}, "
            f"stale_or_historical={readiness_summary.get('stale_or_historical_evidence_count', 0)}"
        )
    for deployment in report.get("deployments") or []:
        findings = list(deployment.get("findings") or [])
        lifecycle = deployment.get("lifecycle") or "unknown"
        warning_count = sum(
            1 for finding in findings if finding.get("severity") == "warning"
        )
        lines.append(
            f"- {deployment.get('name')}: lifecycle={lifecycle}, "
            f"findings={len(findings)}, ok={str(deployment.get('ok')).lower()}"
        )
        for finding in findings:
            if finding.get("severity") != "blocker":
                continue
            location = finding.get("location") or {}
            suffix = (
                f" ({location.get('path')})" if location.get("path") else ""
            )
            lines.append(
                f"  - {finding.get('severity')} {finding.get('code')}: "
                f"{finding.get('message')}{suffix}"
            )
            if finding.get("next_action"):
                lines.append(f"    next: {finding.get('next_action')}")
        if warning_count:
            lines.append(
                f"  warnings: {warning_count} "
                "(run with --json for full warning details)"
            )
    if report.get("findings"):
        for finding in report["findings"]:
            lines.append(
                f"- {finding.get('severity')} {finding.get('code')}: "
                f"{finding.get('message')}"
            )
    return "\n".join(lines)


def _audit_one_deployment(
    manifest_path: Path,
    *,
    root_path: Path,
    contract_level: str,
    env: Mapping[str, str],
) -> dict[str, Any]:
    raw, load_findings = _load_manifest_mapping(manifest_path)
    deployment_name = _deployment_name(raw, manifest_path)
    contract = raw.get("contract") if isinstance(raw, Mapping) else None
    lifecycle = (
        str(contract.get("lifecycle"))
        if isinstance(contract, Mapping)
        and contract.get("lifecycle") is not None
        else None
    )

    findings: list[dict[str, Any]] = list(load_findings)
    if isinstance(raw, Mapping):
        findings.extend(_check_contract_block(
            contract,
            manifest_path=manifest_path,
            deployment_name=deployment_name,
            lifecycle=lifecycle,
            contract_level=contract_level,
        ))
        if lifecycle != "archived":
            findings.extend(_check_akmon_surface(
                raw,
                manifest_path=manifest_path,
                lifecycle=lifecycle,
                contract_level=contract_level,
            ))
            findings.extend(_check_instruction_surface(
                raw,
                manifest_path=manifest_path,
                deployment_name=deployment_name,
                lifecycle=lifecycle,
                contract_level=contract_level,
            ))

    lint_report = lint_deployment(manifest_path, env=env)
    lint_findings = _contract_lint_findings(
        lint_report,
        lifecycle=lifecycle,
        contract_level=contract_level,
    )
    findings.extend(lint_findings)

    sorted_findings = _sorted_findings(findings)
    summary = _summary(sorted_findings)
    normalized_readiness = _normalize_readiness_evidence(
        contract,
        manifest_path=manifest_path,
    )
    return {
        "name": deployment_name,
        "deployment_dir": str(manifest_path.parent),
        "manifest_path": str(manifest_path),
        "relative_path": _relative_to_root(manifest_path, root_path),
        "lifecycle": lifecycle,
        "contract_version": (
            contract.get("version") if isinstance(contract, Mapping) else None
        ),
        "readiness_evidence": normalized_readiness,
        "lint": {
            "ok": bool(lint_report.get("ok")),
            "finding_count": lint_report.get("finding_count", 0),
            "blockers": sum(
                1 for f in lint_report.get("findings", [])
                if f.get("severity") == "blocker"
            ),
            "warnings": sum(
                1 for f in lint_report.get("findings", [])
                if f.get("severity") == "warning"
            ),
            "infos": sum(
                1 for f in lint_report.get("findings", [])
                if f.get("severity") == "info"
            ),
        },
        "ok": summary["blockers"] == 0,
        "finding_count": len(sorted_findings),
        "summary": summary,
        "findings": sorted_findings,
    }


def _discover_manifest_paths(root: Path) -> list[Path]:
    if (root / "deployment.yaml").is_file():
        return [root / "deployment.yaml"]
    direct = sorted(root.glob("*/deployment.yaml"))
    if direct:
        return direct
    deployments_dir = root / "deployments"
    if deployments_dir.is_dir():
        return sorted(deployments_dir.glob("*/deployment.yaml"))
    return []


def _load_manifest_mapping(
    manifest_path: Path,
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    try:
        raw = fastyaml.safe_load(manifest_path.read_text(encoding="utf-8"))
    except Exception as exc:  # noqa: BLE001 - normalized as contract finding.
        return {}, [_finding(
            code="deployment_contract.manifest.yaml_parse",
            severity="blocker",
            stage="manifest",
            message="deployment.yaml could not be parsed",
            why="The lifecycle contract cannot be evaluated until YAML parses.",
            location=_loc(manifest_path),
            observed=type(exc).__name__,
            expected="valid YAML mapping",
            next_action="fix deployment.yaml syntax",
            details={"error": str(exc)},
        )]
    if not isinstance(raw, dict):
        return {}, [_finding(
            code="deployment_contract.manifest.not_mapping",
            severity="blocker",
            stage="manifest",
            message="deployment.yaml must contain a mapping",
            why="The lifecycle contract is a named top-level manifest block.",
            location=_loc(manifest_path),
            observed=type(raw).__name__,
            expected="mapping",
            next_action="replace deployment.yaml with a key/value mapping",
        )]
    return raw, []


def _check_contract_block(
    contract: Any,
    *,
    manifest_path: Path,
    deployment_name: str,
    lifecycle: str | None,
    contract_level: str,
) -> list[dict[str, Any]]:
    findings: list[dict[str, Any]] = []
    if not isinstance(contract, Mapping):
        return [_finding(
            code="deployment_contract.block.missing",
            severity="blocker",
            stage="contract",
            message="deployment.yaml is missing a contract block",
            why=(
                "Every deployment in the portfolio must declare lifecycle, "
                "usage, owner, and evidence expectations."
            ),
            location=_loc(manifest_path, pointer="/contract"),
            expected="contract: {version, lifecycle, owner, intended_usage}",
            next_action="add a contract block to deployment.yaml",
        )]

    version = contract.get("version")
    if version not in (CONTRACT_VERSION, str(CONTRACT_VERSION)):
        findings.append(_finding(
            code="deployment_contract.version.unsupported",
            severity="blocker",
            stage="contract",
            message="deployment contract version is unsupported",
            why="The auditor only knows the v1 deployment lifecycle contract.",
            location=_loc(manifest_path, pointer="/contract/version"),
            observed=version,
            expected=CONTRACT_VERSION,
            next_action="set contract.version to 1",
        ))

    if lifecycle not in LIFECYCLE_STATES:
        findings.append(_finding(
            code="deployment_contract.lifecycle.invalid",
            severity="blocker",
            stage="contract",
            message="deployment contract lifecycle is invalid",
            why="Portfolio automation needs one known lifecycle state.",
            location=_loc(manifest_path, pointer="/contract/lifecycle"),
            observed=lifecycle,
            expected=list(LIFECYCLE_STATES),
            next_action="choose one of the supported lifecycle states",
        ))

    if not _non_empty(contract.get("owner")):
        findings.append(_finding(
            code="deployment_contract.owner.missing",
            severity="blocker",
            stage="contract",
            message="deployment contract owner is missing",
            why="Every deployment needs an accountable operational owner.",
            location=_loc(manifest_path, pointer="/contract/owner"),
            expected="non-empty owner string or mapping",
            next_action="set contract.owner",
        ))

    if not _non_empty(contract.get("intended_usage")):
        findings.append(_finding(
            code="deployment_contract.usage.missing",
            severity="blocker",
            stage="contract",
            message="deployment contract intended_usage is missing",
            why="The portfolio audit distinguishes production, demo, and research use.",
            location=_loc(manifest_path, pointer="/contract/intended_usage"),
            expected="short usage statement",
            next_action="set contract.intended_usage",
        ))

    readiness = _readiness_block(contract)
    if lifecycle in _PROD_LIKE:
        findings.extend(_check_live_readiness(
            readiness,
            manifest_path=manifest_path,
        ))
    elif lifecycle in _NON_PROD_ACTIVE:
        findings.extend(_check_non_prod_readiness(
            readiness,
            manifest_path=manifest_path,
        ))
    elif lifecycle == "blocked":
        findings.extend(_check_blocked_contract(
            contract,
            readiness,
            manifest_path=manifest_path,
        ))
    elif lifecycle == "archived":
        if not _non_empty(contract.get("archive_reason")):
            findings.append(_finding(
                code="deployment_contract.archive_reason.missing",
                severity="blocker",
                stage="contract",
                message="archived deployment lacks an archive reason",
                why="Archived deployments must explain why they are not active.",
                location=_loc(manifest_path, pointer="/contract/archive_reason"),
                expected="archive reason",
                next_action="set contract.archive_reason",
            ))

    if contract_level == "next" and lifecycle in {"prototype", "demo"}:
        if not _non_empty(contract.get("promotion_path")):
            findings.append(_finding(
                code="deployment_contract.promotion_path.missing",
                severity="warning",
                stage="contract",
                message="non-production deployment lacks a promotion path",
                why=(
                    "The next contract expects prototype/demo deployments to "
                    "name what evidence would promote or retire them."
                ),
                location=_loc(manifest_path, pointer="/contract/promotion_path"),
                expected="promotion or retirement path",
                next_action=(
                    "set contract.promotion_path or explicitly archive the deployment"
                ),
            ))

    if contract_level == "next" and lifecycle == "blocked":
        review_by = _review_by(contract)
        today = datetime.now(timezone.utc).date()
        if review_by is not None and review_by < today:
            findings.append(_finding(
                code="deployment_contract.blocker.review_overdue",
                severity="blocker",
                stage="contract",
                message="blocked deployment review date has passed",
                why="Blocked deployments need an active review window.",
                location=_loc(manifest_path, pointer="/contract/blocker/review_by"),
                observed=review_by.isoformat(),
                expected=f"date on or after {today.isoformat()}",
                next_action=f"review or retire {deployment_name}",
            ))
    findings.extend(_check_waivers(contract, manifest_path=manifest_path))
    return findings


def _check_live_readiness(
    readiness: Mapping[str, Any] | None,
    *,
    manifest_path: Path,
) -> list[dict[str, Any]]:
    if not isinstance(readiness, Mapping):
        return [_finding(
            code="deployment_contract.readiness.missing",
            severity="blocker",
            stage="readiness",
            message="production/staging deployment lacks readiness evidence",
            why=(
                "Live lifecycle states must carry fresh evidence from the "
                "readiness/doctor path."
            ),
            location=_loc(manifest_path, pointer="/contract/evidence/readiness"),
            expected="readiness evidence with status, collected_at, and ttl_seconds",
            next_action="run okg deployment ready and attach the evidence reference",
        )]
    findings = _check_readiness_shape(readiness, manifest_path=manifest_path)
    status = str(readiness.get("status") or "")
    if status not in _READY_GOOD:
        findings.append(_finding(
            code="deployment_contract.readiness.not_clean",
            severity="blocker",
            stage="readiness",
            message="production/staging readiness evidence is not clean",
            why="Live deployments must prove they currently pass readiness checks.",
            location=_loc(manifest_path, pointer="/contract/evidence/readiness/status"),
            observed=status or None,
            expected=sorted(_READY_GOOD),
            next_action="refresh readiness evidence or move the lifecycle out of production/staging",
        ))
    collected_at = _parse_datetime(readiness.get("collected_at"))
    ttl_seconds = readiness.get("ttl_seconds")
    if collected_at is None:
        findings.append(_finding(
            code="deployment_contract.readiness.collected_at_missing",
            severity="blocker",
            stage="readiness",
            message="readiness evidence lacks collected_at",
            why="Freshness cannot be evaluated without a collection timestamp.",
            location=_loc(manifest_path, pointer="/contract/evidence/readiness/collected_at"),
            expected="ISO-8601 timestamp",
            next_action="record when the readiness evidence was collected",
        ))
    if not isinstance(ttl_seconds, int) or ttl_seconds <= 0:
        findings.append(_finding(
            code="deployment_contract.readiness.ttl_invalid",
            severity="blocker",
            stage="readiness",
            message="readiness evidence ttl_seconds is invalid",
            why="Freshness cannot be evaluated without a positive TTL.",
            location=_loc(manifest_path, pointer="/contract/evidence/readiness/ttl_seconds"),
            observed=ttl_seconds,
            expected="positive integer seconds",
            next_action="set contract.evidence.readiness.ttl_seconds",
        ))
    elif collected_at is not None:
        age = (datetime.now(timezone.utc) - collected_at).total_seconds()
        if age > ttl_seconds:
            findings.append(_finding(
                code="deployment_contract.readiness.expired",
                severity="blocker",
                stage="readiness",
                message="readiness evidence is expired",
                why="Live deployment evidence must be within its TTL.",
                location=_loc(manifest_path, pointer="/contract/evidence/readiness"),
                observed={"age_seconds": int(age), "ttl_seconds": ttl_seconds},
                expected="age_seconds <= ttl_seconds",
                next_action="refresh readiness evidence",
            ))
        elif age < -60:
            findings.append(_finding(
                code="deployment_contract.readiness.future_collected_at",
                severity="blocker",
                stage="readiness",
                message="readiness evidence collected_at is in the future",
                why="Future-dated readiness evidence can make stale proof look fresh.",
                location=_loc(manifest_path, pointer="/contract/evidence/readiness/collected_at"),
                observed={"age_seconds": int(age), "collected_at": collected_at.isoformat()},
                expected="collected_at must not be in the future",
                next_action="fix the readiness evidence timestamp",
            ))
    findings.extend(_check_live_readiness_slots(
        readiness,
        manifest_path=manifest_path,
    ))
    return findings


def _check_live_readiness_slots(
    readiness: Mapping[str, Any],
    *,
    manifest_path: Path,
) -> list[dict[str, Any]]:
    slots = readiness.get("slots")
    if not isinstance(slots, Mapping):
        return [_finding(
            code="deployment_contract.readiness.slots_missing",
            severity="blocker",
            stage="readiness",
            message="production/staging readiness evidence lacks required slots",
            why=(
                "Live lifecycle states must prove environment, migrations, "
                "generation, source freshness, queues/DLQ, doctor, and MCP read alignment."
            ),
            location=_loc(manifest_path, pointer="/contract/evidence/readiness/slots"),
            expected=list(_LIVE_READINESS_SLOTS),
            next_action="attach readiness evidence from okg deployment ready",
        )]
    findings: list[dict[str, Any]] = []
    for slot_name in _LIVE_READINESS_SLOTS:
        slot = slots.get(slot_name)
        pointer = f"/contract/evidence/readiness/slots/{slot_name}"
        if not isinstance(slot, Mapping):
            findings.append(_finding(
                code="deployment_contract.readiness.slot_missing",
                severity="blocker",
                stage="readiness",
                message=f"readiness evidence lacks required slot {slot_name!r}",
                why="Production/staging readiness must prove every required slot.",
                location=_loc(manifest_path, pointer=pointer),
                expected="slot mapping with status: clean",
                next_action="refresh readiness evidence with a full readiness profile",
                details={"slot": slot_name},
            ))
            continue
        status = str(slot.get("status") or "")
        if status not in _READY_GOOD:
            findings.append(_finding(
                code="deployment_contract.readiness.slot_not_clean",
                severity="blocker",
                stage="readiness",
                message=f"readiness slot {slot_name!r} is not clean",
                why="Production/staging readiness cannot rely on skipped, stale, unavailable, or blocked slots.",
                location=_loc(manifest_path, pointer=f"{pointer}/status"),
                observed=status or None,
                expected=sorted(_READY_GOOD),
                next_action="fix the failing readiness slot or lower the lifecycle",
                details={"slot": slot_name},
            ))
        observed_at = _parse_datetime(
            slot.get("observed_at") or slot.get("collected_at")
        )
        if observed_at is None:
            findings.append(_finding(
                code="deployment_contract.readiness.slot_observed_at_missing",
                severity="blocker",
                stage="readiness",
                message=f"readiness slot {slot_name!r} lacks an observation timestamp",
                why=(
                    "Production/staging readiness slots must be traceable to "
                    "a concrete observation, not just a status stub."
                ),
                location=_loc(manifest_path, pointer=pointer),
                expected="observed_at or collected_at ISO-8601 timestamp",
                next_action="refresh readiness evidence with full slot metadata",
                details={"slot": slot_name},
            ))
        elif (datetime.now(timezone.utc) - observed_at).total_seconds() < -60:
            findings.append(_finding(
                code="deployment_contract.readiness.slot_future_observed_at",
                severity="blocker",
                stage="readiness",
                message=f"readiness slot {slot_name!r} is future-dated",
                why="Future-dated slot evidence can make stale proof look fresh.",
                location=_loc(manifest_path, pointer=pointer),
                observed=observed_at.isoformat(),
                expected="slot timestamp must not be in the future",
                next_action="fix the readiness slot timestamp",
                details={"slot": slot_name},
            ))
        if not _slot_has_provenance(slot):
            findings.append(_finding(
                code="deployment_contract.readiness.slot_provenance_missing",
                severity="blocker",
                stage="readiness",
                message=f"readiness slot {slot_name!r} lacks provenance",
                why=(
                    "Production/staging readiness slots must identify the "
                    "command or artifact that produced the observation."
                ),
                location=_loc(manifest_path, pointer=pointer),
                expected="provenance, path, artifact, or command",
                next_action="refresh readiness evidence with slot provenance",
                details={"slot": slot_name},
            ))
        slot_ttl = slot.get("ttl_seconds")
        slot_expires_at = _parse_datetime(slot.get("expires_at"))
        readiness_ttl = readiness.get("ttl_seconds")
        effective_ttl = (
            slot_ttl
            if isinstance(slot_ttl, int) and slot_ttl > 0
            else readiness_ttl
            if isinstance(readiness_ttl, int) and readiness_ttl > 0
            else None
        )
        if (
            effective_ttl is None
            and slot_expires_at is None
        ):
            findings.append(_finding(
                code="deployment_contract.readiness.slot_ttl_missing",
                severity="blocker",
                stage="readiness",
                message=f"readiness slot {slot_name!r} lacks freshness bounds",
                why=(
                    "Production/staging readiness slots must either carry "
                    "their own freshness window or inherit the readiness TTL."
                ),
                location=_loc(manifest_path, pointer=pointer),
                expected="slot ttl_seconds/expires_at or top-level readiness ttl_seconds",
                next_action="set slot ttl_seconds/expires_at or readiness ttl_seconds",
                details={"slot": slot_name},
            ))
        elif observed_at is not None:
            now = datetime.now(timezone.utc)
            if slot_expires_at is not None and now > slot_expires_at:
                findings.append(_finding(
                    code="deployment_contract.readiness.slot_expired",
                    severity="blocker",
                    stage="readiness",
                    message=f"readiness slot {slot_name!r} evidence is expired",
                    why="Production/staging readiness slots must be fresh.",
                    location=_loc(manifest_path, pointer=pointer),
                    observed=slot_expires_at.isoformat(),
                    expected="expires_at in the future",
                    next_action="refresh readiness evidence for the expired slot",
                    details={"slot": slot_name},
                ))
            elif effective_ttl is not None:
                age_seconds = int((now - observed_at).total_seconds())
                if age_seconds > effective_ttl:
                    findings.append(_finding(
                        code="deployment_contract.readiness.slot_expired",
                        severity="blocker",
                        stage="readiness",
                        message=f"readiness slot {slot_name!r} evidence is expired",
                        why="Production/staging readiness slots must be fresh.",
                        location=_loc(manifest_path, pointer=pointer),
                        observed={
                            "age_seconds": age_seconds,
                            "ttl_seconds": effective_ttl,
                        },
                        expected="age_seconds <= ttl_seconds",
                        next_action=(
                            "refresh readiness evidence for the expired slot"
                        ),
                        details={"slot": slot_name},
                    ))
    return findings


def _check_non_prod_readiness(
    readiness: Mapping[str, Any] | None,
    *,
    manifest_path: Path,
) -> list[dict[str, Any]]:
    if not isinstance(readiness, Mapping):
        return [_finding(
            code="deployment_contract.readiness.not_declared",
            severity="warning",
            stage="readiness",
            message="non-production readiness evidence is not declared",
            why=(
                "Prototype/demo deployments may be static, but the contract "
                "should say whether readiness is historical, unavailable, or "
                "not required."
            ),
            location=_loc(manifest_path, pointer="/contract/evidence/readiness"),
            expected="readiness evidence status",
            next_action="declare contract.evidence.readiness.status",
        )]
    return _check_readiness_shape(readiness, manifest_path=manifest_path)


def _check_readiness_shape(
    readiness: Mapping[str, Any],
    *,
    manifest_path: Path,
) -> list[dict[str, Any]]:
    findings: list[dict[str, Any]] = []
    status = readiness.get("status")
    if status is not None and str(status) not in _READY_ALLOWED:
        findings.append(_finding(
            code="deployment_contract.readiness.status_invalid",
            severity="warning",
            stage="readiness",
            message="readiness evidence status is not recognized",
            why="The auditor normalizes a closed readiness status set.",
            location=_loc(manifest_path, pointer="/contract/evidence/readiness/status"),
            observed=status,
            expected=sorted(_READY_ALLOWED),
            next_action="use a recognized readiness status",
        ))
    ttl_seconds = readiness.get("ttl_seconds")
    if ttl_seconds is not None and (
        not isinstance(ttl_seconds, int) or ttl_seconds <= 0
    ):
        findings.append(_finding(
            code="deployment_contract.readiness.ttl_invalid",
            severity="warning",
            stage="readiness",
            message="readiness evidence ttl_seconds is invalid",
            why="A readiness TTL should be a positive integer when present.",
            location=_loc(manifest_path, pointer="/contract/evidence/readiness/ttl_seconds"),
            observed=ttl_seconds,
            expected="positive integer seconds or null",
            next_action="fix ttl_seconds or set it to null",
        ))
    collected_at = readiness.get("collected_at")
    if collected_at is not None and _parse_datetime(collected_at) is None:
        findings.append(_finding(
            code="deployment_contract.readiness.collected_at_invalid",
            severity="warning",
            stage="readiness",
            message="readiness evidence collected_at is invalid",
            why="Readiness timestamps must be machine-readable.",
            location=_loc(manifest_path, pointer="/contract/evidence/readiness/collected_at"),
            observed=collected_at,
            expected="ISO-8601 timestamp or null",
            next_action="use an ISO-8601 timestamp",
        ))
    parsed_collected_at = _parse_datetime(collected_at)
    if parsed_collected_at is not None:
        age = (datetime.now(timezone.utc) - parsed_collected_at).total_seconds()
        if age < -60:
            findings.append(_finding(
                code="deployment_contract.readiness.future_collected_at",
                severity="warning",
                stage="readiness",
                message="readiness evidence collected_at is in the future",
                why="Future-dated readiness evidence can make stale proof look fresh.",
                location=_loc(manifest_path, pointer="/contract/evidence/readiness/collected_at"),
                observed=parsed_collected_at.isoformat(),
                expected="timestamp not in the future",
                next_action="fix the readiness evidence timestamp",
            ))
    return findings


def _check_blocked_contract(
    contract: Mapping[str, Any],
    readiness: Mapping[str, Any] | None,
    *,
    manifest_path: Path,
) -> list[dict[str, Any]]:
    blocker = contract.get("blocker")
    findings: list[dict[str, Any]] = []
    if not isinstance(blocker, Mapping):
        return [_finding(
            code="deployment_contract.blocker.missing",
            severity="blocker",
            stage="contract",
            message="blocked deployment lacks blocker details",
            why="Blocked deployments must name the reason and review date.",
            location=_loc(manifest_path, pointer="/contract/blocker"),
            expected="blocker: {reason, review_by}",
            next_action="set contract.blocker.reason and review_by",
        )]
    if not _non_empty(blocker.get("reason")):
        findings.append(_finding(
            code="deployment_contract.blocker.reason_missing",
            severity="blocker",
            stage="contract",
            message="blocked deployment lacks a blocker reason",
            why="Operators need to know why the deployment cannot be promoted.",
            location=_loc(manifest_path, pointer="/contract/blocker/reason"),
            expected="short blocker reason",
            next_action="set contract.blocker.reason",
        ))
    if _review_by(contract) is None:
        findings.append(_finding(
            code="deployment_contract.blocker.review_by_invalid",
            severity="blocker",
            stage="contract",
            message="blocked deployment lacks a valid review_by date",
            why="Blocked states need an explicit re-review date.",
            location=_loc(manifest_path, pointer="/contract/blocker/review_by"),
            expected="YYYY-MM-DD date",
            next_action="set contract.blocker.review_by",
        ))
    if not isinstance(readiness, Mapping):
        findings.append(_finding(
            code="deployment_contract.blocked_readiness.missing",
            severity="blocker",
            stage="readiness",
            message="blocked deployment lacks blocked readiness evidence",
            why=(
                "Blocked lifecycle states must identify the unavailable or "
                "blocked evidence class that prevents promotion."
            ),
            location=_loc(manifest_path, pointer="/contract/evidence/readiness"),
            expected="readiness evidence with blocked/unavailable/stale status",
            next_action="set contract.evidence.readiness for the blocker",
        ))
    else:
        findings.extend(_check_readiness_shape(readiness, manifest_path=manifest_path))
        status = str(readiness.get("status") or "")
        if status not in _BLOCKED_READY_STATUSES:
            findings.append(_finding(
                code="deployment_contract.blocked_readiness.status_invalid",
                severity="blocker",
                stage="readiness",
                message="blocked deployment readiness status does not explain the blocker",
                why=(
                    "Blocked deployments should name the unavailable, stale, "
                    "command-failed, or blocked evidence that prevents promotion."
                ),
                location=_loc(manifest_path, pointer="/contract/evidence/readiness/status"),
                observed=status or None,
                expected=sorted(_BLOCKED_READY_STATUSES),
                next_action="set readiness status to blocked, unavailable, stale, or command_failed",
            ))
        if not _blocked_readiness_has_scope(readiness):
            findings.append(_finding(
                code="deployment_contract.blocked_readiness.scope_missing",
                severity="blocker",
                stage="readiness",
                message="blocked deployment readiness evidence lacks a blocked slot",
                why=(
                    "Blocked deployments must identify the unavailable or "
                    "blocked evidence slot/class that prevents promotion."
                ),
                location=_loc(manifest_path, pointer="/contract/evidence/readiness"),
                expected="blocked_slot, unavailable_slot, or a non-clean slot in slots",
                next_action="name the blocked readiness slot or attach slot evidence",
            ))
    return findings


def _check_akmon_surface(
    raw: Mapping[str, Any],
    *,
    manifest_path: Path,
    lifecycle: str | None,
    contract_level: str,
) -> list[dict[str, Any]]:
    agent_tools = raw.get("agent_tools")
    if not isinstance(agent_tools, Mapping):
        return []
    findings: list[dict[str, Any]] = []
    for tool_name in sorted(agent_tools):
        body = agent_tools[tool_name]
        if not isinstance(body, Mapping):
            continue
        if body.get("requires_env") and body.get("boundary") != "external_live":
            severity = _migration_severity(lifecycle, contract_level)
            findings.append(_finding(
                code="deployment_contract.akmon.external_live_boundary_missing",
                severity=severity,
                stage="akmon",
                message="credentialed live agent tool lacks external_live boundary",
                why=(
                    "Agent tools that require live credentials must be labeled "
                    "as external_live so agents do not confuse them with "
                    "generation-pinned OKG reads."
                ),
                location=_loc(
                    manifest_path,
                    pointer=f"/agent_tools/{tool_name}/boundary",
                ),
                observed=body.get("boundary"),
                expected="external_live",
                next_action="set boundary: external_live or remove requires_env",
                details={"tool": tool_name},
            ))
        if body.get("kind") != "operator_macro":
            continue
        operator = body.get("operator")
        if operator in OPERATOR_TOOLS:
            continue
        severity = _migration_severity(lifecycle, contract_level)
        findings.append(_finding(
            code="deployment_contract.akmon.operator_macro_invalid",
            severity=severity,
            stage="akmon",
            message="agent tool uses an operator outside the canonical AKMON read surface",
            why=(
                "operator_macro entries must target the canonical AKMON read "
                "operators exported by okg.substrate.mcp.operators."
            ),
            location=_loc(
                manifest_path,
                pointer=f"/agent_tools/{tool_name}/operator",
            ),
            observed=operator,
            expected=list(OPERATOR_TOOLS),
            next_action=(
                "replace the operator macro with a canonical read operator "
                "or expose the action through an explicit write/admin path"
            ),
            details={"tool": tool_name},
        ))
    return findings


def _check_waivers(
    contract: Mapping[str, Any],
    *,
    manifest_path: Path,
) -> list[dict[str, Any]]:
    waivers = contract.get("waivers")
    if waivers is None:
        return []
    if not isinstance(waivers, list):
        return [_finding(
            code="deployment_contract.waivers.invalid",
            severity="blocker",
            stage="contract",
            message="contract waivers must be a list",
            why="Waivers are bounded exception records, not an unstructured flag.",
            location=_loc(manifest_path, pointer="/contract/waivers"),
            observed=type(waivers).__name__,
            expected="list of waiver records",
            next_action="make contract.waivers a list or remove it",
        )]
    findings: list[dict[str, Any]] = []
    today = datetime.now(timezone.utc).date()
    for index, waiver in enumerate(waivers):
        pointer = f"/contract/waivers/{index}"
        if not isinstance(waiver, Mapping):
            findings.append(_finding(
                code="deployment_contract.waiver.invalid",
                severity="blocker",
                stage="contract",
                message="contract waiver must be a mapping",
                why="Each waiver must carry owner, reason, and expiry.",
                location=_loc(manifest_path, pointer=pointer),
                observed=type(waiver).__name__,
                expected="mapping",
                next_action="replace the waiver with a mapping",
            ))
            continue
        for key in ("owner", "reason"):
            if _non_empty(waiver.get(key)):
                continue
            findings.append(_finding(
                code=f"deployment_contract.waiver.{key}_missing",
                severity="blocker",
                stage="contract",
                message=f"contract waiver lacks {key}",
                why="Waivers must be accountable and reviewable.",
                location=_loc(manifest_path, pointer=f"{pointer}/{key}"),
                expected=f"non-empty {key}",
                next_action=f"set contract.waivers[{index}].{key}",
            ))
        expires_on = _parse_date(waiver.get("expires_on") or waiver.get("expires_at"))
        if expires_on is None:
            findings.append(_finding(
                code="deployment_contract.waiver.expiry_missing",
                severity="blocker",
                stage="contract",
                message="contract waiver lacks a valid expiry",
                why="Unbounded waivers silently weaken the lifecycle contract.",
                location=_loc(manifest_path, pointer=f"{pointer}/expires_on"),
                expected="YYYY-MM-DD expires_on or expires_at",
                next_action=f"set contract.waivers[{index}].expires_on",
            ))
        elif expires_on < today:
            findings.append(_finding(
                code="deployment_contract.waiver.expired",
                severity="blocker",
                stage="contract",
                message="contract waiver is expired",
                why="Expired waivers no longer satisfy the deployment contract.",
                location=_loc(manifest_path, pointer=f"{pointer}/expires_on"),
                observed=expires_on.isoformat(),
                expected=f"date on or after {today.isoformat()}",
                next_action="renew, resolve, or remove the waiver",
            ))
    return findings


def _check_instruction_surface(
    raw: Mapping[str, Any],
    *,
    manifest_path: Path,
    deployment_name: str,
    lifecycle: str | None,
    contract_level: str,
) -> list[dict[str, Any]]:
    findings: list[dict[str, Any]] = []
    terms = _LEGACY_TOOL_TERMS
    if not terms:
        return findings
    patterns = {
        term: re.compile(
            rf"(?<![A-Za-z0-9_]){re.escape(term)}(?![A-Za-z0-9_])"
        )
        for term in terms
    }
    for path in _instruction_paths(raw, manifest_path.parent):
        try:
            if path.stat().st_size > _MAX_INSTRUCTION_BYTES:
                continue
            text = path.read_text(encoding="utf-8")
        except OSError:
            continue
        for term, pattern in patterns.items():
            if not pattern.search(text):
                continue
            migration = CURRENT_TOOL_MIGRATIONS.get(term)
            findings.append(_finding(
                code="deployment_contract.akmon.retired_tool_reference",
                severity=_migration_severity(lifecycle, contract_level),
                stage="akmon",
                message="agent-facing instructions reference a retired AKMON tool",
                why=(
                    "Deployment instructions must match the canonical AKMON "
                    "operator surface so agents do not call removed tools."
                ),
                location=_loc(path),
                observed=term,
                expected=(
                    migration.target
                    if migration is not None
                    else "canonical AKMON operators, not tool_surface filters"
                ),
                next_action=(
                    "rewrite the instruction to use inspect/search/expand/"
                    "filter/map/aggregate/query"
                ),
                details={"deployment": deployment_name},
            ))
    return findings


def _contract_lint_findings(
    report: Mapping[str, Any],
    *,
    lifecycle: str | None,
    contract_level: str,
) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for item in report.get("findings") or []:
        severity = item.get("severity")
        if severity not in {"blocker", "warning", "info"}:
            continue
        lint_code = item.get("code")
        contract_severity, severity_policy = _contract_lint_severity(
            str(lint_code or ""),
            str(severity),
            lifecycle=lifecycle,
            contract_level=contract_level,
        )
        out.append(_finding(
            code=f"deployment_contract.lint.{lint_code}",
            severity=contract_severity,
            stage=f"lint.{item.get('stage') or 'unknown'}",
            message=str(item.get("message") or "deployment lint finding"),
            why=str(item.get("why") or "The deployment lint contract reported a finding."),
            location=item.get("location"),
            observed=item.get("observed"),
            expected=item.get("expected"),
            next_action=item.get("next_action"),
            details={
                "lint_code": lint_code,
                "lint_severity": severity,
                "lifecycle": lifecycle,
                "contract_level": contract_level,
                "severity_policy": severity_policy,
            },
        ))
    return out


def _contract_lint_severity(
    lint_code: str,
    lint_severity: str,
    *,
    lifecycle: str | None,
    contract_level: str,
) -> tuple[str, str]:
    if lint_code in {
        "deployment.env.required_missing",
        "deployment.env.dsn_not_declared",
    }:
        if lifecycle in _PROD_LIKE:
            return "blocker", "live_lifecycle_requires_resolved_env"
        if contract_level == "next" and lifecycle == "prototype":
            return "blocker", "prototype_requires_declared_env_authority"
        return lint_severity, "declared_env_ref_visible_without_secret"
    if lint_code == "deployment.env.required_declared_unset":
        return lint_severity, "declared_env_ref_visible_without_secret"
    return lint_severity, "lint_severity_preserved"


def _normalize_readiness_evidence(
    contract: Any,
    *,
    manifest_path: Path,
) -> dict[str, Any] | None:
    readiness = _readiness_block(contract)
    if readiness is None:
        return None
    collected_at = _parse_datetime(readiness.get("collected_at"))
    ttl_seconds = readiness.get("ttl_seconds")
    age_seconds = None
    fresh = None
    if collected_at is not None:
        age_seconds = int((datetime.now(timezone.utc) - collected_at).total_seconds())
        if isinstance(ttl_seconds, int) and ttl_seconds > 0:
            fresh = age_seconds <= ttl_seconds
    raw_path = readiness.get("path")
    resolved_path = None
    if isinstance(raw_path, str) and raw_path.strip():
        resolved_path = str((manifest_path.parent / raw_path).resolve())
    blocked_scope = _blocked_readiness_scope(readiness)
    return {
        "status": readiness.get("status"),
        "collected_at": (
            collected_at.isoformat() if collected_at is not None else None
        ),
        "ttl_seconds": ttl_seconds,
        "age_seconds": age_seconds,
        "fresh": fresh if age_seconds is None or age_seconds >= -60 else False,
        "provenance": readiness.get("provenance"),
        "path": resolved_path,
        "slots": (
            dict(readiness["slots"])
            if isinstance(readiness.get("slots"), Mapping) else None
        ),
        "blocked_scope": blocked_scope,
    }


def _instruction_paths(raw: Mapping[str, Any], deployment_dir: Path) -> list[Path]:
    candidates: list[Path] = []
    surface = raw.get("agent_surface")
    for raw_instruction in (
        raw.get("mcp_instructions"),
        # The block's replacement for the top-level key (issue #2067).
        surface.get("instructions") if isinstance(surface, Mapping) else None,
    ):
        if isinstance(raw_instruction, str) and raw_instruction.strip():
            candidates.append(deployment_dir / raw_instruction)
    for relative in (
        "mcp_instructions.md",
        "docs/mcp-instructions.md",
        "docs/akmon-instructions.md",
    ):
        candidates.append(deployment_dir / relative)
    skills_dir = raw.get("skills_dir")
    skill_root = (
        deployment_dir / skills_dir
        if isinstance(skills_dir, str) and skills_dir.strip()
        else deployment_dir / "skills"
    )
    if skill_root.is_dir():
        candidates.extend(sorted(skill_root.glob("*.md")))
    return _unique_existing_paths(candidates)


def _unique_existing_paths(paths: Iterable[Path]) -> list[Path]:
    seen: set[Path] = set()
    out: list[Path] = []
    for path in paths:
        resolved = path.resolve()
        if resolved in seen or not resolved.is_file():
            continue
        seen.add(resolved)
        out.append(resolved)
    return out


def _readiness_block(contract: Any) -> Mapping[str, Any] | None:
    if not isinstance(contract, Mapping):
        return None
    evidence = contract.get("evidence")
    if not isinstance(evidence, Mapping):
        return None
    readiness = evidence.get("readiness")
    return readiness if isinstance(readiness, Mapping) else None


def _review_by(contract: Mapping[str, Any]) -> Any:
    blocker = contract.get("blocker")
    if not isinstance(blocker, Mapping):
        return None
    return _parse_date(blocker.get("review_by"))


def _parse_date(raw: Any) -> Any:
    if not isinstance(raw, str):
        return None
    try:
        return datetime.strptime(raw, "%Y-%m-%d").date()
    except ValueError:
        return None


def _deployment_name(raw: Mapping[str, Any] | None, manifest_path: Path) -> str:
    if isinstance(raw, Mapping) and _non_empty(raw.get("name")):
        return str(raw["name"])
    return manifest_path.parent.name


def _migration_severity(lifecycle: str | None, contract_level: str) -> str:
    if contract_level == "next" or lifecycle in _PROD_LIKE:
        return "blocker"
    return "warning"


def _parse_datetime(value: Any) -> datetime | None:
    if not isinstance(value, str) or not value.strip():
        return None
    text = value.strip()
    if text.endswith("Z"):
        text = f"{text[:-1]}+00:00"
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _non_empty(value: Any) -> bool:
    if isinstance(value, str):
        return bool(value.strip())
    if isinstance(value, Mapping):
        return any(_non_empty(v) for v in value.values())
    return value is not None


def _slot_has_provenance(slot: Mapping[str, Any]) -> bool:
    for key in ("provenance", "path", "artifact", "command"):
        if _non_empty(slot.get(key)):
            return True
    return False


def _blocked_readiness_scope(readiness: Mapping[str, Any]) -> dict[str, Any] | None:
    for key in (
        "blocked_slot",
        "blocking_slot",
        "unavailable_slot",
        "evidence_slot",
        "blocked_evidence",
    ):
        value = readiness.get(key)
        if _non_empty(value):
            return {"source": key, "value": value}
    slots = readiness.get("slots")
    if not isinstance(slots, Mapping):
        return None
    for slot_name, slot in slots.items():
        if not isinstance(slot, Mapping):
            continue
        status = str(slot.get("status") or "")
        if status in _BLOCKED_READY_STATUSES:
            return {"source": "slots", "slot": str(slot_name), "status": status}
    return None


def _blocked_readiness_has_scope(readiness: Mapping[str, Any]) -> bool:
    return _blocked_readiness_scope(readiness) is not None


def _relative_to_root(path: Path, root: Path) -> str:
    try:
        return str(path.relative_to(root))
    except ValueError:
        return str(path)


def _summary(findings: Iterable[Mapping[str, Any]]) -> dict[str, int]:
    counts = {"blockers": 0, "warnings": 0, "infos": 0, "skipped": 0}
    for finding in findings:
        severity = finding.get("severity")
        if severity == "blocker":
            counts["blockers"] += 1
        elif severity == "warning":
            counts["warnings"] += 1
        elif severity == "skipped":
            counts["skipped"] += 1
        else:
            counts["infos"] += 1
    return counts


def _portfolio_readiness_summary(
    deployments: Iterable[Mapping[str, Any]],
    findings: Iterable[Mapping[str, Any]],
) -> dict[str, Any]:
    finding_list = [dict(finding) for finding in findings]
    warning_families: dict[str, int] = {}
    for finding in finding_list:
        if finding.get("severity") != "warning":
            continue
        code = str(finding.get("code") or "unknown")
        warning_families[code] = warning_families.get(code, 0) + 1

    deployments_by_lifecycle: dict[str, dict[str, Any]] = {}
    readiness_statuses: dict[str, int] = {}
    blocked_readiness_scope_count = 0
    stale_or_historical_evidence_count = 0
    for deployment in deployments:
        lifecycle = str(deployment.get("lifecycle") or "unknown")
        lifecycle_row = deployments_by_lifecycle.setdefault(
            lifecycle,
            {
                "deployments": 0,
                "blockers": 0,
                "warnings": 0,
                "readiness_statuses": {},
            },
        )
        lifecycle_row["deployments"] += 1
        deployment_summary = deployment.get("summary") or {}
        if isinstance(deployment_summary, Mapping):
            lifecycle_row["blockers"] += int(deployment_summary.get("blockers", 0))
            lifecycle_row["warnings"] += int(deployment_summary.get("warnings", 0))

        readiness = deployment.get("readiness_evidence")
        status = "missing"
        if isinstance(readiness, Mapping):
            status = str(readiness.get("status") or "unknown")
            if readiness.get("blocked_scope") is not None:
                blocked_readiness_scope_count += 1
            if status in {"historical", "stale"} or readiness.get("fresh") is False:
                stale_or_historical_evidence_count += 1
        readiness_statuses[status] = readiness_statuses.get(status, 0) + 1
        lifecycle_statuses = lifecycle_row["readiness_statuses"]
        lifecycle_statuses[status] = lifecycle_statuses.get(status, 0) + 1

    env_var_warning_count = sum(
        1
        for finding in finding_list
        if finding.get("severity") == "warning"
        and finding.get("code") in {
            "deployment_contract.lint.deployment.env.required_missing",
            "deployment_contract.lint.deployment.env.dsn_not_declared",
        }
    )
    missing_governance_count = sum(
        1
        for finding in finding_list
        if finding.get("code")
        in {
            "deployment_contract.lint.deployment.nomos.deployment_posture_missing",
        }
    )
    return {
        "by_lifecycle": deployments_by_lifecycle,
        "readiness_statuses": readiness_statuses,
        "warning_families": warning_families,
        "env_var_warning_count": env_var_warning_count,
        "missing_governance_count": missing_governance_count,
        "blocked_readiness_scope_count": blocked_readiness_scope_count,
        "stale_or_historical_evidence_count": stale_or_historical_evidence_count,
    }


def _sorted_findings(findings: Iterable[Mapping[str, Any]]) -> list[dict[str, Any]]:
    return sorted(
        [dict(finding) for finding in findings],
        key=lambda f: (
            _SEVERITY_ORDER.get(str(f.get("severity")), 9),
            str(f.get("stage") or ""),
            str(f.get("code") or ""),
            str((f.get("location") or {}).get("path") or ""),
        ),
    )


def _finding(
    *,
    code: str,
    severity: str,
    stage: str,
    message: str,
    why: str,
    location: dict[str, Any] | None = None,
    observed: Any = None,
    expected: Any = None,
    next_action: str | None = None,
    details: dict[str, Any] | None = None,
) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "code": code,
        "severity": severity,
        "stage": stage,
        "message": message,
        "why": why,
    }
    if location:
        payload["location"] = dict(location)
    if observed is not None:
        payload["observed"] = observed
    if expected is not None:
        payload["expected"] = expected
    if next_action:
        payload["next_action"] = next_action
    if details:
        payload["details"] = dict(details)
    return payload


def _loc(path: Path, *, pointer: str | None = None) -> dict[str, Any]:
    out: dict[str, Any] = {"path": str(path)}
    if pointer:
        out["pointer"] = pointer
    return out
