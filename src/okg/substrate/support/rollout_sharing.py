"""Share-safe colleague rollout report helpers."""

from __future__ import annotations

from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence

from okg.substrate.support import bug_report


SHARING_POLICY: dict[str, tuple[str, ...]] = {
    "shareable": (
        "deployment name",
        "PACT and task ids",
        "published generation ids and aggregate graph counts",
        "readiness booleans, queue counts, DLQ counts, and doctor counts",
        "source names, freshness states, and owner PACT/task ids",
        "redacted next-action commands and report artifact basenames",
    ),
    "redacted": (
        "absolute local paths",
        "home-directory paths",
        "database DSNs and credentials",
        "access tokens, API keys, passwords, and authorization headers",
        "private Git remotes and credentialed URLs",
        "raw runtime command lines containing local paths or credentials",
    ),
    "local_only": (
        "raw agent transcript text",
        "raw Codex or Claude Code session bodies",
        "raw .okg/local.env content",
        "raw DBOS workflow payloads",
        "unredacted logs, stack traces, and private operator notes",
    ),
}


def build_colleague_rollout_report(
    *,
    deployment: str,
    pact_id: str,
    task_id: str,
    generation_id: str | None,
    readiness: Mapping[str, Any],
    risks: Sequence[Mapping[str, Any]],
    evidence_refs: Sequence[str] = (),
    private_context: Sequence[str] = (),
    repo_root: Path | None = None,
    deployment_dir: Path | None = None,
) -> dict[str, Any]:
    """Render a colleague-facing rollout report with deterministic redaction.

    `private_context` is counted and scanned by the caller's tests, but never
    rendered. The report intentionally carries only aggregate readiness facts,
    owner ids, and redacted next actions.
    """

    lines = [
        "OKG colleague rollout readiness",
        "",
        f"Deployment: {deployment}",
        f"PACT: {pact_id}",
        f"Task: {task_id}",
        f"Generation: {generation_id or 'not-published'}",
        "",
        "Readiness",
    ]
    for key, value in _safe_items(readiness):
        lines.append(f"- {key}: {_stringify(value)}")

    lines.extend(["", "Risks"])
    if risks:
        for risk in risks:
            label = str(risk.get("risk") or risk.get("id") or "risk")
            status = str(risk.get("status") or "unknown")
            owner = str(
                risk.get("owner")
                or risk.get("owner_pact")
                or risk.get("owner_task")
                or "unowned"
            )
            next_action = risk.get("next_action")
            line = f"- {label}: {status}; owner={owner}"
            if next_action:
                line += f"; next={_stringify(next_action)}"
            lines.append(line)
    else:
        lines.append("- none reported")

    if evidence_refs:
        lines.extend(["", "Evidence"])
        for ref in evidence_refs:
            lines.append(f"- {_stringify(ref)}")

    lines.extend([
        "",
        "Sharing Policy",
        "- Shareable: " + "; ".join(SHARING_POLICY["shareable"]),
        "- Redacted: " + "; ".join(SHARING_POLICY["redacted"]),
        "- Local only: " + "; ".join(SHARING_POLICY["local_only"]),
        f"- Private context omitted: {len(private_context)} item(s)",
    ])

    raw_body = "\n".join(lines)
    body, redactions = bug_report.redact_text(
        raw_body,
        repo_root=repo_root,
        deployment_dir=deployment_dir,
    )
    leakage_check = bug_report._check_for_leaks(  # noqa: SLF001
        body,
        repo_root=repo_root,
        deployment_dir=deployment_dir,
    )
    return {
        "ok": bool(leakage_check["ok"]),
        "body": body,
        "policy": SHARING_POLICY,
        "redactions": list(redactions),
        "leakage_check": leakage_check,
        "private_context_omitted": len(private_context),
    }


def _safe_items(mapping: Mapping[str, Any]) -> Iterable[tuple[str, Any]]:
    for key in sorted(mapping):
        if _is_private_key(key):
            continue
        yield str(key), mapping[key]


def _is_private_key(key: object) -> bool:
    text = str(key).lower()
    return any(
        marker in text
        for marker in (
            "dsn",
            "token",
            "secret",
            "password",
            "authorization",
            "transcript",
            "private",
            "local_env",
        )
    )


def _stringify(value: Any) -> str:
    if isinstance(value, (dict, list, tuple)):
        return str(value)
    return str(value)
