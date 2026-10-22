"""Deterministic, redacted deployment bug-report previews."""

from __future__ import annotations

from dataclasses import asdict, dataclass
import hashlib
import importlib.metadata
import json
import os
from pathlib import Path
import re
import subprocess  # nosec B404
import tempfile
from typing import Any, Callable, Sequence

from okg.substrate import fastyaml
from okg.substrate.cli.child_argv import running_okg_argv

from okg.substrate.errors import SubstrateError
from okg.substrate.unknown import Unknown, read_ok


CommandRunner = Callable[[Sequence[str], int], "CommandResult"]


_REPO_RE = re.compile(r"^[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+$")
_DSN_RE = re.compile(
    r"(?i)\b(?:postgres(?:ql)?|mysql|redshift)://[^\s`'\"<>]+"
)
_GITHUB_TOKEN_RE = re.compile(r"\bgh[pousr]_[A-Za-z0-9_]{8,}\b")
_SECRET_ASSIGNMENT_RE = re.compile(
    r"(?i)(?<!<)\b(token|secret|password|api[_-]?key)"
    r"(\s*[:=]\s*)"
    r"([^,\s}\]`'\"<>]+)"
)
# The user name may be empty (`https://:TOKEN@github.com`), and the scheme
# may be upper case (#2812 review). Used both to redact and in the final
# leakage check before a report can be published to a GitHub issue.
_CRED_URL_RE = re.compile(r"(?i)https?://[^/\s:@]*:[^@\s]+@[^\s`'\"<>]+")
_GIT_REMOTE_RE = re.compile(
    r"(?:git@github\.com:|https?://github\.com/)[^\s`'\"<>]+"
)
_ABS_USER_PATH_RE = re.compile(
    r"(?<![\w<])(?:/Users|/home)/[^\s`'\"),\]}]+"
)
_HOME_CODEX_PATH_RE = re.compile(
    r"<home>/\.codex(?:/[^\s`'\"),\]}]+)?"
)


@dataclass(frozen=True)
class CommandResult:
    """Bounded result from a diagnostic command."""

    argv: list[str]
    returncode: int
    stdout: str
    stderr: str
    timed_out: bool = False

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class BugReportPreview:
    """Rendered GitHub issue preview plus structured diagnostics."""

    repo: str
    title: str
    body: str
    labels: tuple[str, ...]
    payload: dict[str, Any]
    body_sha256: str
    redactions: tuple[str, ...]
    leakage_check: dict[str, Any]
    deployment_repo: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "ok": True,
            "mode": "preview",
            "repo": self.repo,
            "deployment_repo": self.deployment_repo,
            "title": self.title,
            "body": self.body,
            "labels": list(self.labels),
            "payload": self.payload,
            "body_sha256": self.body_sha256,
            "redactions": list(self.redactions),
            "leakage_check": self.leakage_check,
        }


def bug_report_preview_from_dict(data: dict[str, Any]) -> BugReportPreview:
    """Rehydrate a reviewed preview JSON payload for exact issue creation."""

    repo = str(data.get("repo") or "")
    if not _REPO_RE.match(repo):
        raise _bug_report_error(
            "invalid preview JSON: repo must be owner/name",
            fix_hint="regenerate the preview with --repo owner/name",
        )

    deployment_repo_raw = data.get("deployment_repo")
    deployment_repo = (
        str(deployment_repo_raw) if deployment_repo_raw is not None else None
    )
    if deployment_repo is not None and not _REPO_RE.match(deployment_repo):
        raise _bug_report_error(
            "invalid preview JSON: deployment_repo must be owner/name",
            fix_hint="regenerate the preview with --deployment-repo owner/name",
        )

    body = str(data.get("body") or "")
    body_sha256 = str(data.get("body_sha256") or "")
    actual_sha256 = hashlib.sha256(body.encode("utf-8")).hexdigest()
    if body_sha256 != actual_sha256:
        raise _bug_report_error(
            "invalid preview JSON: body hash does not match body",
            data={"expected": body_sha256, "actual": actual_sha256},
            fix_hint="regenerate the preview before applying it",
        )

    leakage_check = data.get("leakage_check")
    if not isinstance(leakage_check, dict) or not leakage_check.get("ok"):
        raise _bug_report_error(
            "invalid preview JSON: leakage check did not pass",
            data={"leakage_check": leakage_check},
            fix_hint="regenerate the preview and inspect leakage failures",
        )
    final_leakage_check = _check_for_leaks(
        f"{data.get('title') or ''}\n{body}",
        repo_root=None,
        deployment_dir=None,
    )
    if not final_leakage_check["ok"]:
        raise _bug_report_error(
            "preview JSON failed final leakage checks",
            data=final_leakage_check,
            fix_hint="regenerate the preview and inspect leakage failures",
        )

    labels_raw = data.get("labels") or []
    labels = tuple(str(label) for label in labels_raw)
    payload = data.get("payload") if isinstance(data.get("payload"), dict) else {}
    redactions_raw = data.get("redactions") or []
    redactions = tuple(str(redaction) for redaction in redactions_raw)
    return BugReportPreview(
        repo=repo,
        deployment_repo=deployment_repo,
        title=str(data.get("title") or ""),
        body=body,
        labels=labels,
        payload=payload,
        body_sha256=body_sha256,
        redactions=redactions,
        leakage_check=leakage_check,
    )


def redact_text(
    text: str,
    *,
    repo_root: Path | None = None,
    deployment_dir: Path | None = None,
    home_dir: Path | None = None,
    secret_forms: bool = False,
) -> tuple[str, tuple[str, ...]]:
    """Redact paths, remotes, DSNs, and token-looking values.

    ``secret_forms=True`` also masks signed-URL values, Cookie/Set-Cookie and
    Authorization headers, ``auth=`` and ``*TOKEN=`` assignments. The report
    path that posts to GitHub sets it. The default stays off because the
    github, github_actions, pact and git-history sources store this function's
    output as graph content, and turning it on there masks about 2% of repo
    files (see the follow-up issue); it is not changed in this PR.

    Known OKG roots are replaced with semantic placeholders before the
    generic absolute-path pass runs, preserving useful repo-relative
    context without exposing a collaborator's filesystem.
    """

    out = text
    redactions: list[str] = []

    def _replace_exact(path: Path | None, placeholder: str) -> None:
        nonlocal out
        if path is None:
            return
        try:
            raw = str(path.expanduser().resolve())
        except OSError:
            raw = str(path.expanduser())
        if raw and raw != "/" and raw in out:
            out = out.replace(raw, placeholder)
            redactions.append(placeholder.strip("<>"))

    exact_paths = [
        (deployment_dir, "<deployment>"),
        (repo_root, "<repo>"),
        (home_dir if home_dir is not None else Path.home(), "<home>"),
    ]
    exact_paths.sort(
        key=lambda item: len(str(item[0])) if item[0] is not None else 0,
        reverse=True,
    )
    for path, placeholder in exact_paths:
        _replace_exact(path, placeholder)

    out, codex_path_count = _HOME_CODEX_PATH_RE.subn("<codex-state>", out)
    if codex_path_count:
        redactions.append("codex_state_path")

    substitutions: tuple[tuple[re.Pattern[str], str, str], ...] = (
        (_CRED_URL_RE, "<remote:redacted>", "credential_url"),
        (_GIT_REMOTE_RE, "<remote:redacted>", "git_remote"),
        (_SECRET_ASSIGNMENT_RE, r"\1\2<secret:redacted>", "secret_assignment"),
        (_DSN_RE, "<dsn:redacted>", "dsn"),
        (_GITHUB_TOKEN_RE, "<secret:redacted>", "github_token"),
        (_ABS_USER_PATH_RE, "<path:redacted>", "absolute_user_path"),
    )
    for pattern, replacement, marker in substitutions:
        out, count = pattern.subn(replacement, out)
        if count:
            redactions.append(marker)

    # Signed-URL values, Cookie/Authorization headers, auth= and *TOKEN= forms
    # use the source-health masker (opt-in, see the docstring).
    if secret_forms:
        # Local import: the `okg.substrate.sources` package pulls in the
        # registry and psycopg, and the clean source-sync status owner must
        # stay importable without them (tests/architecture).
        from okg.substrate.sources.redaction import redact_secret_forms

        out, form_markers = redact_secret_forms(out)
        redactions.extend(form_markers)

    return out, tuple(sorted(set(redactions)))


def build_bug_report_preview(
    *,
    deployment: str,
    repo: str,
    deployment_repo: str | None = None,
    dsn: str | None = None,
    title: str | None = None,
    summary: str | None = None,
    body_text: str | None = None,
    labels: Sequence[str] = (),
    command_runner: CommandRunner | None = None,
    command_timeout_seconds: int = 20,
) -> BugReportPreview:
    """Build a redacted GitHub issue preview for a deployment."""

    if not _REPO_RE.match(repo):
        raise _bug_report_error(
            "invalid --repo; expected owner/name",
            fix_hint="pass --repo owner/name such as --repo org/okg",
        )
    if deployment_repo is not None and not _REPO_RE.match(deployment_repo):
        raise _bug_report_error(
            "invalid --deployment-repo; expected owner/name",
            fix_hint=(
                "pass --deployment-repo owner/name such as "
                "--deployment-repo my-org/my-deployment"
            ),
        )
    effective_deployment_repo = deployment_repo or repo

    manifest = _resolve_manifest(deployment)
    deployment_name = manifest.name
    repo_root = _find_repo_root(manifest.deployment_dir)
    source_summary = _source_registry_summary(manifest.source_registry)
    manifest_summary = {
        "name": manifest.name,
        "description": manifest.description,
        "deployment_dir": str(manifest.deployment_dir),
        "schema_dir": str(manifest.schema_dir),
        "source_registry": str(manifest.source_registry),
        "modules": list(manifest.modules),
        "sources": source_summary,
        "runtime_enabled": bool(getattr(manifest.runtime, "enabled", False)),
    }

    diagnostics = _collect_diagnostics(
        deployment=deployment_name,
        dsn=dsn,
        command_runner=command_runner or _run_command,
        command_timeout_seconds=command_timeout_seconds,
    )

    raw_payload = {
        "issue": {
            "target_repo": repo,
            "deployment_repo": effective_deployment_repo,
        },
        "summary": summary or "",
        "user_notes": body_text or "",
        "okg_version": _okg_version(),
        "deployment": manifest_summary,
        "diagnostics": diagnostics,
    }

    sanitized_payload, redactions = _sanitize_value(
        raw_payload,
        repo_root=repo_root,
        deployment_dir=manifest.deployment_dir,
    )
    issue_title = _issue_title(
        deployment=deployment_name,
        title=title,
        summary=str(sanitized_payload["summary"]),
    )
    issue_title, title_redactions = redact_text(
        issue_title,
        repo_root=repo_root,
        deployment_dir=manifest.deployment_dir,
        secret_forms=True,
    )
    redactions = tuple(sorted(set(redactions) | set(title_redactions)))
    body = _render_markdown(
        repo=repo,
        deployment_repo=effective_deployment_repo,
        title=issue_title,
        summary=str(sanitized_payload["summary"]),
        user_notes=str(sanitized_payload["user_notes"]),
        payload=sanitized_payload,
    )
    body, body_redactions = redact_text(
        body,
        repo_root=repo_root,
        deployment_dir=manifest.deployment_dir,
        secret_forms=True,
    )
    redactions = tuple(sorted(set(redactions) | set(body_redactions)))
    leakage_check = _check_for_leaks(
        f"{issue_title}\n{body}",
        repo_root=repo_root,
        deployment_dir=manifest.deployment_dir,
    )
    if not leakage_check["ok"]:
        raise _bug_report_error(
            "bug report preview failed leakage checks",
            data=leakage_check,
            fix_hint="inspect the reported patterns before retrying",
        )

    return BugReportPreview(
        repo=repo,
        deployment_repo=effective_deployment_repo,
        title=issue_title,
        body=body,
        labels=tuple(labels),
        payload=sanitized_payload,
        body_sha256=hashlib.sha256(body.encode("utf-8")).hexdigest(),
        redactions=redactions,
        leakage_check=leakage_check,
    )


def create_github_issue(
    preview: BugReportPreview,
    *,
    gh_binary: str = "gh",
    command_runner: CommandRunner | None = None,
    command_timeout_seconds: int = 30,
) -> dict[str, Any]:
    """Create the previewed GitHub issue through `gh issue create`."""

    final_check = _check_for_leaks(
        "\n".join([preview.title, preview.body, *preview.labels]),
        repo_root=None,
        deployment_dir=None,
    )
    if not final_check["ok"]:
        raise _github_error(
            "GitHub issue not created: the report still holds a secret-looking value",
            data=final_check,
            fix_hint="regenerate the preview and inspect the reported patterns",
        )

    runner = command_runner or _run_command
    body_path: str | None = None
    try:
        with tempfile.NamedTemporaryFile(
            "w", encoding="utf-8", suffix=".md", delete=False
        ) as f:
            f.write(preview.body)
            body_path = f.name
        argv = [
            gh_binary,
            "issue",
            "create",
            "--repo",
            preview.repo,
            "--title",
            preview.title,
            "--body-file",
            body_path,
        ]
        for label in preview.labels:
            argv.extend(["--label", label])
        result = runner(argv, command_timeout_seconds)
    except FileNotFoundError as exc:
        raise _github_error(
            f"GitHub issue creation failed: {gh_binary!r} not found",
            fix_hint="install GitHub CLI (`gh`) and run `gh auth login`",
        ) from exc
    finally:
        if body_path:
            try:
                os.unlink(body_path)
            except OSError:
                # Temporary body cleanup is best-effort after the gh subprocess exits.
                pass

    if result.timed_out:
        raise _github_error(
            "GitHub issue creation timed out",
            data={"argv": _sanitize_argv(result.argv)},
            fix_hint="retry after checking GitHub CLI connectivity",
        )
    if result.returncode != 0:
        stderr, _ = redact_text(result.stderr, secret_forms=True)
        stdout, _ = redact_text(result.stdout, secret_forms=True)
        raise _github_error(
            "GitHub issue creation failed",
            data={
                "returncode": result.returncode,
                "stdout": stdout,
                "stderr": stderr,
                "argv": _sanitize_argv(result.argv),
            },
            fix_hint="install/authenticate `gh` with `gh auth login`",
        )

    url = result.stdout.strip().splitlines()[-1].strip()
    return {
        "ok": True,
        "mode": "apply",
        "repo": preview.repo,
        "deployment_repo": preview.deployment_repo,
        "title": preview.title,
        "issue_url": url,
        "body_sha256": preview.body_sha256,
        "labels": list(preview.labels),
    }


def _resolve_manifest(deployment: str):
    from okg.substrate.deployments import (
        DeploymentResolutionError,
        resolve_deployment,
    )

    try:
        return resolve_deployment(deployment)
    except DeploymentResolutionError as exc:
        raise _bug_report_error(
            f"deployment {deployment!r} could not be resolved",
            fix_hint=(
                "check --deployment, OKG_DEPLOYMENTS_DIR, or pass a "
                "path to deployment.yaml"
            ),
        ) from exc


def _collect_diagnostics(
    *,
    deployment: str,
    dsn: str | None,
    command_runner: CommandRunner,
    command_timeout_seconds: int,
) -> dict[str, Any]:
    latest_generation = (
        _latest_generation(dsn=dsn, deployment=deployment)
        if dsn else {"status": "skipped", "reason": "no DSN"}
    )
    diagnostics: dict[str, Any] = {
        "support_summary": {
            "latest_generation_status": latest_generation.get("status"),
            "queue_depth": {"status": "skipped", "reason": "no DSN"},
            "dlq_count": {"status": "skipped", "reason": "no DSN"},
            "doctor_ok": None,
            "doctor_returncode": None,
            "metrics_returncode": None,
        },
        "latest_generation": latest_generation,
        "metrics": {"status": "skipped", "reason": "no DSN"},
        "doctor": {"status": "skipped", "reason": "no DSN"},
    }
    if not dsn:
        return diagnostics

    metrics = command_runner(
        [
            *running_okg_argv(),
            "metrics",
            "--qa-snapshot",
            "--deployment",
            deployment,
            "--dsn",
            dsn,
            "--json",
        ],
        command_timeout_seconds,
    )
    diagnostics["metrics"] = _command_payload(metrics)

    doctor = command_runner(
        [
            *running_okg_argv(),
            "doctor",
            "--deployment",
            deployment,
            "--dsn",
            dsn,
            "--json",
        ],
        command_timeout_seconds,
    )
    diagnostics["doctor"] = _command_payload(doctor)
    diagnostics["support_summary"] = _support_summary(
        dsn=dsn,
        latest_generation=latest_generation,
        metrics=diagnostics["metrics"],
        doctor=diagnostics["doctor"],
    )
    return diagnostics


def _support_summary(
    *,
    dsn: str,
    latest_generation: dict[str, Any],
    metrics: dict[str, Any],
    doctor: dict[str, Any],
) -> dict[str, Any]:
    doctor_json = doctor.get("json") if isinstance(doctor.get("json"), dict) else {}
    doctor_queue = doctor_json.get("queue_depth")
    runtime_queue = _dbos_runtime_queue_summary(dsn)
    queue_depth = (
        doctor_queue if isinstance(doctor_queue, dict)
        else runtime_queue["queue_depth"]
    )
    return {
        "latest_generation_id": latest_generation.get("generation_id"),
        "latest_generation_status": latest_generation.get("status"),
        "queue_depth": queue_depth,
        "dlq_count": runtime_queue["dlq_count"],
        "doctor_ok": doctor_json.get("ok"),
        "doctor_returncode": doctor.get("returncode"),
        "metrics_returncode": metrics.get("returncode"),
    }


def _dbos_runtime_queue_summary(dsn: str) -> dict[str, Any]:
    try:
        from okg.substrate.db.connection import connect as db_connect

        with db_connect(dsn, connect_timeout=5) as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT to_regclass('dbos.workflow_status')")
                if cur.fetchone()[0] is None:
                    unavailable = {
                        "status": "unavailable",
                        "reason": "dbos.workflow_status table does not exist",
                    }
                    return {
                        "queue_depth": unavailable,
                        "dlq_count": unavailable,
                    }
                cur.execute(
                    """
                    SELECT name, COUNT(*)
                      FROM dbos.workflow_status
                     WHERE upper(status) IN ('ENQUEUED', 'PENDING', 'DELAYED')
                     GROUP BY name
                     ORDER BY name
                    """
                )
                queue_depth = {
                    str(name): int(count)
                    for name, count in cur.fetchall()
                }
                cur.execute(
                    """
                    SELECT COUNT(*)
                      FROM dbos.workflow_status
                     WHERE upper(status) IN (
                       'CANCELLED',
                       'ERROR',
                       'FAILED',
                       'MAX_RECOVERY_ATTEMPTS',
                       'MAX_RECOVERY_ATTEMPTS_EXCEEDED'
                     )
                    """
                )
                dlq_count = int(cur.fetchone()[0])
                return {
                    "queue_depth": queue_depth,
                    "dlq_count": dlq_count,
                }
    except Exception as exc:  # noqa: BLE001 - diagnostic only
        unavailable = {
            "status": "error",
            # Connection error text varies with server timing and may contain
            # environment-specific details. Keep previews deterministic and
            # safe to share while preserving the actionable failure class.
            "error": type(exc).__name__,
            "reason": "runtime queue diagnostic failed",
        }
        return {"queue_depth": unavailable, "dlq_count": unavailable}


def _latest_generation(*, dsn: str, deployment: str) -> dict[str, Any]:
    try:
        from okg.substrate.db.connection import connect as db_connect

        with db_connect(dsn, connect_timeout=5) as conn:
            with conn.cursor() as cur:
                latest = _generation_query(cur, deployment=deployment)
                if latest is None:
                    return {
                        "status": "unavailable",
                        "reason": "no generation rows for deployment",
                    }
                latest_published = _generation_query(
                    cur,
                    deployment=deployment,
                    status="published",
                )
                latest["latest_published"] = latest_published
                return latest
    except Exception as exc:  # noqa: BLE001 - diagnostics must be best-effort
        return {
            "status": "error",
            "error": type(exc).__name__,
            "message": str(exc),
        }


def _generation_query(
    cur,
    *,
    deployment: str,
    status: str | None = None,
) -> dict[str, Any] | None:
    where = " WHERE deployment_name = %s "
    params: list[Any] = [deployment]
    if status is not None:
        where += " AND status = %s "
        params.append(status)
    cur.execute(
        "SELECT generation_id, status, published_at, "
        "       nodes_added, nodes_retracted, "
        "       edges_added, edges_retracted, "
        "       blocked_reason "
        "  FROM okg.graph_generations "
        f"{where}"
        " ORDER BY generation_id DESC LIMIT 1",
        tuple(params),
    )
    row = cur.fetchone()
    if row is None:
        return None
    return {
        "generation_id": int(row[0]),
        "status": str(row[1]),
        "published_at": row[2].isoformat() if row[2] is not None else None,
        "nodes_added": int(row[3] or 0),
        "nodes_retracted": int(row[4] or 0),
        "edges_added": int(row[5] or 0),
        "edges_retracted": int(row[6] or 0),
        "blocked_reason": row[7] or None,
    }


def _command_payload(result: CommandResult) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "argv": _sanitize_argv(result.argv),
        "returncode": result.returncode,
        "timed_out": result.timed_out,
    }
    stdout = _truncate(result.stdout)
    stderr = _truncate(result.stderr)
    parsed = _parse_json(stdout)
    if parsed is not None:
        payload["json"] = parsed
    else:
        payload["stdout"] = stdout
    if stderr:
        payload["stderr"] = stderr
    return payload


def _run_command(argv: Sequence[str], timeout_seconds: int) -> CommandResult:
    try:
        result = subprocess.run(  # nosec B603
            list(argv),
            capture_output=True,
            text=True,
            check=False,
            timeout=timeout_seconds,
        )
    except subprocess.TimeoutExpired as exc:
        return CommandResult(
            argv=list(argv),
            returncode=124,
            stdout=exc.stdout or "",
            stderr=exc.stderr or "",
            timed_out=True,
        )
    return CommandResult(
        argv=list(argv),
        returncode=result.returncode,
        stdout=result.stdout,
        stderr=result.stderr,
    )


def _sanitize_value(
    value: Any,
    *,
    repo_root: Path | None,
    deployment_dir: Path | None,
) -> tuple[Any, tuple[str, ...]]:
    redactions: set[str] = set()

    def _walk(v: Any) -> Any:
        if isinstance(v, str):
            sanitized, markers = redact_text(
                v,
                repo_root=repo_root,
                deployment_dir=deployment_dir,
                secret_forms=True,
            )
            redactions.update(markers)
            return sanitized
        if isinstance(v, dict):
            return {
                str(k): _walk(v[k])
                for k in sorted(v.keys(), key=str)
            }
        if isinstance(v, list | tuple):
            return [_walk(item) for item in v]
        return v

    return _walk(value), tuple(sorted(redactions))


def _render_markdown(
    *,
    repo: str,
    deployment_repo: str | None,
    title: str,
    summary: str,
    user_notes: str,
    payload: dict[str, Any],
) -> str:
    deployment = payload["deployment"]
    diagnostics = payload["diagnostics"]
    latest_generation = diagnostics["latest_generation"]
    support_summary = diagnostics["support_summary"]
    metrics_summary = _metrics_issue_summary(diagnostics["metrics"])
    doctor_summary = _doctor_issue_summary(diagnostics["doctor"])
    deployment_summary = _deployment_issue_summary(deployment)

    deployment_repo_line = (
        f"- Deployment repository: `{deployment_repo}`"
        if deployment_repo
        else "- Deployment repository: _Not specified._"
    )
    deployment_name = deployment.get("name") or "unknown"
    generation_text = _generation_summary_text(latest_generation)
    queue_text = _queue_summary_text(support_summary.get("queue_depth"))
    doctor_text = _doctor_summary_text(doctor_summary)
    sections = [
        "# OKG Deployment Bug Report",
        "",
        "## Summary",
        f"- Problem: {summary or '_No summary provided._'}",
        f"- Deployment: `{deployment_name}`",
        f"- GitHub issue repository: `{repo}`",
        deployment_repo_line,
        f"- OKG version: `{payload.get('okg_version', 'unknown')}`",
        f"- Runtime enabled: `{deployment.get('runtime_enabled')}`",
        f"- Latest generation: {generation_text}",
        f"- Queue depth: {queue_text}",
        f"- DLQ count: `{support_summary.get('dlq_count', 'unknown')}`",
        f"- Doctor: {doctor_text}",
        "",
        "## Notes",
        user_notes.strip() or "_No additional notes provided._",
        "",
        "## Diagnostics",
        _details_block("Doctor failures", doctor_summary),
        _details_block("Metrics summary", metrics_summary),
        _details_block("Latest generation", latest_generation),
        _details_block("Deployment summary", deployment_summary),
        "",
    ]
    return "\n".join(sections)


def _generation_summary_text(latest_generation: dict[str, Any]) -> str:
    generation_id = latest_generation.get("generation_id")
    status = latest_generation.get("status") or "unknown"
    if generation_id is None:
        return f"`{status}`"
    return f"`{generation_id}` (`{status}`)"


def _queue_summary_text(queue_depth: Any) -> str:
    if not isinstance(queue_depth, dict):
        return f"`{queue_depth}`"
    keys = [
        "ingest",
        "link",
        "enrich",
        "publish",
        "embed",
        "enqueued_count",
        "pending_count",
        "ok",
        "code",
    ]
    parts: list[str] = []
    seen: set[str] = set()
    skipped = {"deployment", "message"}
    for key in [*keys, *sorted(str(k) for k in queue_depth)]:
        if key in seen or key in skipped or key not in queue_depth:
            continue
        seen.add(key)
        value = queue_depth[key]
        if isinstance(value, str | int | float | bool) or value is None:
            parts.append(f"{key}={value}")
    return f"`{', '.join(parts) if parts else 'unavailable'}`"


def _doctor_summary_text(doctor_summary: dict[str, Any]) -> str:
    ok = doctor_summary.get("ok")
    unknown_names = [
        str(check.get("check"))
        for check in doctor_summary.get("unknown_checks", [])
        if isinstance(check, dict)
    ]
    if ok is True:
        if unknown_names:
            return "`passed`; no verdict from " + ", ".join(
                f"`{name}`" for name in unknown_names
            )
        return "`passed`"
    codes = _doctor_codes(doctor_summary)
    if ok is False:
        if codes:
            return "`failed` (" + ", ".join(f"`{code}`" for code in codes) + ")"
        return "`failed`"
    return f"`returncode={doctor_summary.get('returncode', 'unknown')}`"


def _doctor_codes(doctor_summary: dict[str, Any]) -> list[str]:
    codes: list[str] = []

    def add(value: Any) -> None:
        if value is None:
            return
        code = str(value)
        if code not in codes:
            codes.append(code)

    for error in doctor_summary.get("errors", []):
        if isinstance(error, dict):
            add(error.get("code"))
    for check in doctor_summary.get("failed_checks", []):
        if isinstance(check, dict):
            add(check.get("code") or check.get("check"))
    return codes


def _details_block(summary: str, value: Any) -> str:
    return "\n".join(
        [
            f"<details><summary>{summary}</summary>",
            "",
            "```json",
            _stable_json(value),
            "```",
            "",
            "</details>",
        ]
    )


def _deployment_issue_summary(deployment: dict[str, Any]) -> dict[str, Any]:
    sources = deployment.get("sources")
    source_names: list[str] = []
    source_count: Any = None
    if isinstance(sources, dict):
        source_count = sources.get("count")
        entries = sources.get("entries")
        if isinstance(entries, list):
            for entry in entries:
                if isinstance(entry, dict) and entry.get("name") is not None:
                    source_names.append(str(entry["name"]))
    return _drop_none(
        {
            "name": deployment.get("name"),
            "description": deployment.get("description"),
            "runtime_enabled": deployment.get("runtime_enabled"),
            "modules": deployment.get("modules"),
            "source_count": source_count,
            "source_names": source_names,
        }
    )


def _metrics_issue_summary(metrics: dict[str, Any]) -> dict[str, Any]:
    summary: dict[str, Any] = {
        "returncode": metrics.get("returncode"),
        "timed_out": metrics.get("timed_out"),
    }
    metrics_json = metrics.get("json")
    if isinstance(metrics_json, dict):
        for key in (
            "generation_id",
            "mode",
            "dlq_count",
            "queue_depth",
            "nodes_live_delta",
            "edges_live_delta",
            "generation_blocked_count",
            "excluded_sources",
            "source_local_invariants_failed",
            "cross_source_invariants_failed",
            "cross_source_invariants_timed_out",
            "total_invariant_budget_exceeded",
            "doctor_exit_code",
        ):
            if key in metrics_json:
                summary[key] = metrics_json[key]
    else:
        for key in ("stdout", "stderr"):
            if metrics.get(key):
                summary[key] = _truncate(str(metrics[key]), limit=2000)
    return _drop_none(summary)


def _doctor_issue_summary(doctor: dict[str, Any]) -> dict[str, Any]:
    summary: dict[str, Any] = {
        "returncode": doctor.get("returncode"),
        "timed_out": doctor.get("timed_out"),
    }
    doctor_json = doctor.get("json")
    if isinstance(doctor_json, dict):
        summary["ok"] = doctor_json.get("ok")
        errors = doctor_json.get("errors")
        if isinstance(errors, list):
            summary["errors"] = [
                _compact_doctor_error(error)
                for error in errors
                if isinstance(error, dict)
            ]
        failed_checks: list[dict[str, Any]] = []
        unknown_checks: list[dict[str, Any]] = []
        for check_name, check in sorted(doctor_json.items()):
            # A doctor check result carries `ok` or a `code`; other dict
            # values (`extensions`, `roles`, `role_superuser`) are maps.
            if not isinstance(check, dict) or not ("ok" in check or "code" in check):
                continue
            # #2573 slice 10: a check with no boolean verdict is unknown and
            # is listed by name, not read as passing.
            verdict = read_ok(check, source=f"doctor_{check_name}")
            if isinstance(verdict, Unknown):
                unknown_checks.append({"check": check_name, "reason": verdict.reason})
                continue
            if verdict:
                continue
            failed_checks.append(
                _drop_none(
                    {
                        "check": check_name,
                        "code": check.get("code"),
                        "message": check.get("message"),
                        "last_success_at": check.get("last_success_at"),
                        "last_run_at": check.get("last_run_at"),
                        "threshold_seconds": check.get("threshold_seconds"),
                        "threshold_minutes": check.get("threshold_minutes"),
                    }
                )
            )
        if failed_checks:
            summary["failed_checks"] = failed_checks
        if unknown_checks:
            summary["unknown_checks"] = unknown_checks
    else:
        for key in ("stdout", "stderr"):
            if doctor.get(key):
                summary[key] = _truncate(str(doctor[key]), limit=2000)
    return _drop_none(summary)


def _compact_doctor_error(error: dict[str, Any]) -> dict[str, Any]:
    return _drop_none(
        {
            "code": error.get("code"),
            "message": error.get("message"),
        }
    )


def _drop_none(value: dict[str, Any]) -> dict[str, Any]:
    return {k: v for k, v in value.items() if v is not None}


def _check_for_leaks(
    body: str,
    *,
    repo_root: Path | None,
    deployment_dir: Path | None,
) -> dict[str, Any]:
    findings: list[dict[str, str]] = []

    def _check_literal(label: str, value: Path | None) -> None:
        if value is None:
            return
        try:
            raw = str(value.expanduser().resolve())
        except OSError:
            raw = str(value.expanduser())
        if raw and raw != "/" and raw in body:
            findings.append({"kind": label, "value": f"<{label}>"})

    _check_literal("repo_root", repo_root)
    _check_literal("deployment_dir", deployment_dir)
    _check_literal("home", Path.home())

    patterns: tuple[tuple[str, re.Pattern[str]], ...] = (
        ("dsn", _DSN_RE),
        ("github_token", _GITHUB_TOKEN_RE),
        ("credential_url", _CRED_URL_RE),
        ("git_remote", _GIT_REMOTE_RE),
        ("absolute_user_path", _ABS_USER_PATH_RE),
    )
    for name, pattern in patterns:
        if pattern.search(body):
            findings.append({"kind": name, "value": "<pattern>"})
    # A form the masker would still change is an unmasked secret value.
    # Local import for the same reason as in redact_text.
    from okg.substrate.sources.redaction import redact_secret_forms

    _masked, unmasked_forms = redact_secret_forms(body)
    for name in unmasked_forms:
        findings.append({"kind": name, "value": "<pattern>"})
    return {"ok": not findings, "findings": findings}


def _source_registry_summary(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {"path": str(path), "count": 0, "entries": []}
    try:
        raw = fastyaml.safe_load(path.read_text()) or {}
    except Exception as exc:  # noqa: BLE001 - diagnostic only
        return {
            "path": str(path),
            "count": 0,
            "entries": [],
            "error": f"{type(exc).__name__}: {exc}",
        }

    entries_raw: Any
    if isinstance(raw, dict):
        entries_raw = raw.get("sources", raw)
    else:
        entries_raw = raw

    entries: list[dict[str, Any]] = []
    if isinstance(entries_raw, dict):
        iterable = entries_raw.items()
        for key, value in iterable:
            if isinstance(value, dict):
                entries.append(_source_entry_summary(str(key), value))
            else:
                entries.append({"name": str(key), "value": str(value)})
    elif isinstance(entries_raw, list):
        for i, value in enumerate(entries_raw):
            if isinstance(value, dict):
                name = str(value.get("name") or value.get("id") or i)
                entries.append(_source_entry_summary(name, value))
            else:
                entries.append({"name": str(i), "value": str(value)})

    entries = sorted(entries, key=lambda item: str(item.get("name", "")))
    return {
        "path": str(path),
        "count": len(entries),
        "entries": entries,
    }


def _source_entry_summary(name: str, value: dict[str, Any]) -> dict[str, Any]:
    return {
        "name": name,
        "module": value.get("module") or value.get("class"),
        "profile": value.get("profile"),
        "enabled": value.get("enabled", True),
    }


def _issue_title(
    *,
    deployment: str,
    title: str | None,
    summary: str | None,
) -> str:
    if title and title.strip():
        return title.strip()
    stem = (summary or "Deployment bug").strip().splitlines()[0]
    if len(stem) > 80:
        stem = stem[:77].rstrip() + "..."
    return f"[okg][{deployment}] {stem}"


def _okg_version() -> str:
    for package in ("okg", "pz-okg"):
        try:
            return importlib.metadata.version(package)
        except importlib.metadata.PackageNotFoundError:
            continue
    return "unknown"


def _find_repo_root(start: Path) -> Path | None:
    for candidate in (start, *start.parents):
        if (candidate / ".git").exists() or (candidate / "pyproject.toml").exists():
            return candidate
    return None


def _parse_json(text: str) -> Any | None:
    if not text.strip():
        return None
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        return None


def _stable_json(value: Any) -> str:
    return json.dumps(value, indent=2, sort_keys=True, default=str)


def _truncate(text: str, *, limit: int = 12000) -> str:
    if len(text) <= limit:
        return text
    return text[:limit] + "\n<truncated>"


def _sanitize_argv(argv: Sequence[str]) -> list[str]:
    out: list[str] = []
    skip_next = False
    for arg in argv:
        if skip_next:
            out.append("<redacted>")
            skip_next = False
            continue
        if arg in {"--dsn", "--body-file"}:
            out.append(arg)
            skip_next = True
        else:
            out.append(str(arg))
    return out


def _bug_report_error(
    message: str,
    *,
    data: dict[str, Any] | None = None,
    fix_hint: str | None = None,
) -> SubstrateError:
    err = SubstrateError(message, data=data, fix_hint=fix_hint)
    err.code = "bug_report_failed"
    return err


def _github_error(
    message: str,
    *,
    data: dict[str, Any] | None = None,
    fix_hint: str | None = None,
) -> SubstrateError:
    err = SubstrateError(message, data=data, fix_hint=fix_hint)
    err.code = "github_issue_create_failed"
    return err
