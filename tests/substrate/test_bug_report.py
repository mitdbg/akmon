from __future__ import annotations

import json
from pathlib import Path

import psycopg
import pytest

from okg.substrate.support import bug_report
from tests.substrate._fake_credentials import fake_credential

#: A fake GitHub token in the real shape; built at runtime.
GITHUB_TOKEN = fake_credential("ghp_", "123456789012345678901234567890123456")


def _write_deployment(root: Path) -> Path:
    dep = root / "demo"
    (dep / "schemas").mkdir(parents=True)
    (dep / "deployment.yaml").write_text(
        "name: demo\n"
        "description: Demo deployment\n"
        "schema_dir: schemas\n"
        "source_registry: source_registry.yaml\n"
        "postgres:\n"
        "  dsn: ${OKG_DSN}\n"
        "modules:\n"
        "  - git_graph\n"
    )
    (dep / "source_registry.yaml").write_text(
        "sources:\n"
        "  docs:\n"
        "    module: sources.docs\n"
        "    profile: reference_catalog\n"
    )
    return dep


def test_redact_text_removes_paths_dsns_tokens_and_remotes(tmp_path: Path):
    dep = tmp_path / "deployments" / "demo"
    repo = tmp_path
    text = (
        f"path={dep}/source_registry.yaml "
        "other=/Users/alice/private/okg "
        "dsn=postgres://user:pass@localhost:5432/okg "
        f"token={GITHUB_TOKEN} "
        "remote=git@github.com:secret/private.git"
    )

    redacted, markers = bug_report.redact_text(
        text,
        repo_root=repo,
        deployment_dir=dep,
    )

    assert str(dep) not in redacted
    assert "/Users/alice" not in redacted
    assert "postgres://user:pass" not in redacted
    assert "ghp_1234567890" not in redacted
    assert "secret/private" not in redacted
    assert "<deployment>" in redacted
    assert "<dsn:redacted>" in redacted
    assert "<secret:redacted>" in redacted
    assert "deployment" in markers
    assert "dsn" in markers


#: Credential URLs the report must never publish (#2812 review): an empty
#: user name with a token in the password slot, and an upper-case scheme.
_CREDENTIAL_URLS = {
    "empty-user-github-pat": "https" + "://:" + fake_credential("github_pat_", "A1b2" * 10) + "@github.com/o/r",
    "empty-user-password": "https" + "://:fake-sentinel-pw@example.test/x",
    "upper-scheme": "HTTPS" + "://u:fake-sentinel-pw@example.test/x",
    "upper-scheme-empty-user": "HTTP" + "://:fake-sentinel-pw@example.test/x",
}


@pytest.mark.parametrize("case", sorted(_CREDENTIAL_URLS))
def test_credential_urls_are_redacted_and_caught_by_the_leakage_check(
    case: str, tmp_path: Path,
) -> None:
    url = _CREDENTIAL_URLS[case]
    secret = url.split("://:", 1)[-1].split("://u:", 1)[-1].split("@", 1)[0]

    redacted, markers = bug_report.redact_text(
        f"clone failed for {url} today", repo_root=tmp_path, deployment_dir=None,
    )
    leaks = bug_report._check_for_leaks(
        f"clone failed for {url}", repo_root=None, deployment_dir=None,
    )

    assert secret not in redacted
    assert "<remote:redacted>" in redacted
    assert "credential_url" in markers
    assert {"kind": "credential_url", "value": "<pattern>"} in leaks["findings"]


def test_a_url_without_a_password_is_not_a_credential_url(tmp_path: Path) -> None:
    for url in ("https" + "://example.test/path", "https" + "://@example.test/x",
                "https" + "://:@example.test/x"):
        leaks = bug_report._check_for_leaks(url, repo_root=None, deployment_dir=None)
        assert {"kind": "credential_url", "value": "<pattern>"} not in leaks["findings"], url


def test_redact_text_collapses_home_codex_state_paths():
    codex_path = Path.home() / ".codex" / "worktrees" / "abcd" / "okg"

    redacted, markers = bug_report.redact_text(f"cwd={codex_path}/pact.md")

    assert str(Path.home()) not in redacted
    assert "/.codex/" not in redacted
    assert "<codex-state>" in redacted
    assert "codex_state_path" in markers


@pytest.mark.postgres
def test_dbos_runtime_queue_summary_counts_exhausted_recovery_as_dlq(
    clean_runtime: str,
) -> None:
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.execute("DROP SCHEMA IF EXISTS dbos CASCADE")
            cur.execute("CREATE SCHEMA dbos")
            cur.execute(
                """
                CREATE TABLE dbos.workflow_status (
                  workflow_uuid text PRIMARY KEY,
                  queue_name text NOT NULL,
                  name text NOT NULL,
                  status text NOT NULL,
                  created_at timestamptz NOT NULL DEFAULT now(),
                  updated_at timestamptz NOT NULL DEFAULT now()
                )
                """
            )
            cur.execute(
                """
                INSERT INTO dbos.workflow_status
                  (workflow_uuid, queue_name, name, status)
                VALUES
                  ('wf-exhausted', 'okg-demo-source-sync', 'okg_source_sync',
                   'MAX_RECOVERY_ATTEMPTS_EXCEEDED')
                """
            )

    summary = bug_report._dbos_runtime_queue_summary(clean_runtime)

    assert summary["dlq_count"] == 1


def test_dbos_runtime_queue_summary_normalizes_connection_errors(
    monkeypatch,
) -> None:
    errors = iter(
        [
            psycopg.OperationalError("connection refused"),
            psycopg.OperationalError(
                "server closed the connection unexpectedly"
            ),
        ]
    )

    def fail_connect(*args, **kwargs):
        del args, kwargs
        raise next(errors)

    monkeypatch.setattr(psycopg, "connect", fail_connect)

    first = bug_report._dbos_runtime_queue_summary("postgresql://unused")
    second = bug_report._dbos_runtime_queue_summary("postgresql://unused")

    expected = {
        "status": "error",
        "error": "OperationalError",
        "reason": "runtime queue diagnostic failed",
    }
    assert first == {"queue_depth": expected, "dlq_count": expected}
    assert second == first


def test_preview_is_deterministic_and_redacted(
    tmp_path: Path,
    monkeypatch,
):
    deployments_root = tmp_path / "deployments"
    dep = _write_deployment(deployments_root)
    monkeypatch.setenv("OKG_DEPLOYMENTS_DIR", str(deployments_root))
    monkeypatch.setattr(
        bug_report,
        "_latest_generation",
        lambda **kwargs: {
            "generation_id": 42,
            "status": "published",
            "nodes_added": 3,
            "edges_added": 4,
        },
    )
    connection_errors = iter(
        [
            psycopg.OperationalError("connection refused"),
            psycopg.OperationalError(
                "server closed the connection unexpectedly"
            ),
        ]
    )

    def fail_runtime_queue_connect(*args, **kwargs):
        del args, kwargs
        raise next(connection_errors)

    monkeypatch.setattr(psycopg, "connect", fail_runtime_queue_connect)

    def runner(argv, timeout):
        assert timeout == 20
        if "metrics" in argv:
            payload = {
                "generation_id": 42,
                "queue_depth": {"ingest": 0},
                "dlq_count": 0,
                "path": str(dep / "private.log"),
            }
        else:
            payload = {
                "ok": True,
                "dsn": "postgres://user:pass@host/db",
                "remote": "https://github.com/private/repo.git",
            }
        return bug_report.CommandResult(
            argv=list(argv),
            returncode=0,
            stdout=json.dumps(payload),
            stderr="",
        )

    kwargs = {
        "deployment": "demo",
        "repo": "owner/repo",
        "deployment_repo": "deployments/demo",
        "dsn": "postgres://user:pass@host/db",
        "summary": "A thing failed",
        "body_text": (
            f"See {dep}/logs/error.txt and "
            f"{GITHUB_TOKEN}"
        ),
        "labels": ("bug", "deployment"),
        "command_runner": runner,
    }
    first = bug_report.build_bug_report_preview(**kwargs)
    second = bug_report.build_bug_report_preview(**kwargs)

    assert first.body == second.body
    assert first.body_sha256 == second.body_sha256
    assert "GitHub issue repository: `owner/repo`" in first.body
    assert "Deployment repository: `deployments/demo`" in first.body
    assert "Doctor failures" in first.body
    assert "Metrics summary" in first.body
    assert "Full Payload" not in first.body
    assert "source_registry" not in first.body
    assert first.payload["issue"]["target_repo"] == "owner/repo"
    assert first.payload["issue"]["deployment_repo"] == "deployments/demo"
    assert "generation_id" in first.body
    assert "published" in first.body
    assert str(tmp_path) not in first.body
    assert "postgres://user:pass" not in first.body
    assert "ghp_1234567890" not in first.body
    assert "private/repo" not in first.body
    assert first.leakage_check == {"ok": True, "findings": []}


def test_preview_can_be_rehydrated_for_exact_apply(
    tmp_path: Path,
    monkeypatch,
):
    deployments_root = tmp_path / "deployments"
    _write_deployment(deployments_root)
    monkeypatch.setenv("OKG_DEPLOYMENTS_DIR", str(deployments_root))
    monkeypatch.setattr(
        bug_report,
        "_latest_generation",
        lambda **kwargs: {"generation_id": 42, "status": "published"},
    )

    def runner(argv, timeout):
        return bug_report.CommandResult(
            argv=list(argv),
            returncode=0,
            stdout=json.dumps({"ok": True}),
            stderr="",
        )

    preview = bug_report.build_bug_report_preview(
        deployment="demo",
        repo="owner/repo",
        deployment_repo="deployments/demo",
        dsn="postgres://user:pass@host/db",
        summary="A thing failed",
        command_runner=runner,
    )

    rehydrated = bug_report.bug_report_preview_from_dict(preview.to_dict())

    assert rehydrated.body == preview.body
    assert rehydrated.body_sha256 == preview.body_sha256
    assert rehydrated.repo == "owner/repo"
    assert rehydrated.deployment_repo == "deployments/demo"


def test_preview_defaults_deployment_repo_to_issue_repo(
    tmp_path: Path,
    monkeypatch,
):
    deployments_root = tmp_path / "deployments"
    _write_deployment(deployments_root)
    monkeypatch.setenv("OKG_DEPLOYMENTS_DIR", str(deployments_root))
    monkeypatch.setattr(
        bug_report,
        "_latest_generation",
        lambda **kwargs: {"generation_id": 42, "status": "published"},
    )

    def runner(argv, timeout):
        return bug_report.CommandResult(
            argv=list(argv),
            returncode=0,
            stdout=json.dumps({"ok": True}),
            stderr="",
        )

    preview = bug_report.build_bug_report_preview(
        deployment="demo",
        repo="owner/repo",
        dsn="postgres://user:pass@host/db",
        summary="A thing failed",
        command_runner=runner,
    )

    assert preview.deployment_repo == "owner/repo"
    assert preview.payload["issue"]["deployment_repo"] == "owner/repo"
    assert "Deployment repository: `owner/repo`" in preview.body


def test_preview_rejects_invalid_deployment_repo(
    tmp_path: Path,
    monkeypatch,
):
    deployments_root = tmp_path / "deployments"
    _write_deployment(deployments_root)
    monkeypatch.setenv("OKG_DEPLOYMENTS_DIR", str(deployments_root))

    try:
        bug_report.build_bug_report_preview(
            deployment="demo",
            repo="owner/repo",
            deployment_repo="not-a-repo",
        )
    except Exception as exc:
        assert "invalid --deployment-repo" in str(exc)
    else:  # pragma: no cover - defensive
        raise AssertionError("expected invalid deployment repo to fail")


def test_preview_redacts_generated_title(tmp_path: Path, monkeypatch):
    deployments_root = tmp_path / "deployments"
    dep = _write_deployment(deployments_root)
    monkeypatch.setenv("OKG_DEPLOYMENTS_DIR", str(deployments_root))
    monkeypatch.setattr(
        bug_report,
        "_latest_generation",
        lambda **kwargs: {"generation_id": 42, "status": "published"},
    )

    def runner(argv, timeout):
        return bug_report.CommandResult(
            argv=list(argv),
            returncode=0,
            stdout="{}",
            stderr="",
        )

    preview = bug_report.build_bug_report_preview(
        deployment="demo",
        repo="owner/repo",
        dsn="postgres://user:pass@host/db",
        summary=(
            f"Sensitive {dep}/logs/error.txt /Users/alice/private "
            "postgres://user:pass@host/db "
            f"{GITHUB_TOKEN} "
            "git@github.com:secret/private.git"
        ),
        command_runner=runner,
    )

    issue_text = f"{preview.title}\n{preview.body}"
    assert str(tmp_path) not in issue_text
    assert "/Users/alice" not in issue_text
    assert "postgres://user:pass" not in issue_text
    assert "ghp_" not in issue_text
    assert "secret/private" not in issue_text
    assert "<secret:redacted>" in issue_text
    assert preview.leakage_check == {"ok": True, "findings": []}


def test_preview_redacts_explicit_title(tmp_path: Path, monkeypatch):
    deployments_root = tmp_path / "deployments"
    _write_deployment(deployments_root)
    monkeypatch.setenv("OKG_DEPLOYMENTS_DIR", str(deployments_root))
    monkeypatch.setattr(
        bug_report,
        "_latest_generation",
        lambda **kwargs: {"generation_id": 42, "status": "published"},
    )

    def runner(argv, timeout):
        return bug_report.CommandResult(
            argv=list(argv),
            returncode=0,
            stdout="{}",
            stderr="",
        )

    preview = bug_report.build_bug_report_preview(
        deployment="demo",
        repo="owner/repo",
        dsn="postgres://user:pass@host/db",
        title=(
            "Crash for /Users/alice/private "
            f"{GITHUB_TOKEN}"
        ),
        summary="A thing failed",
        command_runner=runner,
    )

    issue_text = f"{preview.title}\n{preview.body}"
    assert "/Users/alice" not in issue_text
    assert "ghp_" not in issue_text
    assert "<secret:redacted>" in issue_text
    assert preview.leakage_check == {"ok": True, "findings": []}


def test_create_github_issue_uses_exact_preview_body(tmp_path: Path):
    preview = bug_report.BugReportPreview(
        repo="owner/repo",
        deployment_repo="deployments/demo",
        title="Bug title",
        body="exact body\n",
        labels=("bug",),
        payload={},
        body_sha256="hash",
        redactions=(),
        leakage_check={"ok": True, "findings": []},
    )
    seen = {}

    def runner(argv, timeout):
        body_file = Path(argv[argv.index("--body-file") + 1])
        seen["body"] = body_file.read_text()
        seen["argv"] = list(argv)
        seen["timeout"] = timeout
        return bug_report.CommandResult(
            argv=list(argv),
            returncode=0,
            stdout="https://github.com/owner/repo/issues/1\n",
            stderr="",
        )

    payload = bug_report.create_github_issue(
        preview,
        command_runner=runner,
        command_timeout_seconds=7,
    )

    assert seen["body"] == preview.body
    assert seen["timeout"] == 7
    assert seen["argv"][:5] == ["gh", "issue", "create", "--repo", "owner/repo"]
    assert payload["issue_url"] == "https://github.com/owner/repo/issues/1"
    assert payload["deployment_repo"] == "deployments/demo"
