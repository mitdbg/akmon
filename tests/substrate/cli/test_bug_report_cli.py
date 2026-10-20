from __future__ import annotations

import hashlib
import json
from types import SimpleNamespace

import okg.substrate.cli.deployment as cli
from okg.substrate.support.bug_report import BugReportPreview


def _preview() -> BugReportPreview:
    return BugReportPreview(
        repo="owner/repo",
        deployment_repo="deployments/demo",
        title="Preview title",
        body="preview body",
        labels=("bug",),
        payload={"diagnostics": {"latest_generation": {"status": "published"}}},
        body_sha256=hashlib.sha256(b"preview body").hexdigest(),
        redactions=(),
        leakage_check={"ok": True, "findings": []},
    )


def _args(**kwargs):
    defaults = {
        "subcmd": "bug-report",
        "deployment": "demo",
        "dsn": None,
        "repo": "owner/repo",
        "deployment_repo": "deployments/demo",
        "title": None,
        "summary": "summary",
        "body_file": None,
        "label": ["bug"],
        "apply": False,
        "gh_binary": "gh",
        "preview_json_file": None,
    }
    defaults.update(kwargs)
    return SimpleNamespace(**defaults)


def test_deployment_bug_report_preview_does_not_create_issue(
    monkeypatch,
    capsys,
):
    calls = {}
    monkeypatch.setattr(cli, "_resolve_deployment_dsn", lambda args: "dsn")

    def fake_build(**kwargs):
        calls["build"] = kwargs
        return _preview()

    def fake_create(*args, **kwargs):  # pragma: no cover - failure path
        raise AssertionError("preview mode must not create issues")

    monkeypatch.setattr(
        "okg.substrate.support.bug_report.build_bug_report_preview",
        fake_build,
    )
    monkeypatch.setattr(
        "okg.substrate.support.bug_report.create_github_issue",
        fake_create,
    )

    rc = cli._cmd_deployment_bug_report(_args(), as_json=False)

    assert rc == 0
    out = capsys.readouterr().out
    assert "title: Preview title" in out
    assert "preview body" in out
    assert calls["build"]["dsn"] == "dsn"
    assert calls["build"]["deployment_repo"] == "deployments/demo"
    assert calls["build"]["labels"] == ("bug",)


def test_deployment_bug_report_apply_returns_issue_url(
    monkeypatch,
    capsys,
):
    monkeypatch.setattr(cli, "_resolve_deployment_dsn", lambda args: "dsn")
    monkeypatch.setattr(
        "okg.substrate.support.bug_report.build_bug_report_preview",
        lambda **kwargs: _preview(),
    )
    monkeypatch.setattr(
        "okg.substrate.support.bug_report.create_github_issue",
        lambda preview, gh_binary: {
            "ok": True,
            "mode": "apply",
            "issue_url": "https://github.com/owner/repo/issues/1",
            "body_sha256": preview.body_sha256,
        },
    )

    rc = cli._cmd_deployment_bug_report(_args(apply=True), as_json=True)

    assert rc == 0
    assert "https://github.com/owner/repo/issues/1" in capsys.readouterr().out


def test_deployment_bug_report_apply_from_preview_json_file(
    tmp_path,
    monkeypatch,
    capsys,
):
    preview = _preview()
    preview_json = tmp_path / "preview.json"
    preview_json.write_text(json.dumps(preview.to_dict()))
    calls = {}

    def fake_build(**kwargs):  # pragma: no cover - failure path
        raise AssertionError("saved preview apply must not rebuild diagnostics")

    def fake_create(preview_arg, gh_binary):
        calls["preview"] = preview_arg
        calls["gh_binary"] = gh_binary
        return {
            "ok": True,
            "mode": "apply",
            "issue_url": "https://github.com/owner/repo/issues/2",
            "body_sha256": preview_arg.body_sha256,
        }

    monkeypatch.setattr(
        "okg.substrate.support.bug_report.build_bug_report_preview",
        fake_build,
    )
    monkeypatch.setattr(
        "okg.substrate.support.bug_report.create_github_issue",
        fake_create,
    )

    rc = cli._cmd_deployment_bug_report(
        _args(
            apply=True,
            deployment=None,
            repo=None,
            preview_json_file=str(preview_json),
        ),
        as_json=True,
    )

    assert rc == 0
    assert calls["preview"].body == "preview body"
    assert calls["preview"].body_sha256 == preview.body_sha256
    assert "https://github.com/owner/repo/issues/2" in capsys.readouterr().out
