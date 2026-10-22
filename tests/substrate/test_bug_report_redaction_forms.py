"""Bug-report redaction masks signed URLs, cookies, auth= and Bearer values (#3946).

`redact_text(secret_forms=True)` in support/bug_report.py masks GitHub issue
bodies, and `_check_for_leaks` / `create_github_issue` refuse a body that still
holds one of these forms. Sample secret values are fake and built at runtime so
no secret-looking literal sits in this file.
"""
from __future__ import annotations

import pytest

from okg.substrate.errors import SubstrateError
from okg.substrate.support import bug_report
from okg.substrate.support.bug_report import _check_for_leaks
from okg.substrate.support.bug_report import redact_text as _redact_text


def redact_text(text: str, **kw):
    """The report path: shared secret-form masking on."""
    return _redact_text(text, secret_forms=True, **kw)


FAKE = "fake" + "val" + "9z8y7x"

# (label, text holding FAKE, text that must survive redaction)
_LEAKING_FORMS = [
    ("sig", f"GET https://files.example.org/a.pdf?sig={FAKE}&n=2 failed", "files.example.org/a.pdf"),
    ("x-amz-signature", f"GET https://s3.example.org/o?X-Amz-Signature={FAKE}&n=2", "s3.example.org/o"),
    ("access_token", f"GET https://api.example.org/v1?access_token={FAKE}&n=2", "api.example.org/v1"),
    ("cookie", f"reply\nCookie: sid={FAKE}; Path=/\nnext line", "next line"),
    ("set-cookie", f"reply\nSet-Cookie: sid={FAKE}; HttpOnly\nnext line", "next line"),
    ("cookie-dict", "headers {'Cookie': 'sid=" + FAKE + "'} sent", "sent"),
    ("auth", f"connect auth={FAKE} to host", "to host"),
    ("auth-quoted", f'connect auth="{FAKE}" to host', "to host"),
    ("my-token", f"env MY_TOKEN={FAKE} set", "env"),
    ("db-pass", f"env DB_PASS={FAKE} set", "env"),
    # Header names that merely end in a secret header's name.
    ("x-authorization", f"X-Authorization: Bearer {FAKE}", "X-"),
    ("x-forwarded-authorization", f"X-Forwarded-Authorization: Bearer {FAKE}", "X-Forwarded-"),
    ("grpc-authorization", f"Grpc-Metadata-Authorization: Bearer {FAKE}", "Grpc-Metadata-"),
    ("x-private-token", f"X-Private-Token: {FAKE}", "X-"),
    ("my-x-api-key", f"My-X-Api-Key: {FAKE}", "My-"),
    # Secret names: plural, glued, pass-words.
    ("access-tokens", f"env ACCESS_TOKENS={FAKE} set", "env"),
    ("hf-tokens", f"env HF_TOKENS={FAKE} set", "env"),
    ("x-tokens", f"env x_tokens={FAKE} set", "env"),
    ("tokenx", f"env TOKENX={FAKE} set", "env"),
    ("passphrase", f"env PASSPHRASE={FAKE} set", "env"),
    ("gpg-passphrase", f"env GPG_PASSPHRASE={FAKE} set", "env"),
    ("passcode", f"env PASSCODE={FAKE} set", "env"),
    ("db-pass1", f"env DB_PASS1={FAKE} set", "env"),
    ("passwd", f"env PASSWD={FAKE} set", "env"),
    ("db-passwd", f"env DB_PASSWD={FAKE} set", "env"),
    # A "no secret" word only counts when the value ends there.
    ("true-prefix", f"env GITHUB_TOKEN=true-{FAKE} set", "env"),
    ("none-dot", f"env GITHUB_TOKEN=none.{FAKE} set", "env"),
    ("auth-none", f"connect auth=none:{FAKE} to host", "to host"),
    ("bearer", f"Authorization: Bearer {FAKE}", "Authorization"),
    ("basic", f"Authorization: Basic {FAKE}", "Authorization"),
    ("proxy-auth", f"Proxy-Authorization: Bearer {FAKE}", "Proxy-Authorization"),
]


@pytest.mark.parametrize(
    "label, text, kept", _LEAKING_FORMS, ids=[c[0] for c in _LEAKING_FORMS]
)
def test_leaking_forms_are_masked(label: str, text: str, kept: str) -> None:
    assert FAKE in text
    out, markers = redact_text(text)
    assert FAKE not in out
    assert kept in out
    assert markers, "a masked form must be reported as a redaction"


@pytest.mark.parametrize(
    "label, text, kept", _LEAKING_FORMS, ids=[c[0] for c in _LEAKING_FORMS]
)
def test_leak_check_refuses_an_unmasked_form(label: str, text: str, kept: str) -> None:
    leaks = _check_for_leaks(text, repo_root=None, deployment_dir=None)
    assert leaks["ok"] is False
    assert leaks["findings"]
    assert FAKE not in str(leaks)


def test_leak_check_refuses_a_report_holding_a_bearer_token() -> None:
    body = "## Diagnostics\n\nrequest failed\nAuthorization: Bearer " + FAKE + "\n"
    leaks = _check_for_leaks(body, repo_root=None, deployment_dir=None)
    assert leaks["ok"] is False
    assert {"kind": "authorization_header", "value": "<pattern>"} in leaks["findings"]


@pytest.mark.parametrize(
    "label, text, kept", _LEAKING_FORMS, ids=[c[0] for c in _LEAKING_FORMS]
)
def test_redacted_text_passes_the_leak_check(label: str, text: str, kept: str) -> None:
    out, _ = redact_text(text)
    leaks = _check_for_leaks(out, repo_root=None, deployment_dir=None)
    assert leaks == {"ok": True, "findings": []}
    assert redact_text(out)[0] == out, "masking must be idempotent"


def test_existing_marks_survive_the_shared_pass() -> None:
    out, markers = redact_text(f"token={FAKE} and password: {FAKE}")
    assert out == "token=<secret:redacted> and password: <secret:redacted>"
    assert "secret_assignment" in markers
    assert "token_assignment" not in markers


@pytest.mark.parametrize(
    "text",
    [
        "fix: author=jane oauth=false and unauth=true handled",
        "docs: explain how a cookie jar and Authorization header work",
        "Merge pull request #12 from user/fix-token-refresh-flow",
        "def fetch(session, key, sig_len): return session.get(key)[:sig_len]",
        "refactor: rename authority to origin; see https://example.org/a/b?page=2&q=x",
        "tests: 12 passed in 3.4s, coverage 91%",
        "Co-Authored-By: Jane Doe <jane@example.org>",
        # Shapes found in this repo's own commit messages (#3946 measurement).
        "budget_seconds=null and budget_passed=null, substrate.passed=True",
        "At max_tokens=4000 two of eight; probed_tokens=0 reported",
        "libpq password=, query-string password; see the docs",
        "authentication is on through WEBUI_AUTH=True, the digest",
        "env GITHUB_TOKEN=true and API_TOKEN=none and auth=null are flags",
        "ends next: continuation_token=<token> (kind record_w)",
        "makes TLC fail (BuggySecretPersistence=TRUE) as expected",
    ],
)
def test_harmless_text_is_unchanged(text: str) -> None:
    assert redact_text(text)[0] == text
    assert _check_for_leaks(text, repo_root=None, deployment_dir=None)["ok"]


def test_graph_content_callers_keep_the_default_off() -> None:
    """The github, github_actions, pact and git-history sources call
    redact_text with no flag and store the result as graph content. The shared
    masking is opt-in so their output does not change (follow-up issue)."""
    import inspect

    assert inspect.signature(_redact_text).parameters["secret_forms"].default is False
    text = f"GET https://h.example.org/a?sig={FAKE} Authorization: Bearer {FAKE}"
    assert _redact_text(text)[0] == _redact_text(text, secret_forms=False)[0]


def test_sanitize_value_masks_every_form_in_a_payload() -> None:
    payload = {"log": [f"GET https://h.example.org/a?sig={FAKE}", f"Cookie: sid={FAKE}"]}
    out, markers = bug_report._sanitize_value(payload, repo_root=None, deployment_dir=None)
    assert FAKE not in str(out)
    assert "signed_url_value" in markers


def _preview(body: str, labels: tuple[str, ...] = ()) -> bug_report.BugReportPreview:
    return bug_report.BugReportPreview(
        repo="owner/repo",
        deployment_repo="deployments/demo",
        title="Bug title",
        body=body,
        labels=labels,
        payload={},
        body_sha256="hash",
        redactions=(),
        leakage_check={"ok": True, "findings": []},
    )


def test_create_github_issue_refuses_a_body_holding_a_bearer_token() -> None:
    calls: list[object] = []

    def runner(argv, timeout):
        calls.append(argv)
        raise AssertionError("gh must not run")

    with pytest.raises(SubstrateError) as err:
        bug_report.create_github_issue(
            _preview(f"failed\nAuthorization: Bearer {FAKE}\n"), command_runner=runner
        )
    assert err.value.code == "github_issue_create_failed"
    assert not calls
    assert FAKE not in str(err.value.data)


def test_create_github_issue_checks_the_labels_too() -> None:
    def runner(argv, timeout):
        raise AssertionError("gh must not run")

    with pytest.raises(SubstrateError):
        bug_report.create_github_issue(
            _preview("exact body\n", labels=(f"Authorization: Bearer {FAKE}",)),
            command_runner=runner,
        )


def test_create_github_issue_failure_output_is_masked() -> None:
    def runner(argv, timeout):
        return bug_report.CommandResult(
            argv=list(argv),
            returncode=1,
            stdout="",
            stderr=f"HTTP 401 Authorization: Bearer {FAKE}",
        )

    with pytest.raises(SubstrateError) as err:
        bug_report.create_github_issue(_preview("exact body\n"), command_runner=runner)
    assert FAKE not in str(err.value.data)


def test_rollout_report_masks_signed_url_and_bearer_values() -> None:
    from okg.substrate.support.rollout_sharing import build_colleague_rollout_report

    report = build_colleague_rollout_report(
        deployment="demo",
        pact_id="p",
        task_id="t",
        generation_id=None,
        readiness={"note": f"GET https://h.example.org/a?sig={FAKE} Authorization: Bearer {FAKE}"},
        risks=[],
    )
    assert FAKE not in report["body"]
    assert report["ok"] is True


@pytest.mark.parametrize(
    "unit",
    ["token", "TOKEN_", "PASS", "PASSWD", "PASS_", "auth", "auth=", "Cookie", "Cookie:", "?sig", "&key", "Bearer ", "a=", "a-", "x_"],
)
@pytest.mark.parametrize("tail", ["", "="])
def test_long_adversarial_runs_stay_linear(unit: str, tail: str) -> None:
    import time

    body = unit * 40_000 + tail
    started = time.monotonic()
    redact_text(body)
    _check_for_leaks(body, repo_root=None, deployment_dir=None)
    assert time.monotonic() - started < 3.0, unit


@pytest.mark.parametrize("name", ["ACCESS_TOKEN", "PASSWORD", "DB_PASS", "x_tokens_file", "SECRET"])
def test_a_plain_number_is_masked_except_for_a_tokens_count(name: str) -> None:
    out, _ = redact_text(f"env {name}=12345678 set")
    assert "12345678" not in out
    assert "redacted>" in out
    for count in ("max_tokens=4000", "probed_tokens=0"):
        assert redact_text(count)[0] == count


def test_support_package_exports_the_same_redactor() -> None:
    from okg.substrate import support

    assert support.redact_text is bug_report.redact_text
