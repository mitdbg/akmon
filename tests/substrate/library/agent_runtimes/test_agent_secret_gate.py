from __future__ import annotations

import json
from dataclasses import replace

import pytest

from okg.substrate.library.agent_runtimes.secret_ssh import (
    AgentSecretGrant,
    AgentSecretUseRequest,
    detect_agent_secret_literals,
    evaluate_agent_secret_use,
)
from okg.substrate.nomos import compile_policy_graph
from tests.substrate._fake_credentials import PEM_OPENSSH_BEGIN, fake_credential


SUBJECT = "subject:agent:test"
SECRET_REF = "secret://okg/deploy/openai-token"
CANARY = "sk-live-raw-agent-secret"


def _policy():
    return compile_policy_graph({
        "policy_bundle_id": "nomos:test:agent-secret-gate",
        "policy_version": "v1",
        "vocabulary_strict": True,
        "default_effect": "deny",
        "rules": [
            {
                "rule_id": "allow_agent_secret_use",
                "effect": "allow",
                "actions": ["secret.use"],
                "subjects": [SUBJECT],
                "priority": 100,
                "reason_code": "nomos.test.agent_secret_allowed",
                "obligations": [{"type": "secret_ref_required"}],
            },
        ],
    })


def _deny_policy():
    return compile_policy_graph({
        "policy_bundle_id": "nomos:test:agent-secret-gate-deny",
        "policy_version": "v1",
        "vocabulary_strict": True,
        "default_effect": "deny",
        "rules": [
            {
                "rule_id": "allow_different_subject_secret_use",
                "effect": "allow",
                "actions": ["secret.use"],
                "subjects": ["subject:agent:other"],
                "priority": 100,
                "reason_code": "nomos.test.other_agent_secret_allowed",
                "obligations": [{"type": "secret_ref_required"}],
            },
        ],
    })


def _request(**overrides) -> AgentSecretUseRequest:
    base = AgentSecretUseRequest(
        subject_binding_id=SUBJECT,
        agent_run_id="agent-run-secret",
        profile_id="deploy",
        secret_ref=SECRET_REF,
        action="mcp.connect",
        resource_id="mcp:okg-workspace",
        delivery_mode="brokered_operation",
        deployment="okg-workspace",
        deployment_posture={"deployment_class": "dogfood_workspace"},
        agent_visible_config={
            "env": {"OPENAI_API_KEY": {"secret_ref": SECRET_REF}},
            "argv": ["okg", "mcp-serve"],
        },
    )
    return replace(base, **overrides)


def _grant(**overrides) -> AgentSecretGrant:
    base = AgentSecretGrant(
        grant_id="secret-grant-1",
        subject_binding_id=SUBJECT,
        agent_run_id="agent-run-secret",
        profile_id="deploy",
        secret_ref=SECRET_REF,
        action="mcp.connect",
        resource_id="mcp:okg-workspace",
        delivery_mode="brokered_operation",
        expires_at="2999-01-01T00:00:00Z",
    )
    return replace(base, **overrides)


def test_literal_secret_rejection_redacts_values() -> None:
    bundle = _policy()
    request = _request(
        agent_visible_config={
            "env": {
                "OPENAI_API_KEY": CANARY,
                "DATABASE_URL": "postgres://postgres:secret@localhost/okg",
                "COOKIE": "session=agent-cookie-secret",
                "AWS_ACCESS_KEY_ID": fake_credential("AKIA", "1234567890ABCDEF"),
                "GITHUB_TOKEN": fake_credential("ghp_", "1234567890abcdefghijklmnopqrstuvwxyzABCD"),
                "SLACK_BOT_TOKEN": fake_credential("xoxb-", "123456789012-abcdefghijklmnop"),
            },
            "argv": [
                "tool",
                "--api-key=sk-live-raw-argv-secret",
                "--access-key-id=" + fake_credential("ASIA", "1234567890ABCDEF"),
                "Authorization: Bearer abcdefghijklmnopqrstuvwxyz",
                f"{PEM_OPENSSH_BEGIN}\nabc\n-----END OPENSSH PRIVATE KEY-----",
            ],
            "diagnostics": [
                "password=raw-password-value",
                "secret_access_key=wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
                "github_pat_1234567890abcdefABCDEF_1234567890",
            ],
        },
    )

    result = evaluate_agent_secret_use(
        bundle,
        request,
        grant=_grant(),
        active_policy_fingerprint=bundle.policy_fingerprint,
    )

    assert result.allowed is False
    assert result.reason_code == "nomos.runtime.secret_ref_required"
    classes = {
        finding["class"]
        for finding in result.trace["agent_material_gate"]["literal_findings"]
    }
    assert {
        "token_like",
        "dsn",
        "cookie",
        "assignment",
        "bearer_token",
        "private_key",
        "aws_access_key",
        "github_token",
        "slack_token",
    } <= classes
    serialized = json.dumps(result.trace, sort_keys=True)
    assert CANARY not in serialized
    assert "postgres://postgres:secret" not in serialized
    assert "raw-password-value" not in serialized
    assert "OPENSSH PRIVATE KEY" not in serialized
    for finding in result.trace["agent_material_gate"]["literal_findings"]:
        assert "value_hash" not in finding
        assert "value_length" not in finding


def test_secret_broker_scoped_grants() -> None:
    bundle = _policy()

    allowed = evaluate_agent_secret_use(
        bundle,
        _request(),
        grant=_grant(),
        active_policy_fingerprint=bundle.policy_fingerprint,
    )
    wrong_profile = evaluate_agent_secret_use(
        bundle,
        _request(profile_id="read-only"),
        grant=_grant(),
        active_policy_fingerprint=bundle.policy_fingerprint,
    )
    expired = evaluate_agent_secret_use(
        bundle,
        _request(),
        grant=_grant(expires_at="2000-01-01T00:00:00Z"),
        active_policy_fingerprint=bundle.policy_fingerprint,
    )
    unsupported_delivery = evaluate_agent_secret_use(
        bundle,
        _request(delivery_mode="literal_env"),
        grant=_grant(delivery_mode="literal_env"),
        active_policy_fingerprint=bundle.policy_fingerprint,
    )
    break_glass_without_review = evaluate_agent_secret_use(
        bundle,
        _request(delivery_mode="reviewed_literal_break_glass"),
        grant=_grant(delivery_mode="reviewed_literal_break_glass"),
        active_policy_fingerprint=bundle.policy_fingerprint,
    )

    assert allowed.allowed is True
    assert allowed.resolution["value_materialized"] is False
    assert allowed.resolution["delivery_mode"] == "brokered_operation"
    assert "ref_hash" in allowed.resolution
    assert wrong_profile.allowed is False
    assert wrong_profile.reason_code == "nomos.runtime.secret_grant_scope_mismatch"
    assert expired.allowed is False
    assert expired.reason_code == "nomos.runtime.grant_expired"
    assert unsupported_delivery.allowed is False
    assert unsupported_delivery.reason_code == (
        "nomos.runtime.secret_delivery_unsupported"
    )
    assert break_glass_without_review.allowed is False
    assert break_glass_without_review.reason_code == (
        "nomos.runtime.secret_break_glass_review_required"
    )

    serialized = json.dumps({
        "trace": allowed.trace,
        "resolution": allowed.resolution,
    }, sort_keys=True)
    assert SECRET_REF not in serialized
    assert CANARY not in serialized


def test_secret_ref_rejects_embedded_literal_payload() -> None:
    bundle = _policy()
    smuggled_refs = (
        f"{SECRET_REF}\n{CANARY}",
        f"{SECRET_REF}?token={CANARY}",
        f"secret://user:{CANARY}@okg/deploy/openai-token",
        f"{SECRET_REF}#{CANARY}",
    )

    for smuggled_ref in smuggled_refs:
        result = evaluate_agent_secret_use(
            bundle,
            _request(secret_ref=smuggled_ref),
            grant=_grant(secret_ref=smuggled_ref),
            active_policy_fingerprint=bundle.policy_fingerprint,
        )

        assert result.allowed is False
        assert result.reason_code == "nomos.runtime.secret_ref_required"
        assert result.resolution == {}
        serialized = json.dumps(result.trace, sort_keys=True)
        assert "secret_ref_valid" in serialized
        assert CANARY not in serialized


def test_secret_gate_suppresses_resolution_when_policy_denies() -> None:
    bundle = _deny_policy()

    result = evaluate_agent_secret_use(
        bundle,
        _request(),
        grant=_grant(),
        active_policy_fingerprint=bundle.policy_fingerprint,
    )

    assert result.allowed is False
    assert result.reason_code == "nomos.default_deny"
    assert result.resolution == {}


def test_secret_literal_detector_accepts_secret_refs() -> None:
    findings = detect_agent_secret_literals(
        {
            "api_key": {"secret_ref": SECRET_REF},
            "nested": {"dsn": "vault://okg/database"},
            "notes": "public docs only",
        },
        source="fixture",
    )

    assert findings == ()


# One row per entry, branch and class member of the gate's literal patterns
# (#2778). Values sit in a list, so no key name can classify them; each must
# be classified by exactly the entry named. Tails put every character kind
# their class admits within the pattern's minimum length, so dropping any
# member leaves too short a match. Each value is assembled at runtime so the
# source holds no credential-shaped literal.
_GH_TAIL = "a_B0" + "FAKEsentinelNOTAKEY012345678"  # [A-Za-z0-9_], 30+
_SLACK_TAIL = "aB-0" + "fakesentinel"  # [A-Za-z0-9-], 10+
_GATE_TOKEN_SHAPES = [
    ("pem-two-word-kind", "-----" + "BEGIN SSH2 ENCRYPTED " + "PRIVATE" + " KEY-----", "private_key"),
    ("dsn-postgres", "postgres" + "://db.example.test/okg", "dsn"),
    ("dsn-postgresql", "postgresql" + "://db.example.test/okg", "dsn"),
    ("dsn-mysql", "mysql" + "://db.example.test/okg", "dsn"),
    ("dsn-mongodb", "mongodb" + "://db.example.test/okg", "dsn"),
    ("dsn-redis", "redis" + "://cache.example.test/0", "dsn"),
    ("openai-style", "sk" + "-" + "a_b-fake0sentinel0notakey", "api_key"),
    ("bearer", "Bearer " + "a0._~+/-" + "fakesentinel", "bearer_token"),
    ("aws-long-term", "AK" + "IA" + "FAKE0SENTINEL012", "aws_access_key"),
    ("aws-session", "AS" + "IA" + "FAKE0SENTINEL012", "aws_access_key"),
    ("github-classic", "gh" + "p_" + _GH_TAIL, "github_token"),
    ("github-oauth", "gh" + "o_" + _GH_TAIL, "github_token"),
    ("github-user", "gh" + "u_" + _GH_TAIL, "github_token"),
    ("github-server", "gh" + "s_" + _GH_TAIL, "github_token"),
    ("github-refresh", "gh" + "r_" + _GH_TAIL, "github_token"),
    ("github-fine-grained", "github" + "_pat_" + "a_B0" + "FAKEsentinel0NOTAKEY", "github_token"),
    ("slack-app", "xo" + "xa-" + _SLACK_TAIL, "slack_token"),
    ("slack-bot", "xo" + "xb-" + _SLACK_TAIL, "slack_token"),
    ("slack-user", "xo" + "xp-" + _SLACK_TAIL, "slack_token"),
    ("slack-refresh", "xo" + "xr-" + _SLACK_TAIL, "slack_token"),
    ("slack-session", "xo" + "xs-" + _SLACK_TAIL, "slack_token"),
    ("assign-api-underscore", "api_key" + "=fake-value", "assignment"),
    ("assign-api-hyphen", "api-key" + "=fake-value", "assignment"),
    ("assign-token-colon", "token" + ": fake-value", "assignment"),
    ("assign-secret", "secret" + "=fake-value", "assignment"),
    ("assign-password", "password" + "=fake-value", "assignment"),
    ("assign-cookie", "cookie" + "=fake-value", "assignment"),
    ("assign-access-key-id-underscore", "access_key_id" + "=fake-value", "assignment"),
    ("assign-access-key-id-hyphen", "access-key-id" + "=fake-value", "assignment"),
    ("assign-secret-access-key-underscore", "secret_access_key" + "=fake-value", "assignment"),
    ("assign-secret-access-key-mixed", "secret_access-key" + "=fake-value", "assignment"),
    ("credential-url", "https" + "://user:fake-pw@example.test/repo", "credential_url"),
    # Schemes that END in each character kind the scheme class admits: if the
    # member is dropped, the search cannot restart later in the scheme.
    ("credential-url-scheme-letter", "xy" + "://user:fake-pw@example.test", "credential_url"),
    ("credential-url-scheme-digit", "x0" + "://user:fake-pw@example.test", "credential_url"),
    ("credential-url-scheme-plus", "x+" + "://user:fake-pw@example.test", "credential_url"),
    ("credential-url-scheme-dot", "x." + "://user:fake-pw@example.test", "credential_url"),
    ("credential-url-scheme-hyphen", "x-" + "://user:fake-pw@example.test", "credential_url"),
    # Empty user name, token in the password slot (#2812). The user part may
    # be empty; the password part may itself start with a colon.
    ("credential-url-empty-user-https-repo", "https" + "://:fake-token-0sentinel@github.com/o/r", "credential_url"),
    ("credential-url-empty-user-https", "https" + "://:fake-pw@example.test", "credential_url"),
    ("credential-url-empty-user-amqp", "amqp" + "://:fake-pw@mq", "credential_url"),
    ("credential-url-empty-user-rediss", "rediss" + "://:fake-pw@cache", "credential_url"),
    ("credential-url-empty-user-ftp", "ftp" + "://:fake-pw@h", "credential_url"),
    ("credential-url-empty-user-double-colon", "x" + "://::fake-pw@example.test", "credential_url"),
    # Upper-case scheme: the pattern is case-insensitive.
    ("credential-url-upper-scheme", "HTTPS" + "://u:fake-pw@example.test", "credential_url"),
    ("credential-url-upper-scheme-empty-user", "HTTPS" + "://:fake-pw@example.test", "credential_url"),
]


@pytest.mark.parametrize(
    ("value", "literal_class"),
    [(value, literal_class) for _, value, literal_class in _GATE_TOKEN_SHAPES],
    ids=[name for name, _, _ in _GATE_TOKEN_SHAPES],
)
def test_secret_literal_detector_classifies_each_token_shape(
    value: str,
    literal_class: str,
) -> None:
    findings = detect_agent_secret_literals(["tool", value], source="argv")

    assert value not in json.dumps(findings, sort_keys=True)
    assert [finding["class"] for finding in findings] == [literal_class]


# Text that holds `://`, `:` and `@` but no credential. The credential-URL
# pattern excludes `/`, whitespace and `@` from the user part and whitespace
# from the password part; each row is one that pattern must not flag, and
# dropping the named exclusion would flag it.
_NOT_CREDENTIAL_URLS = [
    ("colon-and-at-in-path", "https" + "://example.test/users:list@v2"),
    ("prose-after-url", "https" + "://example.test and see a:b@c"),
    ("port-then-mail-address", "https" + "://example.test:8080 is down, mail ops@example.test"),
    ("scp-style-remote", "https" + "://git@example.test:org/repo@v1"),
    # No password slot: a user name alone, empty user info, or no `://`
    # authority. Allowing an empty user name must not flag these.
    ("user-without-password", "https" + "://u@h"),
    ("plain-host-path", "https" + "://example.test/path"),
    ("empty-userinfo", "https" + "://@example.test"),
    ("empty-user-empty-password", "https" + "://:@example.test"),
    ("mailto", "mailto" + ":ops@example.test"),
    ("scp-remote-no-scheme", "git" + "@github.com:o/r"),
    ("port-no-userinfo", "https" + "://example.test:8443/v1"),
]


@pytest.mark.parametrize(
    "value",
    [value for _, value in _NOT_CREDENTIAL_URLS],
    ids=[name for name, _ in _NOT_CREDENTIAL_URLS],
)
def test_secret_literal_detector_ignores_urls_without_userinfo(value: str) -> None:
    assert detect_agent_secret_literals(["tool", value], source="argv") == ()
