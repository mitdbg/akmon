"""Absence is not evidence (issue #1007).

Three independent absences in `normalize_identity_claim` used to be read as
satisfaction: an omitted `verification_status`, an absent `auth_time` under a
declared freshness requirement, and an omitted evaluation clock. Each arm is
exercised here on its OWN claim and asserted by its EXACT resulting status,
so one over-broad denial cannot make the other arms pass vacuously. Every
fail-closed assertion is paired with a positive control that supplies the
missing evidence and still authorizes.
"""
from __future__ import annotations

import ast
import re
from dataclasses import replace
from pathlib import Path

import pytest

from okg.substrate.nomos import (
    IdentityProvider,
    NomosPolicyError,
    PermissionGrant,
    SubjectBinding,
    compile_policy_graph,
    evaluate_identity_claim,
    normalize_identity_claim,
)
from okg.substrate.nomos.identity import (
    IDENTITY_CLAIM_STATUSES,
    UNVERIFIED_CLAIM_STATUS,
    VALID_CLAIM_STATUS,
    _is_stale_auth_time,
)

ISSUER = "https://auth.example.edu/realms/okg"
AUDIENCE = "okg-gateway"
SCOPE = "deployment:cms"
NOW = "2026-05-25T12:00:00Z"

# Fixed absolute bounds so "past" and "future" stay past and future when the
# claim is normalized against the real wall clock.
LONG_PAST = "2000-01-01T00:00:00Z"
FAR_FUTURE = "2999-01-01T00:00:00Z"


def _provider(**overrides) -> IdentityProvider:
    """A provider that declares no extra requirements unless asked to.

    Each test turns on exactly the one requirement it is about, so a claim
    can only fail for the reason under test.
    """
    fields = {
        "provider_id": "example-sso",
        "provider_type": "oidc",
        "issuer": ISSUER,
        "audiences": (AUDIENCE,),
        "group_claims_approved": True,
        "status": "active",
    }
    fields.update(overrides)
    return IdentityProvider(**fields)


def _raw_claim(**overrides) -> dict:
    """A claim that satisfies every check the base provider makes."""
    raw = {
        "provider_id": "example-sso",
        "issuer": ISSUER,
        "claims": {
            "sub": "user-123",
            "aud": AUDIENCE,
            "groups": ["cms-comp-ops"],
        },
    }
    raw.update(overrides)
    return raw


def _policy() -> dict:
    return {
        "policy_bundle_id": "nomos:test:absence",
        "policy_version": "v1",
        "default_effect": "deny",
        "rules": [
            {
                "rule_id": "allow_operator_record",
                "effect": "allow",
                "priority": 100,
                "subjects": ["subject:cms:operator"],
                "actions": ["mcp.record"],
                "match": {"deployment": "cms", "scope": SCOPE},
                "reason_code": "nomos.test.allowed",
            }
        ],
    }


def _binding(**overrides) -> SubjectBinding:
    fields = {
        "binding_id": "binding:operator",
        "provider_id": "example-sso",
        "issuer": ISSUER,
        "claim_path": "groups",
        "claim_value": "cms-comp-ops",
        "subject": "subject:cms:operator",
        "scope": SCOPE,
    }
    fields.update(overrides)
    return SubjectBinding(**fields)


def _grant() -> PermissionGrant:
    return PermissionGrant(
        grant_id="grant:operator-record",
        subject="subject:cms:operator",
        actions=("mcp.record",),
        scope=SCOPE,
    )


def _evaluate(claim, *, bindings=None, now=None):
    return evaluate_identity_claim(
        compile_policy_graph(_policy()),
        claim,
        bindings=[_binding()] if bindings is None else bindings,
        permission_grants=[_grant()],
        action="mcp.record",
        resource={"deployment": "cms", "scope": SCOPE},
        scope=SCOPE,
        now=now,
    )


class TestOmittedVerificationStatus:
    """A claim that asserts nothing about verification is not verified."""

    def test_omitted_status_normalizes_to_unverified(self) -> None:
        claim = normalize_identity_claim(_raw_claim(), [_provider()], now=NOW)

        assert claim.verification_status == UNVERIFIED_CLAIM_STATUS
        assert claim.is_valid is False

    def test_empty_status_normalizes_to_unverified(self) -> None:
        claim = normalize_identity_claim(
            _raw_claim(verification_status=""),
            [_provider()],
            now=NOW,
        )

        assert claim.verification_status == UNVERIFIED_CLAIM_STATUS
        assert claim.is_valid is False

    def test_explicit_valid_still_authorizes(self) -> None:
        """Positive control: stating the verification keeps the old result."""
        claim = normalize_identity_claim(
            _raw_claim(verification_status=VALID_CLAIM_STATUS),
            [_provider()],
            now=NOW,
        )

        assert claim.verification_status == VALID_CLAIM_STATUS
        assert claim.is_valid is True
        assert _evaluate(claim, now=NOW).decision.effect == "allow"

    def test_unverified_claim_denies_with_its_own_reason_code(self) -> None:
        claim = normalize_identity_claim(_raw_claim(), [_provider()], now=NOW)
        result = _evaluate(claim, now=NOW)

        assert result.decision.effect == "deny"
        assert result.decision.reason_code == "nomos.identity.unverified"
        assert result.resolved_subject is None

    def test_default_envelope_status_is_unverified(self) -> None:
        """A directly constructed envelope does not assert verification."""
        claim = normalize_identity_claim(
            _raw_claim(verification_status=VALID_CLAIM_STATUS),
            [_provider()],
            now=NOW,
        )

        assert replace(
            claim,
            verification_status=UNVERIFIED_CLAIM_STATUS,
        ).is_valid is False
        assert type(claim).verification_status == UNVERIFIED_CLAIM_STATUS

    @pytest.mark.parametrize(
        ("provider_status", "expected_status"),
        [
            ("revoked", "provider_revoked"),
            ("disabled", "provider_disabled"),
            ("rotating", "provider_rotating"),
        ],
    )
    def test_specific_provider_failure_still_overrides_unverified(
        self,
        provider_status: str,
        expected_status: str,
    ) -> None:
        """Operators keep the most informative cause, not a blanket label."""
        claim = normalize_identity_claim(
            _raw_claim(),
            [_provider(status=provider_status)],
            now=NOW,
        )

        assert claim.verification_status == expected_status
        assert claim.is_valid is False

    @pytest.mark.parametrize(
        ("provider_status", "expected_status"),
        [
            ("revoked", "provider_revoked"),
            ("disabled", "provider_disabled"),
        ],
    )
    def test_overridden_status_still_records_that_nobody_vouched(
        self,
        provider_status: str,
        expected_status: str,
    ) -> None:
        """The specific cause wins, but the absence is not lost."""
        claim = normalize_identity_claim(
            _raw_claim(),
            [_provider(status=provider_status)],
            now=NOW,
        )

        assert claim.verification_status == expected_status
        assert claim.verification_error == UNVERIFIED_CLAIM_STATUS

    def test_asserted_claim_records_no_absence(self) -> None:
        """Positive control: a claim that vouched is not flagged as absent."""
        claim = normalize_identity_claim(
            _raw_claim(verification_status=VALID_CLAIM_STATUS),
            [_provider(status="revoked")],
            now=NOW,
        )

        assert claim.verification_status == "provider_revoked"
        assert claim.verification_error is None

    def test_caller_cannot_suppress_the_absence_marker(self) -> None:
        """`verification_error` is Nomos's finding, not caller input.

        If it were read from the claim, a caller could overwrite the
        "nobody vouched for this" marker and land an unvalidated string
        in the audit row and the graph.
        """
        claim = normalize_identity_claim(
            _raw_claim(verification_error="attacker-chosen"),
            [_provider(status="revoked")],
            now=NOW,
        )

        assert claim.verification_status == "provider_revoked"
        assert claim.verification_error == UNVERIFIED_CLAIM_STATUS

    def test_unrecognized_status_is_rejected_not_carried(self) -> None:
        """An unknown status must not reach the reason code or the graph."""
        with pytest.raises(NomosPolicyError, match="verification_status"):
            normalize_identity_claim(
                _raw_claim(verification_status="totally-made-up"),
                [_provider()],
                now=NOW,
            )

    def test_every_known_status_is_accepted(self) -> None:
        """Positive control: the allowlist covers the real vocabulary."""
        for status in sorted(IDENTITY_CLAIM_STATUSES):
            claim = normalize_identity_claim(
                _raw_claim(verification_status=status),
                [_provider()],
                now=NOW,
            )
            assert claim.verification_status

    def test_allowlist_covers_every_status_the_cascade_can_produce(self) -> None:
        """The vocabulary and the code that assigns it cannot drift apart."""
        source = Path(
            normalize_identity_claim.__code__.co_filename
        ).read_text(encoding="utf-8")
        assigned = set(
            re.findall(r'verification_status = "([a-z_]+)"', source)
        )

        assert assigned, "no literal status assignments found"
        assert assigned <= IDENTITY_CLAIM_STATUSES, (
            "statuses assigned in identity.py but missing from the "
            f"allowlist: {sorted(assigned - IDENTITY_CLAIM_STATUSES)}"
        )

    def test_no_cascade_arm_can_promote_absent_evidence_to_valid(self) -> None:
        """Algebraic check over the enumerated provider/claim inputs.

        The cascade only ever REPLACES the incoming status. This asserts
        that over every combination the fixtures can produce, an omitted
        verification status never comes out as `valid`.
        """
        providers = [
            _provider(),
            _provider(status="revoked"),
            _provider(status="disabled"),
            _provider(status="rotating"),
            _provider(active=False, status=None),
            _provider(mfa_required=True),
            _provider(required_assurance=("mfa",)),
            _provider(freshness_seconds=3600),
            _provider(metadata={"require_verified_by": True}),
            _provider(issuer="https://elsewhere.example.edu"),
        ]
        raw_variants = [
            _raw_claim(),
            _raw_claim(claims={"sub": "user-123", "aud": AUDIENCE, "acr": "mfa"}),
            _raw_claim(auth_time=NOW),
            _raw_claim(verified_by="verifier:gateway", verified_at=NOW),
            _raw_claim(claims={"sub": "user-123", "aud": "wrong-audience"}),
        ]

        for provider in providers:
            for raw in raw_variants:
                claim = normalize_identity_claim(raw, [provider], now=NOW)
                assert claim.verification_status != VALID_CLAIM_STATUS, (
                    provider.provider_id,
                    provider.status_value,
                    raw,
                )
                assert claim.is_valid is False


class TestDeclaredFreshness:
    """A declared freshness requirement is not satisfiable by silence."""

    def test_missing_auth_time_fails_a_declared_requirement(self) -> None:
        claim = normalize_identity_claim(
            _raw_claim(verification_status=VALID_CLAIM_STATUS),
            [_provider(freshness_seconds=3600)],
            now=NOW,
        )

        assert claim.verification_status == "missing_auth_time"
        assert claim.is_valid is False

    def test_missing_auth_time_denies_with_its_own_reason_code(self) -> None:
        claim = normalize_identity_claim(
            _raw_claim(verification_status=VALID_CLAIM_STATUS),
            [_provider(freshness_seconds=3600)],
            now=NOW,
        )
        result = _evaluate(claim, now=NOW)

        assert result.decision.effect == "deny"
        assert result.decision.reason_code == "nomos.identity.missing_auth_time"

    def test_no_declared_requirement_is_unaffected(self) -> None:
        """Positive control: absence fails only against a stated requirement."""
        claim = normalize_identity_claim(
            _raw_claim(verification_status=VALID_CLAIM_STATUS),
            [_provider(freshness_seconds=None)],
            now=NOW,
        )

        assert claim.verification_status == VALID_CLAIM_STATUS
        assert claim.is_valid is True

    def test_present_but_old_auth_time_is_still_stale(self) -> None:
        claim = normalize_identity_claim(
            _raw_claim(
                verification_status=VALID_CLAIM_STATUS,
                auth_time="2026-05-25T10:00:00Z",
            ),
            [_provider(freshness_seconds=3600)],
            now=NOW,
        )

        assert claim.verification_status == "stale_claim"

    def test_present_and_fresh_auth_time_authorizes(self) -> None:
        """Positive control."""
        claim = normalize_identity_claim(
            _raw_claim(
                verification_status=VALID_CLAIM_STATUS,
                auth_time="2026-05-25T11:59:00Z",
            ),
            [_provider(freshness_seconds=3600)],
            now=NOW,
        )

        assert claim.verification_status == VALID_CLAIM_STATUS
        assert _evaluate(claim, now=NOW).decision.effect == "allow"

    def test_staleness_predicate_fails_closed_on_absent_timestamp(self) -> None:
        assert _is_stale_auth_time(None, NOW, 60) is True
        assert _is_stale_auth_time("", NOW, 60) is True
        # No requirement declared, so there is nothing absence could fail.
        assert _is_stale_auth_time(None, NOW, None) is False
        assert _is_stale_auth_time("2026-05-25T11:59:30Z", NOW, 60) is False


class TestEvaluationClock:
    """An omitted clock resolves to real time instead of skipping checks."""

    def test_expired_claim_is_caught_without_a_supplied_clock(self) -> None:
        claim = normalize_identity_claim(
            _raw_claim(
                verification_status=VALID_CLAIM_STATUS,
                expires_at=LONG_PAST,
            ),
            [_provider()],
        )

        assert claim.verification_status == "expired"
        assert claim.is_valid is False

    def test_not_yet_valid_claim_is_caught_without_a_supplied_clock(self) -> None:
        claim = normalize_identity_claim(
            _raw_claim(
                verification_status=VALID_CLAIM_STATUS,
                not_before=FAR_FUTURE,
            ),
            [_provider()],
        )

        assert claim.verification_status == "not_yet_valid"
        assert claim.is_valid is False

    def test_stale_claim_is_caught_without_a_supplied_clock(self) -> None:
        claim = normalize_identity_claim(
            _raw_claim(
                verification_status=VALID_CLAIM_STATUS,
                auth_time=LONG_PAST,
            ),
            [_provider(freshness_seconds=3600)],
        )

        assert claim.verification_status == "stale_claim"
        assert claim.is_valid is False

    @pytest.mark.parametrize(
        "overrides",
        [
            {"expires_at": "2026-05-25T13:00:00Z"},
            {"not_before": "2026-05-25T11:00:00Z"},
        ],
    )
    def test_supplied_clock_still_governs(self, overrides: dict) -> None:
        """Positive control: an explicit clock keeps deterministic results."""
        claim = normalize_identity_claim(
            _raw_claim(verification_status=VALID_CLAIM_STATUS, **overrides),
            [_provider()],
            now=NOW,
        )

        assert claim.verification_status == VALID_CLAIM_STATUS
        assert claim.is_valid is True

    def test_closed_binding_window_is_not_selected_without_a_clock(self) -> None:
        """The resolved clock reaches binding selection, not just the claim."""
        claim = normalize_identity_claim(
            _raw_claim(verification_status=VALID_CLAIM_STATUS),
            [_provider()],
        )
        result = _evaluate(
            claim,
            bindings=[_binding(effective_until=LONG_PAST)],
        )

        assert result.decision.effect == "deny"
        assert result.decision.reason_code == "nomos.identity.no_subject_binding"
        assert result.resolved_subject is None

    def test_open_binding_window_is_selected_without_a_clock(self) -> None:
        """Positive control."""
        claim = normalize_identity_claim(
            _raw_claim(verification_status=VALID_CLAIM_STATUS),
            [_provider()],
        )
        result = _evaluate(
            claim,
            bindings=[_binding(effective_until=FAR_FUTURE)],
        )

        assert result.decision.effect == "allow"
        assert result.resolved_subject == "subject:cms:operator"


class TestDocumentedExample:
    """The runbook example must actually authorize, end to end.

    `docs/runbooks/security/nomos-iam.md` declares a provider with `freshness_seconds`,
    so the documented claim needs both a verification status and an
    `auth_time`. Grepping the doc for those keys would pass while the
    example stayed broken, so the example is reproduced and RUN -- through
    binding and grant resolution, not just normalization, because the
    documented binding matches on a group claim and group claims are
    dropped unless the provider approves them.
    """

    def test_runbook_flow_authorizes(self) -> None:
        provider = IdentityProvider(
            provider_id="example-sso",
            provider_type="oidc",
            issuer="https://sso.example.org/realms/example",
            audiences=("akmon-research",),
            key_reference="secret://example-sso/jwks",
            freshness_seconds=3600,
            group_claims_approved=True,
        )
        claim = normalize_identity_claim(
            {
                "provider_id": "example-sso",
                "verification_status": "valid",
                "issuer": provider.issuer,
                "raw_token": "<bearer token used only for fingerprinting>",
                "claims": {
                    "sub": "example-user-123",
                    "aud": "akmon-research",
                    "groups": ["research-ops"],
                    "acr": "mfa",
                    "exp": "2026-05-25T13:00:00Z",
                    "auth_time": "2026-05-25T11:59:00Z",
                },
            },
            [provider],
            now="2026-05-25T12:00:00Z",
        )

        assert claim.verification_status == VALID_CLAIM_STATUS
        assert claim.is_valid is True
        assert claim.verification_error is None

        scope = "deployment:example-research"
        result = evaluate_identity_claim(
            compile_policy_graph({
                "policy_bundle_id": "nomos:test:runbook",
                "policy_version": "v1",
                "default_effect": "deny",
                "rules": [
                    {
                        "rule_id": "allow_operator_record",
                        "effect": "allow",
                        "priority": 100,
                        "subjects": ["subject:example-research:operator"],
                        "actions": ["mcp.record"],
                        "match": {
                            "deployment": "example-research",
                            "scope": scope,
                        },
                        "reason_code": "nomos.test.runbook_allowed",
                    }
                ],
            }),
            claim,
            bindings=[
                SubjectBinding(
                    binding_id="binding:example:research-operator",
                    provider_id="example-sso",
                    issuer=provider.issuer,
                    claim_path="groups",
                    claim_value="research-ops",
                    subject="subject:example-research:operator",
                    scope=scope,
                )
            ],
            permission_grants=[
                PermissionGrant(
                    grant_id="grant:example-research:operator-record",
                    subject="subject:example-research:operator",
                    actions=("mcp.record",),
                    scope=scope,
                )
            ],
            action="mcp.record",
            resource={"deployment": "example-research", "surface": "mcp"},
            scope=scope,
            now="2026-05-25T12:00:00Z",
        )

        assert result.decision.effect == "allow"
        assert result.resolved_subject == "subject:example-research:operator"

    @pytest.mark.parametrize("doc_path", ["docs/runbooks/security/nomos-iam.md"])
    def test_no_runbook_claim_relies_on_the_removed_default(
        self,
        doc_path: str,
    ) -> None:
        """Every claim example in the Nomos runbooks states its status.

        Slicing one known example would leave the others free to drift, so
        this finds every block that looks like a claim and checks each.
        """
        doc = (_repo_root() / doc_path).read_text(encoding="utf-8")

        blocks = [
            block
            for block in re.split(r"\n(?=\s*[{\w])", doc)
            if '"provider_id"' in block and '"claims"' in block
        ]

        assert blocks, f"no claim examples found in {doc_path}"
        for block in blocks:
            assert '"verification_status"' in block, (
                f"{doc_path} shows a claim that says nothing about "
                f"verification, so it would be denied:\n{block[:400]}"
            )

    def test_runbook_provider_approves_the_group_it_binds_on(self) -> None:
        """The documented provider must support the documented binding."""
        doc = (
            _repo_root() / "docs" / "runbooks" / "security" / "nomos-iam.md"
        ).read_text(encoding="utf-8")
        provider_block = doc[doc.index("provider = IdentityProvider("):]
        provider_block = provider_block[
            : provider_block.index("claim = normalize_identity_claim(")
        ]

        assert "group_claims_approved=True" in provider_block


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[3]


def _raw_claim_literals(root: Path) -> list[tuple[Path, int, set[str]]]:
    """Every dict literal that is shaped like an outbound raw identity claim.

    A raw claim has a `provider_id` and a nested `claims` object. Provider
    serializations, DB row mappings, and CLI payloads carry `provider_id`
    without `claims`, so requiring both keeps them out. A claim's own
    fingerprint-source dict repeats both keys, so literals nested inside
    another matching literal are dropped too -- only the outermost one is
    the claim that reaches `normalize_identity_claim`.
    """
    found: list[tuple[Path, int, set[str]]] = []
    for path in sorted(root.rglob("*.py")):
        try:
            tree = ast.parse(path.read_text(encoding="utf-8"))
        except (SyntaxError, UnicodeDecodeError):
            continue
        matches = [
            node
            for node in ast.walk(tree)
            if isinstance(node, ast.Dict)
            and {"provider_id", "claims"}
            <= {
                key.value
                for key in node.keys
                if isinstance(key, ast.Constant) and isinstance(key.value, str)
            }
        ]
        # A claim's own fingerprint-source repeats both keys, so keep only
        # the outermost literal. Membership is decided by walking each
        # match's subtree rather than comparing line spans -- two claims
        # written on one line have identical spans and would cancel out.
        nested_ids = {
            id(child)
            for node in matches
            for child in ast.walk(node)
            if child is not node
        }
        for node in matches:
            if id(node) in nested_ids:
                continue
            found.append((
                path,
                node.lineno,
                {
                    key.value
                    for key in node.keys
                    if isinstance(key, ast.Constant)
                    and isinstance(key.value, str)
                },
            ))
    return found


class TestCallerAudit:
    """No shipped code may rely on the removed fail-open default.

    This is the mechanical half of the caller audit: a new claim builder
    that forgets to state its verification evidence fails here instead of
    silently denying at runtime.
    """

    @pytest.mark.parametrize("subtree", ["src", "deployments"])
    def test_shipped_claim_builders_state_their_verification(
        self,
        subtree: str,
    ) -> None:
        root = _repo_root() / subtree
        if not root.exists():  # pragma: no cover - layout guard
            pytest.skip(f"{subtree} not present")

        literals = _raw_claim_literals(root)
        silent = [
            f"{path.relative_to(_repo_root())}:{lineno}"
            for path, lineno, keys in literals
            if "verification_status" not in keys
        ]

        assert literals, f"no raw claim literals found under {subtree}/"
        assert not silent, (
            "these shipped raw identity claims say nothing about verification, "
            "so they now normalize to 'unverified' and are denied: "
            + ", ".join(silent)
        )
