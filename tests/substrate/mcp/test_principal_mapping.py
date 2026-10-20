"""PACT ``mcp-principal-mapping`` — per-user principals on the MCP surface.

Two layers, on purpose:

* **Hermetic** tests (no mark) exercise the declaration, the claim builder,
  the refusal vocabulary, and the start-time refusals. They need no database
  and run in the fast lane.
* **Postgres** tests (``postgres`` + ``serial_db``) prove the end-to-end
  behavior the requirements are actually about -- two principals, one server
  process, different answers, and refusal rows in ``okg.nomos_decisions``.

A note on what is NOT asserted anywhere here: decision ROW COUNTS. Decision
identity is content-addressed and inserted ``ON CONFLICT DO NOTHING``, so a
repeat identical call persists nothing and one call over many rows persists
many. Row content is the contract; arithmetic over it is noise.
"""

from __future__ import annotations

import contextlib
import os
import statistics
import time
from dataclasses import replace
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Iterator, Mapping

import psycopg
import pytest

from okg.substrate.errors import SubstrateError
from okg.substrate.graph_store import ChronosGraphStore
from okg.substrate.library.sources.base import EdgeFact, NodeFact
from okg.substrate.mcp.principal_mapping import (
    CONFLICTING_IDENTITY_CHANNEL_REASON,
    MCP_PRINCIPAL_HEADER_PROVIDER_TYPE,
    TRANSPORT_VERIFIER_PREFIX,
    UNIDENTIFIED_PRINCIPAL_REASON,
    UNIDENTIFIED_SUBJECT,
    PrincipalMapping,
    header_claim_for_request,
    parse_principal_mapping,
    principal_mapping_from_manifest,
    refusal_reason_code,
    require_explicit_verification_status,
    validate_mapping_against_providers,
)
from okg.substrate.mcp.effective_surface import InspectKindNotServed
from okg.substrate.mcp.server import build_server, run_server
from okg.substrate.mcp.session import Session
from okg.substrate.nomos import (
    IdentityProvider,
    PermissionGrant,
    SubjectBinding,
    activate_policy_bundle,
    compile_policy_graph,
    upsert_identity_provider,
    upsert_permission_grant,
    upsert_subject_binding,
)
from okg.substrate.nomos.read_masking import REDACTION_MARKER
from okg.substrate.publisher import Publisher
from tests.substrate._db_helpers import seed_catalog_version
from tests.pytest.catalog_fixture_builders import bind_test_catalog_writer  # noqa: E402


DEPLOYMENT = "default"
SCOPE = f"deployment:{DEPLOYMENT}"
PROVIDER_ID = "okg-chat-sso"
ISSUER = "https://chat.example.edu/"
AUDIENCE = "okg-mcp"
PRINCIPAL_HEADER = "X-OKG-Principal"
TRANSPORT_CLIENT_ID = "okg-mcp-static-bearer"
TRANSPORT_VERIFIER = f"{TRANSPORT_VERIFIER_PREFIX}{TRANSPORT_CLIENT_ID}"

PRINCIPAL_A = "alice@example.edu"
PRINCIPAL_B = "bob@example.edu"
SUBJECT_A = "subject:chat:alice"
SUBJECT_B = "subject:chat:bob"

MAPPING = PrincipalMapping(
    provider_id=PROVIDER_ID,
    issuer=ISSUER,
    audience=AUDIENCE,
    subject_claim="email",
    subject_header=PRINCIPAL_HEADER,
    principal_kind="mcp_client",
)


def _provider(
    *,
    metadata: Mapping[str, Any] | None = None,
    provider_id: str = PROVIDER_ID,
) -> IdentityProvider:
    """The header provider under test.

    ``metadata.trusted_verifiers`` is what makes the machinery actually CHECK
    the ``verified_by`` the server stamps; a provider without it is refused at
    load, which is one of the requirements below.
    """
    return IdentityProvider(
        provider_id=provider_id,
        provider_type=MCP_PRINCIPAL_HEADER_PROVIDER_TYPE,
        issuer=ISSUER,
        audiences=(AUDIENCE,),
        subject_claim="email",
        group_claims=(),
        role_claims=(),
        entitlement_claims=(),
        assurance_claim=None,
        freshness_seconds=3600,
        metadata=dict(
            metadata
            if metadata is not None
            else {
                "trusted_verifiers": [TRANSPORT_VERIFIER],
                "max_verifier_age_seconds": 3600,
            }
        ),
    )


# --------------------------------------------------------------------------
# Request-context plumbing: the tests drive the SAME contextvar the SDK sets,
# so the accessor under test is the production one, not a test seam.
# --------------------------------------------------------------------------


def _starlette_request(
    headers: Mapping[str, str],
    *,
    client_id: str | None = TRANSPORT_CLIENT_ID,
) -> Any:
    from mcp.server.auth.middleware.bearer_auth import AuthenticatedUser
    from mcp.server.auth.provider import AccessToken
    from starlette.requests import Request

    scope: dict[str, Any] = {
        "type": "http",
        "http_version": "1.1",
        "method": "POST",
        "scheme": "http",
        "path": "/mcp",
        "raw_path": b"/mcp",
        "query_string": b"",
        "root_path": "",
        "server": ("127.0.0.1", 8100),
        "client": ("127.0.0.1", 54321),
        "headers": [
            (str(key).lower().encode("latin-1"), str(value).encode("latin-1"))
            for key, value in headers.items()
        ],
    }
    if client_id is not None:
        scope["user"] = AuthenticatedUser(
            AccessToken(token="accepted", client_id=client_id, scopes=[])
        )
    return Request(scope)


@contextlib.contextmanager
def request_headers(
    headers: Mapping[str, str],
    *,
    client_id: str | None = TRANSPORT_CLIENT_ID,
) -> Iterator[None]:
    """Install one HTTP request as the in-flight MCP request."""
    from mcp.server.lowlevel.server import request_ctx
    from mcp.shared.context import RequestContext

    token = request_ctx.set(
        RequestContext(
            request_id=1,
            meta=None,
            session=None,
            lifespan_context=None,
            request=_starlette_request(headers, client_id=client_id),
        )
    )
    try:
        yield
    finally:
        request_ctx.reset(token)


# ==========================================================================
# Declaration and claim construction (hermetic)
# ==========================================================================


def test_declared_header_maps_to_nomos_subject_per_request() -> None:
    """The declared header, and only it, supplies the claim's subject."""
    claim = header_claim_for_request(
        MAPPING,
        {PRINCIPAL_HEADER: PRINCIPAL_A},
        transport_identity=TRANSPORT_VERIFIER,
    )
    assert claim["provider_id"] == PROVIDER_ID
    assert claim["issuer"] == ISSUER
    assert claim["audiences"] == [AUDIENCE]
    assert claim["external_subject"] == PRINCIPAL_A
    assert claim["claims"] == {"email": PRINCIPAL_A}

    # Positive control on the difference: a different header value produces a
    # different claim, so the mapping is reading the header rather than a
    # constant.
    other = header_claim_for_request(
        MAPPING,
        {PRINCIPAL_HEADER: PRINCIPAL_B},
        transport_identity=TRANSPORT_VERIFIER,
    )
    assert other["external_subject"] == PRINCIPAL_B
    assert other["external_subject"] != claim["external_subject"]


def test_undeclared_header_is_ignored_not_treated_as_identity() -> None:
    """A header the mapping never declared is not identity."""
    claim = header_claim_for_request(
        MAPPING,
        {
            PRINCIPAL_HEADER: PRINCIPAL_A,
            # Every one of these is a plausible identity header. None is
            # declared, so none may influence the claim.
            "X-Forwarded-User": PRINCIPAL_B,
            "X-Auth-Request-Email": PRINCIPAL_B,
            "Remote-User": PRINCIPAL_B,
            # Nor may a caller assert its own verification evidence.
            "X-OKG-Verified-By": "mcp-transport:attacker",
            "Verified-By": "mcp-transport:attacker",
        },
        transport_identity=TRANSPORT_VERIFIER,
    )
    assert claim["external_subject"] == PRINCIPAL_A
    assert claim["claims"] == {"email": PRINCIPAL_A}
    assert claim["verified_by"] == TRANSPORT_VERIFIER
    assert PRINCIPAL_B not in str(claim)


def test_extra_declared_claim_header_is_read_only_when_declared() -> None:
    mapping = PrincipalMapping(
        provider_id=PROVIDER_ID,
        issuer=ISSUER,
        audience=AUDIENCE,
        subject_claim="email",
        subject_header=PRINCIPAL_HEADER,
        principal_kind="mcp_client",
        extra_claim_headers={"department": "X-OKG-Department"},
    )
    claim = header_claim_for_request(
        mapping,
        {
            PRINCIPAL_HEADER: PRINCIPAL_A,
            "X-OKG-Department": "physics",
            "X-OKG-Undeclared": "chemistry",
        },
        transport_identity=TRANSPORT_VERIFIER,
    )
    assert claim["claims"] == {"email": PRINCIPAL_A, "department": "physics"}


def test_server_stamps_verified_by_from_transport_identity() -> None:
    """The delegation is recorded: who asserted the principal, and when."""
    claim = header_claim_for_request(
        MAPPING,
        {PRINCIPAL_HEADER: PRINCIPAL_A},
        transport_identity=TRANSPORT_VERIFIER,
    )
    assert claim["verified_by"] == TRANSPORT_VERIFIER
    assert claim["verified_at"]
    assert claim["auth_time"] == claim["verified_at"]
    assert claim["verification_method"] == "mcp_transport_delegated_header"
    # The stamp is the TRANSPORT's identity, not the principal's: the audit
    # record must read "app-token holder X asserted principal P".
    assert claim["verified_by"] != PRINCIPAL_A

    # Absence arm: with no authenticated transport identity there is no
    # delegator to record, so the claim is refused rather than stamped blank.
    with pytest.raises(SubstrateError) as excinfo:
        header_claim_for_request(
            MAPPING,
            {PRINCIPAL_HEADER: PRINCIPAL_A},
            transport_identity=None,
        )
    assert refusal_reason_code(excinfo.value) == UNIDENTIFIED_PRINCIPAL_REASON


def test_header_claim_without_verification_status_is_refused() -> None:
    """An unstated verification is refused, never defaulted to valid.

    ``normalize_identity_claim`` defaults an omitted ``verification_status``
    to ``valid`` (identity.py:488) -- an absent signal that reads as a
    satisfied one. This path refuses first. The same reasoning covers the
    verifier stamps, so all three absence arms are checked here rather than
    relying on the general fix tracked as issue #1007.
    """
    good = header_claim_for_request(
        MAPPING,
        {PRINCIPAL_HEADER: PRINCIPAL_A},
        transport_identity=TRANSPORT_VERIFIER,
    )
    # Positive control: the claim the server actually builds passes.
    require_explicit_verification_status(good)

    for absent_key in ("verification_status", "verified_by", "verified_at"):
        stripped = {
            key: value for key, value in good.items() if key != absent_key
        }
        with pytest.raises(SubstrateError) as excinfo:
            require_explicit_verification_status(stripped)
        assert (
            refusal_reason_code(excinfo.value) == UNIDENTIFIED_PRINCIPAL_REASON
        ), absent_key
        # An empty value is an absence too, not a satisfied signal.
        blanked = {**good, absent_key: ""}
        with pytest.raises(SubstrateError):
            require_explicit_verification_status(blanked)


def test_mapping_provider_without_verifier_metadata_refused_at_load() -> None:
    """A provider that never checks verifier evidence cannot back a mapping."""
    # Positive control first, so the refusal below is not vacuous.
    validate_mapping_against_providers(
        MAPPING, [_provider()], deployment_name=DEPLOYMENT
    )

    bare = _provider(metadata={})
    with pytest.raises(SubstrateError, match="trusted_verifiers"):
        validate_mapping_against_providers(
            MAPPING, [bare], deployment_name=DEPLOYMENT
        )

    # require_verified_by alone is sufficient -- the requirement names either.
    validate_mapping_against_providers(
        MAPPING,
        [_provider(metadata={"require_verified_by": True})],
        deployment_name=DEPLOYMENT,
    )

    # `metadata.production` is NOT sufficient, even though the shared helper
    # _requires_trusted_verifier treats it as switching the checks on: with an
    # empty trusted_verifiers list the membership test at identity.py:545 is
    # skipped, so the only surviving check is "verified_by is non-empty" --
    # which this server always satisfies by construction. An empty reference
    # set satisfies every membership test.
    with pytest.raises(SubstrateError, match="trusted_verifiers"):
        validate_mapping_against_providers(
            MAPPING,
            [_provider(metadata={"production": True})],
            deployment_name=DEPLOYMENT,
        )
    with pytest.raises(SubstrateError, match="trusted_verifiers"):
        validate_mapping_against_providers(
            MAPPING,
            [_provider(metadata={"trusted_verifiers": []})],
            deployment_name=DEPLOYMENT,
        )

    # A mapping naming a provider that does not exist is refused too: the
    # operator learns at start, not at the first request.
    with pytest.raises(SubstrateError, match="not registered"):
        validate_mapping_against_providers(
            MAPPING, [], deployment_name=DEPLOYMENT
        )


def test_untrusted_transport_verifier_is_rejected_by_the_machinery() -> None:
    """The declared trusted_verifiers list is load-bearing, not decorative.

    Without this arm the "provider must carry verifier metadata" refusal
    proves only that a config key exists. Here the key is set AND a claim
    stamped by a verifier outside the list is rejected -- so the metadata is
    shown to change an outcome.
    """
    from okg.substrate.nomos.identity import normalize_identity_claim

    provider = _provider()
    trusted = header_claim_for_request(
        MAPPING,
        {PRINCIPAL_HEADER: PRINCIPAL_A},
        transport_identity=TRANSPORT_VERIFIER,
    )
    assert normalize_identity_claim(trusted, [provider]).is_valid

    untrusted = header_claim_for_request(
        MAPPING,
        {PRINCIPAL_HEADER: PRINCIPAL_A},
        transport_identity=f"{TRANSPORT_VERIFIER_PREFIX}some-other-app",
    )
    envelope = normalize_identity_claim(untrusted, [provider])
    assert not envelope.is_valid
    assert envelope.verification_status == "untrusted_verifier"


@pytest.mark.parametrize(
    "field,value",
    [
        ("issuer", "https://other.example.edu/"),
        ("audience", "some-other-audience"),
        ("subject_claim", "upn"),
    ],
)
def test_mapping_skew_from_provider_is_refused_at_load(
    field: str, value: str
) -> None:
    """Manifest/provider skew is caught at start, not as a per-request deny."""
    from dataclasses import replace

    skewed = replace(MAPPING, **{field: value})
    with pytest.raises(SubstrateError, match="disagrees"):
        validate_mapping_against_providers(
            skewed, [_provider()], deployment_name=DEPLOYMENT
        )


def test_principal_mapping_block_is_parsed_strictly() -> None:
    raw = {
        "provider_id": PROVIDER_ID,
        "issuer": ISSUER,
        "audience": AUDIENCE,
        "subject_claim": "email",
        "subject_header": PRINCIPAL_HEADER,
        "principal_kind": "mcp_client",
    }
    assert parse_principal_mapping(raw) == MAPPING

    with pytest.raises(SubstrateError, match="unknown key"):
        parse_principal_mapping({**raw, "subjekt_header": "X-Typo"})
    with pytest.raises(SubstrateError, match="missing required key"):
        parse_principal_mapping({k: v for k, v in raw.items() if k != "subject_header"})
    with pytest.raises(SubstrateError, match="principal_kind"):
        parse_principal_mapping({**raw, "principal_kind": "not_a_kind"})
    with pytest.raises(SubstrateError, match="header field name"):
        parse_principal_mapping({**raw, "subject_header": "X OKG Principal"})


@pytest.mark.parametrize(
    "reserved",
    ["mfa_satisfied", "acr", "amr", "verified_by", "verification_status",
     "exp", "issuer"],
)
def test_extra_claim_headers_cannot_declare_trust_deciding_claims(
    reserved: str,
) -> None:
    """A declared header may carry an attribute, never verification evidence.

    normalize_identity_claim reads the claim payload for assurance, MFA,
    freshness and issuer/audience, and those decide provider.mfa_required,
    provider.required_assurance and SubjectBinding.required_assurance. A
    deployment that mapped a header onto one of them would be letting the
    CALLER assert that it satisfied MFA.
    """
    raw = {
        "provider_id": PROVIDER_ID,
        "issuer": ISSUER,
        "audience": AUDIENCE,
        "subject_claim": "email",
        "subject_header": PRINCIPAL_HEADER,
        "principal_kind": "mcp_client",
        "extra_claim_headers": {reserved: "X-OKG-Injected"},
    }
    with pytest.raises(SubstrateError, match="reserved claim"):
        parse_principal_mapping(raw)

    # Positive control: a non-reserved attribute claim is still accepted.
    ok = parse_principal_mapping(
        {**raw, "extra_claim_headers": {"department": "X-OKG-Department"}}
    )
    assert ok.extra_claim_headers == {"department": "X-OKG-Department"}


def test_manifest_without_principal_mapping_yields_none() -> None:
    """The single-operator case stays ordinary: absent means absent."""

    class _Manifest:
        nomos = {"runtime_enforcement": "enforce"}

    assert principal_mapping_from_manifest(_Manifest()) is None

    class _NoNomos:
        nomos = None

    assert principal_mapping_from_manifest(_NoNomos()) is None


def test_principal_kind_must_be_admitted_by_deployment_posture() -> None:
    block = {
        "provider_id": PROVIDER_ID,
        "issuer": ISSUER,
        "audience": AUDIENCE,
        "subject_claim": "email",
        "subject_header": PRINCIPAL_HEADER,
        "principal_kind": "mcp_client",
    }

    class _Admitted:
        nomos = {
            "principal_mapping": block,
            "deployment_posture": {"read_principals": ["mcp_client", "agent"]},
        }

    assert principal_mapping_from_manifest(_Admitted()) == MAPPING

    class _NotAdmitted:
        nomos = {
            "principal_mapping": block,
            "deployment_posture": {"read_principals": ["agent"]},
        }

    with pytest.raises(SubstrateError, match="read_principals"):
        principal_mapping_from_manifest(_NotAdmitted())


# ==========================================================================
# Fail-closed behavior (hermetic arms)
# ==========================================================================


@pytest.mark.fail_closed
class TestFailClosedClaimConstruction:
    """Every way a principal can fail to arrive, refused by name."""

    @pytest.mark.parametrize(
        "headers,channel",
        [
            ({}, "header_absent"),
            ({PRINCIPAL_HEADER: ""}, "header_empty"),
            ({PRINCIPAL_HEADER: "   "}, "header_empty"),
            ({PRINCIPAL_HEADER: "alice@example.edu, bob@example.edu"},
             "header_ambiguous"),
            ({PRINCIPAL_HEADER: "alice example"}, "header_malformed"),
            ({PRINCIPAL_HEADER: "a" * 400}, "header_malformed"),
        ],
    )
    def test_fail_closed_principal_header_arms_are_refused(
        self, headers: dict[str, str], channel: str
    ) -> None:
        with pytest.raises(SubstrateError) as excinfo:
            header_claim_for_request(
                MAPPING, headers, transport_identity=TRANSPORT_VERIFIER
            )
        error = excinfo.value
        assert refusal_reason_code(error) == UNIDENTIFIED_PRINCIPAL_REASON
        assert error.data["channel"] == channel
        assert error.data["subject"] == UNIDENTIFIED_SUBJECT
        # NOT any of the downgrade targets the requirement names.
        assert error.data["subject"] != "subject:mcp_client"

    def test_fail_closed_missing_principal_header_is_refused(self) -> None:
        with pytest.raises(SubstrateError) as excinfo:
            header_claim_for_request(
                MAPPING, {}, transport_identity=TRANSPORT_VERIFIER
            )
        assert (
            refusal_reason_code(excinfo.value) == UNIDENTIFIED_PRINCIPAL_REASON
        )

    def test_fail_closed_empty_principal_header_is_refused(self) -> None:
        with pytest.raises(SubstrateError) as excinfo:
            header_claim_for_request(
                MAPPING,
                {PRINCIPAL_HEADER: ""},
                transport_identity=TRANSPORT_VERIFIER,
            )
        assert excinfo.value.data["channel"] == "header_empty"

    def test_fail_closed_malformed_principal_header_is_refused(self) -> None:
        with pytest.raises(SubstrateError) as excinfo:
            header_claim_for_request(
                MAPPING,
                {PRINCIPAL_HEADER: "alice\tsmith"},
                transport_identity=TRANSPORT_VERIFIER,
            )
        assert excinfo.value.data["channel"] == "header_malformed"

    def test_fail_closed_malformed_header_value_is_not_echoed(self) -> None:
        """The rejected value is attacker-controlled; it stays out of logs."""
        secret_ish = "alice password=hunter2"
        with pytest.raises(SubstrateError) as excinfo:
            header_claim_for_request(
                MAPPING,
                {PRINCIPAL_HEADER: secret_ish},
                transport_identity=TRANSPORT_VERIFIER,
            )
        assert "hunter2" not in str(excinfo.value)
        assert "hunter2" not in str(excinfo.value.data)

    def test_fail_closed_repeated_principal_header_is_refused(self) -> None:
        """Two raw header tuples, not one comma-joined value.

        Starlette's ``Headers.__getitem__`` returns the FIRST match, so a
        repeated header silently discards the rest -- and a proxy that
        APPENDS the trusted principal header rather than replacing it would
        lose to the client's own header. The check reads getlist(), which is
        why this test builds a real Request instead of a dict.
        """
        request = _starlette_request({})
        raw = [
            (b"x-okg-principal", PRINCIPAL_B.encode()),
            (b"x-okg-principal", PRINCIPAL_A.encode()),
        ]
        request.scope["headers"] = raw
        headers = _starlette_request({}).__class__(
            {**request.scope, "headers": raw}
        ).headers
        assert headers.get(PRINCIPAL_HEADER) == PRINCIPAL_B
        assert len(headers.getlist(PRINCIPAL_HEADER)) == 2

        with pytest.raises(SubstrateError) as excinfo:
            header_claim_for_request(
                MAPPING, headers, transport_identity=TRANSPORT_VERIFIER
            )
        assert excinfo.value.data["channel"] == "header_ambiguous"
        assert excinfo.value.data["occurrences"] == 2

        # Positive control: one occurrence of the same header resolves.
        single = _starlette_request({PRINCIPAL_HEADER: PRINCIPAL_A}).headers
        claim = header_claim_for_request(
            MAPPING, single, transport_identity=TRANSPORT_VERIFIER
        )
        assert claim["external_subject"] == PRINCIPAL_A

    def test_declared_header_lookup_is_case_insensitive(self) -> None:
        """HTTP field names are case-insensitive; the mapping must be too."""
        headers = _starlette_request({"x-okg-PRINCIPAL": PRINCIPAL_A}).headers
        claim = header_claim_for_request(
            MAPPING, headers, transport_identity=TRANSPORT_VERIFIER
        )
        assert claim["external_subject"] == PRINCIPAL_A

    def test_fail_closed_scope_without_authenticated_user_is_refused(
        self,
    ) -> None:
        """An ASGI scope with no `user` key yields no delegator; refuse."""
        from okg.substrate.mcp.principal_mapping import (
            current_transport_identity,
        )

        with request_headers({PRINCIPAL_HEADER: PRINCIPAL_A}, client_id=None):
            assert current_transport_identity() is None
        # Positive control: with an authenticated user it resolves.
        with request_headers({PRINCIPAL_HEADER: PRINCIPAL_A}):
            assert current_transport_identity() == TRANSPORT_VERIFIER

    def test_fail_closed_no_request_context_is_refused(self) -> None:
        """Off an HTTP transport there is no header to read; refuse."""
        with pytest.raises(SubstrateError) as excinfo:
            header_claim_for_request(
                MAPPING, None, transport_identity=TRANSPORT_VERIFIER
            )
        assert excinfo.value.data["channel"] == "no_request_context"


@pytest.mark.fail_closed
def test_env_subject_coalesce_unreachable_in_multi_user_mode(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """read_filter.py's three-way or-chain cannot be reached multi-user.

    The chain ends on ``nomos_subject_from_env``: a None subject silently
    lands on whoever started the server. This proves BOTH directions -- the
    single-operator session still reaches it (so the test is not vacuous) and
    the multi-user session refuses before it.
    """
    from okg.substrate.nomos import read_filter

    reached: list[str] = []

    def _tripwire(*, default_kind: str = "mcp_client") -> str:
        reached.append(default_kind)
        return "subject:env-fallback"

    monkeypatch.setattr(read_filter, "nomos_subject_from_env", _tripwire)
    monkeypatch.setattr(
        read_filter, "deployment_requires_nomos_runtime", lambda name: True
    )
    # The catalog read is unrelated machinery downstream of the guard; stub it
    # so the pass runs with no connection and the guard is what is measured.
    monkeypatch.setattr(
        read_filter, "_load_subtype_pii_classes", lambda conn, cv: {}
    )

    def _session(*, multi_user: bool) -> Session:
        return Session(
            deployment_name=DEPLOYMENT,
            generation_id="gen-1",
            catalog_version_id=0,
            db_fingerprint="fp",
            nomos_subject=None,
            nomos_multi_user=multi_user,
        )

    # Single-operator: the coalesce IS reachable today, and stays reachable.
    visible, written = read_filter._filter_rows_evaluate(
        None,
        _session(multi_user=False),
        [],
        action="graph.read.mcp",
        subject=None,
        identity_claim=None,
        audit=False,
        surface="mcp",
        row_kind="node",
        id_key="node_id",
    )
    assert visible == [] and written is False
    assert reached == ["mcp_client"]

    # Multi-user: refused before the coalesce, and the env helper is never
    # called at all.
    reached.clear()
    with pytest.raises(SubstrateError) as excinfo:
        read_filter._filter_rows_evaluate(
            None,
            _session(multi_user=True),
            [],
            action="graph.read.mcp",
            subject=None,
            identity_claim=None,
            audit=False,
            surface="mcp",
            row_kind="node",
            id_key="node_id",
        )
    assert refusal_reason_code(excinfo.value) == UNIDENTIFIED_PRINCIPAL_REASON
    assert reached == []


@pytest.mark.fail_closed
def test_env_subject_coalesce_unreachable_at_the_public_read_entry(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The guard also covers the enforcement-off early return.

    ``filter_node_rows_for_session`` returns rows UNFILTERED when
    ``deployment_requires_nomos_runtime`` is false -- before the evaluation
    (and its guard) is reached. A deployment whose enforcement was flipped off
    under a running multi-user server would otherwise serve every row to a
    caller nobody identified.
    """
    from okg.substrate.nomos import read_filter

    monkeypatch.setattr(
        read_filter, "deployment_requires_nomos_runtime", lambda name: False
    )

    def _session(*, multi_user: bool) -> Session:
        return Session(
            deployment_name=DEPLOYMENT,
            generation_id="gen-1",
            catalog_version_id=0,
            db_fingerprint="fp",
            nomos_subject=None,
            nomos_multi_user=multi_user,
        )

    rows = [{"node_id": "node:x", "attrs": {}, "source": "public_docs"}]
    # Single-operator with enforcement off: unchanged, rows pass through.
    assert read_filter.filter_node_rows_for_session(
        None, _session(multi_user=False), rows
    ) == rows
    # Multi-user with enforcement off: refused, not served.
    with pytest.raises(SubstrateError) as excinfo:
        read_filter.filter_node_rows_for_session(
            None, _session(multi_user=True), rows
        )
    assert refusal_reason_code(excinfo.value) == UNIDENTIFIED_PRINCIPAL_REASON
    # Edge rows take the same entry and the same guard.
    with pytest.raises(SubstrateError):
        read_filter.filter_edge_rows_for_session(
            None, _session(multi_user=True), [{"edge_id": 1, "attrs": {}}]
        )
    # A multi-user session that DID resolve a subject is served normally.
    resolved = replace(_session(multi_user=True), nomos_subject=SUBJECT_A)
    assert read_filter.filter_node_rows_for_session(None, resolved, rows) == rows


@pytest.mark.fail_closed
def test_fail_closed_multi_user_without_transport_auth_refuses_to_start(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Multi-user over an unauthenticated transport never binds a socket."""
    built: list[bool] = []

    def _never_built(**kwargs: Any) -> Any:  # pragma: no cover - must not run
        built.append(True)
        raise AssertionError("build_server must not run on a refused start")

    monkeypatch.setattr(
        "okg.substrate.mcp.server.build_server", _never_built
    )

    with pytest.raises(SubstrateError) as excinfo:
        run_server(
            transport="streamable-http",
            host="127.0.0.1",
            port=8125,
            deployment_name=DEPLOYMENT,
            multi_user_principals=True,
            auth_token_env=None,
        )
    assert excinfo.value.code == "mcp_multi_user_requires_auth"
    assert "authentication" in str(excinfo.value).lower()
    assert built == []

    # stdio carries no HTTP request at all, so there is no header channel.
    with pytest.raises(SubstrateError) as stdio_excinfo:
        run_server(
            transport="stdio",
            deployment_name=DEPLOYMENT,
            multi_user_principals=True,
        )
    assert stdio_excinfo.value.code == "mcp_multi_user_stdio_incompatible"
    assert built == []

    # Non-vacuity: with a resolvable credential the guard passes and the run
    # reaches build_server (which the stub proves by raising its sentinel).
    monkeypatch.setenv("OKG_TEST_MCP_TOKEN", "x" * 48)
    with pytest.raises(AssertionError, match="must not run on a refused start"):
        run_server(
            transport="streamable-http",
            host="127.0.0.1",
            port=8125,
            deployment_name=DEPLOYMENT,
            multi_user_principals=True,
            auth_token_env="OKG_TEST_MCP_TOKEN",
        )
    assert built == [True]


@pytest.mark.fail_closed
def test_fail_closed_cli_refuses_multi_user_without_auth(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """The CLI refuses before any DB work, with a non-zero exit."""
    from okg.substrate.cli.main import main

    code = main(
        [
            "--json",
            "mcp-serve",
            "--deployment",
            DEPLOYMENT,
            "--transport",
            "streamable-http",
            "--multi-user-principals",
        ]
    )
    assert code != 0
    captured = capsys.readouterr()
    combined = (captured.out + captured.err).lower()
    assert "auth" in combined
    assert "mcp_multi_user_requires_auth" in combined


# ==========================================================================
# End-to-end against Postgres
# ==========================================================================


def _publish_two_source_graph(dsn: str) -> str:
    with psycopg.connect(dsn, autocommit=True) as conn:
        seed_catalog_version(conn)
        store = ChronosGraphStore.from_connection(conn)
        store.apply_facts(
            [
                NodeFact(
                    "node:public-docs",
                    "page",
                    {"classification": "internal", "text": "public docs"},
                    {"id": "node:public-docs"},
                    {"v": 1},
                ),
                EdgeFact(
                    "node:public-docs",
                    "node:public-docs",
                    "references",
                    {"classification": "internal"},
                    source_record_id={"id": "public-self"},
                    source_revision={"v": 1},
                ),
            ],
            source="public_docs",
        )
        store.apply_facts(
            [
                NodeFact(
                    "node:private-drive",
                    "page",
                    {"classification": "internal", "text": "private drive"},
                    {"id": "node:private-drive"},
                    {"v": 1},
                ),
            ],
            source="google_drive",
        )
    result = Publisher(dsn, DEPLOYMENT, catalog_identity=bind_test_catalog_writer(dsn, DEPLOYMENT)).cycle()
    assert result["status"] == "published"
    return str(result["generation_id"])


def _declare_page_pii_classes(dsn: str) -> None:
    """Declare ``page.text`` free_text so a redact decision DROPS the key."""
    with psycopg.connect(dsn, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute(
            """
            UPDATE okg.subtypes
               SET metadata = coalesce(metadata, '{}'::jsonb)
                   || '{"pii_classes": {"text": "free_text"}}'::jsonb
             WHERE name = 'page'
            """
        )


def _boundary_rules(subject: str, suffix: str) -> list[dict[str, Any]]:
    return [
        {
            "rule_id": f"allow_tool_call_{suffix}",
            "effect": "allow",
            "actions": ["mcp.tool.call"],
            "subjects": [subject],
            "match": {
                "deployment": DEPLOYMENT,
                "scope": SCOPE,
                "resource_kind": "mcp_tool",
            },
            "priority": 100,
        },
        {
            "rule_id": f"allow_graph_boundary_{suffix}",
            "effect": "allow",
            "actions": ["graph.read.mcp"],
            "subjects": [subject],
            "match": {
                "deployment": DEPLOYMENT,
                "scope": SCOPE,
                "resource_kind": "mcp_graph_read_boundary",
            },
            "priority": 100,
        },
    ]


def _activate_two_principal_policy(
    dsn: str, *, b_effect_on_drive: str = "deny"
) -> None:
    """A is allowed both sources; B is denied (or redacted) on google_drive.

    This is the differentiating fixture the requirement demands: without a
    declared divergence, A and B would see identical rows and the comparison
    would prove nothing.
    """
    rules: list[dict[str, Any]] = []
    rules += _boundary_rules(SUBJECT_A, "a")
    rules += _boundary_rules(SUBJECT_B, "b")
    for subject, suffix in ((SUBJECT_A, "a"), (SUBJECT_B, "b")):
        rules.append(
            {
                "rule_id": f"allow_public_docs_{suffix}",
                "effect": "allow",
                "actions": ["graph.read.mcp"],
                "subjects": [subject],
                "match": {
                    "deployment": DEPLOYMENT,
                    "scope": SCOPE,
                    "source_id": "public_docs",
                },
                "priority": 100,
            }
        )
    rules.append(
        {
            "rule_id": "allow_drive_a",
            "effect": "allow",
            "actions": ["graph.read.mcp"],
            "subjects": [SUBJECT_A],
            "match": {
                "deployment": DEPLOYMENT,
                "scope": SCOPE,
                "source_id": "google_drive",
            },
            "priority": 100,
        }
    )
    drive_b: dict[str, Any] = {
        "rule_id": "drive_b",
        "effect": b_effect_on_drive,
        "actions": ["graph.read.mcp"],
        "subjects": [SUBJECT_B],
        "match": {
            "deployment": DEPLOYMENT,
            "scope": SCOPE,
            "source_id": "google_drive",
        },
        "priority": 100,
    }
    if b_effect_on_drive == "redact":
        # A redact effect must declare its redaction obligation, or the
        # runtime fails the decision closed with
        # nomos.runtime.obligation_mismatch and the row disappears instead of
        # arriving masked -- which would make this fixture prove the wrong
        # thing.
        drive_b["obligations"] = [{"type": "redaction"}, {"type": "audit"}]
    rules.append(drive_b)
    bundle = compile_policy_graph(
        {
            "policy_bundle_id": "nomos:test:mcp-principal-mapping",
            "policy_version": "v1",
            "default_effect": "deny",
            "rules": rules,
        }
    )
    with psycopg.connect(dsn) as conn:
        activate_policy_bundle(
            conn,
            deployment_name=DEPLOYMENT,
            bundle=bundle,
            scope=SCOPE,
            activated_by="test:operator",
        )
        conn.commit()


def _configure_header_identity(
    dsn: str,
    *,
    principals: Mapping[str, str],
    drive_subjects: tuple[str, ...] = (SUBJECT_A,),
) -> None:
    """Register the header provider, the bindings, and the identity grants."""
    provider = _provider()
    with psycopg.connect(dsn) as conn:
        upsert_identity_provider(
            conn, deployment_name=DEPLOYMENT, provider=provider
        )
        for index, (principal, subject) in enumerate(sorted(principals.items())):
            upsert_subject_binding(
                conn,
                deployment_name=DEPLOYMENT,
                binding=SubjectBinding(
                    binding_id=f"binding:chat:{index}",
                    provider_id=provider.provider_id,
                    issuer=provider.issuer,
                    claim_path="external_subject",
                    claim_value=principal,
                    subject=subject,
                    scope=SCOPE,
                    priority=10,
                ),
            )
        for subject in sorted(set(principals.values())):
            slug = subject.replace(":", "-")
            for resource_kind in ("mcp_tool", "mcp_graph_read_boundary"):
                upsert_permission_grant(
                    conn,
                    deployment_name=DEPLOYMENT,
                    grant=PermissionGrant(
                        grant_id=f"grant:{slug}:{resource_kind}",
                        subject=subject,
                        actions=(
                            "mcp.tool.call"
                            if resource_kind == "mcp_tool"
                            else "graph.read.mcp",
                        ),
                        scope=SCOPE,
                        resource_match={
                            "deployment": DEPLOYMENT,
                            "resource_kind": resource_kind,
                        },
                        classifications=("*",),
                        priority=20,
                    ),
                )
            upsert_permission_grant(
                conn,
                deployment_name=DEPLOYMENT,
                grant=PermissionGrant(
                    grant_id=f"grant:{slug}:public_docs",
                    subject=subject,
                    actions=("graph.read.mcp",),
                    scope=SCOPE,
                    resource_match={"source_id": "public_docs"},
                    classifications=("*",),
                    priority=10,
                ),
            )
        for subject in drive_subjects:
            slug = subject.replace(":", "-")
            upsert_permission_grant(
                conn,
                deployment_name=DEPLOYMENT,
                grant=PermissionGrant(
                    grant_id=f"grant:{slug}:google_drive",
                    subject=subject,
                    actions=("graph.read.mcp",),
                    scope=SCOPE,
                    resource_match={"source_id": "google_drive"},
                    classifications=("*",),
                    priority=10,
                ),
            )
        conn.commit()


@contextlib.contextmanager
def _multi_user_server(dsn: str, **launch: Any) -> Iterator[Any]:
    mcp = build_server(
        dsn=dsn,
        deployment_name=DEPLOYMENT,
        multi_user_principals=True,
        principal_mapping=MAPPING,
        **launch,
    )
    try:
        yield mcp
    finally:
        mcp._okg_pool.close()
        if mcp._okg_write_pool:
            mcp._okg_write_pool.close()


def _filter_nodes(mcp: Any) -> dict[str, Any]:
    return mcp._tool_manager._tools["filter"].fn(
        input={"kind": "nodes"},
        predicate={},
        select=["node_id", "attrs"],
        limit=50,
    )


def _run_with_headers(headers: Mapping[str, str], call: Any) -> Any:
    with request_headers(headers):
        return call()


def _decision_rows(dsn: str, *, limit: int = 200) -> list[dict[str, Any]]:
    """Recent decision rows, ordered by the BIGSERIAL key.

    Never ``ORDER BY decision_id``: that column is a content hash, so sorting
    by it is lexicographic noise that returns arbitrary old rows.
    """
    from psycopg.rows import dict_row

    with psycopg.connect(dsn) as conn, conn.cursor(row_factory=dict_row) as cur:
        cur.execute(
            """
            SELECT decision_row_id, subject, action, effect, reason_code,
                   enforcement_point
              FROM okg.nomos_decisions
             WHERE deployment_name = %s
             ORDER BY decision_row_id DESC
             LIMIT %s
            """,
            (DEPLOYMENT, limit),
        )
        return [dict(row) for row in cur.fetchall()]


@pytest.mark.postgres
@pytest.mark.serial_db
def test_two_calls_in_one_process_carry_distinct_subjects(
    clean_runtime: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    """One server process, two callers, two different persisted subjects."""
    _publish_two_source_graph(clean_runtime)
    _configure_header_identity(
        clean_runtime,
        principals={PRINCIPAL_A: SUBJECT_A, PRINCIPAL_B: SUBJECT_B},
    )
    _activate_two_principal_policy(clean_runtime)
    monkeypatch.setenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", "1")
    # A process-wide subject is set on purpose: if any request fell back to
    # it, the assertions below would see it.
    monkeypatch.setenv("OKG_NOMOS_SUBJECT", "subject:process-wide-operator")

    with _multi_user_server(clean_runtime) as mcp:
        with request_headers({PRINCIPAL_HEADER: PRINCIPAL_A}):
            _filter_nodes(mcp)
        with request_headers({PRINCIPAL_HEADER: PRINCIPAL_B}):
            _filter_nodes(mcp)

    # Equality over the MCP-surface decisions, not membership: a membership
    # test would still pass if the server ALSO evaluated something under a
    # shared subject alongside the per-caller ones. (The fixture's own
    # `policy.activate` decision, subject `test:operator`, is not an MCP
    # request and is excluded by the action predicate.)
    request_subjects = {
        row["subject"]
        for row in _decision_rows(clean_runtime)
        if row["action"] in {"graph.read.mcp", "mcp.tool.call"}
    }
    assert request_subjects == {SUBJECT_A, SUBJECT_B}
    assert SUBJECT_A != SUBJECT_B

    # req.verified-claim-trust (b): the PERSISTED claim must record "app-token
    # holder X asserted principal P", not a bare "P". Asserted on the stored
    # row rather than on the builder's return, because the builder stamping a
    # value proves nothing about what the audit trail ends up holding -- and
    # this trust model's whole blast-radius argument rests on the delegator
    # being recoverable from the record.
    from psycopg.rows import dict_row

    with psycopg.connect(clean_runtime) as conn, conn.cursor(
        row_factory=dict_row
    ) as cur:
        cur.execute(
            """
            SELECT external_subject, verification_status,
                   evidence ->> 'verified_by'   AS verified_by,
                   evidence ->> 'verified_at'   AS verified_at
              FROM okg.nomos_external_claims
             WHERE deployment_name = %s AND verification_status = 'valid'
            """,
            (DEPLOYMENT,),
        )
        claims = [dict(row) for row in cur.fetchall()]
    assert claims, "expected the accepted claims to be persisted"
    # Both principals are recorded, each under the TRANSPORT's verifier id --
    # never under the principal's own value, which is what "the client cannot
    # vouch for itself" means in the stored record.
    assert {row["external_subject"] for row in claims} == {
        PRINCIPAL_A,
        PRINCIPAL_B,
    }
    assert {row["verified_by"] for row in claims} == {TRANSPORT_VERIFIER}
    assert all(row["verified_at"] for row in claims)
    assert PRINCIPAL_A not in {row["verified_by"] for row in claims}


@pytest.mark.postgres
@pytest.mark.serial_db
def test_request_subject_reaches_read_filter_not_process_env(
    clean_runtime: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The row-level source decisions carry the REQUEST's subject."""
    _publish_two_source_graph(clean_runtime)
    _configure_header_identity(
        clean_runtime,
        principals={PRINCIPAL_A: SUBJECT_A, PRINCIPAL_B: SUBJECT_B},
    )
    _activate_two_principal_policy(clean_runtime)
    monkeypatch.setenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", "1")
    monkeypatch.setenv("OKG_NOMOS_SUBJECT", "subject:process-wide-operator")

    with _multi_user_server(clean_runtime) as mcp:
        with request_headers({PRINCIPAL_HEADER: PRINCIPAL_A}):
            result = _filter_nodes(mcp)
    assert result["items"]

    # EQUALITY on the identity enforcement point, not a `"source_lineage" in`
    # substring. The substring also matches `mcp.source_lineage_filter`
    # (read_filter.py:393), the branch taken when NO identity claim is
    # present -- and in multi-user mode that branch would carry SUBJECT_A too,
    # because the request subject is already SUBJECT_A. So the substring form
    # asserted only "the subject arrived", never "the identity CLAIM arrived",
    # and would have passed with the claim dropped entirely.
    rows = _decision_rows(clean_runtime)
    row_reads = [
        row
        for row in rows
        if row["enforcement_point"] == "mcp.source_lineage_identity_filter"
    ]
    assert row_reads, (
        "expected per-row identity-claim decisions; the identity branch at "
        "read_filter.py:328-375 did not run, so the request-scoped "
        "identity_claim never reached the read filter"
    )
    assert {row["subject"] for row in row_reads} == {SUBJECT_A}


@pytest.mark.postgres
@pytest.mark.serial_db
def test_request_path_applies_the_explicit_verification_status_guard(
    clean_runtime: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The guard is enforced by the REQUEST PATH, not merely by the helper.

    ``test_header_claim_without_verification_status_is_refused`` calls
    ``require_explicit_verification_status`` directly, which proves the
    helper works and nothing about whether anything uses it. The production
    call site (server.py:1857-1858) is reached only when
    ``identity_claim_override is not None``, and that override is always the
    return of ``header_claim_for_request``, which ALWAYS sets
    ``verification_status`` (principal_mapping.py:939). So the guard cannot
    fire on any input a caller can construct, and deleting the call site left
    the whole suite green -- a defense-in-depth line that no test defends.

    This drives the claim builder to emit a claim with the field stripped, so
    the request path meets the input the guard exists for. req.verified-claim-
    trust (c) is explicit that such a claim is REFUSED, never defaulted to
    valid, and this proves that end to end.

    MEASURED, and it revises the PACT's stated premise. The refusal arrives as
    ``no_subject_binding``, not ``claim_unverified``: the tracer gate
    (server.py:591-643) resolves the principal to a SUBJECT before the pooled
    session runs, and the machinery already declines to bind an unverified
    claim. req.verified-claim-trust and the PACT's root_cause both assert that
    "an omitted verification_status defaults to valid
    (src/okg/substrate/nomos/identity.py:488)" -- that is STALE. Commit
    6fc8eaf2d ("nomos: fail closed when identity claim verification evidence
    is absent", 2026-08-03) made an omitted status normalize to
    UNVERIFIED_CLAIM_STATUS instead (identity.py:541-542), and an unverified
    claim is not valid (identity.py:230), so it binds nothing.

    So ``require_explicit_verification_status`` is now REDUNDANT defense in
    depth on this path rather than the load-bearing fix, and its call site
    (server.py:1857-1858) is unreachable through the tool surface. The
    requirement's OUTCOME holds either way, which is what this asserts; the
    mechanism is recorded rather than assumed.
    """
    _publish_two_source_graph(clean_runtime)
    _configure_header_identity(
        clean_runtime,
        principals={PRINCIPAL_A: SUBJECT_A, PRINCIPAL_B: SUBJECT_B},
    )
    _activate_two_principal_policy(clean_runtime)
    monkeypatch.setenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", "1")

    real_builder = header_claim_for_request

    def _unstamped(*args: Any, **kwargs: Any) -> dict[str, Any]:
        claim = dict(real_builder(*args, **kwargs))
        claim.pop("verification_status", None)
        return claim

    monkeypatch.setattr(
        "okg.substrate.mcp.principal_mapping.header_claim_for_request",
        _unstamped,
    )

    with _multi_user_server(clean_runtime) as mcp:
        with request_headers({PRINCIPAL_HEADER: PRINCIPAL_A}):
            with pytest.raises(SubstrateError) as excinfo:
                _filter_nodes(mcp)

    error = excinfo.value
    assert refusal_reason_code(error) == UNIDENTIFIED_PRINCIPAL_REASON
    assert error.data["channel"] == "no_subject_binding"
    # The unverified claim was never allowed to bind a subject, so nothing
    # was read under SUBJECT_A on the strength of an unstated verification --
    # which is the requirement, whichever layer enforces it.
    rows = _decision_rows(clean_runtime)
    assert not [row for row in rows if row["action"] == "graph.read.mcp"]
    assert SUBJECT_A not in {row["subject"] for row in rows}
    assert UNIDENTIFIED_SUBJECT in {row["subject"] for row in rows}
    # The CONTROL: the identical fixture with the status intact resolves
    # SUBJECT_A and reads rows. Without this, the assertions above would also
    # pass if the monkeypatch had broken the request for an unrelated reason.
    monkeypatch.undo()
    monkeypatch.setenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", "1")
    with _multi_user_server(clean_runtime) as mcp:
        with request_headers({PRINCIPAL_HEADER: PRINCIPAL_A}):
            allowed = _filter_nodes(mcp)
    assert allowed["items"]
    assert SUBJECT_A in {row["subject"] for row in _decision_rows(clean_runtime)}


@pytest.mark.postgres
@pytest.mark.serial_db
def test_two_user_diff_row_set_differs_between_principals(
    clean_runtime: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The same call, two principals, one process: the row sets differ."""
    _publish_two_source_graph(clean_runtime)
    _configure_header_identity(
        clean_runtime,
        principals={PRINCIPAL_A: SUBJECT_A, PRINCIPAL_B: SUBJECT_B},
        drive_subjects=(SUBJECT_A,),
    )
    _activate_two_principal_policy(clean_runtime, b_effect_on_drive="deny")
    monkeypatch.setenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", "1")

    with _multi_user_server(clean_runtime) as mcp:
        with request_headers({PRINCIPAL_HEADER: PRINCIPAL_A}):
            a_result = _filter_nodes(mcp)
        with request_headers({PRINCIPAL_HEADER: PRINCIPAL_B}):
            b_result = _filter_nodes(mcp)

    a_ids = {item["node_id"] for item in a_result["items"]}
    b_ids = {item["node_id"] for item in b_result["items"]}
    # The fixture must be differentiating: identical results prove nothing.
    assert a_ids != b_ids, "policy fixture is not differentiating"
    assert a_ids.symmetric_difference(b_ids) == {"node:private-drive"}
    assert "node:private-drive" in a_ids
    assert "node:private-drive" not in b_ids
    # B is missing a row, not erroring or truncating.
    assert "node:public-docs" in b_ids


@pytest.mark.postgres
@pytest.mark.serial_db
def test_two_user_diff_masked_attr_present_for_a_and_redacted_for_b(
    clean_runtime: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A sees the free_text attribute; B gets it dropped with the marker."""
    _publish_two_source_graph(clean_runtime)
    _declare_page_pii_classes(clean_runtime)
    _configure_header_identity(
        clean_runtime,
        principals={PRINCIPAL_A: SUBJECT_A, PRINCIPAL_B: SUBJECT_B},
        drive_subjects=(SUBJECT_A, SUBJECT_B),
    )
    _activate_two_principal_policy(clean_runtime, b_effect_on_drive="redact")
    monkeypatch.setenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", "1")

    with _multi_user_server(clean_runtime) as mcp:
        with request_headers({PRINCIPAL_HEADER: PRINCIPAL_A}):
            a_result = _filter_nodes(mcp)
        with request_headers({PRINCIPAL_HEADER: PRINCIPAL_B}):
            b_result = _filter_nodes(mcp)

    def _attrs(result: dict[str, Any], node_id: str) -> dict[str, Any]:
        for item in result["items"]:
            if item["node_id"] == node_id:
                return dict(item.get("attrs") or {})
        raise AssertionError(f"{node_id} absent from {result['items']}")

    a_attrs = _attrs(a_result, "node:private-drive")
    b_attrs = _attrs(b_result, "node:private-drive")
    assert a_attrs != b_attrs, "policy fixture is not differentiating"
    # Key-by-key difference, not a bare inequality. (The MCP read surface
    # fences untrusted source text, so A's value is wrapped, not bare.)
    assert "private drive" in str(a_attrs.get("text"))
    assert "text" not in b_attrs
    assert b_attrs.get(REDACTION_MARKER) is True
    assert REDACTION_MARKER not in a_attrs
    # B's row is present and masked, not truncated or errored.
    assert b_attrs


@pytest.mark.postgres
@pytest.mark.serial_db
@pytest.mark.fail_closed
@pytest.mark.parametrize(
    ("headers", "expected_channel"),
    [
        ({}, "header_absent"),
        ({PRINCIPAL_HEADER: ""}, "header_empty"),
        ({PRINCIPAL_HEADER: "not a principal"}, "header_malformed"),
        ({PRINCIPAL_HEADER: "nobody@example.edu"}, "no_subject_binding"),
    ],
    ids=["missing", "empty", "malformed", "unmappable"],
)
def test_fail_closed_unidentified_principal_persists_sentinel_row(
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
    headers: dict[str, str],
    expected_channel: str,
) -> None:
    """Refused, zero rows, and an audit row under the sentinel subject."""
    _publish_two_source_graph(clean_runtime)
    _configure_header_identity(
        clean_runtime,
        principals={PRINCIPAL_A: SUBJECT_A, PRINCIPAL_B: SUBJECT_B},
    )
    _activate_two_principal_policy(clean_runtime)
    monkeypatch.setenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", "1")
    monkeypatch.setenv("OKG_NOMOS_SUBJECT", "subject:process-wide-operator")

    with _multi_user_server(clean_runtime) as mcp:
        with request_headers(headers):
            with pytest.raises(SubstrateError) as excinfo:
                _filter_nodes(mcp)

    error = excinfo.value
    assert refusal_reason_code(error) == UNIDENTIFIED_PRINCIPAL_REASON
    assert error.code == "nomos_principal_unidentified"
    # Per-arm channel, so the four arms prove four DISTINCT paths. Without
    # this they are assertion-identical: if `nobody@example.edu` regressed to
    # being rejected by the shape check, the [unmappable] arm -- the only one
    # that exercises subject BINDING rather than syntax -- would still pass
    # while proving the malformed case a second time.
    assert error.data["channel"] == expected_channel

    rows = _decision_rows(clean_runtime)
    sentinel = [
        row
        for row in rows
        if row["reason_code"] == UNIDENTIFIED_PRINCIPAL_REASON
    ]
    assert sentinel, "expected an unidentified-principal decision row"
    # Equality on the sentinel, inequality on every downgrade target.
    assert {row["subject"] for row in sentinel} == {UNIDENTIFIED_SUBJECT}
    assert {row["effect"] for row in sentinel} == {"deny"}
    for row in rows:
        assert row["subject"] != "subject:process-wide-operator"
        assert row["subject"] != "subject:mcp_client"
    # "zero graph rows are returned" (acc.fail-closed-principal), in its
    # observable form: the refusal happened BEFORE any row was read, so no
    # graph-read decision was evaluated at all. Asserting on the returned
    # value is impossible -- the call raises rather than returning.
    assert not [row for row in rows if row["action"] == "graph.read.mcp"]
    assert not [
        row for row in rows if "source_lineage" in str(row["enforcement_point"])
    ]


@pytest.mark.postgres
@pytest.mark.serial_db
@pytest.mark.fail_closed
def test_fail_closed_refusal_is_distinct_from_policy_deny(
    clean_runtime: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    """An unidentified caller and a denied caller are different audit facts.

    The nearest existing code, ``nomos.identity.no_subject_binding``, is
    emitted with effect deny for BOTH shapes, which is exactly why this PACT
    mints a distinct one.
    """
    _publish_two_source_graph(clean_runtime)
    # B is bound to a subject the policy grants nothing: a real principal,
    # denied. A's header is simply absent: nobody.
    _configure_header_identity(
        clean_runtime,
        principals={PRINCIPAL_A: SUBJECT_A, PRINCIPAL_B: SUBJECT_B},
        drive_subjects=(),
    )
    bundle = compile_policy_graph(
        {
            "policy_bundle_id": "nomos:test:mcp-principal-deny",
            "policy_version": "v1",
            "default_effect": "deny",
            "rules": _boundary_rules(SUBJECT_A, "a")
            + [
                {
                    "rule_id": "allow_public_docs_a",
                    "effect": "allow",
                    "actions": ["graph.read.mcp"],
                    "subjects": [SUBJECT_A],
                    "match": {
                        "deployment": DEPLOYMENT,
                        "scope": SCOPE,
                        "source_id": "public_docs",
                    },
                    "priority": 100,
                }
            ],
        }
    )
    with psycopg.connect(clean_runtime) as conn:
        activate_policy_bundle(
            conn,
            deployment_name=DEPLOYMENT,
            bundle=bundle,
            scope=SCOPE,
            activated_by="test:operator",
        )
        conn.commit()
    monkeypatch.setenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", "1")

    with _multi_user_server(clean_runtime) as mcp:
        # 1. A real principal the policy denies.
        with request_headers({PRINCIPAL_HEADER: PRINCIPAL_B}):
            with pytest.raises(SubstrateError) as deny_info:
                _filter_nodes(mcp)
        deny_rows = _decision_rows(clean_runtime, limit=20)
        # 2. Nobody at all.
        with request_headers({}):
            with pytest.raises(SubstrateError) as refusal_info:
                _filter_nodes(mcp)

    # The deny is a POLICY deny, named as such -- not merely "not a principal
    # refusal", which any unrelated crash would also satisfy.
    assert refusal_reason_code(deny_info.value) is None
    assert deny_info.value.code == "nomos_identity_denied"
    deny_data = deny_info.value.data
    assert deny_data["reason_code"] != UNIDENTIFIED_PRINCIPAL_REASON
    # THE distinction, stated positively: a policy deny resolved a real
    # principal and then said no. The refusal resolved nobody.
    assert deny_data["resolved_subject"] == SUBJECT_B
    assert deny_data["effect"] == "deny"
    assert refusal_reason_code(refusal_info.value) == UNIDENTIFIED_PRINCIPAL_REASON
    assert refusal_info.value.data["subject"] == UNIDENTIFIED_SUBJECT

    deny_subjects = {row["subject"] for row in deny_rows}
    assert UNIDENTIFIED_SUBJECT not in deny_subjects
    assert not [
        row
        for row in deny_rows
        if row["reason_code"] == UNIDENTIFIED_PRINCIPAL_REASON
    ]

    all_rows = _decision_rows(clean_runtime)
    refusal_rows = [
        row
        for row in all_rows
        if row["reason_code"] == UNIDENTIFIED_PRINCIPAL_REASON
    ]
    assert refusal_rows
    assert {row["subject"] for row in refusal_rows} == {UNIDENTIFIED_SUBJECT}


@pytest.mark.postgres
@pytest.mark.serial_db
@pytest.mark.fail_closed
def test_multi_user_rejects_caller_supplied_authority_identity_claim(
    clean_runtime: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The self-asserted identity channel cannot bypass the header mapping."""
    _publish_two_source_graph(clean_runtime)
    _configure_header_identity(
        clean_runtime,
        principals={PRINCIPAL_A: SUBJECT_A, PRINCIPAL_B: SUBJECT_B},
    )
    _activate_two_principal_policy(clean_runtime)
    monkeypatch.setenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", "1")

    self_asserted = {
        "nomos_identity_claim": {
            "provider_id": PROVIDER_ID,
            "issuer": ISSUER,
            "audiences": [AUDIENCE],
            "external_subject": PRINCIPAL_A,
            "claims": {"email": PRINCIPAL_A},
            "verification_status": "valid",
            "verified_by": TRANSPORT_VERIFIER,
            "verified_at": "2026-08-01T00:00:00+00:00",
        }
    }

    def _call(headers: dict[str, str], mcp: Any) -> SubstrateError:
        with request_headers(headers):
            with pytest.raises(SubstrateError) as excinfo:
                mcp._tool_manager._tools["inspect"].fn(
                    target={"kind": "graph"},
                    authority=self_asserted,
                )
        return excinfo.value

    def _call_positional(mcp: Any) -> SubstrateError:
        """The same claim, passed POSITIONALLY.

        A security check that depends on the caller's calling convention is
        not a check: `inspect(target, generation_id, select, authority)` binds
        `authority` fourth, so a kwargs-only lookup would report a missing
        header to a caller whose actual mistake was supplying its own
        identity.
        """
        with request_headers({}):
            with pytest.raises(SubstrateError) as excinfo:
                mcp._tool_manager._tools["inspect"].fn(
                    {"kind": "graph"}, None, None, self_asserted
                )
        return excinfo.value

    with _multi_user_server(clean_runtime) as mcp:
        # Alone: no header, a self-asserted claim. The refusal is the
        # CHANNEL conflict, not an evaluation of the claim.
        alone = _call({}, mcp)
        # Alongside a valid header: still rejected, never merged and never
        # preferred.
        alongside = _call({PRINCIPAL_HEADER: PRINCIPAL_B}, mcp)
        positional = _call_positional(mcp)

    for error in (alone, alongside, positional):
        assert refusal_reason_code(error) == CONFLICTING_IDENTITY_CHANNEL_REASON
        assert error.code == "nomos_identity_channel_conflict"

    rows = _decision_rows(clean_runtime)
    conflicts = [
        row
        for row in rows
        if row["reason_code"] == CONFLICTING_IDENTITY_CHANNEL_REASON
    ]
    assert conflicts
    assert {row["subject"] for row in conflicts} == {UNIDENTIFIED_SUBJECT}
    # The self-asserted claim was never evaluated as identity: no decision
    # resolved a subject from it.
    assert SUBJECT_A not in {row["subject"] for row in rows}

    with psycopg.connect(clean_runtime) as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT count(*) FROM okg.nomos_external_claims "
            "WHERE deployment_name = %s",
            (DEPLOYMENT,),
        )
        assert cur.fetchone()[0] == 0


@pytest.mark.postgres
@pytest.mark.serial_db
@pytest.mark.fail_closed
def test_fail_closed_session_less_tools_are_refused_without_a_principal(
    clean_runtime: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Tools that never open a read session refuse too.

    Several ``inspect`` kinds return metadata before any session exists, and
    the change-observation tools build a Session directly instead of going
    through the pooled path. If the principal check lived only in
    ``_PooledSession`` an authenticated caller with no principal header would
    still reach them. The gate is composed into the tracer, which every
    registration applies to every tool, so "no principal, no tool call" is
    true by construction.
    """
    _publish_two_source_graph(clean_runtime)
    _configure_header_identity(
        clean_runtime,
        principals={PRINCIPAL_A: SUBJECT_A, PRINCIPAL_B: SUBJECT_B},
    )
    _activate_two_principal_policy(clean_runtime)
    monkeypatch.setenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", "1")
    monkeypatch.setenv("OKG_NOMOS_SUBJECT", "subject:process-wide-operator")

    session_less = [
        ("inspect", {"target": {"kind": "surface"}}),
        ("inspect", {"target": {"kind": "record_prefixes"}}),
        ("inspect", {"target": {"kind": "record_templates"}}),
    ]

    def _refused_without_a_bound_principal(mcp: Any, tool_name: str, arguments: Any) -> None:
        # No header at all.
        with request_headers({}):
            with pytest.raises(SubstrateError) as excinfo:
                mcp._tool_manager._tools[tool_name].fn(**arguments)
        assert (
            refusal_reason_code(excinfo.value) == UNIDENTIFIED_PRINCIPAL_REASON
        ), arguments
        # A WELL-FORMED header naming a principal bound to nothing. This is
        # the arm a shape-only check would pass: the requirement is about
        # binding to a Nomos subject, not about syntax.
        with request_headers({PRINCIPAL_HEADER: "nobody@example.edu"}):
            with pytest.raises(SubstrateError) as unbound:
                mcp._tool_manager._tools[tool_name].fn(**arguments)
        assert (
            refusal_reason_code(unbound.value) == UNIDENTIFIED_PRINCIPAL_REASON
        ), arguments
        assert unbound.value.data["channel"] == "no_subject_binding"

    # The record kinds are withheld by design (effective_surface.py): they
    # are served only where the `record` tool -- a write tool -- is served,
    # and a multi-user launch refuses the write surface. `surface` is served
    # on every launch since #3666 (M10), read-only without a ceiling, and is
    # still refused to a caller with no bound principal.
    surface = session_less[0]
    record_kinds = session_less[1:]

    def _withheld_after_identity(mcp: Any, tool_name: str, arguments: Any) -> None:
        # The principal gate runs FIRST: without a bound principal the refusal
        # is the identity one, not the withheld-kind one, so the kind gate
        # never stands in for the principal check.
        _refused_without_a_bound_principal(mcp, tool_name, arguments)
        # With a bound principal the call gets past identity and is then
        # refused BY NAME as withheld on this launch.
        with request_headers({PRINCIPAL_HEADER: PRINCIPAL_A}):
            with pytest.raises(InspectKindNotServed):
                mcp._tool_manager._tools[tool_name].fn(**arguments)

    with _multi_user_server(
        clean_runtime, tool_profile="discovery", tool_profile_ceiling="reader"
    ) as ceiling_mcp:
        _refused_without_a_bound_principal(ceiling_mcp, *surface)
        # Positive control: the same session-less call succeeds with a BOUND
        # principal, so the refusal is about identity, not the arguments.
        with request_headers({PRINCIPAL_HEADER: PRINCIPAL_A}):
            assert ceiling_mcp._tool_manager._tools[surface[0]].fn(**surface[1])
        for tool_name, arguments in record_kinds:
            _withheld_after_identity(ceiling_mcp, tool_name, arguments)

    with _multi_user_server(clean_runtime) as mcp:
        _refused_without_a_bound_principal(mcp, *surface)
        with request_headers({PRINCIPAL_HEADER: PRINCIPAL_A}):
            listing = mcp._tool_manager._tools[surface[0]].fn(**surface[1])
        assert listing["surface"] == "read_only"
        for tool_name, arguments in record_kinds:
            _withheld_after_identity(mcp, tool_name, arguments)
        # The change-observation tools build a Session directly through
        # `_open_logical` rather than the pooled path, and are gated too.
        with request_headers({PRINCIPAL_HEADER: "nobody@example.edu"}):
            with pytest.raises(SubstrateError) as logical:
                mcp._tool_manager._tools["inspect"].fn(
                    target={"kind": "admission"}
                )
        assert (
            refusal_reason_code(logical.value) == UNIDENTIFIED_PRINCIPAL_REASON
        )
        # And so are the MCP resources, which are not tools and so are not
        # covered by the tracer gate.
        with request_headers({}):
            with pytest.raises(SubstrateError) as digest:
                mcp._okg_build_digest_gated()
        assert (
            refusal_reason_code(digest.value) == UNIDENTIFIED_PRINCIPAL_REASON
        )

    rows = _decision_rows(clean_runtime)
    refusals = [
        row
        for row in rows
        if row["reason_code"] == UNIDENTIFIED_PRINCIPAL_REASON
    ]
    assert refusals, "a refused tool call must leave an audit row"
    assert {row["subject"] for row in refusals} == {UNIDENTIFIED_SUBJECT}
    assert "subject:process-wide-operator" not in {
        row["subject"] for row in rows
    }


@pytest.mark.postgres
@pytest.mark.serial_db
def test_multi_user_start_refused_with_the_write_surface(
    clean_runtime: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Per-user reads must not sit next to shared-principal writes.

    The MCP write path still resolves identity from the caller-supplied
    `authority` argument and from a build-time environment subject -- the two
    things this PACT closes on the read path. Rather than serve a server that
    is per-user in one direction and process-wide in the other, refuse the
    combination and say why.
    """
    monkeypatch.setenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", "1")
    monkeypatch.delenv("OKG_MCP_ALLOW_WRITES", raising=False)
    with psycopg.connect(clean_runtime) as conn:
        upsert_identity_provider(
            conn, deployment_name=DEPLOYMENT, provider=_provider()
        )
        conn.commit()
    with pytest.raises(SubstrateError, match="write surface"):
        build_server(
            dsn=clean_runtime,
            deployment_name=DEPLOYMENT,
            multi_user_principals=True,
            principal_mapping=MAPPING,
            allow_writes=True,
        )
    # The ENVIRONMENT route is the real bypass question: build_server ORs
    # OKG_MCP_ALLOW_WRITES into allow_writes, so a refusal keyed on the
    # keyword argument alone would be a refusal anyone could step around.
    monkeypatch.setenv("OKG_MCP_ALLOW_WRITES", "1")
    with pytest.raises(SubstrateError, match="write surface"):
        build_server(
            dsn=clean_runtime,
            deployment_name=DEPLOYMENT,
            multi_user_principals=True,
            principal_mapping=MAPPING,
        )
    monkeypatch.delenv("OKG_MCP_ALLOW_WRITES")
    # Positive control: read-only multi-user starts.
    mcp = build_server(
        dsn=clean_runtime,
        deployment_name=DEPLOYMENT,
        multi_user_principals=True,
        principal_mapping=MAPPING,
    )
    try:
        assert mcp._okg_write_pool is None
    finally:
        mcp._okg_pool.close()


@pytest.mark.postgres
@pytest.mark.serial_db
def test_multi_user_start_refused_without_declared_mapping(
    clean_runtime: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", "1")
    with pytest.raises(SubstrateError, match="nomos.principal_mapping"):
        build_server(
            dsn=clean_runtime,
            deployment_name=DEPLOYMENT,
            multi_user_principals=True,
            principal_mapping=None,
        )


@pytest.mark.postgres
@pytest.mark.serial_db
def test_multi_user_start_refused_when_provider_lacks_verifier_metadata(
    clean_runtime: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The load-time provider refusal, proven through the real start path."""
    monkeypatch.setenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", "1")
    with psycopg.connect(clean_runtime) as conn:
        upsert_identity_provider(
            conn, deployment_name=DEPLOYMENT, provider=_provider(metadata={})
        )
        conn.commit()
    with pytest.raises(SubstrateError, match="trusted_verifiers"):
        build_server(
            dsn=clean_runtime,
            deployment_name=DEPLOYMENT,
            multi_user_principals=True,
            principal_mapping=MAPPING,
        )

    # Positive control: with the metadata set, the same start succeeds.
    with psycopg.connect(clean_runtime) as conn:
        upsert_identity_provider(
            conn, deployment_name=DEPLOYMENT, provider=_provider()
        )
        conn.commit()
    mcp = build_server(
        dsn=clean_runtime,
        deployment_name=DEPLOYMENT,
        multi_user_principals=True,
        principal_mapping=MAPPING,
    )
    try:
        assert mcp._okg_multi_user_principals is True
        assert mcp._okg_principal_mapping == MAPPING
    finally:
        mcp._okg_pool.close()
        if mcp._okg_write_pool:
            mcp._okg_write_pool.close()


@pytest.mark.postgres
@pytest.mark.serial_db
def test_multi_user_start_refused_when_nomos_runtime_is_off(
    clean_runtime: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Per-user principals with no enforcement would decide nothing."""
    monkeypatch.setenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", "0")
    with pytest.raises(SubstrateError, match="runtime enforcement"):
        build_server(
            dsn=clean_runtime,
            deployment_name=DEPLOYMENT,
            multi_user_principals=True,
            principal_mapping=MAPPING,
        )


# ==========================================================================
# Single-operator regression: the unconfigured path is unchanged
# ==========================================================================


@pytest.mark.postgres
@pytest.mark.serial_db
def test_single_operator_explicit_env_subject_precedence_unchanged(
    clean_runtime: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    from okg.substrate.mcp.server import _resolve_mcp_read_subject

    monkeypatch.setenv("OKG_NOMOS_SUBJECT", "explicit-operator")
    with psycopg.connect(clean_runtime) as conn:
        assert (
            _resolve_mcp_read_subject(conn, deployment_name=DEPLOYMENT)
            == "subject:explicit-operator"
        )
    monkeypatch.delenv("OKG_NOMOS_SUBJECT")
    monkeypatch.setenv("OKG_MCP_NOMOS_SUBJECT", "subject:already-prefixed")
    with psycopg.connect(clean_runtime) as conn:
        assert (
            _resolve_mcp_read_subject(conn, deployment_name=DEPLOYMENT)
            == "subject:already-prefixed"
        )
    monkeypatch.delenv("OKG_MCP_NOMOS_SUBJECT")
    monkeypatch.setenv("OKG_PRINCIPAL", "principal-operator")
    with psycopg.connect(clean_runtime) as conn:
        assert (
            _resolve_mcp_read_subject(conn, deployment_name=DEPLOYMENT)
            == "subject:principal-operator"
        )


@pytest.mark.postgres
@pytest.mark.serial_db
def test_single_operator_baseline_mcp_client_grant_unchanged(
    clean_runtime: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The baseline grant path still yields subject:mcp_client when present."""
    from okg.substrate.mcp import server as server_module

    for name in ("OKG_NOMOS_SUBJECT", "OKG_MCP_NOMOS_SUBJECT", "OKG_PRINCIPAL"):
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setattr(
        "okg.substrate.nomos.baseline_default_mcp_subject",
        lambda conn, *, deployment_name, scope: "subject:mcp_client",
    )
    with psycopg.connect(clean_runtime) as conn:
        assert (
            server_module._resolve_mcp_read_subject(
                conn, deployment_name=DEPLOYMENT
            )
            == "subject:mcp_client"
        )


@pytest.mark.postgres
@pytest.mark.serial_db
def test_single_operator_legacy_user_default_unchanged(
    clean_runtime: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    from okg.substrate.mcp import server as server_module

    for name in ("OKG_NOMOS_SUBJECT", "OKG_MCP_NOMOS_SUBJECT", "OKG_PRINCIPAL"):
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setenv("USER", "legacy-user")
    monkeypatch.setattr(
        "okg.substrate.nomos.baseline_default_mcp_subject",
        lambda conn, *, deployment_name, scope: None,
    )
    with psycopg.connect(clean_runtime) as conn:
        assert (
            server_module._resolve_mcp_read_subject(
                conn, deployment_name=DEPLOYMENT
            )
            == "subject:legacy-user"
        )


@pytest.mark.postgres
@pytest.mark.serial_db
def test_single_operator_request_without_principal_header_is_not_refused(
    clean_runtime: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    """An unconfigured server serves a header-less request exactly as before."""
    _publish_two_source_graph(clean_runtime)
    _activate_single_operator_policy(clean_runtime)
    monkeypatch.setenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", "1")
    monkeypatch.setenv("OKG_NOMOS_SUBJECT", "subject:solo-operator")

    mcp = build_server(dsn=clean_runtime, deployment_name=DEPLOYMENT)
    try:
        assert mcp._okg_multi_user_principals is False
        assert mcp._okg_principal_mapping is None
        with request_headers({}):
            result = _filter_nodes(mcp)
    finally:
        mcp._okg_pool.close()
        if mcp._okg_write_pool:
            mcp._okg_write_pool.close()

    assert {item["node_id"] for item in result["items"]} == {"node:public-docs"}
    subjects = {row["subject"] for row in _decision_rows(clean_runtime)}
    assert "subject:solo-operator" in subjects
    assert UNIDENTIFIED_SUBJECT not in subjects


@pytest.mark.postgres
@pytest.mark.serial_db
def test_single_operator_authority_identity_claim_channel_unchanged(
    clean_runtime: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The POSITIVE CONTROL for the channel-conflict refusal.

    ``test_multi_user_rejects_caller_supplied_authority_identity_claim``
    proves ``authority.nomos_identity_claim`` is refused in multi-user mode.
    On its own that arm cannot tell a MODE-GATED refusal from a blanket one:
    an implementation that rejected the authority dict unconditionally would
    pass it just as well, and would silently break every single-operator
    caller that uses the channel today. req.identity-channel-precedence says
    the opposite in as many words -- "In single-operator mode the existing
    authority-dict behavior is unchanged" -- so the OFF branch of the gate
    (server.py:1603 for the tool gate, server.py:2041 for the session) needs
    its own arm. Every other test in this file supplies
    ``multi_user_principals=True``, so without this one the OFF branch is
    never executed.

    "Unchanged" is asserted as CONSUMED, not merely as un-refused: a channel
    that had been quietly disabled would also fail to raise. The claim must
    still bind the row subject, which is what the pre-change read path did
    (read_filter.py:328-375).
    """
    _publish_two_source_graph(clean_runtime)
    _configure_header_identity(
        clean_runtime,
        principals={PRINCIPAL_A: SUBJECT_A, PRINCIPAL_B: SUBJECT_B},
        drive_subjects=(SUBJECT_A,),
    )
    _activate_two_principal_policy(clean_runtime)
    monkeypatch.setenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", "1")
    # Distinct from the claim's subject on purpose: if the claim were ignored
    # and the process environment answered instead, the subject assertions
    # below would see this value.
    monkeypatch.setenv("OKG_NOMOS_SUBJECT", "subject:process-wide-operator")
    # PACT mcp-oidc-principal-policy made the single-operator channel an
    # explicit launch-time opt-in (unset, the claim is refused as
    # nomos_identity_claim_untrusted; test_oidc_principal_policy.py covers
    # that arm). With the opt-in the channel is consumed exactly as before.
    monkeypatch.setenv("OKG_MCP_TRUST_CALLER_IDENTITY_CLAIMS", "1")

    stamped_at = datetime.now(UTC).isoformat()
    self_asserted = {
        "nomos_identity_claim": {
            "provider_id": PROVIDER_ID,
            "issuer": ISSUER,
            "audiences": [AUDIENCE],
            "external_subject": PRINCIPAL_A,
            "claims": {"email": PRINCIPAL_A},
            "verification_status": "valid",
            "verified_by": TRANSPORT_VERIFIER,
            "verified_at": stamped_at,
            "auth_time": stamped_at,
        }
    }

    mcp = build_server(dsn=clean_runtime, deployment_name=DEPLOYMENT)
    try:
        # The unconfigured server is genuinely in single-operator mode.
        assert mcp._okg_multi_user_principals is False
        assert mcp._okg_principal_mapping is None
        with request_headers({}):
            result = mcp._tool_manager._tools["filter"].fn(
                input={"kind": "nodes"},
                predicate={},
                select=["node_id", "attrs"],
                limit=50,
                authority=self_asserted,
            )
    finally:
        mcp._okg_pool.close()
        if mcp._okg_write_pool:
            mcp._okg_write_pool.close()

    rows = _decision_rows(clean_runtime)
    # 1. Not refused: the multi-user-only channel conflict did not leak into
    #    single-operator mode, on the raise path or in the audit trail.
    assert not [
        row
        for row in rows
        if row["reason_code"] == CONFLICTING_IDENTITY_CHANNEL_REASON
    ]
    assert UNIDENTIFIED_SUBJECT not in {row["subject"] for row in rows}
    # 2. Still CONSUMED: the claim was evaluated as identity, which is the
    #    half a "did not raise" assertion cannot see.
    identity_reads = [
        row
        for row in rows
        if "source_lineage_identity_filter" in str(row["enforcement_point"])
    ]
    assert identity_reads, (
        "the authority identity claim was never evaluated; the channel is "
        "disabled, not unchanged"
    )
    # 3. And it BOUND the subject -- the claim's subject, not the process
    #    environment's.
    assert {row["subject"] for row in identity_reads} == {SUBJECT_A}
    assert "subject:process-wide-operator" not in {
        row["subject"] for row in identity_reads
    }
    # 4. The call returned real rows, so the arm is not passing on an empty
    #    result set that would satisfy every assertion above vacuously.
    assert {item["node_id"] for item in result["items"]} == {
        "node:public-docs",
        "node:private-drive",
    }


def _activate_single_operator_policy(dsn: str) -> None:
    subject = "subject:solo-operator"
    bundle = compile_policy_graph(
        {
            "policy_bundle_id": "nomos:test:mcp-single-operator",
            "policy_version": "v1",
            "default_effect": "deny",
            "rules": _boundary_rules(subject, "solo")
            + [
                {
                    "rule_id": "allow_public_docs_solo",
                    "effect": "allow",
                    "actions": ["graph.read.mcp"],
                    "subjects": [subject],
                    "match": {
                        "deployment": DEPLOYMENT,
                        "scope": SCOPE,
                        "source_id": "public_docs",
                    },
                    "priority": 100,
                }
            ],
        }
    )
    with psycopg.connect(dsn) as conn:
        activate_policy_bundle(
            conn,
            deployment_name=DEPLOYMENT,
            bundle=bundle,
            scope=SCOPE,
            activated_by="test:operator",
        )
        conn.commit()



def _activate_latency_policy(dsn: str) -> None:
    """One bundle granting the solo operator and both header principals."""
    rules: list[dict[str, Any]] = []
    for subject, suffix in (
        ("subject:solo-operator", "solo"),
        (SUBJECT_A, "a"),
        (SUBJECT_B, "b"),
    ):
        rules += _boundary_rules(subject, suffix)
        for source_id in ("public_docs", "google_drive"):
            rules.append(
                {
                    "rule_id": f"allow_{source_id}_{suffix}",
                    "effect": "allow",
                    "actions": ["graph.read.mcp"],
                    "subjects": [subject],
                    "match": {
                        "deployment": DEPLOYMENT,
                        "scope": SCOPE,
                        "source_id": source_id,
                    },
                    "priority": 100,
                }
            )
    bundle = compile_policy_graph(
        {
            "policy_bundle_id": "nomos:test:mcp-principal-latency",
            "policy_version": "v1",
            "default_effect": "deny",
            "rules": rules,
        }
    )
    with psycopg.connect(dsn) as conn:
        activate_policy_bundle(
            conn,
            deployment_name=DEPLOYMENT,
            bundle=bundle,
            scope=SCOPE,
            activated_by="test:operator",
        )
        conn.commit()

# ==========================================================================
# Latency: the per-call identity cost, measured
# ==========================================================================


MULTI_USER_OVERHEAD_BOUND_MS = 50.0
# The figures the latency log line reports, in order. The first is the gated
# statistic; the rest are diagnostics.
LATENCY_FIGURES = (
    "paired_difference_p50_ms",
    "paired_difference_p95_ms",
    "single_operator_p50_ms",
    "single_operator_p95_ms",
    "multi_user_p50_ms",
    "multi_user_p95_ms",
    "p95_overhead_ms",
)


def _latency_verdict(paired_p50_ms: float | None, bound_ms: float) -> str:
    """The ONE place the gate is decided; the assert and the log line use it.

    ``incomplete`` means the measurement raised before the gated statistic
    existed, so there is nothing to compare.
    """
    if paired_p50_ms is None:
        return "incomplete"
    return "pass" if paired_p50_ms <= bound_ms else "fail"


def _latency_log_line(figures: dict[str, float | None], *, bound_ms: float) -> str:
    """One line naming every figure and the verdict, pass, fail or incomplete.

    Emitted on EVERY run (issue #2555 decision 2a): the gate sat at 50.32 ms
    against 50 ms on one docker pod and passed on another with the same code,
    and passing runs left no number behind, so no margin could be derived from
    data. A figure the run never reached is written as ``missing``.
    """
    verdict = _latency_verdict(figures.get(LATENCY_FIGURES[0]), bound_ms)
    fields = " ".join(
        f"{name}=missing" if figures.get(name) is None else f"{name}={figures[name]:.2f}"
        for name in LATENCY_FIGURES
    )
    return (
        "test_multi_user_latency_overhead_within_bound "
        f"verdict={verdict} bound_ms={bound_ms:.2f} {fields}"
    )


def _emit_latency_line(
    figures: dict[str, float | None],
    *,
    bound_ms: float,
    record_property: Any,
    capsys: pytest.CaptureFixture[str],
) -> str:
    """Write the line where a reader of a PASSING CI run can find it.

    scripts/qa.py runs the perf lane with stdout piped and prints its tail only
    when a step fails, so stdout alone never reaches a passing job's log.
    GITHUB_STEP_SUMMARY (passed through by qa.py, which inherits os.environ) is
    the job summary GitHub shows for every run. Locally the stdout print, with
    pytest's capture disabled, is what a reader sees. The same line is also
    the junit property ``latency_log_line``.
    """
    line = _latency_log_line(figures, bound_ms=bound_ms)
    record_property("latency_log_line", line)
    with capsys.disabled():
        print(f"\n[mcp multi-user latency] {line}", flush=True)
    summary = os.environ.get("GITHUB_STEP_SUMMARY")
    if summary:
        with open(summary, "a", encoding="utf-8") as handle:
            handle.write(f"mcp multi-user latency: `{line}`\n\n")
    return line


def test_latency_log_line_names_the_verdict_and_every_figure() -> None:
    """KILLS: a log line that drops a figure, or a verdict that drifts from
    the gate. All seven figures are passed and each must appear."""
    values = dict(zip(LATENCY_FIGURES, (50.3185, 71.0, 41.7, 164.7, 53.05, 225.27, 60.57)))
    line = _latency_log_line(values, bound_ms=MULTI_USER_OVERHEAD_BOUND_MS)
    assert "verdict=fail" in line
    assert "bound_ms=50.00" in line
    for name, value in values.items():
        assert f"{name}={value:.2f}" in line, name
    assert "\n" not in line
    assert _latency_verdict(50.0, 50.0) == "pass"
    assert _latency_verdict(50.01, 50.0) == "fail"


def test_latency_log_line_marks_missing_figures() -> None:
    """KILLS: a measurement that raised leaving no line, or a line that
    invents a number it never measured."""
    partial = dict.fromkeys(LATENCY_FIGURES)
    partial["single_operator_p50_ms"] = 41.7
    line = _latency_log_line(partial, bound_ms=50.0)
    assert "verdict=incomplete" in line
    assert "single_operator_p50_ms=41.70" in line
    for name in LATENCY_FIGURES:
        if name != "single_operator_p50_ms":
            assert f"{name}=missing" in line, name


def test_emit_latency_line_appends_to_the_step_summary(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """KILLS: the line reaching stdout only, which a passing CI run hides."""
    summary = tmp_path / "summary.md"
    summary.write_text("earlier step\n", encoding="utf-8")
    monkeypatch.setenv("GITHUB_STEP_SUMMARY", str(summary))
    recorded: dict[str, str] = {}
    line = _emit_latency_line(
        dict.fromkeys(LATENCY_FIGURES),
        bound_ms=50.0,
        record_property=recorded.__setitem__,
        capsys=capsys,
    )
    text = summary.read_text(encoding="utf-8")
    assert text.startswith("earlier step\n"), "the summary is appended to"
    assert line in text
    assert recorded["latency_log_line"] == line


@pytest.mark.postgres
@pytest.mark.serial_db
@pytest.mark.perf
def test_multi_user_latency_overhead_within_bound(
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
    record_property: Any,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Gate median paired overhead; report p50/p95 and decision-row deltas.

    Hermetic harness, not the live okg-workspace arm: the bound is on the
    OVERHEAD of multi-user over single-operator for one read-operator call,
    which is a difference of two measurements taken on the same machine, the
    same graph, and the same process. The methodology is recorded on the test
    so the number is reproducible rather than asserted.
    """
    # Every figure starts missing; the finally below emits whatever exists,
    # so a run that raises mid-measurement still leaves a line behind.
    figures: dict[str, float | None] = dict.fromkeys(LATENCY_FIGURES)
    try:
        iterations = 60
        warmup = 5
        _publish_two_source_graph(clean_runtime)
        _configure_header_identity(
            clean_runtime,
            principals={PRINCIPAL_A: SUBJECT_A, PRINCIPAL_B: SUBJECT_B},
            drive_subjects=(SUBJECT_A,),
        )
        # ONE activation covering both modes' subjects, and it happens BEFORE
        # enforcement is switched on: `policy.activate` is itself Nomos-governed,
        # so activating under enforcement with no active bundle is denied.
        _activate_latency_policy(clean_runtime)
        monkeypatch.setenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", "1")
        monkeypatch.setenv("OKG_NOMOS_SUBJECT", "subject:solo-operator")

        def _decision_count() -> int:
            with psycopg.connect(clean_runtime) as conn, conn.cursor() as cur:
                cur.execute(
                    "SELECT count(*) FROM okg.nomos_decisions "
                    "WHERE deployment_name = %s",
                    (DEPLOYMENT,),
                )
                return int(cur.fetchone()[0])

        def _percentile(samples: list[float], fraction: float) -> float:
            ordered = sorted(samples)
            rank = max(1, min(len(ordered), int(-(-fraction * len(ordered) // 1))))
            return ordered[rank - 1]

        solo = build_server(dsn=clean_runtime, deployment_name=DEPLOYMENT)
        solo_samples: list[float] = []
        multi_samples: list[float] = []
        paired: list[float] = []
        try:
            with _multi_user_server(clean_runtime) as mcp:

                def _solo_call() -> None:
                    with request_headers({}):
                        _filter_nodes(solo)

                def _multi_call() -> None:
                    with request_headers({PRINCIPAL_HEADER: PRINCIPAL_A}):
                        _filter_nodes(mcp)

                for _ in range(warmup):
                    _solo_call()
                    _multi_call()
                before = _decision_count()
                for index in range(iterations):
                    # Alternate which mode goes first so neither systematically
                    # pays for the other's cache warming.
                    order = (
                        (_solo_call, _multi_call)
                        if index % 2 == 0
                        else (_multi_call, _solo_call)
                    )
                    timings: dict[Any, float] = {}
                    for call in order:
                        start = time.perf_counter()
                        call()
                        timings[call] = (time.perf_counter() - start) * 1000.0
                    solo_samples.append(timings[_solo_call])
                    multi_samples.append(timings[_multi_call])
                    paired.append(timings[_multi_call] - timings[_solo_call])
                after = _decision_count()
        finally:
            solo._okg_pool.close()
            if solo._okg_write_pool:
                solo._okg_write_pool.close()

        solo_p50 = statistics.median(solo_samples)
        solo_p95 = _percentile(solo_samples, 0.95)
        multi_p50 = statistics.median(multi_samples)
        multi_p95 = _percentile(multi_samples, 0.95)
        overhead_p95 = multi_p95 - solo_p95
        paired_p95 = _percentile(paired, 0.95)
        paired_p50 = statistics.median(paired)
        figures.update(
            paired_difference_p50_ms=paired_p50,
            paired_difference_p95_ms=paired_p95,
            single_operator_p50_ms=solo_p50,
            single_operator_p95_ms=solo_p95,
            multi_user_p50_ms=multi_p50,
            multi_user_p95_ms=multi_p95,
            p95_overhead_ms=overhead_p95,
        )
        # Single-operator writes no NEW decision rows after warmup: identical
        # repeat calls are content-addressed and dedup. Multi-user does, because
        # the per-request verified_at stamp is part of the claim and therefore of
        # the decision input -- each request's delegation is a separate audit
        # fact. Measured and recorded here, per the requirement; NOT asserted as
        # an arithmetic contract.
        rows_delta_total = after - before

        # req.latency-bound asks for the row delta "in each mode", which the
        # interleaved window cannot separate. Measure it in a short un-timed loop
        # per mode afterwards, on the already-warm server.
        def _rows_per_call(call: Any, count: int = 10) -> float:
            opening = _decision_count()
            for _ in range(count):
                call()
            return (_decision_count() - opening) / count

        solo = build_server(dsn=clean_runtime, deployment_name=DEPLOYMENT)
        try:
            with _multi_user_server(clean_runtime) as mcp:
                solo_rows_per_call = _rows_per_call(
                    lambda: _run_with_headers({}, lambda: _filter_nodes(solo))
                )
                multi_rows_per_call = _rows_per_call(
                    lambda: _run_with_headers(
                        {PRINCIPAL_HEADER: PRINCIPAL_A},
                        lambda: _filter_nodes(mcp),
                    )
                )
        finally:
            solo._okg_pool.close()
            if solo._okg_write_pool:
                solo._okg_write_pool.close()
        record_property(
            "single_operator_decision_rows_per_call", solo_rows_per_call
        )
        record_property("multi_user_decision_rows_per_call", multi_rows_per_call)

        record_property("single_operator_p50_ms", solo_p50)
        record_property("single_operator_p95_ms", solo_p95)
        record_property("multi_user_p50_ms", multi_p50)
        record_property("multi_user_p95_ms", multi_p95)
        record_property("p50_overhead_ms", multi_p50 - solo_p50)
        record_property("p95_overhead_ms", overhead_p95)
        record_property("paired_difference_p95_ms", paired_p95)
        record_property("paired_difference_p50_ms", paired_p50)
        record_property("median_paired_overhead_bound_ms", MULTI_USER_OVERHEAD_BOUND_MS)
        record_property("p95_gate_status", "diagnostic_only")
        record_property(
            "decision_rows_per_interleaved_iteration",
            rows_delta_total / iterations,
        )
        record_property(
            "methodology",
            f"{iterations} interleaved iterations after {warmup} warmup pairs; "
            "one `filter` read-operator call per mode per iteration, both servers "
            "open in one process against one published generation and one policy "
            "bundle, with the per-iteration order alternated. p95 is nearest-rank "
            "over the interleaved samples. Read paired_difference_p50_ms as the "
            "central estimate and the <= 50 ms contract statistic: it is stable "
            "across runs. p95_overhead_ms is a required diagnostic and a "
            "difference "
            "of two order statistics measured under shared machine noise -- "
            "interleaving makes a hiccup land in both arms with equal "
            "probability, so the difference is unbiased but has wide run-to-run "
            "variance in BOTH directions. paired_difference_p95_ms is the p95 of "
            "the per-iteration (multi - solo) difference; it is an upper bound "
            "dominated by whichever arm absorbed a hiccup, NOT a robust "
            "estimator, and is recorded for that reason rather than asserted on. "
            "The per-mode decision-row deltas come from a separate 10-call "
            "un-timed loop per mode on the already-warm servers, because the "
            "interleaved timing window cannot attribute a row to a mode. "
            "Single-operator repeat calls dedup to zero new rows; multi-user does "
            "NOT, because the per-request verified_at stamp enters the claim "
            "fingerprint and therefore the decision input_hash -- each request's "
            "delegation is a separate audit fact, and the content-address dedup "
            "that absorbs repeat reads (issue #635) does not apply to it. "
            "Measured and recorded per the requirement; NOT asserted as an "
            "arithmetic contract. Hermetic harness, not the live okg-workspace "
            "arm.",
        )
    finally:
        _emit_latency_line(
            figures,
            bound_ms=MULTI_USER_OVERHEAD_BOUND_MS,
            record_property=record_property,
            capsys=capsys,
        )
    # Gate the stable central estimate. Tail latency remains visible above,
    # but is diagnostic because the difference of order statistics is highly
    # sensitive to which arm absorbs a shared-runner hiccup.
    assert _latency_verdict(paired_p50, MULTI_USER_OVERHEAD_BOUND_MS) == "pass", (
        f"median per-call multi-user overhead {paired_p50:.2f} ms exceeds the "
        f"{MULTI_USER_OVERHEAD_BOUND_MS:g} ms bound (single-operator p95 "
        f"{solo_p95:.2f} ms, multi-user "
        f"p95 {multi_p95:.2f} ms, diagnostic p95 overhead "
        f"{overhead_p95:.2f} ms); reduce the cost or record an explicit "
        "operator decision to change the bound"
    )
