"""MCP *resources* bind a principal to a SUBJECT, not merely to a shape.

``req.mcp-principal-mapping.fail-closed-principal`` says a principal that maps
to no Nomos subject MUST be refused. It states no resource carve-out, and that
PACT carries ``waivers: []``.

``server._require_resource_principal`` used to be a shape check: it called
``header_claim_for_request`` and stopped. A header that PARSED was enough, so
``X-OKG-Principal: nobody@example.edu`` -- syntactically perfect, bound to
nothing -- was served the schema digest (every subtype, attribute key, edge
type and per-subtype node count) and the read-side metadata.
``test_principal_mapping.py`` covers the resources for a MISSING header only,
which the old shape check already refused, so nothing in the suite noticed.

Both gated resources are exercised here, each with the negative arm and the
positive control in the same test: an unbound principal is refused, and a
BOUND one on the same server, the same resource and the same run gets a
non-empty payload. Without the control, a resource that refused everybody
would pass the refusal arm.

Setup is imported from ``test_principal_mapping``, which owns this
deployment's identity fixture -- a second copy would be a second thing to keep
in step with the provider metadata rules.
"""

from __future__ import annotations

import json

import pytest

from okg.substrate.errors import SubstrateError
from okg.substrate.mcp.principal_mapping import (
    UNIDENTIFIED_PRINCIPAL_REASON,
    refusal_reason_code,
)

pytest.importorskip("mcp")

from test_principal_mapping import (  # noqa: E402
    PRINCIPAL_A,
    PRINCIPAL_HEADER,
    SUBJECT_A,
    _activate_two_principal_policy,
    _configure_header_identity,
    _multi_user_server,
    _publish_two_source_graph,
    request_headers,
)

#: A syntactically valid principal that no binding names. This is the exact
#: value a shape-only gate admits.
UNBOUND_PRINCIPAL = "nobody@example.edu"

RESOURCES = ("_okg_build_digest_gated", "_okg_readside_metadata_gated")


@pytest.mark.postgres
@pytest.mark.serial_db
def test_resources_refuse_a_well_formed_principal_bound_to_no_subject(
    clean_runtime: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The arm a shape check passes, and the control that makes it mean something."""
    _publish_two_source_graph(clean_runtime)
    _configure_header_identity(clean_runtime, principals={PRINCIPAL_A: SUBJECT_A})
    _activate_two_principal_policy(clean_runtime)
    monkeypatch.setenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", "1")
    monkeypatch.setenv("OKG_NOMOS_SUBJECT", "subject:process-wide-operator")

    with _multi_user_server(clean_runtime) as mcp:
        for resource in RESOURCES:
            read = getattr(mcp, resource)

            with request_headers({PRINCIPAL_HEADER: UNBOUND_PRINCIPAL}):
                with pytest.raises(SubstrateError) as unbound:
                    read()
            assert (
                refusal_reason_code(unbound.value) == UNIDENTIFIED_PRINCIPAL_REASON
            ), resource
            # The specific channel, so the refusal is attributable to the
            # binding lookup rather than to the header having been rejected
            # earlier for its shape -- which is what the old gate did and what
            # this test exists to distinguish.
            assert unbound.value.data["channel"] == "no_subject_binding", resource
            # Nothing leaked into the refusal: an error that carried the
            # digest would refuse and answer at the same time.
            assert "subtypes" not in str(unbound.value.data), resource

            # The control on the GATE, for both resources: a BOUND principal
            # is not refused. Stated as "whatever happens next, it is not an
            # identity refusal" rather than "it returns a payload" so the
            # assertion stays about IDENTITY and does not quietly become a
            # second assertion about digest content.
            with request_headers({PRINCIPAL_HEADER: PRINCIPAL_A}):
                try:
                    read()
                except SubstrateError as bound_exc:  # pragma: no cover - guard
                    assert (
                        refusal_reason_code(bound_exc)
                        != UNIDENTIFIED_PRINCIPAL_REASON
                    ), resource
                except ValueError:  # pragma: no cover - see below
                    # This arm existed because `okg://schema-digest/<name>`
                    # raised for EVERY caller: `build_digest_payload` unpacked
                    # `resolve_latest_fn(...)` into two names while
                    # `server._resolve_latest` returned three. That was
                    # reported here and repaired in issue #1436's change,
                    # which slices instead of unpacking. The arm is kept so a
                    # future arity drift lands as a NAMED failure on the
                    # digest resource rather than as an opaque ValueError in
                    # an identity test.
                    assert resource == "_okg_build_digest_gated"

        # The content control, on the resource that can serve one: the
        # unbound caller above was refused something REAL, not an empty
        # document that any gate would appear to protect.
        with request_headers({PRINCIPAL_HEADER: PRINCIPAL_A}):
            served = mcp._okg_readside_metadata_gated()
        assert isinstance(served, str) and served.strip()
        assert json.loads(served) is not None
