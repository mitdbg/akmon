"""The refusal codes PACT mcp-oidc-principal-policy added are registered.

Kept apart from test_oidc_principal_policy.py on purpose: that file runs in a
PostgreSQL lane, whose scope closure does not include the error-code
registry, so it must not read the registry by path. This file needs no
database.
"""
from __future__ import annotations

from pathlib import Path

import pytest
import yaml

REGISTRY = Path(__file__).resolve().parents[3] / "docs/reference/error_codes.yaml"


@pytest.mark.parametrize(
    "code",
    ["nomos_principal_blocked", "nomos_identity_claim_untrusted", "mcp_oidc_auth_audit_exposed"],
)
def test_new_refusal_codes_are_registered_with_their_data(code: str) -> None:
    from okg.substrate.mcp.oidc_auth import oidc_auth_audit_exposed_error
    from okg.substrate.mcp.principal_mapping import blocked_principal_error
    from okg.substrate.mcp.server import caller_identity_claim_untrusted_error

    error = {
        "nomos_principal_blocked": blocked_principal_error,
        "nomos_identity_claim_untrusted": caller_identity_claim_untrusted_error,
        "mcp_oidc_auth_audit_exposed": lambda: oidc_auth_audit_exposed_error(
            "oidc-principal-policy", host="0.0.0.0",
        ),
    }[code]()
    assert error.code == code
    entries = {row["code"]: row for row in yaml.safe_load(REGISTRY.read_text())["codes"]}
    assert set(entries[code]["data_shape"]) == set(error.data)
