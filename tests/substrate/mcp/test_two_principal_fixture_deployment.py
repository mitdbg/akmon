"""The committed fixture deployment declares the per-user identity contract.

Slice S0 of #1180. `okg mcp-serve --multi-user-principals` refuses to start
unless the deployment it serves declares three things: Nomos runtime
enforcement (otherwise per-user principals would be resolved and then ignored),
a read posture admitting the mapping's principal kind, and a
`nomos.principal_mapping` block naming the header that carries the signed-in
user. No deployment in the tree declared them -- every fixture leaves runtime
enforcement off -- so the real-process proof in
``test_multi_user_http_e2e.py`` had nothing to serve.

These assertions are hermetic: they read the committed manifest through the
same functions the server calls at start, so a manifest edit that would make
the launch refuse fails HERE, in the fast lane, rather than as a confusing
runtime denial inside a database-backed test.
"""

from __future__ import annotations

import pytest

from okg.substrate.deployments import find_deployment_dir
from okg.substrate.mcp.principal_mapping import PrincipalMapping
from okg.substrate.mcp.server import resolve_principal_mapping
from okg.substrate.nomos.runtime_enforcement import (
    deployment_nomos_enforcement_mode,
    deployment_nomos_enforcement_mode_source,
    deployment_requires_nomos_runtime,
)
from tests.substrate.mcp._two_principal_fixture import (
    AUDIENCE,
    DEPLOYMENT,
    ISSUER,
    PRINCIPAL_HEADER,
    PRINCIPAL_KIND,
    PROVIDER_ID,
    SUBJECT_CLAIM,
)


@pytest.fixture(autouse=True)
def _no_enforcement_override(monkeypatch: pytest.MonkeyPatch) -> None:
    """The MANIFEST must carry the declaration, not the environment.

    ``OKG_NOMOS_RUNTIME_ENFORCEMENT`` overrides the manifest wherever the mode
    is read. Left set, it would make these assertions pass on a manifest that
    declares nothing at all -- the exact hole this fixture exists to close.
    """
    monkeypatch.delenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", raising=False)


def test_the_fixture_deployment_is_committed_and_resolvable() -> None:
    """The deployment directory is in the tree, not minted by a test."""
    deployment_dir = find_deployment_dir(DEPLOYMENT)
    assert deployment_dir is not None, (
        f"deployment {DEPLOYMENT!r} does not resolve; the real-process proof "
        "serves it by name from a subprocess and cannot mint it"
    )
    assert (deployment_dir / "deployment.yaml").is_file()


def test_nomos_runtime_enforcement_is_declared_by_the_manifest() -> None:
    """Runtime enforcement is on, and the manifest is why."""
    assert deployment_nomos_enforcement_mode(DEPLOYMENT) == "audit"
    # Not "environment", not "default": the declaration travels with the
    # deployment directory, so a subprocess serving it inherits nothing.
    assert deployment_nomos_enforcement_mode_source(DEPLOYMENT) == "manifest"
    # The start-time gate multi-user mode actually consults.
    assert deployment_requires_nomos_runtime(DEPLOYMENT) is True


def test_the_declared_principal_mapping_is_the_one_the_proof_seeds() -> None:
    """The manifest block and the seeding module cannot drift apart.

    ``resolve_principal_mapping`` is what ``build_server`` calls when the
    operator passes ``--multi-user-principals`` without an explicit mapping,
    and it refuses a block with a missing field, an unknown key, or a
    principal kind the read posture does not admit. Comparing its result to
    the constants the database seeding uses closes the one gap a declaration
    cannot close on its own: a manifest that parses but names a different
    header, issuer or audience than the identity provider row, which would
    deny every request for a reason that looks like policy.
    """
    mapping = resolve_principal_mapping(DEPLOYMENT)
    assert isinstance(mapping, PrincipalMapping)
    assert mapping == PrincipalMapping(
        provider_id=PROVIDER_ID,
        issuer=ISSUER,
        audience=AUDIENCE,
        subject_claim=SUBJECT_CLAIM,
        subject_header=PRINCIPAL_HEADER,
        principal_kind=PRINCIPAL_KIND,
    )
    # No extra claim header is declared: this fixture asserts an identifier
    # and nothing else, so there is no descriptive claim to disagree about.
    assert mapping.extra_claim_headers == {}
