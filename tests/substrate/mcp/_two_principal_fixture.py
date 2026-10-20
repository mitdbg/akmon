"""Seeding for the two-principal MCP proof (#1180, PACT mcp-two-principal-proof).

The committed fixture deployment ``deployments/mcp-two-principal-fixture``
declares the identity CONTRACT -- Nomos runtime enforcement, the read posture,
and the ``nomos.principal_mapping`` block naming the header that carries the
signed-in user. A declaration alone reaches nobody: the identity provider row,
the two subject bindings, the permission grants and the policy bundle live in
the database, so a deployment directory cannot carry them. This module seeds
exactly those rows, through the landed public helpers
(``nomos.identity_store.upsert_subject_binding`` / ``upsert_permission_grant``
and ``activate_policy_bundle``) rather than hand-written SQL, so the proof
exercises the same write path an operator would.

The constants here MUST agree with the fixture deployment's manifest; the
hermetic test in ``test_two_principal_fixture_deployment.py`` asserts that
agreement, so a manifest edit that drifts from this module fails there rather
than at a confusing runtime denial.
"""

from __future__ import annotations

from typing import Any, Mapping

import psycopg

from okg.substrate.graph_store import ChronosGraphStore
from okg.substrate.library.sources.base import EdgeFact, NodeFact
from okg.substrate.mcp.principal_mapping import (
    MCP_PRINCIPAL_HEADER_PROVIDER_TYPE,
    TRANSPORT_VERIFIER_PREFIX,
)
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
from okg.substrate.publisher import Publisher
from tests.pytest.catalog_fixture_builders import bind_test_catalog_writer
from tests.substrate._db_helpers import seed_catalog_version


#: The committed fixture deployment this module seeds.
DEPLOYMENT = "mcp-two-principal-fixture"
SCOPE = f"deployment:{DEPLOYMENT}"

#: Mirrors deployments/mcp-two-principal-fixture/deployment.yaml.
PROVIDER_ID = "okg-chat-sso"
ISSUER = "https://chat.example.edu/"
AUDIENCE = "okg-mcp"
SUBJECT_CLAIM = "email"
PRINCIPAL_HEADER = "X-OKG-Principal"
PRINCIPAL_KIND = "mcp_client"

#: The one client id `StaticTokenVerifier` issues for every accepted caller.
TRANSPORT_CLIENT_ID = "okg-mcp-static-bearer"
TRANSPORT_VERIFIER = f"{TRANSPORT_VERIFIER_PREFIX}{TRANSPORT_CLIENT_ID}"

PRINCIPAL_A = "alice@example.edu"
PRINCIPAL_B = "bob@example.edu"
SUBJECT_A = "subject:chat:alice"
SUBJECT_B = "subject:chat:bob"

#: The two nodes the differential turns on: both principals reach the first,
#: only A reaches the second.
PUBLIC_NODE = "node:public-docs"
PRIVATE_NODE = "node:private-drive"


def identity_provider() -> IdentityProvider:
    """The header provider the manifest's mapping names.

    ``metadata.trusted_verifiers`` is what makes the machinery CHECK the
    ``verified_by`` the server stamps; without it the stamps are recorded but
    never verified, and `validate_mapping_against_providers` refuses the launch.
    """
    return IdentityProvider(
        provider_id=PROVIDER_ID,
        provider_type=MCP_PRINCIPAL_HEADER_PROVIDER_TYPE,
        issuer=ISSUER,
        audiences=(AUDIENCE,),
        subject_claim=SUBJECT_CLAIM,
        group_claims=(),
        role_claims=(),
        entitlement_claims=(),
        assurance_claim=None,
        freshness_seconds=3600,
        metadata={
            "trusted_verifiers": [TRANSPORT_VERIFIER],
            "max_verifier_age_seconds": 3600,
        },
    )


def publish_two_source_graph(dsn: str) -> str:
    """Publish one generation holding a public node and a private one."""
    with psycopg.connect(dsn, autocommit=True) as conn:
        seed_catalog_version(conn)
        store = ChronosGraphStore.from_connection(conn)
        store.apply_facts(
            [
                NodeFact(
                    PUBLIC_NODE,
                    "page",
                    {"classification": "public", "text": "public docs"},
                    {"id": PUBLIC_NODE},
                    {"v": 1},
                ),
                EdgeFact(
                    PUBLIC_NODE,
                    PUBLIC_NODE,
                    "references",
                    {"classification": "public"},
                    source_record_id={"id": "public-self"},
                    source_revision={"v": 1},
                ),
            ],
            source="public_docs",
        )
        store.apply_facts(
            [
                NodeFact(
                    PRIVATE_NODE,
                    "page",
                    {"classification": "public", "text": "private drive"},
                    {"id": PRIVATE_NODE},
                    {"v": 1},
                ),
            ],
            source="google_drive",
        )
    result = Publisher(
        dsn,
        DEPLOYMENT,
        catalog_identity=bind_test_catalog_writer(dsn, DEPLOYMENT),
    ).cycle()
    if result["status"] != "published":
        raise AssertionError(f"fixture publish did not publish: {result}")
    return str(result["generation_id"])


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


def activate_two_principal_policy(dsn: str) -> None:
    """A reaches both sources; B is denied the private one.

    The fixture must DIFFERENTIATE: if both principals saw the same rows, an
    equal-results comparison would pass while proving nothing.
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
    rules.append(
        {
            "rule_id": "deny_drive_b",
            "effect": "deny",
            "actions": ["graph.read.mcp"],
            "subjects": [SUBJECT_B],
            "match": {
                "deployment": DEPLOYMENT,
                "scope": SCOPE,
                "source_id": "google_drive",
            },
            "priority": 100,
        }
    )
    bundle = compile_policy_graph(
        {
            "policy_bundle_id": "nomos:fixture:mcp-two-principal-proof",
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


def configure_header_identity(
    dsn: str,
    *,
    principals: Mapping[str, str] | None = None,
    drive_subjects: tuple[str, ...] = (SUBJECT_A,),
) -> None:
    """Register the provider, the subject bindings, and the grants."""
    if principals is None:
        principals = {PRINCIPAL_A: SUBJECT_A, PRINCIPAL_B: SUBJECT_B}
    provider = identity_provider()
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


def seed_all(dsn: str) -> str:
    """Publish the graph and install the identity + policy rows."""
    generation_id = publish_two_source_graph(dsn)
    configure_header_identity(dsn)
    activate_two_principal_policy(dsn)
    return generation_id
