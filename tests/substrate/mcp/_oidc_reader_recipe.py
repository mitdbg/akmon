"""The documented read-only OIDC recipe, held as literals.

The PostgreSQL test ``test_oidc_reader_baseline_policy.py`` runs the recipe
from these values. It must not read the security runbook itself: its lane's
scope closure does not include the docs tree. The database-free
``test_oidc_reader_recipe_docs.py`` parses the runbook's "CERN recipe for
a read-only server" section and asserts it equals these literals, so a
runbook edit that changes the recipe still fails a test.

Do not name the runbook's path in this module; the PostgreSQL test imports it.
"""
from __future__ import annotations

from typing import Any

# The deployment manifest's ``nomos`` block (runbook step 1).
NOMOS: dict[str, Any] = {
    "runtime_enforcement": "enforce",
    "oidc_auth": {
        "provider_id": "cern-sso",
        "issuer": "https://auth.cern.ch/auth/realms/cern",
        "audience": "my-graph",
        "subject_claim": "cern_upn",
        "roles_claim": "cern_roles",
        "principal_kind": "human_operator",
    },
}

# The ``okg nomos ... add`` arguments (runbook step 2), without --dsn,
# --deployment and --json; the tests supply their own target.
PROVIDER: list[str] = ["--for-oidc-auth"]
BINDING: list[str] = [
    "--provider-id", "cern-sso",
    "--claim-path", "roles",
    "--claim-value", "my-graph-reader",
    "--subject", "subject:mcp_client",
]
GRANT: list[str] = [
    "--grant-id", "grant:my-graph:mcp-client:read",
    "--subject", "subject:mcp_client",
    "--action", "graph.read.mcp",
    "--action", "mcp.tool.call",
]


def recipe() -> dict[str, Any]:
    """The recipe in the shape the tests read: fresh copies every call."""
    import copy

    binding = list(BINDING)
    grant = list(GRANT)
    return {
        "nomos": copy.deepcopy(NOMOS),
        "provider": list(PROVIDER),
        "binding": binding,
        "grant": grant,
        "role": binding[binding.index("--claim-value") + 1],
        "subject": binding[binding.index("--subject") + 1],
        "grant_id": grant[grant.index("--grant-id") + 1],
    }
