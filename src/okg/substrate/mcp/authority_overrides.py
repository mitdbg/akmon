"""Refuse client attempts to steer the Nomos evaluation of an MCP write.

Issue #2891 follow-up. The MCP ``record``/``propose`` tools take a free-form
``authority`` dict from the client. The identity-claim record path used to
read three policy inputs from it:

* ``nomos_bundle_path`` / ``policy_bundle_path`` -- which policy file on the
  server Nomos loaded, so a client could point it at a permissive bundle;
* ``nomos_scope`` -- the scope the decision was evaluated in;
* ``nomos_resource`` -- extra resource facts merged into the request.

Every one of these is server-side state: the policy is the deployment's active
Postgres bundle, and the scope and resource are built by the server. A client
that still sends a bundle path is stale or hostile, so it is refused rather
than ignored. A scope or resource fact equal to the server's value changes
nothing and is allowed; any other value (including a resource key the server
does not set) is refused -- the same rule #2887 applies to reserved resource
facts.
"""
from __future__ import annotations

from typing import Any, Mapping

from okg.substrate.errors import SubstrateError

__all__ = [
    "BUNDLE_PATH_KEYS",
    "ClientPolicyOverrideRefused",
    "client_policy_overrides",
    "refuse_client_policy_overrides",
]

#: Authority keys that once named a policy bundle file.
BUNDLE_PATH_KEYS: tuple[str, ...] = ("nomos_bundle_path", "policy_bundle_path")

_MISSING = object()


class ClientPolicyOverrideRefused(SubstrateError):
    """A client tried to choose the policy, scope, or resource facts."""

    code = "nomos_client_policy_override_refused"


def client_policy_overrides(
    authority: Mapping[str, Any],
    *,
    scope: str,
    resource: Mapping[str, Any],
) -> list[str]:
    """Name every client-supplied policy input the server must refuse.

    Returns a sorted list of ``authority`` field paths, empty when the call is
    honest. ``None`` counts as not sent.
    """
    refused: list[str] = []
    for key in BUNDLE_PATH_KEYS:
        if authority.get(key) is not None:
            refused.append(key)
    declared_scope = authority.get("nomos_scope")
    if declared_scope is not None and declared_scope != scope:
        refused.append("nomos_scope")
    declared_resource = authority.get("nomos_resource")
    if declared_resource is not None:
        if not isinstance(declared_resource, Mapping):
            refused.append("nomos_resource")
        else:
            for key, value in declared_resource.items():
                if resource.get(str(key), _MISSING) != value:
                    refused.append(f"nomos_resource.{key}")
    return sorted(refused)


def refuse_client_policy_overrides(
    authority: Mapping[str, Any],
    *,
    scope: str,
    resource: Mapping[str, Any],
    surface: str,
) -> None:
    """Raise :class:`ClientPolicyOverrideRefused` when any input is refused."""
    fields = client_policy_overrides(authority, scope=scope, resource=resource)
    if not fields:
        return
    raise ClientPolicyOverrideRefused(
        "MCP authority cannot choose the Nomos policy bundle, scope, or "
        f"resource facts; refused fields: {', '.join(fields)}",
        data={"surface": surface, "fields": fields},
        fix_hint=(
            "drop these authority fields; the server evaluates the "
            "deployment's active policy bundle with its own scope and "
            "resource facts"
        ),
    )
