"""Fixed identities used by the fake review deployment.

The API process resolves the ``X-E2E-Identity`` selector through this fixture;
the request body never carries an actor.  In production this lookup is the
OIDC/mTLS identity boundary, while the acceptance fixture keeps its subjects
stable and its credentials non-secret.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Mapping

from okg.substrate.external_actions.auth import VerifiedPrincipal


@dataclass(frozen=True)
class Identity:
    name: str
    principal: str
    separation_subject: str
    provider_id: str
    subject_binding_id: str
    auth_method: str = "mtls"

    def verified(self) -> VerifiedPrincipal:
        return VerifiedPrincipal(
            self.principal,
            self.separation_subject,
            self.provider_id,
            self.subject_binding_id,
            issuer="fake-identity-fixture",
            auth_method=self.auth_method,
            sender_key_thumbprint=f"thumb:{self.name}",
        )


IDENTITIES: Mapping[str, Identity] = {
    "proposer": Identity(
        "proposer",
        "oidc:automation.example:service:triage-bot",
        "subject:automation:triage-bot",
        "fake-provider:oidc",
        "fake-binding:proposer",
        "jwt_dpop",
    ),
    "approver": Identity(
        "approver",
        "oidc:login.example:user:operator-7",
        "subject:human:operator-7",
        "fake-provider:oidc",
        "fake-binding:approver",
        "jwt_dpop",
    ),
    # Two credential/principal spellings intentionally share one stable
    # separation subject to exercise proposer/approver separation.
    "proposer-alias": Identity(
        "proposer-alias",
        "oidc:alternate.example:user:operator-7",
        "subject:automation:triage-bot",
        "fake-provider:oidc",
        "fake-binding:proposer-alias",
        "jwt_dpop",
    ),
    "worker": Identity(
        "worker",
        "mtls:workers.example:ticket-worker-1",
        "subject:worker:ticket-service",
        "fake-provider:mtls",
        "fake-binding:worker-1",
    ),
    "worker-2": Identity(
        "worker-2",
        "mtls:workers.example:ticket-worker-2",
        "subject:worker:ticket-service-2",
        "fake-provider:mtls",
        "fake-binding:worker-2",
    ),
}


def resolve(name: str) -> VerifiedPrincipal:
    try:
        return IDENTITIES[name].verified()
    except KeyError as exc:
        # The API process turns this into its ordinary authentication failure;
        # do not leak arbitrary selector text into an audit record.
        raise ValueError("unknown fake identity") from exc


__all__ = ["IDENTITIES", "Identity", "resolve"]
