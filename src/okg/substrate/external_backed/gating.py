"""Nomos gate for privileged `probe` hydration.

Restores the deny-by-default control the superseded live-reading-plane enforced:
EVERY `probe` field MUST pass a Nomos check (action ``external.probe.invoke``)
as the caller's resolved subject BEFORE any external call. There is no default
subject: an empty principal is refused without asking the bundle (#2361); it
used to be decided as ``service:okg-runtime``. The handle cannot
opt out — a missing ``access.permission`` is deny, not ungated. No bundle, or
any non-allow effect, denies.
"""
from __future__ import annotations

from pathlib import Path
from typing import Optional

from okg.substrate.external_backed.handle import ExternalField

PROBE_ACTION = "external.probe.invoke"
PROBE_PRINCIPAL_UNRESOLVED_REASON = "probe_principal_unresolved"
BUNDLE_FILENAME = "nomos_external_probe.json"


def build_probe_gate(
    deployment_name: str, *, deployments_root: str | Path | None = None
):
    """Return gate(field, principal) -> bool backed by the deployment bundle.

    No bundle on disk ⇒ a gate that always denies (Nomos default-deny posture).
    """
    from okg.substrate.nomos.core import (
        DecisionInput,
        compile_policy_graph,
        evaluate,
        load_policy_graph,
    )

    if deployments_root is None:
        from okg.substrate.deployments import find_deployments_root

        deployments_root = find_deployments_root()
    path = Path(deployments_root) / deployment_name / BUNDLE_FILENAME
    bundle = compile_policy_graph(load_policy_graph(path)) if path.is_file() else None

    def gate(field: ExternalField, principal: Optional[str]) -> bool:
        subject = (principal or "").strip()
        if not subject:
            return False
        decision = evaluate(
            bundle,
            DecisionInput(
                subject=subject,
                action=PROBE_ACTION,
                resource={
                    "deployment": deployment_name,
                    "permission": field.permission,
                },
                classification="internal",
            ),
            enforcement_point="external.probe.invoke",
        )
        return getattr(decision, "effect", "deny") == "allow"

    return gate
