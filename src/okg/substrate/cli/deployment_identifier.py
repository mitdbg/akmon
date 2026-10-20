"""Typed identifiers at the deployment CLI boundary (issue #1289).

`okg deployment ready <deployment>` and `okg dogfood status --deployment
<deployment>` take a GRAPH DEPLOYMENT NAME. An ENVIRONMENT ID is a
different kind of identifier: the environment `okg-workspace-production`
hosts the graph deployment `okg-workspace`, and every durable row —
checkpoints, ownership, catalog versions, Nomos activations — is keyed by
the latter.

Passing the wrong kind used to continue under a mixed identity rather
than fail at the boundary. `okg dogfood status` forwarded the raw
argument into the live checks, so the generation resolver matched none of
the deployment's checkpoints, reported "no latest published generation is
available", and `nomos.source_lineage` failed behind it on the empty
generation id. The command reported eight blockers where the correct name
reported three — five of them describing a read path that was in fact
permitting every standard read subject under enforce mode.

That is absence dressed as agreement: a name that resolves to nothing is
reported as a system that is broken, and the operator has no signal
separating the two. It redirected a recovery toward read denial and
deployment corruption while the published graph stayed readable.

This module answers one question — "is this identifier the wrong KIND?" —
and refuses before any health probe runs.

Deliberately narrow:

* Only a POSITIVELY IDENTIFIED environment id whose graph deployment name
  DIFFERS is refused. An unknown name is not this error; it stays
  whatever it was, so "wrong kind" and "no such thing" remain distinct
  causes with different repairs.
* An environment whose id equals its graph deployment name is not a
  mismatch — there is nothing to confuse.
* Detection never fails closed. A malformed or unreadable descriptor
  elsewhere in the environments directory must not block readiness for a
  healthy deployment; if we cannot positively identify an environment we
  say nothing and let the normal path run.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from okg.substrate.errors import SubstrateError

IDENTIFIER_KIND_MISMATCH = "identifier_kind_mismatch"


@dataclass(frozen=True)
class EnvironmentIdentifier:
    """A supplied identifier positively identified as an environment id."""

    supplied: str
    graph_deployment_name: str
    tier: str | None
    descriptor_path: str | None

    def to_dict(self) -> dict[str, Any]:
        # The two identities are bound under SEPARATE keys and never
        # merged into one `deployment` field, so a reader cannot mistake
        # which one a value refers to.
        return {
            "schema": "okg.identifier_kind_mismatch.v1",
            "reason_code": IDENTIFIER_KIND_MISMATCH,
            "supplied_identifier": self.supplied,
            "supplied_identifier_kind": "environment_id",
            "expected_identifier_kind": "graph_deployment_name",
            "environment_id": self.supplied,
            "graph_deployment_name": self.graph_deployment_name,
            "environment_tier": self.tier,
            "environment_descriptor": self.descriptor_path,
        }


def environment_identifier_for(name: str) -> EnvironmentIdentifier | None:
    """Return the environment iff `name` is an env id naming a DIFFERENT deployment.

    Returns None for a graph deployment name, an unknown name, an
    environment whose id equals its deployment name, or any descriptor we
    cannot read. Never raises: identification is best-effort by design.
    """
    candidate = (name or "").strip()
    if not candidate:
        return None
    try:
        from okg.substrate import fastyaml
        from okg.substrate.deployments.environment import find_environment_path

        descriptor = find_environment_path(candidate)
        if not descriptor.is_file():
            return None
        # Read the descriptor directly rather than going through
        # `load_environment`. Identifying the KIND of an identifier must
        # not require the environment to be fully resolvable: a real
        # descriptor interpolates env vars (`${OKG_WORKSPACE_SYNC_ROOT}`,
        # a DSN secret ref) that may be unset in the process asking the
        # question, and an environment that cannot load is exactly when a
        # confused operator is most likely to reach for the wrong name.
        raw = fastyaml.safe_load(descriptor.read_text(encoding="utf-8"))
    except Exception:  # noqa: BLE001 - absence of an environment is not an error
        return None

    if not isinstance(raw, dict):
        return None
    # The descriptor must actually claim this id, so a stray file named
    # after a deployment cannot masquerade as an environment.
    if str(raw.get("id") or "").strip() != candidate:
        return None
    graph_deployment_name = str(raw.get("graph_deployment_name") or "").strip()
    if not graph_deployment_name or graph_deployment_name == candidate:
        return None
    return EnvironmentIdentifier(
        supplied=candidate,
        graph_deployment_name=graph_deployment_name,
        tier=str(raw.get("tier") or "").strip() or None,
        descriptor_path=str(descriptor),
    )


def require_graph_deployment_name(
    name: str,
    *,
    correct_command: str,
) -> None:
    """Refuse at the boundary when `name` is an environment id.

    `correct_command` is a format string containing `{deployment}`; the
    refusal renders it with the graph deployment name so the operator is
    handed the exact command rather than a description of one.
    """
    identified = environment_identifier_for(name)
    if identified is None:
        return
    rendered = correct_command.format(deployment=identified.graph_deployment_name)
    raise SubstrateError(
        f"{name!r} is an environment id, not a graph deployment name; "
        f"environment {identified.supplied!r} hosts graph deployment "
        f"{identified.graph_deployment_name!r}",
        data=identified.to_dict(),
        fix_hint=f"run: {rendered}",
    )
