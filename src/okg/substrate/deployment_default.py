"""Which deployment a command acts on when the operator names none.

Several commands used to fall back to one hard-coded deployment name (the
development repository's own workspace graph). A public user has no such
deployment, so those commands acted on a name that resolves to nothing and
failed somewhere downstream, or reported that deployment as unavailable.

The rule every such command now follows, in order:

1. the deployment the operator passed (``--deployment`` or a positional name);
2. ``OKG_DEPLOYMENT`` in the environment;
3. the repository's configured default: ``graph_projection.deployment`` in
   ``.okg/coordination.yaml`` (or ``pact/project.yaml``) of the enclosing
   checkout -- the deployment okg's own tooling talks to;
4. otherwise the command refuses with :class:`DeploymentNotSpecified`, which
   names all three ways to supply one.

Nothing is guessed: a malformed configured default is a loud error, never a
reason to fall through to another source.
"""

from __future__ import annotations

import os
from collections.abc import Mapping
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from okg.substrate.errors import SubstrateError

DEPLOYMENT_ENV = "OKG_DEPLOYMENT"
CONFIG_HINT = "graph_projection.deployment in .okg/coordination.yaml"


class DeploymentNotSpecified(SubstrateError):
    """A command needs a deployment and none was given or configured."""

    code = "deployment_not_specified"


def _configured_root(start: Path) -> Path | None:
    for candidate in (start, *start.parents):
        if (candidate / ".okg" / "coordination.yaml").is_file() or (
            candidate / "pact" / "project.yaml"
        ).is_file():
            return candidate
    return None


def configured_default_deployment(start: Path | None = None) -> str | None:
    """The enclosing checkout's configured default deployment, if it has one."""
    from okg.substrate.pact.project_settings import graph_projection_deployment

    base = (start if start is not None else Path.cwd()).resolve()
    root = _configured_root(base)
    if root is None:
        return None
    return graph_projection_deployment(root)


def resolve_command_deployment(
    explicit: str | None,
    *,
    command: str,
    start: Path | None = None,
    environ: Mapping[str, str] | None = None,
) -> str:
    """Return the deployment ``command`` acts on, or refuse by name."""
    if explicit is not None and str(explicit).strip():
        return str(explicit).strip()
    env = os.environ if environ is None else environ
    active = (env.get(DEPLOYMENT_ENV) or "").strip()
    if active:
        return active
    configured = configured_default_deployment(start)
    if configured:
        return configured
    raise DeploymentNotSpecified(
        f"{command} needs a deployment and none was given or configured",
        data={"command": command},
        fix_hint=(
            f"pass --deployment <name>, set {DEPLOYMENT_ENV}=<name>, or set "
            f"{CONFIG_HINT}"
        ),
    )


FLEET_KEY = "deployment_fleet"
FLEET_HINT = f"{FLEET_KEY} in .okg/coordination.yaml"


@dataclass(frozen=True)
class DeploymentFleet:
    """The deployment names a repository's own tooling acts on.

    They are repository configuration (``deployment_fleet`` in
    ``.okg/coordination.yaml``), never package constants: a checkout that
    declares none -- the public copy, any other user's repository -- gets an
    empty fleet, and each consumer either works on the empty list and says so
    or refuses by name.
    """

    source: str | None = None
    #: ``okg up --all`` sweeps these, in order.
    kept: tuple[str, ...] = ()
    #: The Nomos rollout audits require these to be present.
    nomos_required: tuple[str, ...] = ()
    #: Required by the Nomos audits only when the scanned inventory has them.
    nomos_required_when_present: tuple[str, ...] = ()
    #: Deployments whose ``pact`` source gets the strict smoke-policy check,
    #: mapped to the source family its admission scope must name.
    strict_pact_smoke: Mapping[str, str] = field(default_factory=dict)
    #: Database names a dev auto-create must never target, beyond the built-in
    #: server databases.
    protected_databases: tuple[str, ...] = ()


def _name_list(raw: Any, *, field_name: str, source: str) -> tuple[str, ...]:
    if raw is None:
        return ()
    if not isinstance(raw, list) or not all(
        isinstance(item, str) and item.strip() for item in raw
    ):
        raise SubstrateError(
            f"{FLEET_KEY}.{field_name} must be a list of non-empty names",
            data={"path": source, "field": f"{FLEET_KEY}.{field_name}"},
        )
    return tuple(item.strip() for item in raw)


def deployment_fleet(start: Path | None = None) -> DeploymentFleet:
    """Read the enclosing checkout's ``deployment_fleet`` block.

    Absent file or block: an empty fleet (``source`` is ``None``). A present
    but malformed block is a loud error, never an empty fleet.
    """
    from okg.substrate.pact.project_settings import (
        load_runtime_settings,
        runtime_settings_source,
    )

    base = (start if start is not None else Path.cwd()).resolve()
    root = _configured_root(base)
    if root is None:
        return DeploymentFleet()
    raw = load_runtime_settings(root).get(FLEET_KEY)
    source = str(runtime_settings_source(root))
    if raw is None:
        return DeploymentFleet()
    known = {
        "kept",
        "nomos_required",
        "nomos_required_when_present",
        "strict_pact_smoke",
        "protected_databases",
    }
    if not isinstance(raw, Mapping) or set(raw) - known:
        raise SubstrateError(
            f"{FLEET_KEY} must be a mapping with only these keys: "
            + ", ".join(sorted(known)),
            data={"path": source, "unknown": sorted(set(raw) - known)
                  if isinstance(raw, Mapping) else None},
        )
    strict_raw = raw.get("strict_pact_smoke")
    if strict_raw is None:
        strict_raw = {}
    if not isinstance(strict_raw, Mapping) or not all(
        isinstance(name, str) and name.strip()
        and isinstance(spec, Mapping)
        and set(spec) == {"source_family"}
        and isinstance(spec.get("source_family"), str)
        and spec["source_family"].strip()
        for name, spec in strict_raw.items()
    ):
        raise SubstrateError(
            f"{FLEET_KEY}.strict_pact_smoke must map each deployment to "
            "{source_family: <name>}",
            data={"path": source, "field": f"{FLEET_KEY}.strict_pact_smoke"},
        )
    return DeploymentFleet(
        source=source,
        kept=_name_list(raw.get("kept"), field_name="kept", source=source),
        nomos_required=_name_list(
            raw.get("nomos_required"), field_name="nomos_required", source=source
        ),
        nomos_required_when_present=_name_list(
            raw.get("nomos_required_when_present"),
            field_name="nomos_required_when_present",
            source=source,
        ),
        strict_pact_smoke={
            name.strip(): str(spec["source_family"]).strip()
            for name, spec in strict_raw.items()
        },
        protected_databases=_name_list(
            raw.get("protected_databases"),
            field_name="protected_databases",
            source=source,
        ),
    )


__all__ = [
    "CONFIG_HINT",
    "DEPLOYMENT_ENV",
    "FLEET_HINT",
    "DeploymentFleet",
    "DeploymentNotSpecified",
    "configured_default_deployment",
    "deployment_fleet",
    "resolve_command_deployment",
]
