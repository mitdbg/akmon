"""Canonical construction boundary for product publisher paths."""
from __future__ import annotations

from pathlib import Path
from typing import Any, Mapping

from okg.substrate.publisher import deployment_stack as _deployment_stack
from okg.substrate.publisher.deployment_stack import DeploymentPublisherStack


_RESERVED_OVERRIDES = frozenset({
    "dsn",
    "deployment_name",
    "deployment_dir",
    "branch",
    "branch_name",
    "catalog_identity",
    "checkpoint_attestors",
    "repository_provenance",
    "packaged_release",
    "hard_statement_timeout_ms",
    "hard_statement_timeout_source",
    "lock_hold_headroom_factor",
    "lock_hold_headroom_factor_source",
    "allow_raised_statement_timeout",
    "allow_raised_statement_timeout_source",
    "first_publish_warning_budget",
    "first_publish_warning_budget_source",
})

_DEPLOYMENT_PUBLISHER_AUTHORITY = frozenset({
    "hard_statement_timeout_ms",
    "hard_statement_timeout_source",
    "lock_hold_headroom_factor",
    "lock_hold_headroom_factor_source",
    "allow_raised_statement_timeout",
    "allow_raised_statement_timeout_source",
    "first_publish_warning_budget",
    "first_publish_warning_budget_source",
})


def build_deployment_publisher(
    *,
    dsn: str,
    deployment_name: str,
    command: str,
    deployment_dir: Path | str | None = None,
    branch_name: str = "default",
    stack: DeploymentPublisherStack | None = None,
    publisher_overrides: Mapping[str, Any] | None = None,
) -> Any:
    """Build ``Publisher`` with one deployment stack and authority identity.

    Product callers may tune cycle-specific publisher options through
    ``publisher_overrides``.  They cannot replace the deployment's attestors or
    catalog identity: those authority-bearing components always come from the
    same resolved stack and manifest.
    """

    resolved_dir = Path(deployment_dir) if deployment_dir is not None else None
    from okg.substrate.distributions.packaged_release import current_verified_release
    # Revalidate the active packaged scope before stack construction can load
    # deployment code; the release itself is resolved once the stack directory
    # is known below.
    candidate_dir = resolved_dir or getattr(stack, "deployment_dir", None)
    current_verified_release(Path(candidate_dir) if candidate_dir is not None else None)
    resolved_stack = stack or _deployment_stack.build_deployment_publisher_stack(
        dsn=dsn,
        deployment_name=deployment_name,
        deployment_dir=resolved_dir,
    )
    stack_deployment_name = getattr(resolved_stack, "deployment_name", None)
    if stack_deployment_name != deployment_name:
        raise ValueError(
            "publisher stack deployment name does not match factory authority"
        )
    stack_dir = getattr(resolved_stack, "deployment_dir", resolved_dir)
    if resolved_dir is not None and stack_dir is not None:
        if resolved_dir.resolve() != Path(stack_dir).resolve():
            raise ValueError(
                "publisher stack deployment directory does not match factory authority"
            )

    overrides = dict(publisher_overrides or {})
    forbidden = sorted(set(overrides) & _RESERVED_OVERRIDES)
    if forbidden:
        raise ValueError(
            "publisher overrides cannot replace deployment authority: "
            + ", ".join(forbidden)
        )

    stack_kwargs = dict(resolved_stack.publisher_kwargs())
    kwargs = dict(stack_kwargs)
    kwargs.update(_deployment_stack.publisher_kwargs_from_env())
    # Release policy is manifest authority. Keep historical environment knobs
    # for unrelated publisher tuning, then restore these exact deployment
    # values so an operator patch cannot rewrite the proof after admission.
    # Restore only what the stack actually declared. `resolved_stack` is
    # duck-typed -- callers pass their own object with a `publisher_kwargs()`
    # -- so indexing these keys unconditionally would raise KeyError on any
    # stack that does not carry them, rather than leaving the publisher to
    # apply its own documented defaults. A key the stack never declared has
    # no deployment authority to protect.
    kwargs.update({
        key: stack_kwargs[key]
        for key in _DEPLOYMENT_PUBLISHER_AUTHORITY
        if key in stack_kwargs
    })
    kwargs.update(overrides)
    if stack_dir is not None:
        # Revalidate again against the stack's own directory before the
        # catalog identity is built.
        current_verified_release(Path(stack_dir))
    catalog_identity = _deployment_stack.build_runtime_catalog_identity(
        dsn=dsn,
        deployment_name=deployment_name,
        deployment_dir=stack_dir,
        command=command,
    )
    from okg.substrate.publisher.repository_provenance import (
        RepositoryProvenance,
        deployment_is_outside_repository,
        tracking_repository_provenance,
    )
    from okg.substrate.deployment_release import (
        StandaloneReleaseConflict,
        standalone_release_declared,
    )
    from okg.substrate.runtime.release_authority import (
        verify_worker_release_authority,
    )

    from okg.substrate.distributions.packaged_release import current_verified_release
    packaged = current_verified_release(Path(stack_dir)) if stack_dir is not None else None
    sealed = verify_worker_release_authority(
        deployment=deployment_name,
        dsn=dsn,
    )
    # #1955: an ordinary `okg install` writes the deployment to the documented
    # XDG user-state root, which is never a Git working tree, and the tracking
    # branch below then refused every publish. `release.kind: standalone` is
    # the operator's explicit declaration that this deployment lives outside
    # version control on purpose. It is read from the deployment, so it is
    # recorded in the deployment and auditable; it is never inferred from Git
    # being absent.
    standalone = (
        standalone_release_declared(Path(stack_dir))
        if stack_dir is not None
        else False
    )
    if packaged is not None:
        if sealed is not None:
            raise ValueError("packaged and Git worker authority cannot be combined")
        if standalone:
            raise StandaloneReleaseConflict(
                "standalone and packaged release authority cannot be combined",
                data={
                    "deployment": deployment_name,
                    "deployment_dir": str(stack_dir),
                    "declared_kind": "standalone",
                    "conflicting_authority": "packaged",
                },
                fix_hint=(
                    "a packaged deployment already records provenance as "
                    "unavailable under its packaged release identity; remove "
                    "`release.kind: standalone` from its deployment.yaml"
                ),
            )
        repository_provenance = RepositoryProvenance.unavailable()
        kwargs["packaged_release"] = packaged
    elif sealed is not None:
        if standalone:
            raise StandaloneReleaseConflict(
                "standalone and sealed worker release authority cannot be combined",
                data={
                    "deployment": deployment_name,
                    "deployment_dir": str(stack_dir),
                    "declared_kind": "standalone",
                    "conflicting_authority": "sealed_worker",
                },
                fix_hint=(
                    "this worker runs under a sealed release, which pins exact "
                    "Git ids for the generation; remove `release.kind: "
                    "standalone` from the deployment.yaml, or stop sealing "
                    "this deployment's releases"
                ),
            )
        repository_provenance = RepositoryProvenance.exact(
            core_revision=str(sealed["code_revision"]),
            deployment_revision=str(sealed["deployment_revision"]),
            deployment_tree_sha=str(sealed["deployment_tree_sha"]),
        )
    elif standalone and deployment_is_outside_repository(Path(stack_dir)):
        # Declared, and the directory really is outside every repository:
        # record the same `unavailable` provenance the packaged branch above
        # records, so a reader of the generation can tell that no repository
        # authority backed it.
        repository_provenance = RepositoryProvenance.unavailable()
    else:
        # Everything else, INCLUDING a deployment that declares standalone but
        # does sit inside a repository, takes the tracking path and keeps every
        # existing refusal.
        if stack_dir is None:
            raise ValueError("publisher repository authority requires a deployment directory")
        repository_provenance = tracking_repository_provenance(Path(stack_dir))

    from okg.substrate.publisher.publisher import Publisher

    kwargs["branch_name"] = branch_name
    kwargs["catalog_identity"] = catalog_identity
    kwargs["repository_provenance"] = repository_provenance
    return Publisher(dsn, deployment_name, **kwargs)


__all__ = ["build_deployment_publisher"]
