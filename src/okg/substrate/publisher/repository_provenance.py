"""Immutable repository authority recorded on graph checkpoints."""

from __future__ import annotations

import os
import re
import subprocess
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping

from okg.substrate.errors import SubstrateError


_GIT_ID = re.compile(r"(?:[0-9a-f]{40}|[0-9a-f]{64})")


@dataclass(frozen=True)
class RepositoryProvenance:
    """Repository authority recorded on a checkpoint.

    ``exact``: okg and the deployment both resolved to Git object ids.
    ``distribution``: okg runs from a verified installed wheel, so the
    deployment side is exact Git ids and the okg side is the wheel's
    identity (``core_distribution``) -- ``core_revision`` stays null because
    the build commit the wheel names is the builder's claim, not verified Git
    authority (decision "wheel A", 2026-10-04).
    ``unavailable``: no repository authority backed the generation.
    """

    status: str
    core_revision: str | None = None
    deployment_revision: str | None = None
    deployment_tree_sha: str | None = None
    core_distribution: dict[str, Any] | None = None

    @classmethod
    def unavailable(cls) -> "RepositoryProvenance":
        return cls(status="unavailable")

    @classmethod
    def exact(
        cls,
        *,
        core_revision: str,
        deployment_revision: str,
        deployment_tree_sha: str,
    ) -> "RepositoryProvenance":
        values = (core_revision, deployment_revision, deployment_tree_sha)
        if any(
            re.fullmatch(r"(?:[0-9a-f]{40}|[0-9a-f]{64})", value) is None
            for value in values
        ):
            raise ValueError(
                "repository provenance requires lowercase full Git object ids"
            )
        return cls(
            status="exact",
            core_revision=core_revision,
            deployment_revision=deployment_revision,
            deployment_tree_sha=deployment_tree_sha,
        )

    @classmethod
    def distribution(
        cls,
        *,
        core_distribution: Mapping[str, Any],
        deployment_revision: str,
        deployment_tree_sha: str,
    ) -> "RepositoryProvenance":
        from okg.substrate.distributions.installed_framework import (
            InstalledFramework,
        )

        if any(
            _GIT_ID.fullmatch(value) is None
            for value in (deployment_revision, deployment_tree_sha)
        ):
            raise ValueError(
                "repository provenance requires lowercase full Git object ids"
            )
        identity = InstalledFramework.from_mapping(core_distribution).to_dict()
        return cls(
            status="distribution",
            core_revision=None,
            deployment_revision=deployment_revision,
            deployment_tree_sha=deployment_tree_sha,
            core_distribution=identity,
        )

    def to_dict(self) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "status": self.status,
            "core_revision": self.core_revision,
            "deployment_revision": self.deployment_revision,
            "deployment_tree_sha": self.deployment_tree_sha,
        }
        if self.core_distribution is not None:
            payload["core_distribution"] = dict(self.core_distribution)
        return payload

    @classmethod
    def from_metadata(cls, value: Any) -> "RepositoryProvenance":
        if not isinstance(value, Mapping):
            return cls.unavailable()
        if value.get("status") == "distribution":
            try:
                return cls.distribution(
                    core_distribution=value["core_distribution"],
                    deployment_revision=str(value["deployment_revision"]),
                    deployment_tree_sha=str(value["deployment_tree_sha"]),
                )
            except (KeyError, TypeError, ValueError):
                return cls.unavailable()
        if value.get("status") != "exact":
            return cls.unavailable()
        try:
            return cls.exact(
                core_revision=str(value["core_revision"]),
                deployment_revision=str(value["deployment_revision"]),
                deployment_tree_sha=str(value["deployment_tree_sha"]),
            )
        except (KeyError, TypeError, ValueError):
            return cls.unavailable()

    def matches_exact(self, expected: "RepositoryProvenance") -> bool:
        return self.status == expected.status == "exact" and self == expected


def tracking_repository_provenance(deployment_dir: Path) -> RepositoryProvenance:
    """Derive exact authority and reject deployment inputs outside HEAD."""
    from okg.substrate.deployments import DeploymentManifest
    from okg.substrate.deployment_release import current_release_artifact
    from okg.substrate.deployment_upgrade import _resolve_repo_authorities

    deployment_dir = deployment_dir.resolve()
    manifest = DeploymentManifest.from_yaml(deployment_dir / "deployment.yaml")
    release_artifact = current_release_artifact(manifest)
    if release_artifact.framework_distribution is not None:
        return _distribution_repository_provenance(
            deployment_dir, release_artifact.framework_distribution,
        )
    release = manifest.release if isinstance(manifest.release, dict) else {}
    pin = release.get("pin") if isinstance(release.get("pin"), dict) else {}
    explicit = pin.get("code_repository") if "code_repository" in pin else None
    code_root, deployment_root = _resolve_repo_authorities(
        deployment_dir,
        explicit_code_repository=str(explicit) if explicit is not None else None,
        requested_code_revision=(
            str(pin["code_revision"]) if pin.get("code_revision") else None
        ),
    )
    try:
        deployment_dir.relative_to(deployment_root)
    except ValueError as exc:
        raise SubstrateError("deployment directory is outside its repository") from exc
    checked_roots: set[Path] = set()
    for repository_root in (deployment_root.resolve(), code_root.resolve()):
        if repository_root in checked_roots:
            continue
        checked_roots.add(repository_root)
        dirty = _git(
            repository_root, "status", "--porcelain", "--untracked-files=all"
        )
        if dirty:
            raise SubstrateError(
                "repository provenance inputs are not committed",
                data={
                    "repository": str(repository_root),
                    "paths": dirty.splitlines(),
                },
                # #1791: without a hint the staged-source guard on the next
                # run fell back to "resolve the blocker" and the operator was
                # never told the blocker was an uncommitted tree.
                fix_hint=(
                    "commit or gitignore every path listed in error.data.paths "
                    f"in {repository_root} (git add <paths> && git commit; "
                    "never add secrets), then re-run the failed command -- "
                    "the publish reads the committed tree, and `okg ingest "
                    "--deployment <name> --json` republishes the staged "
                    "records once the tree is clean"
                ),
            )
    # #1962. `.exact()` requires a full lowercase Git object id. A release
    # artifact with no repository revision would reach it as the literal
    # string "None" and be refused with a message about object-id formatting,
    # which names neither the deployment nor the reason. Refuse here instead,
    # with the cause.
    #
    # This is reachable only if a caller routes a declared-standalone
    # deployment into the tracking path. `build_deployment_publisher` does
    # not -- the standalone branch is taken first -- and neither does
    # `current_release_artifact`, whose standalone branch additionally
    # requires the deployment to be outside every repository. The guard exists
    # because the failure mode is silent-looking rather than because a live
    # caller is known to hit it.
    if not release_artifact.records_repository_revision:
        # Bound to a name rather than written inline: an implicitly
        # concatenated literal sitting as a dict value reads to CodeQL's
        # py/implicit-string-concatenation check as a missing comma.
        reason = (
            "the deployment's release artifact records no repository "
            "revision, so exact repository provenance cannot be derived "
            "for it"
        )
        raise SubstrateError(
            "repository provenance requires a repository revision",
            data={
                "repository": str(deployment_root),
                "deployment_dir": str(deployment_dir),
                "operation": "resolve_core_revision",
                "release_kind": release_artifact.release_kind,
                "reason": reason,
            },
            fix_hint=(
                "this deployment records no repository revision (a "
                "`release.kind: standalone` or packaged deployment), so it "
                "cannot be stamped with exact repository provenance. Publish "
                "it through its own release path, or move it into a "
                "committed Git checkout and remove the declaration"
            ),
        )
    return RepositoryProvenance.exact(
        core_revision=str(release_artifact.code_revision),
        deployment_revision=_git(deployment_root, "rev-parse", "HEAD"),
        deployment_tree_sha=_git(deployment_root, "rev-parse", "HEAD^{tree}"),
    )


def _distribution_repository_provenance(
    deployment_dir: Path, framework_distribution: Mapping[str, Any],
) -> RepositoryProvenance:
    """Provenance when okg runs from a verified installed wheel.

    Only the deployment repository is resolved and required clean: there is
    no okg checkout to check. The okg side is the wheel identity the release
    artifact already verified against its RECORD.
    """
    from okg.substrate.deployment_upgrade import (
        RevisionAuthorityError,
        _git_repo_root,
    )

    try:
        deployment_root = _git_repo_root(deployment_dir)
    except (OSError, ValueError, subprocess.SubprocessError) as exc:
        raise RevisionAuthorityError(
            authority_role="deployment",
            repository=str(deployment_dir),
            requested_revision=None,
            resolved_revision=None,
            operation="resolve_deployment_repository",
            reason=str(exc),
        ) from exc
    dirty = _git(deployment_root, "status", "--porcelain", "--untracked-files=all")
    if dirty:
        raise SubstrateError(
            "repository provenance inputs are not committed",
            data={
                "repository": str(deployment_root),
                "paths": dirty.splitlines(),
            },
            fix_hint=(
                "commit or gitignore every path listed in error.data.paths "
                f"in {deployment_root} (git add <paths> && git commit; "
                "never add secrets), then re-run the failed command"
            ),
        )
    return RepositoryProvenance.distribution(
        core_distribution=framework_distribution,
        deployment_revision=_git(deployment_root, "rev-parse", "HEAD"),
        deployment_tree_sha=_git(deployment_root, "rev-parse", "HEAD^{tree}"),
    )


def deployment_is_outside_repository(deployment_dir: Path) -> bool:
    """Whether no Git repository contains ``deployment_dir``.

    Used only to bound the `release.kind: standalone` declaration (#1955): a
    deployment that declares standalone but does in fact sit inside a
    repository keeps the ordinary tracking path and every refusal that comes
    with it (drift, uncommitted inputs, split-repository checks). The
    declaration can therefore never be used to silence a drift refusal in a
    real checkout.

    That guarantee only holds if the answer depends on the DIRECTORY alone and
    the function fails CLOSED otherwise, so:

    * the ambient Git environment is stripped. ``GIT_DIR`` in particular makes
      `git` report "not a git repository" from inside a real checkout, which
      would waive the drift refusals for a declaring deployment that really is
      inside one -- exactly the case this function exists to protect.
    * the locale is pinned to C. The "outside" answer is read out of Git's
      own message, which is translated: under a German locale git says
      "Kein Git-Repository", so an unpinned locale would refuse every
      genuinely-outside deployment and blame the wrong cause.
    * only two answers are accepted: a resolved toplevel means inside, and
      Git's own "not a git repository" means outside. Anything else -- `git`
      missing, a dubious-ownership repository, a timeout -- refuses rather
      than guessing. Same stance as :func:`_git` below.
    """
    # Discovery must follow `deployment_dir` upward and nothing else, and the
    # message below must come back in English.
    environment = {
        key: value
        for key, value in os.environ.items()
        if key
        not in {
            "GIT_DIR",
            "GIT_WORK_TREE",
            "GIT_COMMON_DIR",
            "GIT_CEILING_DIRECTORIES",
            "GIT_DISCOVERY_ACROSS_FILESYSTEM",
            "GIT_OBJECT_DIRECTORY",
        }
    }
    environment["LC_ALL"] = "C"
    environment["LANGUAGE"] = ""
    try:
        result = subprocess.run(
            ["git", "rev-parse", "--show-toplevel"],
            cwd=deployment_dir,
            env=environment,
            check=False,
            capture_output=True,
            text=True,
            timeout=10,
        )
    except (OSError, subprocess.SubprocessError) as exc:
        raise SubstrateError(
            "repository provenance is unavailable",
            data={
                "repository": str(deployment_dir),
                "operation": "rev-parse --show-toplevel",
                "reason": str(exc),
            },
            fix_hint=(
                "install a working `git` so the publisher can tell whether "
                "this deployment is inside a repository; a deployment that "
                "declares `release.kind: standalone` still needs that answer "
                "before it may publish without repository provenance"
            ),
        ) from exc
    if result.returncode == 0 and result.stdout.strip():
        return False
    if "not a git repository" in (result.stderr or "").lower():
        return True
    raise SubstrateError(
        "repository provenance is unavailable",
        data={
            "repository": str(deployment_dir),
            "operation": "rev-parse --show-toplevel",
            "exit_code": result.returncode,
            "reason": (result.stderr or "").strip() or "git gave no answer",
        },
        fix_hint=(
            "`git rev-parse --show-toplevel` neither resolved a repository nor "
            "reported that there is none, so the publisher cannot tell whether "
            "this deployment is inside a checkout. Repair the Git environment "
            "(a stale GIT_DIR, a dubious-ownership repository, or a broken "
            "install will each do this) and re-run the failed command"
        ),
    )


def _git(root: Path, *args: str) -> str:
    try:
        result = subprocess.run(
            ["git", *args],
            cwd=root,
            check=True,
            capture_output=True,
            text=True,
            timeout=10,
        )
    except (OSError, subprocess.SubprocessError) as exc:
        raise SubstrateError(
            "repository provenance is unavailable",
            data={"repository": str(root), "operation": " ".join(args)},
        ) from exc
    return result.stdout.strip()


__all__ = [
    "RepositoryProvenance",
    "deployment_is_outside_repository",
    "tracking_repository_provenance",
]
