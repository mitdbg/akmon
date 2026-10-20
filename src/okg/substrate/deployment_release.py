"""Deployment release contracts and version-skew checks.

This module owns the first slice of ``standardize-deployment-upgrade``:
deployment manifests can declare a release channel, prod deployments pin the
artifact they should run, and startup/readiness surfaces can refuse when the
process, database, and deployment contract disagree.
"""
from __future__ import annotations

import hashlib
import subprocess
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from okg.substrate.deployments import DeploymentManifest, resolve_deployment
from okg.substrate.errors import SubstrateError


RELEASE_CHANNELS = frozenset({"dev", "prod"})
PIN_FIELDS = (
    "code_revision",
    "schema_checksum",
    "catalog_fingerprint",
    "skill_bundle_hash",
)

# Values `release.kind` may take in deployment.yaml. Absent means the ordinary
# checkout-tracking deployment: repository provenance is derived from Git and
# every drift refusal applies.
#
#   packaged   -- the deployment came from a verified package. Recorded
#                 provenance is `unavailable`; the packaged release identity
#                 carries the authority instead.
#   standalone -- (#1955) the operator has deliberately placed this deployment
#                 outside any Git repository, which is what `okg install` does
#                 by default (the XDG user-state deployments root is never a
#                 working tree). Recorded provenance is `unavailable`, in the
#                 same shape the packaged branch records, so a reader can tell
#                 that no repository authority backed the generation.
#
# `standalone` is a DECLARATION, never a fallback. A deployment that does not
# declare it and is not in a repository still refuses to publish, exactly as
# before; and a deployment that declares it but does sit inside a repository
# keeps the ordinary tracking path and every one of its refusals.
#
#   distribution -- okg runs from a verified installed wheel (decision
#                 "wheel A", 2026-10-04). DETECTED, not declared: how okg is
#                 installed belongs to the runtime, and detection fails closed
#                 (an editable or unlisted install keeps the Git path). A
#                 deployment may still declare it to require it, and a prod
#                 pin names it with `framework_payload_digest`.
RELEASE_KINDS = frozenset({"packaged", "standalone", "distribution"})

# `ReleaseArtifact.release_kind` records which kind of deployment the artifact
# was built for (#1962). It is the field that says WHY a `code_revision` is
# absent: standalone and packaged artifacts both carry a null revision, so the
# revision alone cannot tell them apart, and neither can
# `records_repository_revision`, which is False for both.
STANDALONE_RELEASE_KIND = "standalone"
PACKAGED_RELEASE_KIND = "packaged"
DISTRIBUTION_RELEASE_KIND = "distribution"


class ReleaseKindInvalid(SubstrateError):
    """`release.kind` in deployment.yaml is not a kind the substrate knows."""

    code = "release_kind_invalid"


class StandaloneReleaseConflict(SubstrateError):
    """`release.kind: standalone` was combined with another release authority.

    Standalone means "no release authority backs this deployment". A verified
    packaged release and a sealed worker release each assert the opposite, so
    the combination is refused rather than silently resolved in either
    direction -- the same stance as the existing packaged-plus-sealed refusal.
    """

    code = "standalone_release_conflict"


@dataclass(frozen=True)
class ReleaseArtifact:
    code_revision: str | None
    schema_checksum: str | None
    catalog_fingerprint: str | None
    skill_bundle_hash: str | None
    # Optional split-repository authority metadata. The four fields above
    # remain the required release pin for backwards compatibility; deployment
    # upgrade owns the cross-repository resolution semantics.
    code_repository: str | None = None
    deployment_revision: str | None = None
    packaged_release: dict[str, str] | None = None
    # #1962. Set to `STANDALONE_RELEASE_KIND` when this artifact describes a
    # deployment that declared `release.kind: standalone` AND really does sit
    # outside every Git repository. It is the typed statement that
    # `code_revision` is absent BY DECLARATION rather than unknown.
    release_kind: str | None = None
    # Decision "wheel A". The verified identity of the installed okg wheel
    # (InstalledFramework.to_dict()) on a current artifact, and the payload
    # digest a prod pin requires of it.
    framework_distribution: dict[str, Any] | None = None
    framework_payload_digest: str | None = None

    @property
    def records_repository_revision(self) -> bool:
        """Whether `code_revision` is a Git object id a consumer may use.

        A consumer that formats, compares, or hashes the revision asks this
        first. False means the artifact carries no repository revision; it
        never means "look again later". It does not say WHICH kind of
        deployment this is -- it is False for standalone and packaged
        alike. Read `release_kind` for that.
        """
        return self.code_revision is not None

    @classmethod
    def standalone(
        cls,
        *,
        schema_checksum: str | None,
        catalog_fingerprint: str | None,
        skill_bundle_hash: str | None,
    ) -> "ReleaseArtifact":
        """The artifact for a declared, verified-outside-Git deployment.

        `code_revision` is absent and `release_kind` says why. The remaining
        three fields are real measurements of the deployment as it stands, so
        schema-skew and catalog-drift detection keep working unchanged: only
        the repository half of the identity is waived.
        """
        return cls(
            code_revision=None,
            schema_checksum=schema_checksum,
            catalog_fingerprint=catalog_fingerprint,
            skill_bundle_hash=skill_bundle_hash,
            release_kind=STANDALONE_RELEASE_KIND,
        )

    def to_dict(self) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "code_revision": self.code_revision,
            "schema_checksum": self.schema_checksum,
            "catalog_fingerprint": self.catalog_fingerprint,
            "skill_bundle_hash": self.skill_bundle_hash,
        }
        if self.packaged_release is not None:
            payload["packaged_release"] = self.packaged_release
        if self.code_repository is not None:
            payload["code_repository"] = self.code_repository
        if self.deployment_revision is not None:
            payload["deployment_revision"] = self.deployment_revision
        if self.release_kind is not None:
            payload["release_kind"] = self.release_kind
        if self.framework_distribution is not None:
            payload["framework_distribution"] = dict(self.framework_distribution)
        if self.framework_payload_digest is not None:
            payload["framework_payload_digest"] = self.framework_payload_digest
        return payload

    @classmethod
    def from_mapping(cls, raw: dict[str, Any]) -> "ReleaseArtifact":
        packaged = None
        if "packaged_release" in raw:
            from okg.substrate.distributions.packaged_release import PackagedReleaseIdentity
            packaged = PackagedReleaseIdentity.from_metadata(raw["packaged_release"]).to_dict()
            if raw.get("code_revision") is not None or raw.get("deployment_revision") is not None or raw.get("code_repository") is not None:
                raise ValueError("packaged and Git release identity cannot be combined")
        code_revision = _optional_str(raw.get("code_revision"))
        release_kind = _optional_str(raw.get("release_kind"))
        if release_kind is not None and release_kind not in RELEASE_KINDS:
            raise ReleaseKindInvalid(
                "release pin release_kind is not a known release kind",
                data={
                    "observed": release_kind,
                    "allowed": sorted(RELEASE_KINDS),
                },
                fix_hint=(
                    "set `release.pin.release_kind` to one of "
                    f"{', '.join(sorted(RELEASE_KINDS))} -- matching the "
                    "deployment's own `release.kind` -- or remove the key "
                    "for a deployment pinned to a Git revision"
                ),
            )
        # #1962. A pin that claims standalone while also naming a repository
        # identity describes two different deployments. Refusing here means
        # the operator is told at contract-load time, rather than reading a
        # `release_pin_mismatch` on a field the pin could never satisfy.
        if release_kind == STANDALONE_RELEASE_KIND:
            conflicting = [
                field
                for field in ("code_revision", "deployment_revision", "code_repository")
                if _optional_str(raw.get(field)) is not None
            ]
            if packaged is not None:
                conflicting.append("packaged_release")
            if conflicting:
                raise StandaloneReleaseConflict(
                    "standalone release pin cannot also pin a repository "
                    "or packaged identity",
                    data={
                        "declared_kind": STANDALONE_RELEASE_KIND,
                        "conflicting_authority": ",".join(sorted(conflicting)),
                    },
                    fix_hint=(
                        "a standalone deployment records no repository "
                        "revision, so remove "
                        f"{', '.join(sorted(conflicting))} from "
                        "`release.pin`, or drop `release_kind: standalone` "
                        "from the pin"
                    ),
                )
        framework_payload_digest = _optional_str(raw.get("framework_payload_digest"))
        if release_kind == DISTRIBUTION_RELEASE_KIND:
            conflicting = [
                field
                for field in ("code_revision", "code_repository")
                if _optional_str(raw.get(field)) is not None
            ]
            if packaged is not None:
                conflicting.append("packaged_release")
            if conflicting:
                raise ValueError(
                    "a distribution release pin names the installed wheel by "
                    "framework_payload_digest and cannot also pin "
                    + ", ".join(sorted(conflicting))
                )
        elif framework_payload_digest is not None:
            raise ValueError(
                "framework_payload_digest pins an installed wheel; set "
                "release_kind: distribution beside it"
            )
        return cls(
            packaged_release=packaged,
            code_revision=code_revision,
            framework_payload_digest=framework_payload_digest,
            schema_checksum=_optional_str(raw.get("schema_checksum")),
            catalog_fingerprint=_optional_str(raw.get("catalog_fingerprint")),
            skill_bundle_hash=_optional_str(raw.get("skill_bundle_hash")),
            code_repository=_optional_str(raw.get("code_repository")),
            deployment_revision=_optional_str(raw.get("deployment_revision")),
            release_kind=release_kind,
        )


@dataclass(frozen=True)
class ReleaseContract:
    channel: str
    pin: ReleaseArtifact | None

    def to_dict(self) -> dict[str, Any]:
        return {
            "channel": self.channel,
            "pin": self.pin.to_dict() if self.pin else None,
        }


@dataclass(frozen=True)
class DatabaseReleaseState:
    ledger_exists: bool
    schema_checksum: str | None
    schema_ok: bool
    pending_migrations: tuple[dict[str, Any], ...]
    blocking_pending_migrations: tuple[dict[str, Any], ...]
    checksum_mismatches: tuple[dict[str, Any], ...]
    checksum_compatibilities: tuple[dict[str, Any], ...]
    latest_catalog_version_id: int | None = None
    latest_catalog_fingerprint: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "ledger_exists": self.ledger_exists,
            "schema_checksum": self.schema_checksum,
            "schema_ok": self.schema_ok,
            "pending_migrations": list(self.pending_migrations),
            "blocking_pending_migrations": list(
                self.blocking_pending_migrations,
            ),
            "checksum_mismatches": list(self.checksum_mismatches),
            "checksum_compatibilities": list(self.checksum_compatibilities),
            "latest_catalog_version_id": self.latest_catalog_version_id,
            "latest_catalog_fingerprint": self.latest_catalog_fingerprint,
        }


def _optional_str(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _fix_hint(deployment: str) -> str:
    return f"run `okg up {deployment}`"


# Problem kinds whose only cure is applying migrations. `okg up` does many
# things and reaches the same place eventually; naming the one command that
# fixes THIS is what a fresh install needs. See `_problems_fix_hint`.
_MIGRATION_PROBLEM_KINDS = frozenset({
    "schema_ledger_missing",
    "schema_migrations_pending",
})


def _problems_fix_hint(
    deployment: str, problems: list[dict[str, Any]],
) -> str:
    """The fix hint for a set of release-contract problems.

    Issue #2127. A fresh `okg install` now writes a `release` block, so
    `enforce_release_contract_if_declared` no longer short-circuits and the
    database checks actually run. The first command an operator points at a
    brand-new, unmigrated database therefore refuses with
    `schema_ledger_missing` -- and the generic hint told them to run
    `okg up`, which is not the command that creates the ledger. Name
    `okg migrate` for the two problems migrations fix, and leave every
    other problem on the general hint.

    The deployment name goes in `--deployment`. `okg migrate` takes no
    positional argument, so the first form of this hint --
    `okg migrate --apply <name>` -- exited 2 with `unrecognized
    arguments` the moment an operator pasted it. A hint that does not
    parse is worse than the generic one it replaced. The tests in
    `tests/substrate/test_deployment_release.py` now run every hinted
    command through the real CLI parser.
    """
    kinds = {str(p.get("kind")) for p in problems}
    migrate_command = f"okg migrate --deployment {deployment} --apply"
    # "Create" only when there is no ledger to apply into.
    ledger_missing = "schema_ledger_missing" in kinds
    migrate_does = (
        "create and apply the schema migration ledger"
        if ledger_missing else "apply the pending schema migrations"
    )
    if kinds and kinds <= _MIGRATION_PROBLEM_KINDS:
        return (
            f"run `{migrate_command}` to {migrate_does}, "
            f"then re-run the failed command"
        )
    if kinds & _MIGRATION_PROBLEM_KINDS:
        why = (
            "this database has no applied schema ledger"
            if ledger_missing else "migrations are pending"
        )
        return (
            f"run `{migrate_command}` first -- {why} -- then "
            f"{_fix_hint(deployment)} for the remaining skew"
        )
    return _fix_hint(deployment)


def _deployment_repository_fix_hint(
    deployment: str,
    data: dict[str, Any],
    *,
    declared_kind: str | None = None,
) -> str:
    """Name the remedy that actually matches the deployment-authority failure.

    Before #1955 every deployment-authority failure was answered with
    "run `okg up <name>`". When the cause is that the deployment directory is
    not in a Git repository at all -- the state `okg install` leaves behind by
    default -- `okg up` cannot fix it, and the operator was left with no
    reachable next step.

    ``declared_kind`` keeps that new hint honest. A deployment that ALREADY
    declares standalone must never be told to declare standalone.

    Since #1962 that case means something narrow and worth naming. A declared
    standalone deployment that really is outside every repository never
    reaches this refusal at all -- it takes the standalone branch in
    :func:`current_release_artifact` and is served. Reaching here while
    declaring standalone therefore means the two Git questions DISAGREED: the
    publisher's probe (which strips ``GIT_DIR`` and friends) answered
    "inside a repository", and repository-root resolution (which does not)
    then failed. A stale ``GIT_DIR`` in the environment is the usual cause,
    and "declare standalone" would not fix it.
    """
    if str(data.get("operation")) != "resolve_deployment_repository":
        return _fix_hint(deployment)
    if declared_kind == STANDALONE_RELEASE_KIND:
        return (
            "this deployment declares `release.kind: standalone`, and a "
            "standalone deployment outside every Git repository is served "
            "without a repository revision -- so reaching this refusal means "
            "Git reported that the deployment IS inside a repository and then "
            "failed to resolve that repository's root. Clear any stale "
            "`GIT_DIR`, `GIT_WORK_TREE` or `GIT_COMMON_DIR` from the "
            "environment, repair the checkout that contains "
            f"`{deployment}`, and re-run the failed command"
        )
    return (
        "this deployment directory is not inside a Git repository. Either "
        "move it into one and commit it, or -- if it is deliberately outside "
        "version control -- declare that in its deployment.yaml with BOTH "
        "`release.channel: dev` and `release.kind: standalone`, then re-run "
        "the failed command. A standalone deployment publishes with "
        "repository provenance recorded as `unavailable`. Naming "
        "both keys is what makes one hand-written block satisfy every "
        "command. `kind` alone lets the publish through and is then refused "
        "by `okg run` and `okg mcp-serve`, which load the full release "
        "contract and require a channel. Deployments that `okg install` "
        "scaffolds outside a repository now carry both keys already"
    )


def _framework_repository_fix_hint() -> str:
    return (
        "set `release.pin.code_repository` to the directory holding the "
        "framework checkout (relative to the deployment repository root or "
        "absolute), then restart the worker"
    )


def _framework_revision_fix_hint(repository: Path) -> str:
    return (
        f"repair the framework Git checkout at `{repository}` so HEAD "
        "resolves, or set `release.pin.code_repository` to a valid framework "
        "checkout, then restart the worker"
    )


def _framework_provenance_fix_hint(
    repository: Path,
    revision: str,
) -> str:
    return (
        "run or reinstall okg from the framework checkout "
        f"`{repository}` at revision `{revision}`, then restart the worker"
    )


def declared_release_kind(manifest: DeploymentManifest) -> str | None:
    """Return the validated `release.kind`, or None when none is declared.

    Raises :class:`ReleaseKindInvalid` on any other value rather than ignoring
    it: a typo'd kind must not read as "no declaration" and then fail much
    later with an unrelated repository-authority message.
    """
    release = manifest.release if isinstance(manifest.release, dict) else {}
    kind = _optional_str(release.get("kind"))
    if kind is None:
        return None
    if kind not in RELEASE_KINDS:
        raise ReleaseKindInvalid(
            "deployment release.kind is not a known release kind",
            data={
                "deployment": manifest.name,
                "observed": kind,
                "allowed": sorted(RELEASE_KINDS),
            },
            fix_hint=(
                "set `release.kind` to one of "
                f"{', '.join(sorted(RELEASE_KINDS))}, or remove the key for an "
                "ordinary deployment tracked in a Git checkout"
            ),
        )
    return kind


def standalone_release_declared(deployment_dir: Path) -> bool:
    """Whether the deployment at ``deployment_dir`` declares standalone mode.

    A directory with no ``deployment.yaml`` declares nothing, so this returns
    False and the caller keeps whatever behavior it had before. That is not a
    fallback that hides a failure: a manifest that exists but cannot be parsed
    still raises, and an unknown ``release.kind`` is still refused.

    Read through :meth:`DeploymentManifest.from_yaml_lenient` deliberately.
    This runs while the publisher is being constructed, on every publish,
    including paths that never previously parsed the manifest at all (a
    packaged or sealed release). The STRICT loader additionally requires the
    manifest's full structure (``schema_dir``, ``source_registry`` and their
    on-disk targets), so using it here would turn "reads one declared key"
    into a new structural gate on publisher construction and would refuse
    deployments that publish correctly today. Both loaders share one parsing
    implementation and both populate ``release``.
    """
    manifest_path = Path(deployment_dir) / "deployment.yaml"
    if not manifest_path.is_file():
        return False
    return declared_release_kind(
        DeploymentManifest.from_yaml_lenient(manifest_path)
    ) == "standalone"


def load_release_contract(manifest: DeploymentManifest) -> ReleaseContract:
    raw = manifest.release
    if not isinstance(raw, dict):
        raise SubstrateError(
            "deployment release.channel is required",
            data={"deployment": manifest.name, "field": "release.channel"},
            fix_hint=(
                "set `release.channel: dev` for checkout-tracking "
                f"deployments, or {_fix_hint(manifest.name)} to pin prod"
            ),
        )

    channel = _optional_str(raw.get("channel"))
    if channel not in RELEASE_CHANNELS:
        raise SubstrateError(
            "deployment release.channel must be dev or prod",
            data={
                "deployment": manifest.name,
                "observed": channel,
                "allowed": sorted(RELEASE_CHANNELS),
            },
            fix_hint=(
                "set `release.channel: dev` for checkout-tracking "
                "deployments or `release.channel: prod` for pinned "
                "deployments"
            ),
        )

    pin_raw = raw.get("pin")
    if channel == "dev":
        pin = (
            ReleaseArtifact.from_mapping(pin_raw)
            if isinstance(pin_raw, dict) else None
        )
        return ReleaseContract(channel=channel, pin=pin)

    if not isinstance(pin_raw, dict):
        raise SubstrateError(
            "prod deployments require release.pin",
            data={"deployment": manifest.name, "missing": ["release.pin"]},
            fix_hint=_fix_hint(manifest.name),
        )
    required_fields = (
        ("framework_payload_digest", *PIN_FIELDS[1:])
        if _optional_str(pin_raw.get("release_kind")) == DISTRIBUTION_RELEASE_KIND
        else PIN_FIELDS
    )
    missing = [field for field in required_fields if field not in pin_raw]
    if missing:
        raise SubstrateError(
            "prod deployment release.pin is incomplete",
            data={"deployment": manifest.name, "missing": missing},
            fix_hint=_fix_hint(manifest.name),
        )
    return ReleaseContract(
        channel=channel,
        pin=ReleaseArtifact.from_mapping(pin_raw),
    )


def current_release_artifact(manifest: DeploymentManifest) -> ReleaseArtifact:
    from okg.substrate.catalog.modules_compose import compose_catalog
    from okg.substrate.catalog.ownership import catalog_identity_for
    from okg.substrate.db.management import current_known_migrations
    from okg.substrate.deployment_upgrade import (
        RevisionAuthorityError,
        _installed_framework_matches_revision,
        _resolve_repo_authorities,
    )

    schema_checksum = current_known_migrations()[0].checksum
    release = manifest.release if isinstance(manifest.release, dict) else {}
    pin = release.get("pin") if isinstance(release.get("pin"), dict) else {}
    # Validate the declaration FIRST. A typo'd `release.kind` must refuse as
    # `release_kind_invalid` here, not read as "nothing declared" and then
    # surface many frames later as an unrelated repository-authority message.
    declared_kind = declared_release_kind(manifest)

    def _catalog_fingerprint() -> str | None:
        return catalog_identity_for(
            composed=compose_catalog(manifest),
            deployment_dir=manifest.deployment_dir,
            command="okg deployment release",
        ).catalog_fingerprint

    from okg.substrate.distributions.packaged_release import current_verified_release
    packaged = current_verified_release(Path(manifest.deployment_dir))
    if packaged is not None:
        if declared_kind == STANDALONE_RELEASE_KIND:
            raise StandaloneReleaseConflict(
                "standalone and packaged release authority cannot be combined",
                data={
                    "deployment": manifest.name,
                    "deployment_dir": str(manifest.deployment_dir),
                    "declared_kind": STANDALONE_RELEASE_KIND,
                    "conflicting_authority": "packaged",
                },
                fix_hint=(
                    "a packaged deployment already records provenance as "
                    "unavailable under its packaged release identity; remove "
                    "`release.kind: standalone` from its deployment.yaml"
                ),
            )
        # `release_kind` is set here for the same reason it is set on the
        # standalone artifact, and because a pin may declare
        # `release_kind: packaged`: without it that pin compared `packaged`
        # against None and refused EVERY startup of a correctly packaged
        # deployment with `release_pin_mismatch field=release_kind`.
        return ReleaseArtifact(
            code_revision=None, schema_checksum=schema_checksum,
            catalog_fingerprint=_catalog_fingerprint(),
            skill_bundle_hash=_hash_tree(manifest.skills_dir),
            packaged_release=packaged.identity.to_dict(),
            release_kind=PACKAGED_RELEASE_KIND,
        )
    if declared_kind == "packaged":
        raise SubstrateError("packaged release requires verified launcher context")
    # #1962. The read-and-serve half of #1955. `okg runtime worker`,
    # `okg mcp-serve` and `okg deployment release` all reach this function
    # through `build_release_report`, and before this branch existed every one
    # of them fell through to `_resolve_repo_authorities` and refused with
    # "deployment release authority is unavailable" -- so a deployment that
    # could publish still could not be served.
    #
    # The branch carries the publisher's directory bound (#1955): the
    # declaration alone is not enough, the deployment must ALSO really be
    # outside every repository, checked with the publisher's own probe rather
    # than a second implementation of it. A deployment that declares
    # standalone but sits inside a repository keeps the tracking path below
    # and every refusal that comes with it -- which is also what keeps
    # `tracking_repository_provenance` supplied with a real Git object id for
    # `RepositoryProvenance.exact`.
    #
    # It does NOT carry the publisher's sealed-authority bound. This function
    # takes no DSN and so cannot see whether a sealed worker release is
    # active; `build_deployment_publisher` (publisher/factory.py) can, and
    # refuses standalone-plus-sealed there with `standalone_release_conflict`.
    # A deployment in that state therefore PASSES the release contract and is
    # refused later, at publisher construction. The packaged conflict is
    # caught here because a verified packaged release is readable from the
    # deployment directory alone.
    if declared_kind == STANDALONE_RELEASE_KIND:
        from okg.substrate.publisher.repository_provenance import (
            deployment_is_outside_repository,
        )

        if deployment_is_outside_repository(Path(manifest.deployment_dir)):
            return ReleaseArtifact.standalone(
                schema_checksum=schema_checksum,
                catalog_fingerprint=_catalog_fingerprint(),
                skill_bundle_hash=_hash_tree(manifest.skills_dir),
            )
    explicit_code_repository = (
        pin.get("code_repository")
        if isinstance(pin, dict) and "code_repository" in pin
        else None
    )
    # Decision "wheel A": okg running from a verified installed wheel names
    # itself by that wheel when there is no okg Git checkout to name it by.
    # Git authority is tried FIRST and keeps every existing path: a sealed
    # release installs okg non-editably inside the framework repository, and
    # it must stay on its pinned Git revision. The wheel identity is used only
    # when Git cannot find an okg checkout at all, or when the deployment
    # declares `release.kind: distribution`. An explicit
    # `release.pin.code_repository` always keeps its meaning.
    def _distribution_artifact() -> ReleaseArtifact | None:
        from okg.substrate.distributions.installed_framework import (
            installed_framework_distribution,
        )

        installed = installed_framework_distribution()
        if installed is None:
            return None
        deployment_revision = None
        if isinstance(pin, dict) and "deployment_revision" in pin:
            from okg.substrate.deployment_upgrade import _git_repo_root

            deployment_revision = _git_revision(
                _git_repo_root(Path(manifest.deployment_dir)),
            )
        return ReleaseArtifact(
            code_revision=None,
            schema_checksum=schema_checksum,
            catalog_fingerprint=_catalog_fingerprint(),
            skill_bundle_hash=_hash_tree(manifest.skills_dir),
            deployment_revision=deployment_revision,
            release_kind=DISTRIBUTION_RELEASE_KIND,
            framework_distribution=installed.to_dict(),
        )

    if declared_kind == DISTRIBUTION_RELEASE_KIND:
        artifact = (
            _distribution_artifact() if explicit_code_repository is None else None
        )
        if artifact is not None:
            return artifact
        raise SubstrateError(
            "deployment requires okg installed from a wheel",
            data={
                "deployment": manifest.name,
                "declared_kind": DISTRIBUTION_RELEASE_KIND,
                "operation": "detect_installed_framework",
                "reason": (
                    "the running okg is not a file its installed wheel's "
                    "RECORD lists (an editable install or a source checkout), "
                    "or release.pin.code_repository is set"
                ),
            },
            fix_hint=(
                "install okg from a built wheel (`pip install "
                "okg-<version>.whl`) into the environment that runs this "
                "command, or remove `release.kind: distribution`"
            ),
        )
    requested_code_revision = _optional_str(pin.get("code_revision"))
    try:
        code_root, deployment_root = _resolve_repo_authorities(
            Path(manifest.deployment_dir),
            explicit_code_repository=(
                str(explicit_code_repository)
                if explicit_code_repository is not None
                else None
            ),
            requested_code_revision=requested_code_revision,
        )
    except RevisionAuthorityError as exc:
        authority_role = str(exc.data.get("authority_role") or "framework")
        if (
            authority_role == "framework"
            and exc.data.get("operation") == "discover_framework_repository"
            and explicit_code_repository is None
        ):
            artifact = _distribution_artifact()
            if artifact is not None:
                return artifact
        if authority_role == "deployment":
            message = "deployment release authority is unavailable"
            fix_hint = _deployment_repository_fix_hint(
                manifest.name,
                exc.data,
                declared_kind=declared_kind,
            )
        else:
            message = "framework release authority is unavailable"
            fix_hint = _framework_repository_fix_hint()
        raise SubstrateError(
            message,
            data=exc.data,
            fix_hint=fix_hint,
        ) from exc
    code_revision = _git_revision(code_root)
    planner_path = code_root / "src/okg/substrate/deployment_upgrade.py"
    if not planner_path.is_file():
        raise SubstrateError(
            "framework release authority is incomplete",
            data={
                "authority_role": "framework",
                "repository": str(code_root),
                "requested_revision": pin.get("code_revision"),
                "resolved_revision": code_revision,
                "operation": "validate_framework_repository",
                "reason": (
                    "required planner path is missing: "
                    "src/okg/substrate/deployment_upgrade.py"
                ),
            },
            fix_hint=_framework_repository_fix_hint(),
        )
    if not code_revision:
        raise SubstrateError(
            "framework release revision is unavailable",
            data={
                "authority_role": "framework",
                "repository": str(code_root),
                "requested_revision": pin.get("code_revision"),
                "resolved_revision": None,
                "operation": "resolve_framework_revision",
                "reason": "framework repository HEAD is unavailable",
            },
            fix_hint=_framework_revision_fix_hint(code_root),
        )
    provenance = _installed_framework_matches_revision(
        code_root,
        code_revision,
        Path(__file__).resolve(),
    )
    if not provenance.matched:
        raise SubstrateError(
            "framework release provenance is not proven",
            data={
                "authority_role": "framework",
                "repository": str(code_root),
                "requested_revision": pin.get("code_revision"),
                "resolved_revision": code_revision,
                "operation": "verify_installed_framework_provenance",
                "reason": provenance.reason,
                "evaluation": provenance.to_dict(),
            },
            fix_hint=_framework_provenance_fix_hint(
                code_root,
                code_revision,
            ),
        )
    deployment_revision = (
        _git_revision(deployment_root)
        if isinstance(pin, dict) and "deployment_revision" in pin
        else None
    )
    composed = compose_catalog(manifest)
    catalog_identity = catalog_identity_for(
        composed=composed,
        deployment_dir=manifest.deployment_dir,
        command="okg deployment release",
    )
    return ReleaseArtifact(
        code_revision=code_revision,
        schema_checksum=schema_checksum,
        catalog_fingerprint=catalog_identity.catalog_fingerprint,
        skill_bundle_hash=_hash_tree(manifest.skills_dir),
        code_repository=(
            str(explicit_code_repository).strip()
            if explicit_code_repository is not None
            else None
        ),
        deployment_revision=deployment_revision,
    )


def load_database_release_state(dsn: str) -> DatabaseReleaseState:
    from okg.substrate.db.connection import connect as db_connect
    from psycopg.rows import dict_row

    from okg.substrate.db.management import (
        SCHEMA_SNAPSHOT_MIGRATION_ID,
        migration_status,
    )

    status = migration_status(dsn)
    schema_checksum: str | None = None
    for row in status.get("applied") or ():
        if row.get("migration_id") == SCHEMA_SNAPSHOT_MIGRATION_ID:
            schema_checksum = _optional_str(row.get("checksum"))
            break

    latest_catalog_version_id: int | None = None
    latest_catalog_fingerprint: str | None = None
    # Issue #2127. `migration_status` reports a missing ledger instead of
    # raising, but this query then asked an UNMIGRATED database for
    # `okg.catalog_versions` and got `UndefinedTable` -- an unhandled
    # psycopg traceback out of `okg mcp-serve`, in place of the
    # `schema_ledger_missing` refusal the caller is built to report. Before
    # this change the path was unreachable from a fresh install, because the
    # installer wrote no `release` block and the contract was skipped
    # entirely; writing one is what made an operator meet it on their first
    # command. No ledger means no schema, so there is nothing to read here
    # and the refusal is left to say so.
    if status.get("ledger_exists"):
        with db_connect(dsn, connect_timeout=5) as conn:
            with conn.cursor(row_factory=dict_row) as cur:
                cur.execute(
                    """
                    SELECT catalog_version_id, catalog_fingerprint
                      FROM okg.catalog_versions
                     WHERE status = 'applied'
                     ORDER BY catalog_version_id DESC
                     LIMIT 1
                    """
                )
                row = cur.fetchone()
                if row is not None:
                    latest_catalog_version_id = int(row["catalog_version_id"])
                    latest_catalog_fingerprint = _optional_str(
                        row.get("catalog_fingerprint"),
                    )

    pending = tuple(dict(row) for row in status.get("pending") or ())
    blocking_pending = tuple(
        row for row in pending if not bool(row.get("destructive"))
    )
    mismatches = tuple(
        dict(row) for row in status.get("checksum_mismatches") or ()
    )
    compatibilities = tuple(
        dict(row) for row in status.get("checksum_compatibilities") or ()
    )
    return DatabaseReleaseState(
        ledger_exists=bool(status.get("ledger_exists")),
        schema_checksum=schema_checksum,
        schema_ok=bool(status.get("ledger_exists")) and not blocking_pending
        and not mismatches,
        pending_migrations=pending,
        blocking_pending_migrations=blocking_pending,
        checksum_mismatches=mismatches,
        checksum_compatibilities=compatibilities,
        latest_catalog_version_id=latest_catalog_version_id,
        latest_catalog_fingerprint=latest_catalog_fingerprint,
    )


# The one answer every surface reads from a release report (PACT
# unknown-s7-release-verdict; design `unknown-as-a-value` section 4.5).
#
#   serve     -- no problems. A pending DESTRUCTIVE migration is not a
#                problem: it is a cleanup `okg up` reports and defers, and a
#                database with one pending still serves correctly (see
#                `load_database_release_state` and
#                `test_destructive_pending_migrations_do_not_block_release`).
#   read_only -- the only problems are pending additive migrations, and
#                every one declares `reads_safe_behind: true` in
#                migrations/manifest.yaml: code reading a database without
#                them returns the same answers.
#   refuse    -- anything else, including one pending additive migration
#                that does not declare it.
#
# The verdict never reclassifies a problem: `ok` and `problems` are computed
# exactly as before, so `read_only` still has `ok: False` and every caller
# that enforces `ok` still refuses. Reading the verdict is how a caller tells
# a database it may read from one it may not.
RELEASE_VERDICT_SERVE = "serve"
RELEASE_VERDICT_READ_ONLY = "read_only"
RELEASE_VERDICT_REFUSE = "refuse"
READS_SAFE_BEHIND_KEY = "reads_safe_behind"


def _migration_ids(rows: Any) -> list[str]:
    return [str(row.get("migration_id")) for row in rows]


def _plural(count: int, noun: str) -> str:
    return f"{count} {noun}" if count == 1 else f"{count} {noun}s"


def _problem_phrase(
    kind: str,
    problems: list[dict[str, Any]],
    *,
    behind: list[str],
    unsafe: list[str],
) -> str:
    if kind == "schema_ledger_missing":
        return "the database has no okg.schema_migrations ledger"
    if kind == "schema_migrations_pending":
        phrase = (
            f"the database is {_plural(len(behind), 'migration')} behind "
            f"this code ({', '.join(behind)})"
        )
        if unsafe:
            verb = "does" if len(unsafe) == 1 else "do"
            phrase += (
                f", and {', '.join(unsafe)} {verb} not declare "
                f"{READS_SAFE_BEHIND_KEY}, so reads behind "
                f"{'it' if len(unsafe) == 1 else 'them'} could return old "
                "answers with no error"
            )
        return phrase
    if kind == "schema_checksum_mismatch":
        return "an applied migration or the schema checksum does not match this code"
    if kind == "release_pin_missing":
        return "the prod release declares no pin"
    if kind == "release_pin_mismatch":
        fields = sorted({
            str(p.get("field")) for p in problems if p.get("kind") == kind
        })
        return f"the running code differs from the prod pin on {', '.join(fields)}"
    if kind == "standalone_release_pin_unsatisfiable":
        return (
            "the prod pin names a code revision, which a standalone "
            "release never has"
        )
    return kind


def _release_verdict(
    problems: list[dict[str, Any]],
    database: DatabaseReleaseState,
) -> dict[str, Any]:
    blocking = list(database.blocking_pending_migrations)
    behind = _migration_ids(blocking)
    unsafe = _migration_ids(
        row for row in blocking if row.get(READS_SAFE_BEHIND_KEY) is not True
    )
    # With no ledger every known migration reads as pending; that list is
    # not a distance, so the distance is unknown rather than a number.
    schema_behind_by = len(behind) if database.ledger_exists else None
    base = {
        "schema_behind_by": schema_behind_by,
        "behind_migrations": behind,
        "reads_unsafe_migrations": unsafe,
    }
    if not problems:
        destructive = _migration_ids(
            row for row in database.pending_migrations
            if bool(row.get("destructive"))
        )
        message = None
        if destructive:
            message = (
                f"serving; {_plural(len(destructive), 'destructive cleanup migration')} "
                f"pending ({', '.join(destructive)}), which `okg up` reports "
                "and applies only on confirmation"
            )
        return {
            **base,
            "verdict": RELEASE_VERDICT_SERVE,
            "verdict_reason": "release_compatible",
            "verdict_message": message,
        }
    kinds = list(dict.fromkeys(str(p.get("kind")) for p in problems))
    # Fail closed (decision 2026-09-28): any schema snapshot checksum
    # difference refuses, even beside declared reads-safe migrations, because
    # schema.sql and views.sql also carry view, function, grant and policy
    # changes the migration ledger does not describe. Read-only starts only
    # when the fingerprint matches exactly; a per-migration fingerprint that
    # could tell the two apart is follow-up #2797.
    if kinds == ["schema_migrations_pending"] and behind and not unsafe:
        read_only_message = (
            f"read-only; the database is "
            f"{_plural(len(behind), 'migration')} behind this code "
            f"({', '.join(behind)}), and every one declares "
            f"{READS_SAFE_BEHIND_KEY}"
        )
        return {
            **base,
            "verdict": RELEASE_VERDICT_READ_ONLY,
            "verdict_reason": "schema_behind_reads_safe",
            "verdict_message": read_only_message,
        }
    phrases = [
        _problem_phrase(kind, problems, behind=behind, unsafe=unsafe)
        for kind in kinds
    ]
    return {
        **base,
        "verdict": RELEASE_VERDICT_REFUSE,
        "verdict_reason": kinds[0],
        "verdict_message": "refused: " + "; ".join(phrases),
    }


def evaluate_release_contract(
    *,
    deployment: str,
    contract: ReleaseContract,
    current: ReleaseArtifact,
    database: DatabaseReleaseState,
    process_role: str,
) -> dict[str, Any]:
    problems: list[dict[str, Any]] = []
    if not database.ledger_exists:
        problems.append({
            "kind": "schema_ledger_missing",
            "message": "database has no okg.schema_migrations ledger",
        })
    if database.blocking_pending_migrations:
        problems.append({
            "kind": "schema_migrations_pending",
            "pending": list(database.blocking_pending_migrations),
        })
    if database.checksum_mismatches:
        problems.append({
            "kind": "schema_checksum_mismatch",
            "mismatches": list(database.checksum_mismatches),
        })
    if (
        database.schema_checksum is not None
        and current.schema_checksum is not None
        and database.schema_checksum != current.schema_checksum
    ):
        problems.append({
            "kind": "schema_checksum_mismatch",
            "expected": current.schema_checksum,
            "observed": database.schema_checksum,
        })

    if contract.channel == "prod":
        pin = contract.pin
        if pin is None:
            problems.append({"kind": "release_pin_missing"})
        else:
            # #1962. A standalone artifact carries no repository revision by
            # declaration. Comparing it against a pinned Git id would report
            # a generic `release_pin_mismatch` on a field this deployment can
            # never satisfy, which reads as drift the operator could fix by
            # syncing code. Report the real cause instead, once, and drop the
            # revision from the field-by-field comparison so the two do not
            # both fire.
            standalone_current = (
                current.release_kind == STANDALONE_RELEASE_KIND
            )
            distribution_current = (
                current.release_kind == DISTRIBUTION_RELEASE_KIND
            )
            unsatisfiable_revision = (
                (standalone_current or distribution_current)
                and pin.code_revision is not None
            )
            if unsatisfiable_revision:
                problems.append({
                    "kind": (
                        "distribution_release_pin_unsatisfiable"
                        if distribution_current
                        else "standalone_release_pin_unsatisfiable"
                    ),
                    "field": "code_revision",
                    "expected": pin.code_revision,
                    "observed": None,
                })
            if distribution_current:
                problems.extend(_distribution_prod_problems(current, pin))
            release_fields = [
                ("schema_checksum", current.schema_checksum, pin.schema_checksum),
                (
                    "catalog_fingerprint",
                    current.catalog_fingerprint,
                    pin.catalog_fingerprint,
                ),
                (
                    "skill_bundle_hash",
                    current.skill_bundle_hash,
                    pin.skill_bundle_hash,
                ),
            ]
            if not unsatisfiable_revision:
                release_fields.append(
                    ("code_revision", current.code_revision, pin.code_revision)
                )
            if pin.release_kind is not None:
                release_fields.append(
                    ("release_kind", current.release_kind, pin.release_kind)
                )
            if pin.code_repository is not None:
                release_fields.append(
                    ("code_repository", current.code_repository, pin.code_repository)
                )
            if pin.deployment_revision is not None:
                release_fields.append(
                    (
                        "deployment_revision",
                        current.deployment_revision,
                        pin.deployment_revision,
                    )
                )
            for field, observed, expected in release_fields:
                if observed != expected:
                    problems.append({
                        "kind": "release_pin_mismatch",
                        "field": field,
                        "expected": expected,
                        "observed": observed,
                    })

    verdict = _release_verdict(problems, database)
    # Under `read_only` the ledger exists and only migrations are pending;
    # the hint is the command doctor's finding names, `okg up <dep>`, so
    # mcp-serve, doctor and the release report give one command.
    if not problems:
        fix_hint = None
    elif verdict["verdict"] == RELEASE_VERDICT_READ_ONLY:
        fix_hint = _fix_hint(deployment)
    else:
        fix_hint = _problems_fix_hint(deployment, problems)
    report: dict[str, Any] = {
        "ok": not problems,
        "deployment": deployment,
        "process_role": process_role,
        "channel": contract.channel,
        "current": current.to_dict(),
        "database": database.to_dict(),
        "pin": contract.pin.to_dict() if contract.pin else None,
        "problems": problems,
        **verdict,
        "fix_hint": fix_hint,
    }
    report["refusal_message"] = release_refusal_message(report)
    return report


def _distribution_prod_problems(
    current: ReleaseArtifact, pin: ReleaseArtifact,
) -> list[dict[str, Any]]:
    """Prod refuses a wheel it cannot tie to a clean, named build.

    Dev records whatever is true; prod requires the pin to name the wheel's
    payload digest and the wheel to name the clean commit it was built from
    (decision "wheel A", option C).
    """
    identity = current.framework_distribution or {}
    problems: list[dict[str, Any]] = []
    if pin.framework_payload_digest is None:
        problems.append({
            "kind": "distribution_release_pin_missing_digest",
            "field": "framework_payload_digest",
            "observed": identity.get("payload_digest"),
        })
    elif pin.framework_payload_digest != identity.get("payload_digest"):
        problems.append({
            "kind": "release_pin_mismatch",
            "field": "framework_payload_digest",
            "expected": pin.framework_payload_digest,
            "observed": identity.get("payload_digest"),
        })
    if identity.get("source_revision") is None or identity.get("source_dirty") is not False:
        problems.append({
            "kind": "distribution_build_unverified",
            "source_revision": identity.get("source_revision"),
            "source_dirty": identity.get("source_dirty"),
        })
    return problems


def assert_release_compatible(
    *,
    deployment: str,
    contract: ReleaseContract,
    current: ReleaseArtifact,
    database: DatabaseReleaseState,
    process_role: str,
) -> dict[str, Any]:
    report = evaluate_release_contract(
        deployment=deployment,
        contract=contract,
        current=current,
        database=database,
        process_role=process_role,
    )
    if report["ok"]:
        return report
    raise _release_skew_error(deployment, report)


def build_release_report(
    *,
    deployment: str,
    dsn: str,
    process_role: str = "cli",
) -> dict[str, Any]:
    manifest = resolve_deployment(deployment)
    contract = load_release_contract(manifest)
    current = current_release_artifact(manifest)
    database = load_database_release_state(dsn)
    return evaluate_release_contract(
        deployment=deployment,
        contract=contract,
        current=current,
        database=database,
        process_role=process_role,
    )


def enforce_release_contract_if_declared(
    *,
    deployment: str,
    dsn: str,
    process_role: str,
    serve_read_only: bool = False,
) -> dict[str, Any] | None:
    """Refuse a start-up surface the release verdict does not let serve.

    Returns the report (or ``None`` with no declared release) when the
    surface may start. ``serve_read_only`` is for a surface that can serve
    reads alone -- only `okg mcp-serve` (PACT unknown-s8-mcp-readonly): it
    also returns a `read_only` report, and the caller must then refuse every
    write. The verdict is read, never recomputed; `ok` stays False under
    `read_only`, so every other caller still refuses.
    """
    report = release_report_if_declared(
        deployment=deployment,
        dsn=dsn,
        process_role=process_role,
    )
    if report is None or report["ok"]:
        return report
    if serve_read_only and report.get("verdict") == RELEASE_VERDICT_READ_ONLY:
        return report
    raise _release_skew_error(deployment, report)


def release_report_if_declared(
    *,
    deployment: str,
    dsn: str,
    process_role: str,
) -> dict[str, Any] | None:
    """The release report, or ``None`` when the deployment declares no release.

    ``None`` is not a verdict: it means no release contract exists, so the
    caller keeps its own rule (issue #2128: report pending migrations and
    continue). Doctor reads this without raising; the start-up surfaces go
    through `enforce_release_contract_if_declared`.
    """
    manifest = resolve_deployment(deployment)
    if manifest.release is None:
        return None
    return build_release_report(
        deployment=deployment,
        dsn=dsn,
        process_role=process_role,
    )


def release_refusal_message(report: dict[str, Any]) -> str | None:
    """The one refusal sentence for a release report, or None when it is ok.

    Built once and stored in the report as `refusal_message`, so
    `okg deployment release`, doctor, `okg mcp-serve` and `okg runtime
    worker` print the same words for the same database.

    Under `read_only` the sentence says what still works: `okg mcp-serve`
    serves reads and refuses writes (PACT unknown-s8-mcp-readonly); every
    other surface refuses until the database is migrated.
    """
    if report.get("ok"):
        return None
    message = (
        "deployment release/version skew detected: "
        f"{report['verdict_message']}"
    )
    if report.get("verdict") == RELEASE_VERDICT_READ_ONLY:
        message += (
            "; only `okg mcp-serve` serves this database, read-only, until "
            "it is migrated"
        )
    return message


def _release_skew_error(
    deployment: str, report: dict[str, Any],
) -> SubstrateError:
    """The one refusal a start-up surface raises for a report that is not ok.

    The message carries the verdict's own sentence, so the operator reads the
    same reason doctor and `okg deployment release` give. A `read_only`
    verdict refuses here too, except for `okg mcp-serve`, which asks for it
    with ``serve_read_only`` and serves reads alone.
    """
    return SubstrateError(
        report["refusal_message"],
        data=report,
        # The report already chose the hint that matches its problems
        # (`_problems_fix_hint`); re-deriving a generic one here is how the
        # migrate hint got lost on the way to the operator.
        fix_hint=report["fix_hint"] or _fix_hint(deployment),
    )


def _git_revision(start: Path) -> str | None:
    try:
        proc = subprocess.run(
            ["git", "rev-parse", "HEAD"],
            cwd=start,
            check=False,
            capture_output=True,
            text=True,
            timeout=5,
        )
    except Exception:  # noqa: BLE001 - release report should stay inspectable.
        return None
    if proc.returncode != 0:
        return None
    return proc.stdout.strip() or None


def _hash_tree(path: Path | None) -> str | None:
    if path is None or not path.exists():
        return None
    h = hashlib.sha256()
    root = path.resolve()
    if root.is_file():
        h.update(root.name.encode("utf-8"))
        h.update(root.read_bytes())
        return "sha256:" + h.hexdigest()
    for file_path in sorted(p for p in root.rglob("*") if p.is_file()):
        if _ignored_hash_path(file_path):
            continue
        rel = file_path.relative_to(root).as_posix()
        h.update(rel.encode("utf-8"))
        h.update(b"\0")
        h.update(file_path.read_bytes())
        h.update(b"\0")
    return "sha256:" + h.hexdigest()


def _ignored_hash_path(path: Path) -> bool:
    ignored_parts = {".DS_Store", "__pycache__", ".pytest_cache"}
    if any(part in ignored_parts for part in path.parts):
        return True
    return path.suffix in {".pyc", ".pyo"}
