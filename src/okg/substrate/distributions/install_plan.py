"""Resolve a verified package plus a bundle into an install plan and a lock.

Slice 5 of #1179. Slices 1-4 made catalog composition resolve modules
through owner-keyed providers, gave the package manifest an
``okg_compatibility`` window and ``module_declarations``, and made a
verified package serve those modules. This joins them into the operator
verb: name a package, a bundle, and an instance config, and get back an
inspectable **plan** and a **lock**.

**Nothing here runs.** No database is opened, no service starts, no apply
is staged, and no packaged code is imported: the catalog is composed in
memory only to validate the selection, and the only bytes written are
``plan.json`` and ``lock.json`` under the operator-named output
directory. Executing a packaged asset stays #1181's broker boundary.

**Secrets are opaque references, never values or names.** An instance
config binds each secret to an ``env:NAME`` reference *and* to the
owner-qualified id of the selected source or tool declaration it belongs
to. This checks that the declaration is one the bundle actually selects
and whether the variable resolves *now*, then records the declaration, its
digest, and an opaque ``secretref:<uuid5>`` token derived from the
release digest, the instance name, the declaration and the variable
name. A secret is required unless its entry says ``required: false``: an
unset required variable refuses, and an optional one is recorded as a
deferrable entry (``required: false``, ``readiness_policy: deferred``)
whether or not its variable is set, so the plan digest never depends on
the environment. Neither the variable name nor its value reaches the plan or the
lock; the value reaches no refusal payload either, and the name appears
in an unset-variable refusal, which is never persisted (see
``_resolve_secret_references``). An absent reference, an unselected
declaration, a declaration that is not a source or tool, and the older
bare ``name: env:NAME`` form all refuse rather than defaulting.

**Refusal precedence.** Two orderings are deliberate and are pinned by
tests, because a conformance receipt records the refusal *code*, so a
consumer comparing two runs must be able to predict which of several
simultaneous faults is reported.

* Instance-config *shape* refusals come first. ``_read_instance_secrets``
  runs at the top of :func:`resolve_install_plan`, before the package is
  verified, so an operator holding an unverifiable package and an old bare
  ``name: env:NAME`` config sees ``declaration_missing``, not
  ``install_package_unverified``. The shape of the operator's own file is
  readable without trusting any package byte, and it is the fault they can
  fix.
* *Package and selection* refusals come before the secret *resolution*.
  ``_resolve_secret_references`` needs the selected assets and the release
  digest, so it runs after verification, the compatibility window and
  bundle selection. With the variable unset and the package also
  incompatible, the refusal is ``install_okg_incompatible``; with an
  unknown bundle id, ``install_bundle_unknown``. This is a change from the
  prior ordering, where an unset variable beat every package-level
  refusal.

**Idempotence.** Both documents are canonical JSON over inputs that carry
no timestamp, no absolute path, and no ambient state: a package is named
by its transport-bound filename, so a second run with identical inputs
writes byte-identical files. A re-run against an output directory whose
lock disagrees is drift and refuses unless ``replace_lock`` is set.

**Feeding the #1185 receipt.** This writes no conformance receipt, but the
plan and lock carry exactly the fields the landed evidence blocks need
later:

* ``plan["plan_digest"]`` -> ``plan_digest`` on both
  :class:`ConformanceReapplyEvidence` and :class:`ConformanceRecoveryEvidence`.
* ``lock["lock_digest"]`` -> ``lock_digest`` on the same two blocks.
* ``lock["package_selection"]["transport_digest"]`` and the release digest
  under it -> the baseline/candidate/restored ``*_sealed_release_artifact_id``
  fences of :class:`ConformanceRecoveryEvidence`, which slice 7 binds when it
  seals a release from this lock.
* ``plan["assets"]`` and ``plan["declared_inventory_digest"]`` name the
  SELECTED PACKAGE ASSETS.  They never feed
  :class:`ConformanceLegacyAuthorityEvidence`: the legacy_authority arm
  digests the consumer's readable legacy-input inventory
  (``lifecycle_receipts.legacy_input_inventory_declaration``), and a
  selected-asset count is not a legacy inventory.
* ``plan["secret_references"]`` records a required reference as ``{name,
  declaration_ref, declaration_digest, reference, resolved: true}`` (a
  successful plan implies it resolved) and an optional one as ``{name,
  declaration_ref, declaration_digest, reference, required: false,
  readiness_policy: deferred, supply_later}``. Nothing in an entry depends
  on whether a variable is set now: the entry is digested into the plan,
  the lock and the packaged release identity, so recording that state
  would make supplying a deferred credential later refuse as release
  drift. Whether each credential is supplied now comes from
  :func:`split_plan_secrets`, the one helper every producer of supplied
  tokens must use. ``declaration_ref`` and ``declaration_digest`` are the
  owner-qualified id and the TypedAsset digest of a selected asset, which
  is exactly the pair
  :class:`ConformanceOptionalCredentialBinding` compares against the
  request selection; ``reference`` is the opaque ``secretref:<uuid5>``
  that model requires. An unset required variable refuses here, before
  apply, so a conformance receipt records it as a ``first_publish``
  failure with ``mutation: none``; an entry :func:`split_plan_secrets`
  reports as deferred is the input to a deferred
  ``ConformanceOptionalCredentialBinding``.

  Protocol v2 does not require a provider connection: a package need not
  carry a ``provider-connection`` asset, and a v2 binding may omit
  ``provider_connection_ref`` and ``provider_connection_digest``. When a
  binding names one, the validator still requires it to be selected, of
  that kind and digest-bound. Protocol v1 is unchanged.

**Compatibility.** ``INSTALL_PLAN_SCHEMA`` is unchanged: a plan with no
secrets is byte-identical to one this verb wrote before. A plan *with*
secrets changes shape, so its ``plan_digest`` and the lock digest that
carries it both change, and re-planning over an existing output directory
refuses with :class:`InstallLockDrift` until the operator passes
``--replace-lock``.
"""
from __future__ import annotations

import json
import os
import re
import tempfile
import uuid
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any, Optional

from okg.substrate.catalog.module_providers import (
    ModuleProvider,
    builtin_module_providers,
    providers_from_modules_root,
)
from okg.substrate.catalog.modules_compose import compose_catalog
from okg.substrate.catalog.package_module_provider import package_module_providers
from okg.substrate.cli.command_reference import okg_command
from okg.substrate.contracts.deployment_product.lock import (
    canonical_json_bytes,
    sha256_digest,
)
from okg.substrate.distributions.package import (
    DistributionPackageError,
    check_okg_compatibility,
    installed_okg_version,
    verify_distribution_package,
)
from okg.substrate.errors import SubstrateError


__all__ = [
    "INSTALL_LOCK_SCHEMA",
    "INSTALL_PLAN_SCHEMA",
    "InstallBundleUnknown",
    "InstallConnectorAssetMalformed",
    "InstallConnectorRequiredModuleMissing",
    "InstallConnectorSdkBoundInvalid",
    "InstallConnectorSdkUnsupported",
    "InstallConnectorVocabularyUnknown",
    "InstallDeclaredAssetMissing",
    "InstallLockDrift",
    "InstallOkgIncompatible",
    "InstallOutputPathExists",
    "InstallPackageUnverified",
    "InstallPlanError",
    "InstallSecretDeclarationUnselected",
    "InstallSecretReferenceUnresolved",
    "LOCK_FILENAME",
    "PLAN_FILENAME",
    "resolve_install_plan",
    "write_install_plan",
]


INSTALL_PLAN_SCHEMA = "okg.deployment_product.install_plan/v1"
INSTALL_LOCK_SCHEMA = "okg.deployment_product.install_lock/v1"
PLAN_FILENAME = "plan.json"
LOCK_FILENAME = "lock.json"

# Only an environment reference is supported. A literal value in the
# instance config is refused rather than accepted, because accepting it
# would put a secret into an operator file this verb then digests.
_ENV_REFERENCE_RE = re.compile(r"^env:[A-Za-z_][A-Za-z0-9_]*$")
_INSTANCE_KEYS = frozenset({"name", "modules", "secrets"})
_SECRET_KEYS = frozenset({"reference", "declaration", "required", "aliases"})

# The plan records an opaque token, never the variable name. The namespace
# is a fixed version-5 UUID so the token is reproducible from the plan's
# own public fields and unrelated to any other OKG identifier. It was
# derived once with uuid5(uuid.NAMESPACE_URL, ...) from a fixed namespace
# url; the literal below is the contract, and the tests pin it byte for
# byte, so it cannot move silently.
NAMESPACE_OKG_SECRETREF = uuid.UUID("99c54f1e-9424-5e07-980a-6e206ac915b5")

# The owner-qualified id the conformance validator matches against a
# selected manifest asset. Mirrors contracts/deployment_product/
# conformance.py::_IDENTIFIER_RE so a plan entry that passes here also
# passes there.
_DECLARATION_REF_RE = re.compile(r"^[a-z0-9][a-z0-9_.:-]{0,127}$")

# The asset kinds a deferred credential may be declared by, copied from
# the validator's protocol-v2 `declaration_kinds` (conformance.py) so the
# producer and the consumer refuse the same set. `source-template` is in it
# because the in-repo package builders (the example connector and the
# daily-update fixture) give every bundle file that kind, source-defaults
# files included; protocol v1 does not accept it.
_SOURCE_DECLARATION_KINDS = frozenset(
    {"source", "source-binding", "source-definition", "source-template"}
)
_TOOL_DECLARATION_KINDS = frozenset({"tool", "tool-binding", "tool-definition"})
_CREDENTIAL_DECLARATION_KINDS = _SOURCE_DECLARATION_KINDS | _TOOL_DECLARATION_KINDS


class InstallPlanError(SubstrateError):
    """Base for every refusal this verb raises, all before any write."""

    code = "install_plan_refused"


class InstallOutputPathExists(InstallPlanError):
    """The output directory already holds something that is not our lock."""

    code = "install_output_path_exists"


class InstallBundleUnknown(InstallPlanError):
    """The package declares no bundle by the requested id."""

    code = "install_bundle_unknown"


class InstallOkgIncompatible(InstallPlanError):
    """This OKG build falls outside the package's declared version window."""

    code = "install_okg_incompatible"


class InstallPackageUnverified(InstallPlanError):
    """The package failed transport, manifest, or object digest verification."""

    code = "install_package_unverified"


class InstallDeclaredAssetMissing(InstallPlanError):
    """A selected asset reference names nothing the package carries.

    Mirrors the landed conformance code
    ``conformance_distribution_declared_asset_missing``, so a refusal here
    and the receipt arm that reports it read as one vocabulary.
    """

    code = "install_declared_asset_missing"


class InstallSecretReferenceUnresolved(InstallPlanError):
    """An instance secret reference is absent, or is not a reference.

    The refusal names the reference, never a value, and never reports a
    partially resolved set as satisfied.
    """

    code = "install_secret_reference_unresolved"


class InstallSecretDeclarationUnselected(InstallPlanError):
    """A secret names a declaration this bundle did not select.

    Mirrors the conformance validator, which accepts an optional credential
    binding only against a digest-bound source or tool declaration the
    selection carries. A shipped-but-unselected asset is not a declaration.
    """

    code = "install_secret_declaration_unselected"


class InstallLockDrift(InstallPlanError):
    """A lock already exists at the output path and does not match."""

    code = "install_lock_drift"


class InstallConnectorAssetMalformed(InstallPlanError):
    """A declared `connector` asset does not read back as a registration."""

    code = "install_connector_asset_malformed"


class InstallConnectorVocabularyUnknown(InstallPlanError):
    """A declared connector names a capability or auth mode this build lacks.

    Both vocabularies are closed against landed mechanisms, and a package
    may have been built against a wider one, so they are re-checked here
    rather than trusted from declaration time.
    """

    code = "install_connector_vocabulary_unknown"


class InstallConnectorSdkBoundInvalid(InstallPlanError):
    """A declared SDK bound is not a plain PEP 440 release version."""

    code = "install_connector_sdk_bound_invalid"


class InstallConnectorSdkUnsupported(InstallPlanError):
    """The running `okg.deployment` falls outside a connector's SDK window."""

    code = "install_connector_sdk_unsupported"


class InstallConnectorRequiredModuleMissing(InstallPlanError):
    """A connector requires an ontology module this install does not resolve."""

    code = "install_connector_required_module_missing"


def _refuse(
    error: type[InstallPlanError],
    message: str,
    *,
    fix_hint: str,
    **data: object,
) -> InstallPlanError:
    return error(message, data=dict(data), fix_hint=fix_hint)


def secret_reference_token(
    *,
    release_digest: str,
    instance_name: str,
    declaration_ref: str,
    variable: str,
) -> str:
    """Derive the opaque ``secretref:<uuid5>`` this plan records.

    The token is a pure function of four values the plan already states in
    the clear -- the release digest, the instance name, the declaration it
    binds -- plus the environment variable name, which the plan never
    states. It is therefore stable across runs and machines (so the plan
    stays byte-identical), different for every declaration, instance and
    release, and carries no recoverable variable name or value: a reader
    holding the plan cannot invert the hash, and can only confirm a
    variable name they already guessed.
    """

    seed = f"{release_digest}\n{instance_name}\n{declaration_ref}\n{variable}"
    return f"secretref:{uuid.uuid5(NAMESPACE_OKG_SECRETREF, seed)}"


# Fixed text, because it is digested with the entry: naming the variable
# would put it into the plan, and anything environment-dependent would move
# the release identity.
_SUPPLY_LATER = (
    "export the variable this source's credential_refs names (okg doctor "
    "sources lists them) before its next sync; the install identity does "
    "not change, so a later okg install on the same target still matches"
)


def _supplied(
    reference: str, aliases: Sequence[str], environ: Mapping[str, str]
) -> bool:
    """Whether ``reference`` or any alias names a variable set in ``environ``.

    An alias is another name the same source reads the credential from
    (``credential_aliases``); it satisfies presence, but the token stays
    derived from the declared name so it does not move with whichever
    alias the operator happened to export. This is the one presence rule:
    the planner's refusal and :func:`split_plan_secrets` both use it.
    """

    return any(
        item.split(":", 1)[1] in environ for item in (reference, *aliases)
    )


def split_plan_secrets(
    plan: Mapping[str, Any],
    instance: Mapping[str, Any],
    environ: Optional[Mapping[str, str]] = None,
) -> tuple[list[dict[str, object]], list[dict[str, object]]]:
    """Split a plan's secrets into ``(supplied, deferred)`` for *now*.

    The plan records policy only (see :func:`_resolve_secret_references`),
    so a deferred-capable entry and a supplied one carry the same opaque
    token and nothing inside the plan says which it is. Every producer that
    reports or emits supplied credential tokens must take them from the
    first list this returns, never from ``plan["secret_references"]``
    directly: a deferred token copied into a supplied list would claim a
    credential the environment does not hold.

    ``instance`` is the instance config the plan was made from; it is the
    only place the variable names live. A plan entry the instance does not
    declare, or the reverse, refuses rather than being guessed, and so does
    an entry whose ``secretref:`` token the instance does not reproduce: the
    token is recomputed from the plan's release digest and instance name and
    the instance's declaration and variable, so an instance that names a
    different variable or declaration under the same secret name is refused
    instead of being checked against the wrong environment variable.
    """

    environ = os.environ if environ is None else environ
    instance_name, _, secrets = _read_instance(instance)
    entries = plan.get("secret_references") or []
    names = [str(item.get("name")) for item in entries]
    if sorted(names) != sorted(secrets):
        raise _refuse(
            InstallPlanError,
            "the plan's secrets do not match the instance config's secrets",
            plan_secret_names=sorted(names),
            instance_secret_names=sorted(secrets),
            reason="plan_instance_secret_mismatch",
            fix_hint="report credentials from the instance config the plan was made from",
        )
    release_digest = str(plan["package"]["release"]["release_digest"])
    supplied: list[dict[str, object]] = []
    deferred: list[dict[str, object]] = []
    for item in entries:
        entry = secrets[str(item["name"])]
        expected = secret_reference_token(
            release_digest=release_digest,
            instance_name=instance_name,
            declaration_ref=str(entry["declaration"]),
            variable=str(entry["reference"]).split(":", 1)[1],
        )
        if (
            plan.get("instance_name") != instance_name
            or item.get("reference") != expected
            or item.get("declaration_ref") != entry["declaration"]
        ):
            raise _refuse(
                InstallPlanError,
                f"the plan's secret {item['name']!r} was not made from this instance config",
                secret_name=item["name"],
                reason="plan_instance_secret_mismatch",
                fix_hint="report credentials from the instance config the plan was made from",
            )
        row = {
            "name": item["name"],
            "declaration_ref": item["declaration_ref"],
            "reference": item["reference"],
        }
        if _supplied(str(entry["reference"]), entry["aliases"], environ):
            supplied.append(row)
        elif item.get("required", True) is False:
            deferred.append(
                {
                    **row,
                    # `okg install` names profile secrets
                    # `<source_id>.credential.<n>`; an instance-config secret
                    # has no source, and says so.
                    "source_id": (
                        str(item["name"]).split(".credential.", 1)[0]
                        if ".credential." in str(item["name"])
                        else None
                    ),
                    "supply_later": item["supply_later"],
                    # Issue #2560: `okg doctor sources` does not parse.
                    # `--check source-auth` reports each source's
                    # credential_refs and runs its auth preflight.
                    "check_with": okg_command(
                        "doctor", deployment=instance_name, check="source-auth"
                    ).display(),
                }
            )
        else:
            raise _refuse(
                InstallSecretReferenceUnresolved,
                f"required secret {item['name']!r} is unset",
                secret_name=item["name"],
                reason="environment_variable_absent",
                fix_hint="export the required variable and plan again",
            )
    return supplied, deferred


def _resolve_secret_references(
    secrets: Mapping[str, Mapping[str, Any]],
    environ: Mapping[str, str],
    *,
    instance_name: str,
    release_digest: str,
    selected_assets: Sequence[Mapping[str, Any]],
) -> tuple[dict[str, object], ...]:
    """Bind every secret to a selected declaration and an opaque token.

    Nothing this function *records* carries the variable name or its value:
    the recorded entry holds only the secret name, the declaration, its
    digest and the opaque token.

    A refusal is different, and the distinction matters to a reader. The
    unset-variable refusal repeats the variable name in three places -- the
    ``fix_hint``, the message, and ``data["reference"]`` as ``env:NAME`` --
    because the operator wrote that name themselves and cannot act on the
    refusal without it. None of the three is persisted: every refusal
    happens inside :func:`resolve_install_plan`, before
    :func:`write_install_plan` creates the output directory, so no plan, no
    lock and no partial file exists; the CLI prints the envelope to stderr
    and this module logs nothing. The variable *value* appears in no
    refusal payload at all, and the literal-value branch deliberately omits
    ``reference`` because the value would be the thing it named.
    """

    assets_by_ref = {
        f"{item['owner']}:{item['id']}": item for item in selected_assets
    }
    resolved: list[dict[str, object]] = []
    for name in sorted(secrets):
        entry = secrets[name]
        reference = str(entry["reference"])
        declaration_ref = str(entry["declaration"])
        required = entry.get("required", True) is not False
        aliases = tuple(str(item) for item in entry.get("aliases") or ())
        if not _ENV_REFERENCE_RE.fullmatch(reference):
            raise _refuse(
                InstallSecretReferenceUnresolved,
                f"instance secret {name!r} is not an `env:NAME` reference",
                secret_name=name,
                reason="not_a_reference",
                fix_hint=(
                    "write `env:VARIABLE_NAME`; a literal value must not appear "
                    "in an instance config"
                ),
            )
        if not _DECLARATION_REF_RE.fullmatch(declaration_ref):
            raise _refuse(
                InstallSecretDeclarationUnselected,
                f"instance secret {name!r} declares a malformed reference",
                secret_name=name,
                reason="declaration_malformed",
                fix_hint=(
                    "write the declaration as the owner-qualified id of a "
                    "selected asset, `owner:asset-id`"
                ),
            )
        declaration = assets_by_ref.get(declaration_ref)
        if declaration is None:
            raise _refuse(
                InstallSecretDeclarationUnselected,
                (
                    f"instance secret {name!r} names declaration "
                    f"{declaration_ref!r}, which this bundle does not select"
                ),
                secret_name=name,
                declaration_ref=declaration_ref,
                reason="declaration_unselected",
                selected_asset_refs=sorted(assets_by_ref),
                fix_hint=(
                    "bind the secret to an asset the selected bundle carries; "
                    "a shipped-but-unselected asset declares no credential"
                ),
            )
        kind = str(declaration.get("kind"))
        if kind not in _CREDENTIAL_DECLARATION_KINDS:
            raise _refuse(
                InstallSecretDeclarationUnselected,
                (
                    f"instance secret {name!r} names declaration "
                    f"{declaration_ref!r} of kind {kind!r}, which declares "
                    "no credential"
                ),
                secret_name=name,
                declaration_ref=declaration_ref,
                declaration_kind=kind,
                reason="declaration_kind_unsupported",
                accepted_kinds=sorted(_CREDENTIAL_DECLARATION_KINDS),
                fix_hint=(
                    "only a typed source or tool declaration carries a "
                    "deferred credential"
                ),
            )
        variable = reference.split(":", 1)[1]
        token = secret_reference_token(
            release_digest=release_digest,
            instance_name=instance_name,
            declaration_ref=declaration_ref,
            variable=variable,
        )
        if not required:
            # An optional credential may be DEFERRED: the install proceeds
            # whether or not the variable is set now. The recorded entry is
            # the same in both cases -- it states the policy, never the
            # current environment -- because it is digested into the plan,
            # the lock and the packaged release identity. Recording whether
            # the variable happened to be set would make supplying it later
            # (or a daily update run from another shell) refuse as release
            # drift. Whether it is supplied *now* is reported by
            # :func:`split_plan_secrets`, outside every digest.
            resolved.append(
                {
                    "name": name,
                    "declaration_ref": declaration_ref,
                    "declaration_digest": declaration["asset_digest"],
                    "reference": token,
                    "required": False,
                    "readiness_policy": "deferred",
                    "supply_later": _SUPPLY_LATER,
                }
            )
            continue
        if not _supplied(reference, aliases, environ):
            alias_list = list(aliases)
            raise _refuse(
                InstallSecretReferenceUnresolved,
                f"instance secret {name!r} references {reference}, which is unset",
                secret_name=name,
                reference=reference,
                aliases=alias_list,
                declaration_ref=declaration_ref,
                reason="environment_variable_absent",
                fix_hint=(
                    f"export {variable}"
                    + (
                        " (or one of its aliases: "
                        + ", ".join(item.split(":", 1)[1] for item in alias_list)
                        + ")"
                        if alias_list
                        else ""
                    )
                    + " before planning; an absent reference is refused rather "
                    "than defaulted"
                ),
            )
        resolved.append(
            {
                "name": name,
                "declaration_ref": declaration_ref,
                # The TypedAsset digest, which is what the manifest asset
                # references carry and what the conformance validator
                # compares at conformance.py:670 -- NOT the object digest.
                "declaration_digest": declaration["asset_digest"],
                "reference": token,
                "resolved": True,
            }
        )
    return tuple(resolved)


def _read_instance(
    instance: Mapping[str, Any],
) -> tuple[str, tuple[str, ...], Mapping[str, Mapping[str, Any]]]:
    """Read the closed instance-config shape, refusing anything unrecognized."""

    unknown = sorted(set(instance) - _INSTANCE_KEYS)
    if unknown:
        raise _refuse(
            InstallPlanError,
            f"instance config carries unrecognized keys: {', '.join(unknown)}",
            unknown_keys=unknown,
            accepted_keys=sorted(_INSTANCE_KEYS),
            fix_hint=(
                "this verb reads only name, modules and secrets; endpoints "
                "and schedules belong to the deployment wiring, which no "
                "plan or lock records"
            ),
        )
    name = str(instance.get("name") or "instance")
    modules = tuple(str(item) for item in (instance.get("modules") or ()))
    secrets = _read_instance_secrets(instance.get("secrets") or {})
    return name, modules, secrets


def _read_instance_secrets(
    secrets: Any,
) -> dict[str, dict[str, Any]]:
    """Read the closed secret shape: name -> {reference, declaration}.

    Two keys are optional. ``required`` (a bool, default true) says whether
    an unset variable refuses the plan (true) or is recorded as a deferred
    credential (false). ``aliases`` lists further ``env:NAME`` references
    the same source also reads; any one of them being set counts as
    supplied.

    The older bare-string form (``name: env:NAME``) is refused rather than
    accepted with an invented declaration: the conformance validator binds
    every deferred credential to a named selected declaration, and there is
    no truthful default for one.
    """

    if not isinstance(secrets, Mapping):
        raise _refuse(
            InstallPlanError,
            "instance `secrets` must be a mapping of name to reference and declaration",
            fix_hint=(
                "write secrets as a mapping of name to {reference: env:NAME, "
                "declaration: owner:asset-id}"
            ),
        )
    read: dict[str, dict[str, Any]] = {}
    for key in sorted(secrets):
        name = str(key)
        entry = secrets[key]
        if isinstance(entry, str):
            raise _refuse(
                InstallSecretReferenceUnresolved,
                (
                    f"instance secret {name!r} is a bare reference with no "
                    "declaration"
                ),
                secret_name=name,
                reason="declaration_missing",
                fix_hint=(
                    "write {reference: env:VARIABLE_NAME, declaration: "
                    "owner:asset-id}; a deferred credential must name the "
                    "selected source or tool declaration it belongs to"
                ),
            )
        if not isinstance(entry, Mapping):
            raise _refuse(
                InstallSecretReferenceUnresolved,
                f"instance secret {name!r} is not a mapping",
                secret_name=name,
                reason="declaration_missing",
                fix_hint=(
                    "write {reference: env:VARIABLE_NAME, declaration: "
                    "owner:asset-id}"
                ),
            )
        unknown = sorted(set(map(str, entry)) - _SECRET_KEYS)
        if unknown:
            raise _refuse(
                InstallPlanError,
                (
                    f"instance secret {name!r} carries unrecognized keys: "
                    f"{', '.join(unknown)}"
                ),
                secret_name=name,
                unknown_keys=unknown,
                accepted_keys=sorted(_SECRET_KEYS),
                fix_hint="a secret reads only reference and declaration",
            )
        declaration = entry.get("declaration")
        if not isinstance(declaration, str) or not declaration:
            raise _refuse(
                InstallSecretReferenceUnresolved,
                f"instance secret {name!r} names no declaration",
                secret_name=name,
                reason="declaration_missing",
                fix_hint=(
                    "set declaration to the owner-qualified id of the selected "
                    "source or tool asset this credential belongs to"
                ),
            )
        reference = entry.get("reference")
        if not isinstance(reference, str):
            raise _refuse(
                InstallSecretReferenceUnresolved,
                f"instance secret {name!r} is not an `env:NAME` reference",
                secret_name=name,
                reason="not_a_reference",
                fix_hint=(
                    "write `env:VARIABLE_NAME`; a literal value must not appear "
                    "in an instance config"
                ),
            )
        required = entry.get("required", True)
        if not isinstance(required, bool):
            raise _refuse(
                InstallPlanError,
                f"instance secret {name!r} has a non-boolean `required`",
                secret_name=name,
                reason="required_not_boolean",
                fix_hint="write required: true or required: false",
            )
        aliases = entry.get("aliases", ())
        if isinstance(aliases, str) or not isinstance(aliases, Sequence) or any(
            not isinstance(item, str) or not _ENV_REFERENCE_RE.fullmatch(item)
            for item in aliases
        ):
            raise _refuse(
                InstallSecretReferenceUnresolved,
                f"instance secret {name!r} has aliases that are not `env:NAME` references",
                secret_name=name,
                reason="not_a_reference",
                fix_hint="write aliases as a list of `env:VARIABLE_NAME` references",
            )
        read[name] = {
            "reference": reference,
            "declaration": declaration,
            "required": required,
            "aliases": tuple(aliases),
        }
    return read


def _selected_module_ids(
    manifest: Any,
    selected_refs: frozenset[str],
) -> tuple[str, ...]:
    """Modules whose every declared asset the selection actually carries."""

    selected: list[str] = []
    for declaration in tuple(getattr(manifest, "module_declarations", ()) or ()):
        refs = {ref.qualified_id for ref in declaration.asset_refs}
        if refs and refs <= selected_refs:
            selected.append(declaration.module_id)
    return tuple(sorted(selected))


def _module_object_digests(
    manifest: Any,
    module_ids: Sequence[str],
) -> dict[str, list[str]]:
    """Map each selected module id to the object digests behind it."""

    object_by_ref = {
        item.asset.qualified_id: item.locator.object_digest for item in manifest.assets
    }
    wanted = set(module_ids)
    return {
        declaration.module_id: sorted(
            object_by_ref[ref.qualified_id]
            for ref in declaration.asset_refs
            if ref.qualified_id in object_by_ref
        )
        for declaration in tuple(getattr(manifest, "module_declarations", ()) or ())
        if declaration.module_id in wanted
    }


CONNECTOR_ASSET_KIND = "connector"


def _running_sdk_version() -> str:
    """The `okg.deployment.__version__` this install would run against.

    Resolved through `sys.modules`, for the reason the SDK's own copy of
    this helper records: a fresh `import` builds a *second* package object
    whenever a test has evicted the first, and reads a version the caller
    cannot see.
    """

    import importlib
    import sys

    package = sys.modules.get("okg.deployment") or importlib.import_module(
        "okg.deployment"
    )
    return str(package.__version__)


def _registration_data(exc: Exception) -> dict[str, Any]:
    """A refusal's declaration-time payload, minus the keys we add here."""

    data = getattr(exc, "data", None) or {}
    return {
        key: value
        for key, value in data.items()
        if key not in ("asset_ref", "registration_error_code")
    }


def _resolved_connectors(
    package: Any,
    assets: Sequence[Mapping[str, Any]],
    module_refs: Sequence[str],
    sdk_version: str,
) -> tuple[dict[str, Any], ...]:
    """Read every selected `connector` asset back into a registration.

    A declaration is data on disk, authored by whatever SDK the packager
    ran, so every check the SDK makes at declaration time is made again
    here -- the closed vocabularies included -- and the SDK window is
    checked against the build that would actually run the connector.
    Nothing packaged is imported: the asset is JSON, read through the
    package's own verified reader.
    """

    from okg.deployment import ConnectorRegistration
    from okg.substrate.catalog.module_providers import parse_module_ref
    from okg.substrate.errors import (
        ConnectorAuthModeUnknown,
        ConnectorCapabilityUnknown,
        ConnectorRegistrationError,
        ConnectorSdkBoundInvalid,
    )

    # A `modules:` entry may be a bare built-in name; a connector always
    # names its requirement as `owner:name`, so both are compared in the
    # qualified spelling rather than as written.
    resolved_modules = frozenset(
        ":".join(parse_module_ref(ref)) for ref in module_refs
    )
    records: list[dict[str, Any]] = []
    for item in sorted(assets, key=lambda entry: (entry["owner"], entry["id"])):
        if item.get("kind") != CONNECTOR_ASSET_KIND:
            continue
        asset_ref = f"{item['owner']}:{item['id']}"
        try:
            payload = json.loads(package.read_asset(asset_ref).decode("utf-8"))
        except Exception as exc:
            raise _refuse(
                InstallConnectorAssetMalformed,
                f"connector asset {asset_ref} is not readable JSON: {exc}",
                asset_ref=asset_ref,
                reason=type(exc).__name__,
                fix_hint=(
                    "package the body `as_asset_payload()` returns, encoded "
                    "as UTF-8 JSON"
                ),
            ) from exc
        try:
            registration = ConnectorRegistration.from_asset_payload(payload)
        except (ConnectorCapabilityUnknown, ConnectorAuthModeUnknown) as exc:
            raise _refuse(
                InstallConnectorVocabularyUnknown,
                f"connector asset {asset_ref} declares a value this build "
                f"does not implement: {exc}",
                asset_ref=asset_ref,
                registration_error_code=exc.code,
                **_registration_data(exc),
                fix_hint=(
                    "declare only capabilities and auth modes this OKG "
                    "release implements, or install a release that does"
                ),
            ) from exc
        except ConnectorSdkBoundInvalid as exc:
            raise _refuse(
                InstallConnectorSdkBoundInvalid,
                f"connector asset {asset_ref} declares an unusable SDK "
                f"bound: {exc}",
                asset_ref=asset_ref,
                registration_error_code=exc.code,
                **_registration_data(exc),
                fix_hint=(
                    "declare plain release bounds; the running build is "
                    "compared by its base release"
                ),
            ) from exc
        except ConnectorRegistrationError as exc:
            raise _refuse(
                InstallConnectorAssetMalformed,
                f"connector asset {asset_ref} does not read back as a "
                f"registration: {exc}",
                asset_ref=asset_ref,
                registration_error_code=exc.code,
                fix_hint=(
                    "rebuild the asset with `as_asset_payload()` from the "
                    "SDK version the package declares"
                ),
            ) from exc

        window = {
            "min_version": registration.sdk_range.min_version,
            "max_version_exclusive": (
                registration.sdk_range.max_version_exclusive
            ),
        }
        if not registration.sdk_range.supports(sdk_version):
            raise _refuse(
                InstallConnectorSdkUnsupported,
                f"connector {registration.connector_type!r} requires "
                f"okg.deployment in [{window['min_version']}, "
                f"{window['max_version_exclusive']}), running {sdk_version}",
                asset_ref=asset_ref,
                connector_type=registration.connector_type,
                sdk_version=sdk_version,
                **window,
                fix_hint=(
                    "install an OKG release inside the declared window, or "
                    "take a connector build that supports this one"
                ),
            )
        missing = sorted(
            set(registration.required_modules) - resolved_modules
        )
        if missing:
            raise _refuse(
                InstallConnectorRequiredModuleMissing,
                f"connector {registration.connector_type!r} requires modules "
                f"this install does not resolve: {', '.join(missing)}",
                asset_ref=asset_ref,
                connector_type=registration.connector_type,
                missing_modules=missing,
                resolved_modules=sorted(resolved_modules),
                fix_hint=(
                    "select a bundle that carries the module, or name it in "
                    "the instance config's `modules`"
                ),
            )
        records.append({
            "asset_ref": asset_ref,
            "connector_type": registration.connector_type,
            "connector_version": registration.version,
            "object_digest": item["object_digest"],
            "sdk_range": window,
            "sdk_satisfied": True,
            "required_modules": sorted(registration.required_modules),
        })
    return tuple(records)


def _digest_without(document: Mapping[str, Any], key: str) -> str:
    body = {name: value for name, value in document.items() if name != key}
    return sha256_digest(canonical_json_bytes(body))


def resolve_install_plan(
    package_path: Path | str,
    bundle_id: str,
    instance: Mapping[str, Any],
    *,
    okg_version: Optional[str] = None,
    sdk_version: Optional[str] = None,
    environ: Optional[Mapping[str, str]] = None,
    builtin_modules_root: Optional[Path] = None,
    cross_links_root: Optional[Path] = None,
) -> tuple[dict[str, Any], dict[str, Any]]:
    """Verify, select, compose, and return ``(plan, lock)`` without writing.

    Every refusal happens here, before :func:`write_install_plan` creates a
    file, so a failed plan leaves no partial output behind.
    """

    from okg.substrate.deployment_product_lifecycle import (
        package_selection_from_verified,
    )

    environ = os.environ if environ is None else environ
    name, extra_modules, secrets = _read_instance(instance)
    package_path = Path(package_path)

    try:
        package = verify_distribution_package(package_path)
    except DistributionPackageError as exc:
        raise _refuse(
            InstallPackageUnverified,
            f"distribution package verification failed: {exc}",
            package_error_code=exc.code,
            logical_path=exc.logical_path,
            fix_hint="re-fetch the package; its bytes do not match its digests",
        ) from exc

    try:
        manifest = package.manifest
        try:
            check_okg_compatibility(manifest, okg_version=okg_version)
        except DistributionPackageError as exc:
            raise _refuse(
                InstallOkgIncompatible,
                f"package refuses this OKG build: {exc}",
                package_error_code=exc.code,
                declared=(
                    None
                    if manifest.okg_compatibility is None
                    else {
                        "min_version": manifest.okg_compatibility.min_version,
                        "max_version_exclusive": (
                            manifest.okg_compatibility.max_version_exclusive
                        ),
                    }
                ),
                fix_hint="install an OKG release inside the declared window",
            ) from exc

        declared_bundles = sorted(item.bundle_id for item in manifest.bundles)
        if bundle_id not in declared_bundles:
            raise _refuse(
                InstallBundleUnknown,
                f"package declares no bundle {bundle_id!r}",
                bundle_id=bundle_id,
                declared_bundles=declared_bundles,
                fix_hint=f"select one of: {', '.join(declared_bundles) or '(none)'}",
            )

        try:
            selection = package_selection_from_verified(package, bundle_id=bundle_id)
        except Exception as exc:
            raise _refuse(
                InstallDeclaredAssetMissing,
                f"bundle {bundle_id!r} selects assets the package does not carry: {exc}",
                bundle_id=bundle_id,
                fix_hint="rebuild the package so every selected asset reference resolves",
            ) from exc

        secret_references = _resolve_secret_references(
            secrets,
            environ,
            instance_name=name,
            release_digest=str(selection.distribution_release["release_digest"]),
            selected_assets=selection.assets,
        )

        selected_refs = frozenset(
            f"{item['owner']}:{item['id']}" for item in selection.assets
        )
        module_ids = _selected_module_ids(manifest, selected_refs)
        module_refs = tuple(dict.fromkeys((*module_ids, *extra_modules)))
        connectors = _resolved_connectors(
            package,
            selection.assets,
            module_refs,
            sdk_version or _running_sdk_version(),
        )

        version = okg_version or installed_okg_version()
        with tempfile.TemporaryDirectory(prefix="okg-install-plan-") as scratch:
            scratch_root = Path(scratch)
            providers: dict[str, ModuleProvider] = dict(
                providers_from_modules_root(builtin_modules_root)
                if builtin_modules_root is not None
                else builtin_module_providers()
            )
            providers.update(
                package_module_providers(package, scratch_root / "modules")
            )
            deployment_dir = scratch_root / "deployment"
            deployment_dir.mkdir()
            (deployment_dir / "deployment.yaml").write_text(
                json.dumps({"name": name, "modules": list(module_refs)}),
            )
            composed = compose_catalog(
                deployment_dir,
                providers=providers,
                cross_links_root=cross_links_root,
            )
            catalog = {
                "module_refs": list(module_refs),
                "subtypes": sorted(composed.subtypes),
                "subtype_origin": dict(sorted(composed.subtype_origin.items())),
            }
    finally:
        package.close_runtime_authority()

    catalog["catalog_digest"] = sha256_digest(canonical_json_bytes(catalog))
    assets = [dict(item) for item in selection.assets]
    plan: dict[str, Any] = {
        "schema": INSTALL_PLAN_SCHEMA,
        "package": {
            # The filename is transport-bound (`<sha256>.okgdist.tar`), so
            # naming the file rather than its path keeps the plan identical
            # across machines and directories.
            "filename": package_path.name,
            "transport_digest": selection.transport_digest,
            "release": dict(selection.distribution_release),
        },
        "bundle": dict(selection.bundle),
        "okg": {
            "version": version,
            "compatibility": (
                None
                if manifest.okg_compatibility is None
                else {
                    "min_version": manifest.okg_compatibility.min_version,
                    "max_version_exclusive": (
                        manifest.okg_compatibility.max_version_exclusive
                    ),
                }
            ),
            "satisfied": True,
        },
        "modules": [
            {
                "module_id": module_id,
                "provider_kind": "package",
                "object_digests": digests,
            }
            for module_id, digests in sorted(
                _module_object_digests(manifest, module_ids).items()
            )
        ],
        "connectors": [dict(item) for item in connectors],
        "assets": assets,
        "declared_inventory_digest": sha256_digest(canonical_json_bytes(assets)),
        "declared_inventory_entry_count": len(assets),
        "catalog": catalog,
        "secret_references": [dict(item) for item in secret_references],
        "instance_name": name,
    }
    plan["plan_digest"] = _digest_without(plan, "plan_digest")

    lock: dict[str, Any] = {
        "schema": INSTALL_LOCK_SCHEMA,
        "plan_digest": plan["plan_digest"],
        "okg_version": version,
        "bundle_id": bundle_id,
        "package_selection": selection.to_mapping(),
        "package_selection_digest": selection.digest,
        "modules": {
            item["module_id"]: item["object_digests"] for item in plan["modules"]
        },
        # The connector ids and the object digest behind each: a repackaged
        # connector changes its digest, which changes the lock digest, which
        # is what `write_install_plan` reports as drift.
        "connectors": {
            item["asset_ref"]: item["object_digest"] for item in connectors
        },
    }
    lock["lock_digest"] = _digest_without(lock, "lock_digest")
    return plan, lock


def write_install_plan(
    package_path: Path | str,
    bundle_id: str,
    instance: Mapping[str, Any],
    output_dir: Path | str,
    *,
    replace_lock: bool = False,
    okg_version: Optional[str] = None,
    sdk_version: Optional[str] = None,
    environ: Optional[Mapping[str, str]] = None,
    builtin_modules_root: Optional[Path] = None,
    cross_links_root: Optional[Path] = None,
) -> dict[str, Any]:
    """Resolve, then write ``plan.json`` and ``lock.json`` into `output_dir`.

    A directory that already holds a matching lock is rewritten with the
    identical bytes, which is what makes a repeat run idempotent. One that
    holds a lock that disagrees is drift and refuses unless `replace_lock`
    is set; one that holds anything else refuses outright.
    """

    output_dir = Path(output_dir)
    lock_path = output_dir / LOCK_FILENAME
    existing: Optional[Mapping[str, Any]] = None
    if output_dir.exists():
        if not (output_dir.is_dir() and lock_path.is_file()):
            raise _refuse(
                InstallOutputPathExists,
                f"output path {output_dir} already exists and holds no install lock",
                output_dir=str(output_dir),
                fix_hint="name an unused output directory, or point at a prior install",
            )
        try:
            existing = json.loads(lock_path.read_text())
        except (OSError, ValueError) as exc:
            raise _refuse(
                InstallOutputPathExists,
                f"existing lock at {lock_path} could not be read",
                output_dir=str(output_dir),
                fix_hint="remove the unreadable lock, or name a fresh output directory",
            ) from exc

    plan, lock = resolve_install_plan(
        package_path,
        bundle_id,
        instance,
        okg_version=okg_version,
        sdk_version=sdk_version,
        environ=environ,
        builtin_modules_root=builtin_modules_root,
        cross_links_root=cross_links_root,
    )

    if existing is not None and not replace_lock:
        recorded = existing.get("lock_digest")
        if recorded != lock["lock_digest"]:
            raise _refuse(
                InstallLockDrift,
                "the resolved install no longer matches the recorded lock",
                output_dir=str(output_dir),
                recorded_lock_digest=recorded,
                resolved_lock_digest=lock["lock_digest"],
                fix_hint="re-run with --replace-lock to accept the new resolution",
            )

    # Reported, never digested: whether each optional credential is set in
    # this environment. Only an instance with secrets gets the key, so a
    # secret-free plan's summary is unchanged.
    deferred: Optional[list[dict[str, object]]] = None
    if instance.get("secrets"):
        _, deferred = split_plan_secrets(plan, instance, environ)

    output_dir.mkdir(parents=True, exist_ok=True)
    plan_path = output_dir / PLAN_FILENAME
    plan_path.write_bytes(canonical_json_bytes(plan) + b"\n")
    lock_path.write_bytes(canonical_json_bytes(lock) + b"\n")
    return {
        "plan_path": str(plan_path),
        "lock_path": str(lock_path),
        "plan_digest": plan["plan_digest"],
        "lock_digest": lock["lock_digest"],
        "bundle_id": bundle_id,
        "modules": list(lock["modules"]),
        "connectors": list(lock["connectors"]),
        "replaced_lock": bool(existing is not None and replace_lock),
        **({} if deferred is None else {"deferred_credentials": deferred}),
    }
