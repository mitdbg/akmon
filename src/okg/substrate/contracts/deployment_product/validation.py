"""Wave 1 validation API for deployment-product selections."""

from __future__ import annotations

from collections.abc import Iterable, Mapping, Sequence
import hashlib
import json
from typing import Any

from pydantic import BaseModel, ConfigDict, model_validator

from .models import (
    API_VERSION,
    CONTRACT_VERSION,
    AuthoredDeploymentDefinition,
    DistributionRelease,
    DistributionReleaseReference,
    OwnerQualifiedReference,
    ProductBundle,
    ProductBundleReference,
    ProductIdentity,
    ProductModel,
    ProvenanceRecord,
)


SELECTION_SCHEMA = "okg.deployment_product.validated_selection/v1"


class SelectionValidationError(ValueError):
    """Raised when a definition cannot select the supplied product artifacts."""


class ValidatedProductSelection(ProductModel):
    """Validated identity/provenance aggregate consumed by later resolution.

    This is intentionally not a lock and does not perform the Wave 2 asset DAG
    or semantic merge.  Its job is to establish one compatible release/bundle
    selection and stable owner-qualified provenance.
    """

    model_config = ConfigDict(
        extra="forbid",
        frozen=True,
        populate_by_name=True,
        strict=True,
        validate_assignment=True,
    )

    schema_version: str = SELECTION_SCHEMA
    definition: AuthoredDeploymentDefinition
    distribution_release: DistributionRelease
    bundles: tuple[ProductBundle, ...] = ()
    identity: ProductIdentity
    provenance: tuple[ProvenanceRecord, ...] = ()
    contract_version: str = CONTRACT_VERSION
    api_version: str = API_VERSION

    @model_validator(mode="after")
    def _identity_is_consistent(self) -> "ValidatedProductSelection":
        if self.identity.definition_id != self.definition.definition_id:
            raise ValueError("selection identity does not match the authored definition")
        if self.identity.distribution_id != self.distribution_release.distribution_id:
            raise ValueError("selection identity does not match the distribution release")
        if self.identity.distribution_version != self.distribution_release.version:
            raise ValueError("selection identity does not match the distribution version")
        if self.identity.bundle_ids != tuple(bundle.bundle_id for bundle in self.bundles):
            raise ValueError("selection identity does not match the selected bundles")
        return self

    @property
    def product_bundles(self) -> tuple[ProductBundle, ...]:
        return self.bundles


def _model(value: Any, cls: type[BaseModel]) -> BaseModel:
    try:
        if isinstance(value, cls):
            # ``model_copy(update=...)`` is guarded on ProductModel, but this
            # round-trip is still required here so callers cannot pass a
            # previously smuggled/constructed instance around the public API.
            value = value.model_dump(mode="python", round_trip=True)
        return cls.model_validate(value)
    except Exception as exc:  # pydantic's error includes the useful path
        raise SelectionValidationError(str(exc)) from exc


def _release_reference(value: DistributionReleaseReference | DistributionRelease) -> DistributionReleaseReference:
    if isinstance(value, DistributionRelease):
        return value.reference
    return value


def _bundle_reference(value: ProductBundleReference | ProductBundle) -> ProductBundleReference:
    if isinstance(value, ProductBundle):
        return value.reference
    return value


def _deterministic_digest(value: object) -> str:
    """Digest a small contract value without implementing Wave 2 composition."""

    encoded = json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return "sha256:" + hashlib.sha256(encoded).hexdigest()


def _reference_digest(reference: OwnerQualifiedReference) -> str:
    if reference.digest is not None:
        return reference.digest
    return _deterministic_digest(
        {
            "owner": reference.owner,
            "id": reference.ref,
            "kind": reference.kind,
            "version": reference.version,
        }
    )


def _reference_with_digest(reference: OwnerQualifiedReference) -> OwnerQualifiedReference:
    if reference.digest is not None:
        return reference
    return OwnerQualifiedReference(
        owner=reference.owner,
        id=reference.ref,
        kind=reference.kind,
        version=reference.version,
        digest=_reference_digest(reference),
    )


def _selection_provenance(
    release: DistributionRelease,
    bundles: Sequence[ProductBundle],
    definition: AuthoredDeploymentDefinition,
) -> tuple[ProvenanceRecord, ...]:
    records: list[ProvenanceRecord] = []
    release_ref = OwnerQualifiedReference(
        owner=release.owner,
        id=release.distribution_id,
        kind="distribution_release",
        version=release.version,
        digest=release.release_digest,
    )
    records.append(
        ProvenanceRecord(
            path="distribution_release",
            owner=release.owner,
            reference=release_ref,
            source="distribution",
            value_digest=release.release_digest,
        )
    )
    for bundle in bundles:
        bundle_ref = OwnerQualifiedReference(
            owner=bundle.owner,
            id=bundle.bundle_id,
            kind="product_bundle",
            version=bundle.version,
            digest=bundle.bundle_digest,
        )
        records.append(
            ProvenanceRecord(
                path=f"bundles.{bundle.bundle_id}",
                owner=bundle.owner,
                reference=bundle_ref,
                source="bundle",
                value_digest=bundle.bundle_digest,
            )
        )
        for asset in bundle.asset_refs:
            records.append(
                ProvenanceRecord(
                    path=f"bundles.{bundle.bundle_id}.assets.{asset.ref}",
                    owner=asset.owner,
                    reference=_reference_with_digest(asset),
                    source="bundle",
                    value_digest=_reference_digest(asset),
                )
            )
    for asset in release.asset_refs:
        records.append(
            ProvenanceRecord(
                path=f"distribution_release.assets.{asset.ref}",
                owner=asset.owner,
                reference=_reference_with_digest(asset),
                source="distribution",
                value_digest=_reference_digest(asset),
            )
        )
    for asset in definition.local_assets:
        ref = OwnerQualifiedReference(
            owner=asset.namespace,
            id=asset.asset_id,
            kind=asset.kind,
            digest=asset.digest,
        )
        records.append(
            ProvenanceRecord(
                path=f"local_assets.{asset.asset_id}",
                owner=asset.namespace,
                reference=ref,
                source="local",
                value_digest=asset.digest,
            )
        )
    for connection in definition.connection_references:
        reference = _reference_with_digest(connection)
        records.append(
            ProvenanceRecord(
                path=f"connection_references.{connection.ref}",
                owner=connection.owner,
                reference=reference,
                source="definition",
                value_digest=_reference_digest(reference),
            )
        )
    for override in definition.overrides:
        value_digest = _deterministic_digest(override.value)
        ref = OwnerQualifiedReference(
            owner=override.owner,
            id=override.slot_id,
            kind="override_slot",
            version=override.value_type,
            digest=value_digest,
        )
        records.append(
            ProvenanceRecord(
                path=f"overrides.{override.slot_id}",
                owner=override.owner,
                reference=ref,
                source="override",
                value_digest=value_digest,
            )
        )
    for key, value in sorted(definition.instance_values.items()):
        value_digest = _deterministic_digest(value)
        ref = OwnerQualifiedReference(
            owner="instance",
            id=key,
            kind="instance_value",
            digest=value_digest,
        )
        records.append(
            ProvenanceRecord(
                path=f"instance_values.{key}",
                owner="instance",
                reference=ref,
                source="definition",
                value_digest=value_digest,
            )
        )
    return tuple(records)


def validate_selection(
    definition: AuthoredDeploymentDefinition | Mapping[str, Any] | DistributionRelease,
    distribution_release: DistributionRelease | Mapping[str, Any] | Sequence[Any] | None = None,
    bundles: Iterable[ProductBundle | Mapping[str, Any]] | AuthoredDeploymentDefinition | None = None,
    *,
    distribution: DistributionRelease | Mapping[str, Any] | None = None,
) -> ValidatedProductSelection:
    """Validate embedded or external product inputs through one public path.

    The normal call is ``validate_selection(definition, release, bundles)``.
    For ergonomic fixture loading, ``distribution=`` is an alias for the
    release argument.  The inverse positional form
    ``validate_selection(release, bundles, definition)`` is also accepted so
    packaged products need no distribution-specific loader.
    """

    # Support the inverse positional form without introducing a second
    # resolver: release, bundle sequence, definition.
    if isinstance(definition, DistributionRelease) and isinstance(bundles, AuthoredDeploymentDefinition):
        definition, distribution_release, bundles = bundles, definition, distribution_release
    elif isinstance(definition, DistributionRelease) and isinstance(bundles, Mapping):
        try:
            candidate = AuthoredDeploymentDefinition.model_validate(bundles)
        except Exception:
            candidate = None
        if candidate is not None:
            definition, distribution_release, bundles = candidate, definition, distribution_release

    definition_model = _model(definition, AuthoredDeploymentDefinition)
    if distribution is not None:
        if distribution_release is not None:
            raise SelectionValidationError("pass only one of distribution_release or distribution")
        distribution_release = distribution

    selected_release: DistributionRelease | None = None
    if isinstance(distribution_release, DistributionRelease):
        selected_release = _model(distribution_release, DistributionRelease)  # type: ignore[assignment]
    elif distribution_release is not None and not isinstance(distribution_release, Sequence):
        selected_release = _model(distribution_release, DistributionRelease)  # type: ignore[assignment]
    elif isinstance(definition_model.distribution_release, DistributionRelease):
        selected_release = definition_model.distribution_release

    if selected_release is None:
        raise SelectionValidationError(
            "one DistributionRelease artifact is required; a definition reference alone is not executable"
        )

    release_ref = _release_reference(definition_model.distribution_release)
    if release_ref.distribution_id != selected_release.distribution_id:
        raise SelectionValidationError("definition selects a different distribution release")
    if release_ref.version != selected_release.version:
        raise SelectionValidationError("definition selects a different distribution release version")
    if release_ref.digest and release_ref.digest != selected_release.release_digest:
        raise SelectionValidationError("distribution release digest does not match the selected artifact")

    selected_bundles: list[ProductBundle]
    if bundles is None:
        embedded = [item for item in definition_model.bundle_refs if isinstance(item, ProductBundle)]
        if len(embedded) != len(definition_model.bundle_refs):
            raise SelectionValidationError("bundle artifacts are required for every bundle reference")
        selected_bundles = embedded
    else:
        # ``distribution_release`` may be the bundle sequence in the inverse
        # positional form; normalise that case before validating artifacts.
        raw_bundles = bundles
        if isinstance(raw_bundles, AuthoredDeploymentDefinition):
            raise SelectionValidationError("definition argument is missing its bundle artifacts")
        selected_bundles = [_model(item, ProductBundle) for item in raw_bundles]  # type: ignore[list-item]

    refs = [_bundle_reference(item) for item in definition_model.bundle_refs]
    if len(refs) != len(selected_bundles):
        raise SelectionValidationError("definition bundle references and supplied bundle artifacts differ in count")
    by_id = {bundle.bundle_id: bundle for bundle in selected_bundles}
    if len(by_id) != len(selected_bundles):
        raise SelectionValidationError("bundle selection contains duplicate bundle ids")
    for ref in refs:
        bundle = by_id.get(ref.bundle_id)
        if bundle is None:
            raise SelectionValidationError(f"definition references missing bundle {ref.bundle_id!r}")
        if ref.version is not None and ref.version != bundle.version:
            raise SelectionValidationError(f"bundle {ref.bundle_id!r} version does not match its reference")
        if ref.digest is not None:
            if bundle.bundle_digest != ref.digest:
                raise SelectionValidationError(f"bundle {ref.bundle_id!r} digest cannot be verified")

    if selected_release.bundle_ids:
        allowed = set(selected_release.bundle_ids)
        unknown = set(by_id) - allowed
        if unknown:
            raise SelectionValidationError(
                f"bundles {sorted(unknown)!r} are not published by distribution release {selected_release.distribution_id!r}"
            )
    for bundle in selected_bundles:
        if bundle.distribution_id != selected_release.distribution_id:
            raise SelectionValidationError(f"bundle {bundle.bundle_id!r} belongs to another distribution")
        if bundle.distribution_version != selected_release.version:
            raise SelectionValidationError(f"bundle {bundle.bundle_id!r} belongs to another release version")
        selected_ids = set(by_id)
        missing = set(bundle.requires_bundle_ids) - selected_ids
        if missing:
            raise SelectionValidationError(f"bundle {bundle.bundle_id!r} requires missing bundles {sorted(missing)!r}")
        conflicts = set(bundle.conflicts_with_bundle_ids) & selected_ids
        if conflicts:
            raise SelectionValidationError(f"bundle {bundle.bundle_id!r} conflicts with {sorted(conflicts)!r}")

    # Bundle order is not precedence.  Sort only the returned aggregate so
    # equivalent embedded/external inputs have the same typed shape.
    selected_bundles.sort(key=lambda item: (item.owner, item.bundle_id, item.version))
    identity = ProductIdentity(
        distribution_id=selected_release.distribution_id,
        distribution_version=selected_release.version,
        bundle_ids=tuple(bundle.bundle_id for bundle in selected_bundles),
        definition_id=definition_model.definition_id,
        origin=selected_release.origin,
    )
    return ValidatedProductSelection(
        definition=definition_model,
        distribution_release=selected_release,
        bundles=tuple(selected_bundles),
        identity=identity,
        provenance=_selection_provenance(selected_release, selected_bundles, definition_model),
    )


__all__ = ["SELECTION_SCHEMA", "SelectionValidationError", "ValidatedProductSelection", "validate_selection"]
