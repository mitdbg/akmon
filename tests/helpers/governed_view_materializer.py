"""Canonical read-asset wrapper for native-view test materializers."""
from __future__ import annotations

from dataclasses import replace
import hashlib
from typing import Any

from okg.substrate.contracts.deployment_product import (
    build_read_asset,
    resolve_read_assets,
)
from okg.substrate.views import ViewMaterializer as _ViewMaterializer
from okg.substrate.views.base import (
    READ_ASSET_FACTORY_AUTHORITY_ATTRIBUTE,
    ResolvedReadAssetFactoryAuthority,
)


def ViewMaterializer(*args: Any, **kwargs: Any) -> _ViewMaterializer:
    """Build a test materializer with the same canonical token production uses."""

    materializer = _ViewMaterializer(*args, **kwargs)
    marker_missing = object()
    marker = getattr(
        materializer.view_def_factory,
        READ_ASSET_FACTORY_AUTHORITY_ATTRIBUTE,
        marker_missing,
    )
    authority = (
        materializer.asset_id,
        materializer.asset_digest,
        materializer.asset_execution_token,
    )
    authority_presence = tuple(item is not None for item in authority)
    if any(authority_presence) and not all(authority_presence):
        raise ValueError(
            "native view canonical authority fields must be all derived or all explicit"
        )
    if type(marker) is ResolvedReadAssetFactoryAuthority:
        if not all(authority_presence):
            raise ValueError(
                "marked native view factory did not derive complete canonical authority"
            )
        return materializer

    view_name = materializer.name or "unnamed-native-view"
    stable_id = "fixture-" + hashlib.sha256(view_name.encode("utf-8")).hexdigest()[:20]
    contract = build_read_asset(
        owner="native-view-test",
        asset_id=stable_id,
        kind="view",
        config_schema="okg.deployment_product.asset.view.config/v1",
        result_schema="okg.deployment_product.asset.view.result/v1",
        capability_schema="okg.deployment_product.asset.view.capability/v1",
        output_identity=f"native-view:{stable_id}",
        refresh_policy={"mode": "generation"},
        policy_requirements={
            "audiences": ["test-reader"],
            "tenants": ["test-tenant"],
            "visible_fields": ["content"],
            "decision_ids": [f"nomos:test:{stable_id}"],
        },
        lineage_sources=[f"fixture:{stable_id}"],
        config={"view_id": view_name},
        provenance={"source": "local", "fixture": "native-view-test"},
    )
    resolution = resolve_read_assets([contract], roots=[contract.qualified_id])
    return replace(
        materializer,
        asset_id=contract.qualified_id,
        asset_digest=contract.asset.digest,
        asset_execution_token=resolution.execution_token(contract.qualified_id),
    )


__all__ = ["ViewMaterializer"]
