"""Catalog-load-time validators (M3.4+).

Substrate-side loaders that apply structural rules from the JSON
schemas at catalog-load time, surfacing the same errors as
schema validation but with `okg.substrate.errors.SubstrateError`
codes that map to the runbook registry.
"""
from okg.substrate.catalog.invariants import (
    DEFAULT_TIMEOUT_MS,
    InvariantSpec,
    load_invariants,
    sql_contains_writes,
)
from okg.substrate.catalog.linker_config import (
    ALLOWED_TEMPLATE_FIELDS,
    LinkerRoute,
    load_linker_config,
    validate_dst_template,
)
from okg.substrate.catalog.modules_compose import (
    ComposedCatalog,
    NarrowingRow,
    compose_catalog,
    load_modules_root,
)
from okg.substrate.catalog.narrowings import (
    NarrowingChecker,
    assert_narrowing_permitted,
)
from okg.substrate.catalog.reconcile import (
    CatalogDiff,
    NarrowingChange,
    ReconcileApplyResult,
    apply_plan,
    classify_narrowings,
    count_violators,
    emit_plan_yaml,
    load_plan,
)
from okg.substrate.catalog.deployment_product_composition import (
    EXECUTABLE_ASSET_KINDS,
    NONSEMANTIC_DISPOSITION,
    SEMANTIC_ASSET_KINDS,
    SemanticCatalog,
    SemanticCompositionError,
    SemanticDiagnostic,
    SemanticNarrowing,
    SemanticSubtype,
    apply_semantic_catalog,
    compose_semantic_catalog,
)
from okg.substrate.catalog.composition_inspection import (
    CompositionInspection,
    CompositionInspectionError,
    CompositionRefusal,
    INSPECTION_SCHEMA,
    REFUSAL_SCHEMA,
    composition_inspection_bytes,
    composition_inspection_digest,
    inspect_composition,
    inspect_refusal,
)


__all__ = [
    "ALLOWED_TEMPLATE_FIELDS",
    "CatalogDiff",
    "ComposedCatalog",
    "DEFAULT_TIMEOUT_MS",
    "InvariantSpec",
    "LinkerRoute",
    "NarrowingChange",
    "NarrowingChecker",
    "NarrowingRow",
    "ReconcileApplyResult",
    "apply_plan",
    "assert_narrowing_permitted",
    "classify_narrowings",
    "compose_catalog",
    "count_violators",
    "emit_plan_yaml",
    "load_invariants",
    "load_linker_config",
    "load_modules_root",
    "load_plan",
    "sql_contains_writes",
    "validate_dst_template",
    "EXECUTABLE_ASSET_KINDS",
    "NONSEMANTIC_DISPOSITION",
    "SEMANTIC_ASSET_KINDS",
    "SemanticCatalog",
    "SemanticCompositionError",
    "SemanticDiagnostic",
    "SemanticNarrowing",
    "SemanticSubtype",
    "apply_semantic_catalog",
    "compose_semantic_catalog",
    "CompositionInspection",
    "CompositionInspectionError",
    "CompositionRefusal",
    "INSPECTION_SCHEMA",
    "REFUSAL_SCHEMA",
    "composition_inspection_bytes",
    "composition_inspection_digest",
    "inspect_composition",
    "inspect_refusal",
]
