"""Delta-centric graph transform IR (PACT graph-transform-ir).

One representation for all graph-observable transformation: programs are pure,
pinned functions from registered surfaces to typed ``DeltaSet``s,
``AssertionResult``s, and ``Evidence``. They never commit; the substrate commit
boundary applies admitted deltas and enforces the commit-witness invariant.

Public surface (wave 1 / task.gtir.models):
- AST:        ``TransformProgram``, ``Block``, ``Binding``, ``Op``, ``Terminator``
- values:     ``DeltaRow``, ``DeltaSet``, ``AssertionResult``, ``Evidence``
- vocabulary: ``OpKind``, ``TerminatorKind``, ``Effect``, ``Determinism``,
              ``DeltaOp``, ``SurfaceKind``
- registry:   ``Surface``, ``FunctionSpec``, ``SurfaceRegistry``,
              ``FunctionRegistry``, ``Registries``
- errors:     ``IRError``, ``IRValidationError``
"""

from __future__ import annotations

from .ir import (
    AssertionResult,
    Binding,
    Block,
    DeltaRow,
    DeltaSet,
    Evidence,
    IRError,
    IRValidationError,
    Op,
    Terminator,
    TransformProgram,
)
from .consistency import (
    cardinality_constrained_binding,
    catalog_subtypes_binding,
    live_rows_witnessed_program,
    narrowing_allowed_binding,
    narrowing_conformance_program,
    no_dangling_edges_program,
    node_subtype_conformance_program,
)
from .expr import (
    and_, col, eq, func, ge, gt, in_, is_null, json_contains, json_exists,
    json_get, le, lit, lt, ne, not_, or_, param,
)
from .interpreter import EvalResult, InterpreterError, evaluate
from .lower import LoweringError, lower_query
from .normalize import Sugar, SugarKind, expand, normalize_block, walk_blocks
from .programs import (
    admission_program,
    enrichment_program,
    ingestion_program,
    projection_program,
)
from .surfaces import (
    CHRONOS_SURFACES,
    SUBSTRATE_SURFACES,
    chronos_surface_registry,
    substrate_surface_registry,
)
from .registry import (
    ORDER_BEHAVIOR,
    DeltaOp,
    Determinism,
    Effect,
    FunctionRegistry,
    FunctionSpec,
    JoinHow,
    OpKind,
    OrderBehavior,
    Registries,
    Surface,
    SurfaceKind,
    SurfaceRegistry,
    TerminatorKind,
)

__all__ = [
    "AssertionResult",
    "Binding",
    "Block",
    "DeltaOp",
    "DeltaRow",
    "DeltaSet",
    "Determinism",
    "Effect",
    "EvalResult",
    "Evidence",
    "FunctionRegistry",
    "FunctionSpec",
    "IRError",
    "IRValidationError",
    "InterpreterError",
    "JoinHow",
    "LoweringError",
    "admission_program",
    "and_",
    "col",
    "enrichment_program",
    "eq",
    "func",
    "ge",
    "gt",
    "in_",
    "ingestion_program",
    "is_null",
    "json_contains",
    "json_exists",
    "json_get",
    "le",
    "lit",
    "lt",
    "cardinality_constrained_binding",
    "catalog_subtypes_binding",
    "live_rows_witnessed_program",
    "lower_query",
    "narrowing_allowed_binding",
    "narrowing_conformance_program",
    "ne",
    "no_dangling_edges_program",
    "node_subtype_conformance_program",
    "not_",
    "or_",
    "param",
    "projection_program",
    "ORDER_BEHAVIOR",
    "Op",
    "OpKind",
    "OrderBehavior",
    "Registries",
    "CHRONOS_SURFACES",
    "SUBSTRATE_SURFACES",
    "chronos_surface_registry",
    "Sugar",
    "SugarKind",
    "Surface",
    "SurfaceKind",
    "SurfaceRegistry",
    "Terminator",
    "TerminatorKind",
    "TransformProgram",
    "evaluate",
    "expand",
    "normalize_block",
    "substrate_surface_registry",
    "walk_blocks",
]
