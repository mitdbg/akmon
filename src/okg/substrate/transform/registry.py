"""Registries and primitive vocabulary for the delta-centric graph transform IR.

This module holds the *mechanism* the IR validates against — the enumerations,
the ``Surface`` / ``FunctionSpec`` descriptors, and the ``SurfaceRegistry`` /
``FunctionRegistry`` lookups. The *content* (the real substrate surfaces seeded
into a registry) is added separately so the grammar contains zero hard-coded
surface names. See ``pact/changes/graph-transform-ir/designs/01-nouns-and-verbs.md``.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Mapping


class OpKind(str, Enum):
    """The sixteen dataflow operators (verbs in rings 1-3).

    The three control verbs (RETURN / IF / ITERATE) are terminators, not ops;
    together with these sixteen they make the nineteen verbs of the language.
    RANK is the sixteenth, added by operator decision D1 (PACT
    consistency-claim-model-core, designs/graph-derivation-model.md §3): the
    claim-model ≻-resolution needs per-group ranked top-k with comparison
    provenance, which the fifteen-op core could only fake with a fragile
    multi-stage anti-join cascade that the flat-SELECT lowering cannot render.
    """

    # Ring 1 - relational core
    FROM = "from"
    WHERE = "where"
    SELECT = "select"
    JOIN = "join"
    GROUP = "group"
    UNION = "union"
    ORDER = "order"
    LIMIT = "limit"
    UNNEST = "unnest"
    RANK = "rank"
    # Ring 2 - retrieval
    SEARCH = "search"
    # Ring 3 - boundary
    DIFF = "diff"
    CALL = "call"
    ASSERT = "assert"
    DELTA = "delta"
    EMIT = "emit"


class TerminatorKind(str, Enum):
    """The three control verbs. There is deliberately no unbounded ``WHILE``."""

    RETURN = "return"
    IF = "if"
    ITERATE = "iterate"


class JoinHow(str, Enum):
    """JOIN variants. ``left``/``full`` are how an antijoin is expressed
    (``LEFT JOIN ... WHERE endpoint IS NULL``), so the core needs no ``MINUS``."""

    INNER = "inner"
    LEFT = "left"
    FULL = "full"


class OrderBehavior(str, Enum):
    """How an operator treats order on the bag carrier (see design 01).

    The carrier is a bag (SQL multiset). ``INTRODUCE`` operators yield an ordered
    relation; ``PRESERVE`` operators keep an input's order if it has one; ``BAG``
    operators are order-agnostic (a sound rewrite may reorder their output).
    """

    INTRODUCE = "introduce"
    PRESERVE = "preserve"
    BAG = "bag"


class Effect(str, Enum):
    """Declared effect of a program or a CALL kernel.

    ``read_only`` provably emits no ``DeltaRow`` and performs no external/random
    work. ``delta`` may emit deltas. ``external`` / ``random`` are the
    non-reproducible flavours that fork 2 forbids from sharing a program with a
    ``DELTA`` output.
    """

    READ_ONLY = "read_only"
    DELTA = "delta"
    EXTERNAL = "external"
    RANDOM = "random"


class Determinism(str, Enum):
    DETERMINISTIC = "deterministic"
    SEEDED = "seeded"
    OBSERVED_REPLAY = "observed_replay"


class DeltaOp(str, Enum):
    """The four mutation operations a ``DeltaRow`` may carry."""

    UPSERT = "upsert"
    RETRACT = "retract"
    REPLACE = "replace"
    PATCH = "patch"


class SurfaceKind(str, Enum):
    """The seven kinds of registered surface. Replaces the prior 22 named inputs."""

    FACT_LOG = "fact_log"
    LIVE = "live"
    CATALOG_POLICY = "catalog_policy"
    INDEX = "index"
    ARTIFACT = "artifact"
    LEDGER = "ledger"
    TRACE = "trace"


@dataclass(frozen=True)
class Surface:
    """A typed, named relation the IR can read from or write to.

    ``binding`` names the concrete table or view; ``pinned`` declares whether
    reads are stable under a generation pin. A deployment registers instances;
    the language has only the single ``Surface`` noun.
    """

    name: str
    kind: SurfaceKind
    binding: str
    pinned: bool = True

    def to_dict(self) -> dict:
        return {
            "name": self.name,
            "kind": self.kind.value,
            "binding": self.binding,
            "pinned": self.pinned,
        }


@dataclass(frozen=True)
class FunctionSpec:
    """Contract for a registered ``CALL`` kernel.

    The IR reasons about ``effect`` and ``determinism``; ``input_schema`` and
    ``output_schema`` are field-name tuples kept light for wave 1. The remaining
    contract knobs (cost/cardinality/quality/replay) ride along as opaque
    metadata until the optimizer consumes them.
    """

    function_id: str
    effect: Effect
    determinism: Determinism
    input_schema: tuple[str, ...] = ()
    output_schema: tuple[str, ...] = ()
    contract: Mapping[str, object] = field(default_factory=dict)

    def to_dict(self) -> dict:
        return {
            "function_id": self.function_id,
            "effect": self.effect.value,
            "determinism": self.determinism.value,
            "input_schema": list(self.input_schema),
            "output_schema": list(self.output_schema),
            "contract": dict(self.contract),
        }


class SurfaceRegistry:
    """Resolves surface names to ``Surface`` descriptors."""

    def __init__(self, surfaces: object = ()) -> None:
        self._surfaces: dict[str, Surface] = {}
        for surface in surfaces:
            self.register(surface)

    def register(self, surface: Surface) -> None:
        if surface.name in self._surfaces:
            raise ValueError(f"surface already registered: {surface.name}")
        self._surfaces[surface.name] = surface

    def resolve(self, name: str) -> Surface | None:
        return self._surfaces.get(name)

    def __contains__(self, name: object) -> bool:
        return name in self._surfaces

    def names(self) -> tuple[str, ...]:
        return tuple(sorted(self._surfaces))


class FunctionRegistry:
    """Resolves CALL function ids to ``FunctionSpec`` contracts."""

    def __init__(self, specs: object = ()) -> None:
        self._specs: dict[str, FunctionSpec] = {}
        for spec in specs:
            self.register(spec)

    def register(self, spec: FunctionSpec) -> None:
        if spec.function_id in self._specs:
            raise ValueError(f"function already registered: {spec.function_id}")
        self._specs[spec.function_id] = spec

    def resolve(self, function_id: str) -> FunctionSpec | None:
        return self._specs.get(function_id)

    def __contains__(self, function_id: object) -> bool:
        return function_id in self._specs


@dataclass(frozen=True)
class Registries:
    """Bundle passed to ``TransformProgram.validate``."""

    surfaces: SurfaceRegistry = field(default_factory=SurfaceRegistry)
    functions: FunctionRegistry = field(default_factory=FunctionRegistry)


# Per-operator order discipline on the bag carrier (design 01).
ORDER_BEHAVIOR: dict[OpKind, OrderBehavior] = {
    OpKind.FROM: OrderBehavior.BAG,
    OpKind.WHERE: OrderBehavior.PRESERVE,
    OpKind.SELECT: OrderBehavior.PRESERVE,
    OpKind.JOIN: OrderBehavior.BAG,
    OpKind.GROUP: OrderBehavior.BAG,
    OpKind.UNION: OrderBehavior.BAG,
    OpKind.ORDER: OrderBehavior.INTRODUCE,
    OpKind.LIMIT: OrderBehavior.PRESERVE,
    OpKind.UNNEST: OrderBehavior.BAG,
    # RANK's ordering lives IN the rank column, not in row order — the lowered
    # SQL gives no row-order guarantee, so the relation stays a bag.
    OpKind.RANK: OrderBehavior.BAG,
    OpKind.SEARCH: OrderBehavior.INTRODUCE,
    OpKind.DIFF: OrderBehavior.BAG,
    OpKind.CALL: OrderBehavior.PRESERVE,
    OpKind.ASSERT: OrderBehavior.PRESERVE,
    OpKind.DELTA: OrderBehavior.PRESERVE,
    OpKind.EMIT: OrderBehavior.PRESERVE,
}

# The maximum static nesting depth of ITERATE terminators. Beyond this the
# decreasing step counter stops being a tractable cost/termination measure.
MAX_ITERATE_NESTING = 3
