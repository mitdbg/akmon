"""Phase A projection (M3.2).

Per the M2.5-revised tasks.md 3.2: Phase A is an intra-cycle
executor owned by `Publisher.cycle()`, not a generic queue
consumer. The Projector partitions the dirty node-id set, runs
each partition through a short-txn batch worker, and writes
`valid_from_gen = g_next` rows to `nodes_live`. cycle()'s thread
pool joins before Phase B starts — that join is the structural
guarantee for the "Phase A/B/C is one atomic stride" invariant.
"""
from okg.substrate.projection.extractor import (
    EntityExtractor,
    ExtractedEntity,
    NoOpExtractor,
    RegexEntityExtractor,
    SqlIdentifierExtractor,
)
from okg.substrate.projection.projector import (
    BatchResult,
    Projector,
)
from okg.substrate.projection.router import (
    AliasResolver,
    DerivedEdge,
    LinkerRouter,
    PgAliasResolver,
    StaticAliasResolver,
)


__all__ = [
    "AliasResolver",
    "BatchResult",
    "DerivedEdge",
    "EntityExtractor",
    "ExtractedEntity",
    "LinkerRouter",
    "NoOpExtractor",
    "PgAliasResolver",
    "Projector",
    "RegexEntityExtractor",
    "SqlIdentifierExtractor",
    "StaticAliasResolver",
]
