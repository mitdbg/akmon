"""IR consistency check for the injection-benchmark ring-0 fixture.

``contested_bucket_program`` flags every bucket whose ≻ resolution is
CONTESTED on the claim-state surface (``okg.claim_projection``) — the
read-side consumer the benchmark's stage-3/4 scoring observes. Same shape as
the consistency-fixture check; distinct program id and module name so the
two deployments cannot collide on ``sys.path``.
"""

from __future__ import annotations

from okg.substrate.consistency.claim_model.precedence import OUTCOME_CONTESTED
from okg.substrate.derived_artifacts.claim_projection_derivation import (
    ROW_KIND_RESOLUTION,
)
from okg.substrate.transform import (
    Binding,
    Block,
    Effect,
    Op,
    OpKind,
    Terminator,
    TerminatorKind,
    TransformProgram,
)
from okg.substrate.transform.expr import and_, col, eq, lit

_PROJECTION_SURFACE = "claim_projection_rows"


def contested_bucket_program() -> TransformProgram:
    """Flag every CONTESTED bucket resolution on the claim-state surface."""
    return TransformProgram(
        program_id="consistency_injection.claim_state.contested_bucket",
        effect=Effect.READ_ONLY,
        outputs=("offending",),
        entry_block="entry",
        attrs={"check": {
            "predicate_key": "claim_bucket_resolved",
            "normalized_predicate": "claim_bucket_contested",
            "participant_roles": {"bucket": "row_key"},
            "severity": "medium",
            "normalized_value_columns": ["row_key", "outcome"],
            "finding_scope_columns": ["row_key"],
        }},
        blocks=(
            Block(
                block_id="entry",
                bindings=(
                    Binding(
                        Op(OpKind.FROM, {
                            "surface": _PROJECTION_SURFACE, "alias": "r",
                        }),
                        output="r",
                    ),
                    Binding(
                        Op(OpKind.WHERE, {"expr": and_(
                            eq(col("r.row_kind"), lit(ROW_KIND_RESOLUTION)),
                            eq(col("r.outcome"), lit(OUTCOME_CONTESTED)),
                        )}),
                        output="flagged", inputs=("r",),
                    ),
                    Binding(
                        Op(OpKind.SELECT, {"project": {
                            "row_key": col("r.row_key"),
                            "outcome": col("r.outcome"),
                            "predicate": col("r.predicate"),
                            "bucket_key": col("r.bucket_key"),
                        }}),
                        output="offending", inputs=("flagged",),
                    ),
                    Binding(Op(OpKind.EMIT, {"target": "offending"}),
                            inputs=("offending",)),
                ),
                terminator=Terminator(
                    TerminatorKind.RETURN, {"values": ["offending"]}
                ),
            ),
        ),
    )
