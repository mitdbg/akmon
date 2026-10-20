"""IR consistency checks for the differential-consistency fixture.

One claim-state check, ``contested_bucket_program``: it reads the post-merge,
post-resolution claim state (``okg.claim_projection``, the claim-state surface —
PACT consistency-claim-graph-binding, req.binding.resolution-visible) and flags
every bucket whose ≻ resolution is CONTESTED (an unresolved conflict the operator
must adjudicate). This is the read-side consumer that proves detection reads the
resolved claim state through the wired publish cycle — registration alone proves
nothing (req.binding.derivations-live).

The finding-identity contract is declared in ``attrs['check']``
(``normalized_value_columns`` + ``finding_scope_columns``); the executor refuses
a claim-state check that omits it. Referenced by ``deployment.yaml``'s
``ir_consistency_checks[0].program_ref``; the module name is distinctive so it
cannot collide with another deployment's check modules on ``sys.path``.
"""
from __future__ import annotations

from okg.substrate.consistency.claim_model.precedence import OUTCOME_CONTESTED
from okg.substrate.derived_artifacts.claim_projection_derivation import (
    CLAIM_PROJECTION_TABLE,
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
from okg.substrate.transform.expr import (
    and_,
    col,
    concat,
    eq,
    is_null,
    lit,
    or_,
    starts_with,
)

# The claim-state surface: a deployment-chosen name bound (in deployment.yaml)
# to okg.claim_projection. Recognition is by the kept table the binding reads,
# so the name is arbitrary; it must match the surfaces alias in deployment.yaml.
_PROJECTION_SURFACE = "claim_projection_rows"

# The dependent-rule surfaces for contested_target_program: the resolution
# projection (which claim is canonical per bucket) JOINED to the effective
# claims (the canonical claim's VALUE). Both are claim-state kept tables, so
# both are pinned to the run's generation by the checkout session. The
# column-renaming subquery bindings (declared in deployment.yaml, mirrored here
# for readers) keep the two surfaces' columns disjoint so the JOIN is
# unambiguous. A "retired:" preferred_target is the downstream violation — a
# preferred target pointing at a decommissioned endpoint (the fixture's stand-in
# for the motivating "citation into the future": a value that is only wrong
# once you read WHICH claim the deployment holds canonical).
_EFFECTIVE_SURFACE = "canonical_effective_rows"
_RESOLUTION_SURFACE = "canonical_resolution_rows"
# The violating value prefix — a retired/decommissioned preferred target.
RETIRED_VALUE_PREFIX = "retired:"


def contested_bucket_program() -> TransformProgram:
    """Flag every CONTESTED bucket resolution on the claim-state surface.

    Subject: the bucket (its (predicate, key) row_key). Reads the projection
    surface, never the raw claim nodes. Per-assignment finding identity is
    (row_key, outcome), scoped by row_key so each bucket's resolution stays
    independently resolvable.
    """
    return TransformProgram(
        program_id="consistency_fixture.claim_state.contested_bucket",
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


def canonical_retired_target_program() -> TransformProgram:
    """Flag every bucket whose CANONICAL preferred_target is a retired endpoint.

    The dependent rule that makes detection READ resolution (PACT
    consistency-claim-graph-binding, req.binding.end-to-end-witness): its
    violation condition depends on WHICH claim the deployment holds canonical.
    It reads the effective claims (the value) LEFT-JOINed to the ≻ resolution
    (the winner), and fires for an effective claim whose value is retired AND
    which is canonical — i.e. it is the resolution winner, OR its bucket has no
    resolution row at all (a sole claim is trivially canonical; the projection
    only resolves CONFLICTED buckets). A contested bucket has no winner and
    never fires. So:

    * seed a lone crawl claim with a retired value  -> canonical (sole) -> FIRES;
    * add a registry claim with decisive authority  -> canonical flips to the
      registry's good value -> the crawl bucket no longer offends -> RESOLVES,
      with zero operator corrections.

    Subject: the bucket ((predicate | key)). Per-assignment finding identity is
    (bucket, canonical_value), scoped by bucket so the finding resolves in place
    when the canonical value changes.
    """
    return TransformProgram(
        program_id="consistency_fixture.claim_state.canonical_retired_target",
        effect=Effect.READ_ONLY,
        outputs=("offending",),
        entry_block="entry",
        attrs={"check": {
            "predicate_key": "claim_canonical_target_active",
            "normalized_predicate": "canonical_target_retired",
            "participant_roles": {"bucket": "bucket"},
            "severity": "high",
            "normalized_value_columns": ["bucket", "canonical_value"],
            "finding_scope_columns": ["bucket"],
        }},
        blocks=(
            Block(
                block_id="entry",
                bindings=(
                    Binding(
                        Op(OpKind.FROM, {
                            "surface": _EFFECTIVE_SURFACE, "alias": "eff",
                        }),
                        output="eff",
                    ),
                    Binding(
                        Op(OpKind.FROM, {
                            "surface": _RESOLUTION_SURFACE, "alias": "res",
                        }),
                        output="res",
                    ),
                    # LEFT JOIN on the (predicate, key) bucket: a bucket with no
                    # resolution row (sole claim) yields NULL res columns.
                    Binding(
                        Op(OpKind.JOIN, {"how": "left", "on": and_(
                            eq(col("res_predicate"), col("eff_predicate")),
                            eq(col("res_bucket_key"), col("eff_key")),
                        )}),
                        output="joined", inputs=("eff", "res"),
                    ),
                    Binding(
                        Op(OpKind.WHERE, {"expr": and_(
                            # the value is a retired preferred_target...
                            starts_with(
                                col("eff_value"), lit(RETIRED_VALUE_PREFIX)
                            ),
                            # ...carried by the CANONICAL claim: this claim is the
                            # resolution winner, OR the bucket is unresolved
                            # (sole claim -> trivially canonical).
                            or_(
                                is_null(col("res_bucket_key")),
                                eq(col("res_winner"), col("eff_claim_id")),
                            ),
                        )}),
                        output="flagged", inputs=("joined",),
                    ),
                    Binding(
                        Op(OpKind.SELECT, {"project": {
                            "bucket": concat(
                                col("eff_predicate"), lit("|"), col("eff_key")
                            ),
                            "canonical_claim_id": col("eff_claim_id"),
                            "canonical_value": col("eff_value"),
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


# The kept table this check reads, exposed for tests/readers.
CLAIM_STATE_TABLE = CLAIM_PROJECTION_TABLE
