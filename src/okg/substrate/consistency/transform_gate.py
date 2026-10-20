"""Transform-IR consistency publish gate.

Runs the three transform-IR structural programs plus the cardinality SQL check as
the publisher's Phase B gate. The IR programs lower via ``lower_query``; the
cardinality group query is identified separately in durable coverage metadata.
A generation with a structural violation is BLOCKED with the offending edge ids.

The Chronos-native graph cutover made okg.graph_nodes / okg.graph_edges the
canonical graph store, so this gate lowers over the Chronos surfaces and executes
through a pinned Chronos checkout session.
"""

from __future__ import annotations

from collections import Counter
from dataclasses import dataclass, replace
import logging
import time
from typing import Any, Mapping

import psycopg
import yaml

from okg.substrate.consistency.gates import (
    ConsistencyPublishGateEvaluation,
    persist_precomputed_gate_evaluation,
)
from okg.substrate.consistency.missing_patterns import (
    empty_pattern_details,
    violation_pattern_details,
)
from okg.substrate.consistency.observations import (
    CandidateCoverageReport,
    ConsistencyCheckDefinition,
    complete_consistency_run,
    coverage_is_scoped_exact,
    record_candidate_coverage,
    scope_complement_recipe,
    scope_identity,
    start_consistency_run,
)
from okg.substrate.consistency.sql_gate import _fail_run
from okg.substrate.derived_artifacts import stable_hash
from okg.substrate.transform import (
    FunctionRegistry,
    Registries,
    Surface,
    SurfaceKind,
    cardinality_constrained_binding,
    catalog_subtypes_binding,
    chronos_surface_registry,
    lower_query,
    narrowing_allowed_binding,
    narrowing_conformance_program,
    no_dangling_edges_program,
    node_subtype_conformance_program,
)
from okg.substrate.transform.operators import to_chronos_sql

# Historical definitions stay immutable. Cardinality introduced v2; checking
# both endpoints instead of destination-only is v3; derived-node liveness is v4.
_LEGACY_CHECK_ID = "transform.consistency.no_dangling+narrowing"
_LEGACY_CARDINALITY_CHECK_ID = (
    "transform.consistency.no_dangling+narrowing+cardinality.v2"
)
_LEGACY_ENDPOINT_CARDINALITY_CHECK_ID = (
    "transform.consistency.no_dangling+narrowing+cardinality.v3"
)
_CHECK_ID = (
    "transform.consistency.no_dangling+narrowing+cardinality+"
    "derived_node_liveness.v4"
)

log = logging.getLogger(__name__)

_EDGE_ID_SAMPLE_LIMIT = 100
_SOURCE_BUCKET_LIMIT = 50
_CLASSIFICATION_BUCKET_LIMIT = 100


def _transform_gate_definition(deployment_name: str) -> ConsistencyCheckDefinition:
    """The transform gate's stable synthetic check definition.

    Registered on the fly by ``start_consistency_run`` (which upserts both the
    ``derived_artifact_definitions`` and ``consistency_check_definitions`` rows)
    so the ``consistency_gate_results.check_id`` /
    ``derived_artifact_runs.definition_id`` foreign keys hold. All hashes are
    constants -- the gate's compute is fixed substrate code, not config -- so the
    identity is deterministic per deployment. ``check_kind='rule_based'``: the
    gate enforces deterministic catalog / narrowing / node-subtype conformance.
    """
    return ConsistencyCheckDefinition(
        check_id=_CHECK_ID,
        deployment_name=deployment_name,
        predicate_family="transform_structural_integrity",
        candidate_rule_hash=stable_hash(
            {
                "kind": "transform_gate_candidate_rule_v4",
                "programs": [
                    "no_unsupported_derived_nodes",
                    "no_dangling_edges",
                    "narrowing_conformance",
                    "node_subtype_conformance",
                    "narrowing_cardinality",
                ],
            }
        ),
        evaluator_ref="okg.evaluator.transform.structural.v4",
        evaluator_hash=stable_hash({"kind": "transform_gate_evaluator_v4"}),
        confidence_type="deterministic_boolean",
        compute_hash=stable_hash({"kind": "transform_gate_compute_v4"}),
        output_schema_hash=stable_hash({"kind": "transform_gate_output_schema_v4"}),
        renderer_hash=stable_hash({"kind": "transform_gate_renderer_v4"}),
        dirty_rule_hash=stable_hash({"kind": "transform_gate_dirty_rule_v4"}),
        check_kind="rule_based",
    )


def _legacy_cardinality_transform_gate_definition(
    deployment_name: str,
) -> ConsistencyCheckDefinition:
    """Immutable destination-only v2 definition used by historical runs."""
    return ConsistencyCheckDefinition(
        check_id=_LEGACY_CARDINALITY_CHECK_ID,
        deployment_name=deployment_name,
        predicate_family="transform_structural_integrity",
        candidate_rule_hash=stable_hash({
            "kind": "transform_gate_candidate_rule_v2",
            "programs": [
                "no_dangling_edges",
                "narrowing_conformance",
                "node_subtype_conformance",
                "narrowing_cardinality",
            ],
        }),
        evaluator_ref="okg.evaluator.transform.structural.v2",
        evaluator_hash=stable_hash({"kind": "transform_gate_evaluator_v2"}),
        confidence_type="deterministic_boolean",
        compute_hash=stable_hash({"kind": "transform_gate_compute_v2"}),
        output_schema_hash=stable_hash(
            {"kind": "transform_gate_output_schema_v2"}
        ),
        renderer_hash=stable_hash({"kind": "transform_gate_renderer_v2"}),
        dirty_rule_hash=stable_hash({"kind": "transform_gate_dirty_rule_v2"}),
        check_kind="rule_based",
    )


def _legacy_endpoint_cardinality_transform_gate_definition(
    deployment_name: str,
) -> ConsistencyCheckDefinition:
    """Immutable endpoint-aware v3 definition used by historical runs."""
    return ConsistencyCheckDefinition(
        check_id=_LEGACY_ENDPOINT_CARDINALITY_CHECK_ID,
        deployment_name=deployment_name,
        predicate_family="transform_structural_integrity",
        candidate_rule_hash=stable_hash({
            "kind": "transform_gate_candidate_rule_v3",
            "programs": [
                "no_dangling_edges",
                "narrowing_conformance",
                "node_subtype_conformance",
                "narrowing_cardinality",
            ],
        }),
        evaluator_ref="okg.evaluator.transform.structural.v3",
        evaluator_hash=stable_hash({"kind": "transform_gate_evaluator_v3"}),
        confidence_type="deterministic_boolean",
        compute_hash=stable_hash({"kind": "transform_gate_compute_v3"}),
        output_schema_hash=stable_hash(
            {"kind": "transform_gate_output_schema_v3"}
        ),
        renderer_hash=stable_hash({"kind": "transform_gate_renderer_v3"}),
        dirty_rule_hash=stable_hash({"kind": "transform_gate_dirty_rule_v3"}),
        check_kind="rule_based",
    )


def _legacy_transform_gate_definition(
    deployment_name: str,
) -> ConsistencyCheckDefinition:
    """Immutable three-check definition used to prove history stays distinct."""
    return ConsistencyCheckDefinition(
        check_id=_LEGACY_CHECK_ID,
        deployment_name=deployment_name,
        predicate_family="transform_structural_integrity",
        candidate_rule_hash=stable_hash({
            "kind": "transform_gate_candidate_rule_v1",
            "programs": [
                "no_dangling_edges",
                "narrowing_conformance",
                "node_subtype_conformance",
            ],
        }),
        evaluator_ref="okg.evaluator.transform.structural.v1",
        evaluator_hash=stable_hash({"kind": "transform_gate_evaluator_v1"}),
        confidence_type="deterministic_boolean",
        compute_hash=stable_hash({"kind": "transform_gate_compute_v1"}),
        output_schema_hash=stable_hash(
            {"kind": "transform_gate_output_schema_v1"}
        ),
        renderer_hash=stable_hash({"kind": "transform_gate_renderer_v1"}),
        dirty_rule_hash=stable_hash({"kind": "transform_gate_dirty_rule_v1"}),
        check_kind="rule_based",
    )


@dataclass(frozen=True)
class _GateFields:
    """The transform gate's precomputed verdict, before it is bound to a run.

    Separates the structural COMPUTE (the antijoins + dangling policy) from
    PERSISTENCE so the incremental fallback can augment ``details``
    (scope_mode / scope_fallback_reason) BEFORE the row is written -- the durable
    row then matches the returned evaluation exactly, and the block decision is
    copied verbatim (never re-derived from a nonexistent observation ledger).
    """

    result: str
    blocks_publish: bool
    reason: str
    open_observation_count: int
    coverage_complete: bool
    coverage_basis: str
    elapsed_ms: int
    preflight: Mapping[str, Any]
    details: Mapping[str, Any]
    # A SCOPED run must also land a consistency_candidate_coverage row, so the
    # schema's one structural honesty constraint --
    # CHECK (NOT (coverage_complete AND scoped)) -- actually applies to it.
    # Writing no row at all bypassed that CHECK entirely. None = the run was a
    # full sweep and the gate row alone is the coverage record.
    coverage: CandidateCoverageReport | None = None


def _any_predicate(
    column: str,
    values: list[int] | list[str],
    *,
    prefix: str,
) -> tuple[str, dict[str, int | str]]:
    if not values:
        return "FALSE", {}
    key = f"{prefix}_values"
    return f"{column} = ANY(:{key})", {key: values}


def _catalog_equivalence(
    conn: psycopg.Connection[Any],
    *,
    deployment_name: str,
    previous_catalog_version_id: int | None,
    catalog_version_id: int,
) -> dict[str, Any]:
    """Prove whether two applied catalog rows have identical content identity.

    Catalog loading may mint a new row even when the composed catalog, schema,
    and writer are byte-for-byte identical. The transform gate depends on that
    content, not on the apply event's surrogate id. Treating every new id as a
    semantic change forces an unnecessary full-graph sweep after a no-op load.

    The cross-version proof is deliberately strict: both rows must be applied,
    owned by this deployment, carry every identity fingerprint, and match on
    catalog, schema, and writer identity. Missing legacy metadata falls back to
    the full gate.
    """
    previous = (
        None
        if previous_catalog_version_id is None
        else int(previous_catalog_version_id)
    )
    current = int(catalog_version_id)
    disclosure: dict[str, Any] = {
        "equivalent": False,
        "basis": "unproven",
        "previous_catalog_version_id": previous,
        "catalog_version_id": current,
        "compared_fields": [],
    }
    if previous is None:
        disclosure["basis"] = "no_previous_generation"
        return disclosure
    if previous == current:
        disclosure.update({
            "equivalent": True,
            "basis": "same_catalog_version_id",
        })
        return disclosure

    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT catalog_version_id, status, deployment_name,
                   catalog_fingerprint, schema_fingerprint,
                   writer_fingerprint
              FROM okg.catalog_versions
             WHERE catalog_version_id = ANY(%s)
            """,
            ([previous, current],),
        )
        rows = {int(row[0]): row for row in cur.fetchall()}

    if set(rows) != {previous, current}:
        disclosure["basis"] = "catalog_version_not_found"
        return disclosure
    previous_row = rows[previous]
    current_row = rows[current]
    if previous_row[1] != "applied" or current_row[1] != "applied":
        disclosure["basis"] = "catalog_version_not_applied"
        return disclosure
    if (
        previous_row[2] != deployment_name
        or current_row[2] != deployment_name
    ):
        disclosure["basis"] = "catalog_deployment_mismatch"
        return disclosure

    compared_fields = [
        "catalog_fingerprint",
        "schema_fingerprint",
        "writer_fingerprint",
    ]
    disclosure["compared_fields"] = compared_fields
    previous_identity = tuple(previous_row[3:6])
    current_identity = tuple(current_row[3:6])
    if not all(previous_identity) or not all(current_identity):
        disclosure["basis"] = "catalog_identity_incomplete"
        return disclosure
    if previous_identity != current_identity:
        disclosure["basis"] = "catalog_content_identity_changed"
        return disclosure
    disclosure.update({
        "equivalent": True,
        "basis": "matching_applied_content_identity",
    })
    return disclosure


def _topology_dirty_node_ids(
    dirty_scope: tuple[dict[str, list[str]], dict[str, list[int]], bool] | None,
    dirty_node_ids: list[str],
) -> list[str]:
    if not dirty_node_ids or dirty_scope is None or len(dirty_scope) < 4:
        return dirty_node_ids
    attr_changes_by_subtype = dirty_scope[3]
    if attr_changes_by_subtype is None:
        return dirty_node_ids
    topology_dirty: list[str] = []
    for node_changes in attr_changes_by_subtype.values():
        for node_id, fields in node_changes.items():
            field_set = {str(field) for field in fields}
            if "__deleted__" in field_set or "__subtype__" in field_set:
                topology_dirty.append(str(node_id))
    if not topology_dirty:
        return []
    dirty_node_set = set(dirty_node_ids)
    return [
        node_id
        for node_id in dict.fromkeys(topology_dirty)
        if node_id in dirty_node_set
    ]


def _narrowing_gate_active(
    conn: psycopg.Connection[Any],
    catalog_version_id: int,
) -> bool:
    """Mirror writer-path NarrowingChecker activation semantics."""
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT EXISTS (
                SELECT 1
                  FROM okg.narrowings
                 WHERE catalog_version_id = %s
            )
            """,
            (catalog_version_id,),
        )
        row = cur.fetchone()
    return bool(row and row[0])


def _cardinality_gate_active(
    conn: psycopg.Connection[Any],
    catalog_version_id: int,
) -> bool:
    """True iff any narrowing at the active cv carries an at-most-one-target
    cardinality annotation (fix-projection-alias-identity #710). Un-annotated
    catalogs skip the check entirely -- adoption is per-deployment opt-in."""
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT EXISTS (
                SELECT 1
                  FROM okg.narrowings
                 WHERE catalog_version_id = %s
                   AND attr_rules->>'cardinality' = 'at_most_one_target'
            )
            """,
            (catalog_version_id,),
        )
        row = cur.fetchone()
    return bool(row and row[0])


# --- constraint-family disclosure -------------------------------------------
# Skipping an INACTIVE family is by design (per-deployment opt-in, see the
# _cardinality_gate_active docstring); the defect this closes is that the skip
# was INVISIBLE. Pre-change, a run whose cardinality family never activated was
# byte-identical to a clean complete run -- result=evaluated,
# reason=no_violations, coverage_complete=true, empty violation list -- on a
# corpus where the violation was real (an annotated twin run blocked on the
# exact edge ids). An operator could not tell "cardinality: clean" from
# "cardinality: never ran".
#
# Every run now names the families it EVALUATED and the families it SKIPPED,
# each skipped one carrying the activation predicate that was false. The names
# also compose ``coverage_basis`` -- a FIRST-CLASS consistency_gate_results
# column, not a nested details key -- so the two runs differ in a field a
# reader cannot miss, and the basis stops naming a check (``cardinality_sql``)
# that did not run.

_ALWAYS_ACTIVE = "always_active"


@dataclass(frozen=True)
class _FamilyActivation:
    """One constraint family and the predicate that decides whether it runs."""

    name: str
    evaluated: bool
    activation_predicate: str
    activation_source: str


def _family_activations(
    conn: psycopg.Connection[Any],
    catalog_version_id: int,
) -> tuple[_FamilyActivation, ...]:
    """Resolve every constraint family's activation for this catalog version.

    Evaluated ONCE per gate run and threaded into both the evaluation helpers
    and the persisted fields, so what the gate discloses is exactly what it
    branched on (a second, independently-executed probe could disagree with the
    run it claims to describe).
    """

    return (
        _FamilyActivation("dangling", True, _ALWAYS_ACTIVE, "-"),
        _FamilyActivation("node_subtype", True, _ALWAYS_ACTIVE, "-"),
        _FamilyActivation(
            "narrowing",
            _narrowing_gate_active(conn, catalog_version_id),
            "EXISTS (SELECT 1 FROM okg.narrowings WHERE "
            f"catalog_version_id = {int(catalog_version_id)})",
            "okg.narrowings",
        ),
        _FamilyActivation(
            "cardinality",
            _cardinality_gate_active(conn, catalog_version_id),
            "EXISTS (SELECT 1 FROM okg.narrowings WHERE "
            f"catalog_version_id = {int(catalog_version_id)} AND "
            "attr_rules->>'cardinality' = 'at_most_one_target')",
            "okg.narrowings",
        ),
    )


def _is_active(activations: tuple[_FamilyActivation, ...], name: str) -> bool:
    for activation in activations:
        if activation.name == name:
            return activation.evaluated
    raise KeyError(f"unknown constraint family {name!r}")


def _family_disclosure(
    activations: tuple[_FamilyActivation, ...],
) -> dict[str, Any]:
    """The details payload naming evaluated vs skipped families."""

    return {
        "evaluated_families": [a.name for a in activations if a.evaluated],
        "skipped_families": [
            {
                "family": a.name,
                "skip_reason": "activation_predicate_false",
                "activation_predicate": a.activation_predicate,
                "activation_source": a.activation_source,
            }
            for a in activations
            if not a.evaluated
        ],
    }


def _coverage_basis_for_families(
    base: str,
    activations: tuple[_FamilyActivation, ...],
) -> str:
    """Compose coverage_basis from the families that ACTUALLY ran.

    ``+cardinality_sql`` is appended only when the cardinality family ran, and
    any skipped family is named inline, so the first-class column itself
    distinguishes a partial constraint set from a complete one.
    """

    basis = base
    if _is_active(activations, "cardinality"):
        basis += "+cardinality_sql"
    skipped = [a.name for a in activations if not a.evaluated]
    if skipped:
        basis += "+skipped_families:" + ",".join(skipped)
    return basis


# The cardinality violation query, shared by the full and scoped eval paths.
# It returns every edge_id in a group of >1 live edges sharing (src, edge_type)
# under an at-most-one-target narrowing. `grp_src_predicate` (a SQL fragment on
# `e.src`, empty = all live edges) bounds which sources' groups are considered,
# for the incremental path; the group's DISTINCT-dst count still scans every
# live edge of that source, so a >1 group is never missed.
def _cardinality_violation_sql(
    catalog_version_id: int,
    *,
    grp_src_predicate: str = "",
) -> str:
    constrained = cardinality_constrained_binding(catalog_version_id)
    src_where = f" WHERE {grp_src_predicate}" if grp_src_predicate else ""
    return f"""
        WITH constrained AS {constrained},
        grp AS (
          SELECT e.src, e.edge_type
            FROM okg.graph_edges e
            JOIN okg.graph_nodes ns ON ns.node_id = e.src
            JOIN constrained c
              ON c.edge_type = e.edge_type
             AND c.src_subtype = ns.subtype
          {src_where}
           GROUP BY e.src, e.edge_type
          HAVING count(DISTINCT e.dst) > 1
        )
        SELECT e.edge_id
          FROM okg.graph_edges e
          JOIN grp g ON g.src = e.src AND g.edge_type = e.edge_type
    """


def _side_table_generation_token(
    conn: psycopg.Connection[Any],
    target_generation_id: int | str,
) -> int:
    """Resolve a generation reference to the BIGINT token the consistency
    side tables store.

    ``okg.derived_artifact_runs`` / ``okg.consistency_gate_results`` keep a
    BIGINT ``target_generation_id`` by design (database-schema.md
    §3.2.1: generation ORDER is an ordinal-domain operation, so these
    order-consumed internal columns stay BIGINT). The live publisher passes
    its compat token (already an int) — unchanged here. A chronos TEXT
    checkpoint ref (``okg:<deployment>:branch:<b>:gen:<ts>:<hash>``) is
    resolved to a RECORDED value, in order:

      1. the compat token the publisher stamped into the checkpoint
         metadata (``target_generation_id``) — so a ref and the compat int
         for the same checkpoint resolve to the SAME stored value;
      2. the minted generation ordinal (``okg.generation_ordinals``) for
         checkpoints without a compat stamp (direct gate callers).

    Never parsed out of the ref text and never defaulted: a ref with
    neither recorded value raises (resolve_generation_ordinal fails loud).
    """
    if isinstance(target_generation_id, int):
        return target_generation_id
    text = str(target_generation_id).strip()
    if text.isdigit():
        # A compat token rendered as digits (D2 §3.2) is the token itself.
        return int(text)
    from okg.substrate.graph_store import ChronosGraphStore
    from okg.substrate.graph_store_internal.generation_ordinal import (
        resolve_generation_ordinal,
    )

    store = ChronosGraphStore.from_connection(conn, ensure_schema=False)
    checkpoint = store.get_generation(text)
    metadata = dict(getattr(checkpoint, "metadata", None) or {})
    stamped = metadata.get("target_generation_id")
    if stamped is not None:
        if not str(stamped).isdigit():
            from okg.substrate.errors import SubstrateError

            # A PRESENT but malformed stamp must not silently fall to the
            # ordinal: publisher rows for this deployment carry the compat
            # token domain, and quietly switching one row to the ordinal
            # domain would corrupt the induction-basis ORDER comparisons.
            raise SubstrateError(
                "checkpoint metadata target_generation_id is present but "
                f"not an integer token: {stamped!r} (checkpoint {text!r})",
                data={"checkpoint_id": text, "target_generation_id": stamped},
            )
        return int(str(stamped))
    return resolve_generation_ordinal(store.context.db, text)


class TransformConsistencyGate:
    """A ConsistencyPublishGate that runs the transform-IR consistency programs as
    the publisher's Phase B gate.

    ``dangling_policy`` controls the no-dangling half:
    - ``"block"`` (default): a dangling edge BLOCKS the publish (the strict
      integrity contract).
    - ``"prune"``: dangling edges are DROPPED from the branch about to be
      checkpointed (logged in ``details.pruned_dangling_edge_ids``) and do NOT
      block. For deployments that ingest history where some edges legitimately
      reference now-absent endpoints — e.g. git-history emits ``affects``->file
      edges "regardless of whether" the file still exists — blocking forever is
      wrong; the unresolved forward-references are pruned at publish instead.
    Narrowing violations ALWAYS block, under either policy (they are real
    catalog-conformance errors, never a tolerated condition)."""

    name = "transform_consistency_gate"

    def __init__(self, *, dangling_policy: str = "block") -> None:
        if dangling_policy not in ("block", "prune"):
            raise ValueError(
                f"dangling_policy must be 'block' or 'prune', got {dangling_policy!r}"
            )
        self.dangling_policy = dangling_policy

    def cache_identity(self) -> dict[str, str]:
        """Stable semantics included in the publisher's no-op cache key."""
        return {
            "check_id": _CHECK_ID,
            "dangling_policy": self.dangling_policy,
        }

    def _published_induction_basis(
        self,
        conn: psycopg.Connection[Any],
        *,
        deployment_name: str,
        branch_name: str,
    ) -> dict[str, Any]:
        """Verify the immediately preceding published checkpoint carries v4.

        A clean full v4 result establishes the basis. A clean scoped v4 result
        may carry it forward only when that published row recorded a verified
        induction. Missing, blocking, destination-only, or skipped rows fail
        closed to a full sweep on the next mutating publish.

        The basis must also be the branch's LATEST checkpoint. The caller's
        dirty scope reaches back only to the latest checkpoint of any status, so
        an intervening blocked checkpoint leaves the writes it refused outside
        every later scope (#1425); that also fails closed to a full sweep.
        """
        from okg.substrate.graph_store import ChronosGraphStore

        store = ChronosGraphStore.from_connection(conn, ensure_schema=False)
        previous = store.latest_generation(
            deployment_name=deployment_name,
            branch=branch_name,
            status="published",
        )
        basis_branch = branch_name
        inherited_from_fork_base = False
        if previous is None and branch_name != "default":
            # A fork's first publish has no checkpoint on the child branch yet.
            # Its immediately preceding graph is the recorded fork base, so that
            # checkpoint's clean v4 result is the induction basis for validating
            # only the child continuation segment. The shared resolver verifies
            # that the base is a published generation for this deployment.
            previous = store.resolve_fork_base_checkpoint(
                branch_name, expected_deployment=deployment_name
            )
            if previous is not None:
                basis_branch = "default"
                inherited_from_fork_base = True
        if previous is None:
            return {
                "verified": False,
                "reason": "no_previous_published_checkpoint",
                "checkpoint_id": None,
                "basis_branch": basis_branch,
                "inherited_from_fork_base": False,
            }
        checkpoint_id = str(getattr(previous, "checkpoint_id", ""))
        # #1425. The caller's dirty scope is the CURRENT continuation segment --
        # everything written since the latest checkpoint of ANY status. This
        # induction baseline is the latest PUBLISHED checkpoint. The two agree
        # only while no unpublished checkpoint sits between them. A BLOCKED
        # checkpoint seals the segment that carried the violation it blocked, so
        # the next cycle's segment starts AFTER the violating writes: a scoped
        # pass would see an empty scope, re-derive "clean", and publish the graph
        # the previous cycle refused. Fail closed to the full sweep until a
        # published checkpoint re-anchors the chain.
        latest_checkpoint = store.latest_generation(
            deployment_name=deployment_name,
            branch=branch_name,
            status=None,
        )
        latest_checkpoint_id = (
            None
            if latest_checkpoint is None
            else str(getattr(latest_checkpoint, "checkpoint_id", ""))
        )
        # A fork inheriting its base's basis has no checkpoint of its own yet, so
        # its segment IS everything since the base. Any checkpoint on the fork
        # branch means that is no longer true.
        expected_latest_checkpoint_id = (
            None if inherited_from_fork_base else checkpoint_id
        )
        if latest_checkpoint_id != expected_latest_checkpoint_id:
            return {
                "verified": False,
                "reason": "unpublished_checkpoint_after_published_basis",
                "checkpoint_id": checkpoint_id,
                "latest_checkpoint_id": latest_checkpoint_id,
                "basis_branch": basis_branch,
                "inherited_from_fork_base": inherited_from_fork_base,
            }
        metadata = dict(getattr(previous, "metadata", None) or {})
        invariant_summary = dict(metadata.get("invariant_summary") or {})
        gates = dict(invariant_summary.get("consistency_gates") or {})
        results = gates.get("results") or []
        for raw in results:
            result = dict(raw or {})
            if result.get("check_id") != _CHECK_ID:
                continue
            if result.get("gate_name") != self.name:
                continue
            details = dict(result.get("details") or {})
            clean = (
                result.get("result") == "evaluated"
                and result.get("blocks_publish") is False
            )
            full = (
                result.get("coverage_complete") is True
                and details.get("scoped") is False
            )
            scoped_chain = (
                details.get("scoped") is True
                and details.get("verdict_universe_valid_by_induction") is True
            )
            if clean and (full or scoped_chain):
                return {
                    "verified": True,
                    "reason": None,
                    "checkpoint_id": checkpoint_id,
                    "basis_kind": (
                        "published_full_v4"
                        if full
                        else "published_scoped_v4_verified_induction"
                    ),
                    "basis_branch": basis_branch,
                    "inherited_from_fork_base": inherited_from_fork_base,
                }
            return {
                "verified": False,
                "reason": "previous_v4_result_not_clean_or_verified",
                "checkpoint_id": checkpoint_id,
                "basis_branch": basis_branch,
                "inherited_from_fork_base": inherited_from_fork_base,
            }
        return {
            "verified": False,
            "reason": "previous_published_checkpoint_has_no_v4_result",
            "checkpoint_id": checkpoint_id,
            "basis_branch": basis_branch,
            "inherited_from_fork_base": inherited_from_fork_base,
        }

    @staticmethod
    def _validate_retracted_node_markers(
        conn: psycopg.Connection[Any],
        *,
        deployment_name: str,
        branch_name: str,
        delta_counts: Mapping[str, Any] | None,
        dirty_scope: tuple[
            dict[str, list[str]],
            dict[str, list[int]],
            bool,
            dict[str, dict[str, list[str]]] | None,
        ] | None,
    ) -> dict[str, Any]:
        """Prove deletion markers equal the actual previous-to-current deletes."""
        expected = int((delta_counts or {}).get("nodes_retracted", 0) or 0)
        if dirty_scope is None or len(dirty_scope) < 4 or dirty_scope[3] is None:
            return {
                "verified": False,
                "reason": "node_attribute_change_markers_unavailable",
                "expected_retraction_count": expected,
            }
        deleted_marker_ids = sorted(
            {
                str(node_id)
                for changes_by_id in dirty_scope[3].values()
                for node_id, fields in changes_by_id.items()
                if "__deleted__" in {str(field) for field in fields}
            }
        )
        if not deleted_marker_ids:
            if expected == 0:
                return {
                    "verified": True,
                    "reason": None,
                    "expected_retraction_count": 0,
                    "deleted_marker_count": 0,
                    "actual_marked_retraction_count": 0,
                }
            return {
                "verified": False,
                "reason": "deleted_marker_set_empty",
                "expected_retraction_count": expected,
                "deleted_marker_count": 0,
            }
        if expected == 0:
            return {
                "verified": False,
                "reason": "unexpected_deleted_markers_for_zero_retraction_delta",
                "expected_retraction_count": 0,
                "deleted_marker_count": len(deleted_marker_ids),
                "actual_marked_retraction_count": None,
            }

        from okg.substrate.graph_store import ChronosGraphStore

        store = ChronosGraphStore.from_connection(conn, ensure_schema=False)
        previous = store.latest_generation(
            deployment_name=deployment_name,
            branch=branch_name,
            status="published",
        )
        if previous is None:
            return {
                "verified": False,
                "reason": "no_previous_published_checkpoint",
                "expected_retraction_count": expected,
                "deleted_marker_count": len(deleted_marker_ids),
            }
        predicate, params = _any_predicate(
            "node_id", deleted_marker_ids, prefix="deleted_marker_node_id"
        )
        previous_session = store.checkout_generation(
            str(previous.checkpoint_id), branch=branch_name
        )
        current_session = store.checkout(branch=branch_name)
        previous_live = {
            str(row["node_id"])
            for row in previous_session.query(
                f"SELECT node_id FROM okg.graph_nodes WHERE {predicate}", params
            )
        }
        current_live = {
            str(row["node_id"])
            for row in current_session.query(
                f"SELECT node_id FROM okg.graph_nodes WHERE {predicate}", params
            )
        }
        actual_marked = previous_live - current_live
        verified = len(actual_marked) == expected and actual_marked == set(
            deleted_marker_ids
        )
        return {
            "verified": verified,
            "reason": None if verified else "marker_set_not_exact_actual_retractions",
            "expected_retraction_count": expected,
            "deleted_marker_count": len(deleted_marker_ids),
            "actual_marked_retraction_count": len(actual_marked),
        }

    def evaluate(
        self,
        conn: psycopg.Connection[Any],
        *,
        target_generation_id: int | str,
        catalog_version_id: int,
        deployment_name: str,
        branch_name: str = "default",
    ) -> ConsistencyPublishGateEvaluation:
        # Resolve the side-table BIGINT token up front so a chronos TEXT ref
        # fails loud BEFORE the (expensive) sweep, not at persist time. The
        # raw reference keeps flowing to the chronos read path, which pins
        # its checkout by checkpoint ref.
        persist_generation_id = _side_table_generation_token(
            conn, target_generation_id
        )
        fields = self._evaluate_full_fields(
            conn,
            target_generation_id=target_generation_id,
            catalog_version_id=catalog_version_id,
            deployment_name=deployment_name,
            branch_name=branch_name,
        )
        return self._persist(
            conn,
            deployment_name=deployment_name,
            target_generation_id=persist_generation_id,
            trigger_kind="full",
            fields=fields,
        )

    def _cleanup_derived_nodes(
        self,
        conn: psycopg.Connection[Any],
        *,
        deployment_name: str,
        branch_name: str,
        frontier_node_ids: set[str] | None = None,
    ) -> dict[str, Any]:
        """Freeze support evidence and tombstone unsupported nodes under fence."""
        from okg.substrate.derived_node_liveness import plan_derived_node_cleanup
        from okg.substrate.graph_store import ChronosGraphStore
        from okg.substrate.graph_store_internal.common import EDGE_TABLE, NODE_TABLE

        store = ChronosGraphStore.from_connection(conn, ensure_schema=False)
        previous = store.latest_generation(
            deployment_name=deployment_name,
            branch=branch_name,
            status="published",
        )
        if previous is None and branch_name != "default":
            # A fork's first publish has no generation on the child branch.
            # Its immutable prior support evidence is the recorded published
            # fork base; without it a member removed on the fork looks
            # never-observed (unknown) instead of known-dead.
            previous = store.resolve_fork_base_checkpoint(
                branch_name,
                expected_deployment=deployment_name,
            )
        prior_rows: list[dict[str, Any]] = []
        if previous is not None:
            checkpoint_id = str(previous.checkpoint_id)
            checkout_checkpoint = getattr(
                getattr(store, "context", None), "checkout_checkpoint", None
            )
            prior_session = (
                checkout_checkpoint(checkpoint_id)
                if callable(checkout_checkpoint)
                else store.checkout_generation(checkpoint_id, branch=branch_name)
            )
            prior_rows = [
                dict(row)
                for row in prior_session.query(
                    "SELECT node_id, subtype, source, source_record_id "
                    "FROM okg.graph_nodes ORDER BY node_id"
                )
            ]

        candidate_session = store.checkout(branch_name)
        with candidate_session.transaction():
            store._lock_branch_write(branch_name)
            candidate_rows = [
                dict(row)
                for row in candidate_session.query(
                    "SELECT node_id, subtype, source, source_record_id "
                    "FROM okg.graph_nodes ORDER BY node_id"
                )
            ]
            plan = plan_derived_node_cleanup(
                candidate_rows,
                prior_rows,
                frontier_node_ids=frontier_node_ids,
            )
            requested_retractions = [
                str(node_id) for node_id in plan["retract_node_ids"]
            ]
            incident_edge_ids: list[int] = []
            present_node_ids: list[str] = []
            if requested_retractions:
                present_node_ids = sorted(
                    str(row["node_id"])
                    for row in candidate_session.query(
                        "SELECT node_id FROM okg.graph_nodes "
                        "WHERE node_id = ANY(:node_ids)",
                        {"node_ids": requested_retractions},
                    )
                )
                incident_edge_ids = sorted(
                    int(row["edge_id"])
                    for row in candidate_session.query(
                        "SELECT edge_id FROM okg.graph_edges "
                        "WHERE src = ANY(:node_ids) OR dst = ANY(:node_ids)",
                        {"node_ids": requested_retractions},
                    )
                )
                if incident_edge_ids:
                    candidate_session.delete_keys(
                        EDGE_TABLE,
                        [{"edge_id": edge_id} for edge_id in incident_edge_ids],
                    )
                if present_node_ids:
                    candidate_session.delete_keys(
                        NODE_TABLE,
                        [{"node_id": node_id} for node_id in present_node_ids],
                    )

            remaining = []
            if requested_retractions:
                remaining = sorted(
                    str(row["node_id"])
                    for row in candidate_session.query(
                        "SELECT node_id FROM okg.graph_nodes "
                        "WHERE node_id = ANY(:node_ids)",
                        {"node_ids": requested_retractions},
                    )
                )

        blocking_declarations = list(plan["blocking_declarations"])
        blocking_node_ids = {
            str(item.get("node_id") or "")
            for item in blocking_declarations
            if item.get("node_id")
        }
        blocking_node_ids.update(remaining)
        # A blocker suppresses cleanup so the branch remains inspectable. Any
        # otherwise unsupported live declaration is then also a named blocker.
        if blocking_declarations:
            blocking_node_ids.update(plan["unsupported_node_ids"])
        check = {
            "check_id": "no_unsupported_derived_nodes",
            "result": "blocked" if blocking_node_ids else "clean",
            "blocks_publish": bool(blocking_node_ids),
            "blocking_node_ids": sorted(blocking_node_ids),
            "blocking_declarations": blocking_declarations,
        }
        return {
            "schema": "okg.derived_node_cleanup.v2",
            # Count and digest only (#2718); the ids can be the whole graph.
            "frontier_node_count": plan["frontier_node_count"],
            "frontier_node_digest_sha256": plan["frontier_node_digest_sha256"],
            "frontier_node_digest_algorithm": plan["frontier_node_digest_algorithm"],
            "changed_declaration_ids": plan["changed_declaration_ids"],
            "evaluated_node_ids": plan["evaluated_node_ids"],
            "support_fixed_point_node_ids": plan["support_fixed_point_node_ids"],
            "unsupported_node_ids": plan["unsupported_node_ids"],
            "retracted_node_ids": present_node_ids,
            "retracted_node_count": len(present_node_ids),
            "retracted_incident_edge_ids": incident_edge_ids[:_EDGE_ID_SAMPLE_LIMIT],
            "retracted_incident_edge_count": len(incident_edge_ids),
            "retracted_incident_edge_ids_truncated": (
                len(incident_edge_ids) > _EDGE_ID_SAMPLE_LIMIT
            ),
            "unevaluated_legacy_node_ids": plan["unevaluated_legacy_node_ids"],
            "remaining_unsupported_node_ids": remaining,
            "no_unsupported_derived_nodes": check,
            "blocking_node_ids": check["blocking_node_ids"],
        }

    def _evaluate_full_fields(
        self,
        conn: psycopg.Connection[Any],
        *,
        target_generation_id: int | str,
        catalog_version_id: int,
        deployment_name: str,
        branch_name: str = "default",
    ) -> _GateFields:
        # `branch_name` is the branch being published. The gate reads (and, under
        # prune policy, mutates) THIS branch -- a fork publish must validate the
        # fork head, not `default`, or its delta is invisible to the gate
        # (vacuous pass) and its dangling edges are pruned on the wrong branch
        # (worktree-fork-writes: fork-publish-soundness).
        t0 = time.monotonic()
        cleanup = self._cleanup_derived_nodes(
            conn,
            deployment_name=deployment_name,
            branch_name=branch_name,
        )
        activations = _family_activations(conn, catalog_version_id)
        dangling, narrowing, node_subtype, cardinality = self._eval_chronos(
            conn, target_generation_id, catalog_version_id,
            branch_name=branch_name, activations=activations,
        )

        # Auto-retract ORPHANED LIFECYCLE-OWNED facts before dangling policy.
        # A derived or enricher-owned edge whose endpoint a re-ingest retracted is
        # an orphan -- okg
        # produced it and a later re-ingest stopped producing it, so retracting it
        # (a Chronos tombstone, as-of this generation) completes the retraction the
        # re-ingest implied; it is NOT a deployment policy choice. okg's source-scoped
        # incident-edge cleanup misses these when the edge's source differs from the
        # retracted endpoint's. This runs under BOTH policies, so the strict `block`
        # contract still holds for genuinely-dangling EXPLICIT (source-asserted)
        # forward references -- those alone fall to `dangling_policy` below.
        orphans_retracted: list[int] = []
        if dangling:
            from okg.substrate.graph_store import ChronosGraphStore

            store = ChronosGraphStore.from_connection(conn, ensure_schema=False)
            if store.retract_orphaned_derived_edges(branch=branch_name):
                before = set(dangling)
                dangling, narrowing, node_subtype, cardinality = self._eval_chronos(
                    conn, target_generation_id, catalog_version_id,
                    branch_name=branch_name, activations=activations,
                )
                orphans_retracted = sorted(before - set(dangling))

        # `prune` policy: drop the dangling edges from the branch about to be
        # checkpointed rather than block. They no longer count toward the publish
        # block; narrowing AND node-subtype violations still do.
        pruned: list[int] = []
        prune_attribution = self._empty_prune_attribution()
        if dangling and self.dangling_policy == "prune":
            pruned, prune_attribution = self._prune_dangling(
                conn,
                dangling,
                branch_name=branch_name,
            )
            log.info(
                "transform_consistency_gate: pruned %d dangling edge(s) at publish "
                "(deployment=%s gen=%s branch=%s)",
                len(pruned), deployment_name, target_generation_id, branch_name,
            )
            dangling = sorted(set(dangling) - set(pruned))

        offending = (
            len(dangling)
            + len(narrowing)
            + len(node_subtype)
            + len(cardinality)
            + len(cleanup["blocking_node_ids"])
        )
        patterns = self._violation_patterns(
            conn, target_generation_id,
            branch_name=branch_name,
            narrowing=narrowing,
            node_subtype=node_subtype,
        )
        elapsed_ms = int((time.monotonic() - t0) * 1000)
        # Structural gate: the antijoin ran cleanly, so the evaluation is HEALTHY
        # regardless of violations (task.policy.result-fields). The block decision
        # is carried by blocks_publish (structural violations ALWAYS block) and
        # the violation tally by open_observation_count. newly_opened_count /
        # resolved_count stay 0: a point-in-time antijoin has no observation
        # ledger, so it does not track finding novelty.
        return _GateFields(
            result="evaluated",
            blocks_publish=bool(offending),
            reason=(
                "transform_consistency_violation" if offending else "no_violations"
            ),
            open_observation_count=offending,
            # The families that DID run swept the whole live graph, so coverage
            # is universe-complete for the constraint set named in
            # coverage_basis. A skipped family narrows that SET, not the sweep
            # -- it is disclosed by name below and in the basis, never by
            # silently downgrading a complete sweep to incomplete.
            coverage_complete=True,
            coverage_basis=_coverage_basis_for_families(
                "chronos_transform_ir", activations
            ),
            elapsed_ms=elapsed_ms,
            preflight={"catalog_version_id": catalog_version_id},
            details={
                **_family_disclosure(activations),
                "derived_node_cleanup": cleanup,
                # Stated on BOTH paths so a reader distinguishes "full sweep, so
                # scoped-exact is inapplicable" from "the producer forgot the
                # field" -- absence must never be readable as either answer.
                "scoped": False,
                "scoped_exact": False,
                "scope_coverage_basis": "exact_postgres",
                "dangling_edge_ids": dangling,
                "pruned_dangling_edge_ids": pruned[:_EDGE_ID_SAMPLE_LIMIT],
                "pruned_dangling_edge_count": len(pruned),
                "pruned_dangling_edge_ids_truncated": (
                    len(pruned) > _EDGE_ID_SAMPLE_LIMIT
                ),
                "pruned_dangling_edge_attribution": prune_attribution,
                "orphan_retracted_derived_edge_ids": (
                    orphans_retracted[:_EDGE_ID_SAMPLE_LIMIT]
                ),
                "orphan_retracted_derived_edge_count": len(orphans_retracted),
                "orphan_retracted_derived_edge_ids_truncated": (
                    len(orphans_retracted) > _EDGE_ID_SAMPLE_LIMIT
                ),
                "narrowing_violation_edge_ids": narrowing,
                "node_subtype_violation_node_ids": node_subtype,
                "cardinality_violation_edge_ids": cardinality,
                # #3624: each missing pattern by name, with its count and the
                # library module that declares it, so the block says what to
                # add to deployment.yaml rather than only listing row ids.
                **patterns,
                # Same rule as coverage_basis, for the same reason: this field
                # named cardinality_sql UNCONDITIONALLY, so a catalog that never
                # activated the family still read as though the check had run.
                # Fixing the basis alone would have left the identical false
                # claim standing one field over.
                "runtime": _coverage_basis_for_families(
                    "chronos_transform_ir", activations
                ),
                "dangling_policy": self.dangling_policy,
            },
        )

    def evaluate_incremental(
        self,
        conn: psycopg.Connection[Any],
        *,
        target_generation_id: int | str,
        catalog_version_id: int,
        deployment_name: str,
        previous_catalog_version_id: int | None,
        delta_counts: Mapping[str, Any] | None,
        dirty_scope: tuple[dict[str, list[str]], dict[str, list[int]], bool] | None,
        branch_name: str = "default",
    ) -> ConsistencyPublishGateEvaluation:
        """Evaluate only changed edges when the publish delta proves it is safe.

        A previously published generation already passed this gate. If the active
        catalog has the same applied content identity, only edges written in the
        current segment and edges incident to nodes written in the current segment
        can change the result of no-dangling or narrowing conformance. Everything
        else is identical to the last published pass. Missing dirty-scope evidence
        falls back to the full transform-IR program.
        """

        # Same up-front resolution as evaluate(): the side tables and the
        # induction-basis ORDER comparison consume the BIGINT token; the raw
        # reference keeps flowing to the chronos read path.
        persist_generation_id = _side_table_generation_token(
            conn, target_generation_id
        )
        catalog_equivalence = _catalog_equivalence(
            conn,
            deployment_name=deployment_name,
            previous_catalog_version_id=previous_catalog_version_id,
            catalog_version_id=catalog_version_id,
        )
        published_basis = self._published_induction_basis(
            conn,
            deployment_name=deployment_name,
            branch_name=branch_name,
        )
        retraction_markers = self._validate_retracted_node_markers(
            conn,
            deployment_name=deployment_name,
            branch_name=branch_name,
            delta_counts=delta_counts,
            dirty_scope=dirty_scope,
        )
        fallback = self._incremental_fallback_reason(
            previous_catalog_version_id=previous_catalog_version_id,
            catalog_equivalent=bool(catalog_equivalence["equivalent"]),
            delta_counts=delta_counts,
            dirty_scope=dirty_scope,
            published_basis_verified=bool(published_basis["verified"]),
            retraction_markers_verified=bool(retraction_markers["verified"]),
        )
        if fallback is not None:
            # Compute the FULL verdict, then augment details with the fallback
            # reason BEFORE persisting so the durable row records that this
            # incremental run widened to a full sweep (not a silent scoped pass).
            fields = self._evaluate_full_fields(
                conn,
                target_generation_id=target_generation_id,
                catalog_version_id=catalog_version_id,
                deployment_name=deployment_name,
                branch_name=branch_name,
            )
            fields = replace(
                fields,
                details={
                    **dict(fields.details),
                    "scope_mode": "full",
                    "scope_fallback_reason": fallback,
                    "catalog_equivalence": catalog_equivalence,
                    "published_induction_basis": published_basis,
                    "retraction_marker_validation": retraction_markers,
                },
            )
            return self._persist(
                conn,
                deployment_name=deployment_name,
                target_generation_id=persist_generation_id,
                trigger_kind="full",
                fields=fields,
            )

        assert dirty_scope is not None
        dirty_edge_ids = sorted({
            edge_id
            for edge_ids_by_type in dirty_scope[1].values()
            for edge_id in edge_ids_by_type
        })
        dirty_node_ids = sorted({
            node_id
            for node_ids_by_subtype in dirty_scope[0].values()
            for node_id in node_ids_by_subtype
        })
        topology_dirty_node_ids = _topology_dirty_node_ids(
            dirty_scope,
            dirty_node_ids,
        )
        t0 = time.monotonic()
        cleanup = self._cleanup_derived_nodes(
            conn,
            deployment_name=deployment_name,
            branch_name=branch_name,
            frontier_node_ids=set(dirty_node_ids),
        )
        activations = _family_activations(conn, catalog_version_id)
        edge_ids = self._candidate_edge_ids(
            conn,
            target_generation_id,
            dirty_edge_ids=dirty_edge_ids,
            dirty_node_ids=topology_dirty_node_ids,
            branch_name=branch_name,
        )
        dangling, narrowing = self._eval_chronos_scoped_edges(
            conn,
            target_generation_id,
            catalog_version_id,
            edge_ids=edge_ids,
            branch_name=branch_name,
            activations=activations,
        )
        # Node-subtype conformance over the DIRTY nodes only (the incremental
        # analogue of the full antijoin): a dirty node whose subtype is not in the
        # active catalog blocks. Scoped to the dirty node set so the incremental
        # path stays O(delta).
        node_subtype = self._eval_node_subtype_scoped(
            conn,
            target_generation_id,
            catalog_version_id,
            node_ids=dirty_node_ids,
            branch_name=branch_name,
        )
        # #710: at-most-one-target cardinality over the srcs touched by the
        # dirty scope (see _eval_cardinality_scoped). Runs even when no edge is
        # narrowing/dangling-violating -- a twin target is neither.
        cardinality = self._eval_cardinality_scoped(
            conn,
            target_generation_id,
            catalog_version_id,
            edge_ids=edge_ids,
            node_ids=dirty_node_ids,
            branch_name=branch_name,
            activations=activations,
        )
        # Auto-retract lifecycle-owned orphan facts (see evaluate()): a derived or
        # enricher-owned edge whose endpoint was retracted is not a policy choice. The
        # incremental candidate set already includes edges incident to the dirty
        # nodes, so a freshly-orphaned derived edge is in `dangling` here; retract it
        # and re-check the scoped edges before applying any dangling policy.
        orphans_retracted: list[int] = []
        if dangling:
            from okg.substrate.graph_store import ChronosGraphStore

            store = ChronosGraphStore.from_connection(conn, ensure_schema=False)
            if store.retract_orphaned_derived_edges(branch=branch_name):
                before = set(dangling)
                dangling, narrowing = self._eval_chronos_scoped_edges(
                    conn,
                    target_generation_id,
                    catalog_version_id,
                    edge_ids=edge_ids,
                    branch_name=branch_name,
                    activations=activations,
                )
                orphans_retracted = sorted(before - set(dangling))
        pruned: list[int] = []
        prune_attribution = self._empty_prune_attribution()
        if dangling and self.dangling_policy == "prune":
            pruned, prune_attribution = self._prune_dangling(
                conn,
                dangling,
                branch_name=branch_name,
            )
            dangling = sorted(set(dangling) - set(pruned))

        offending = (
            len(dangling)
            + len(narrowing)
            + len(node_subtype)
            + len(cardinality)
            + len(cleanup["blocking_node_ids"])
        )
        patterns = self._violation_patterns(
            conn, target_generation_id,
            branch_name=branch_name,
            narrowing=narrowing,
            node_subtype=node_subtype,
        )
        elapsed_ms = int((time.monotonic() - t0) * 1000)
        # Structural gate: the antijoin ran cleanly, so the evaluation is HEALTHY
        # regardless of violations (task.policy.result-fields). The block decision
        # is carried by blocks_publish (structural violations ALWAYS block) and
        # the violation tally by open_observation_count. newly_opened_count /
        # resolved_count stay 0: a point-in-time antijoin has no observation
        # ledger, so it does not track finding novelty.
        # Scope IDENTITY, not counts. Pre-change this path persisted only
        # dirty_edge_count / scoped_edge_count, so the unevaluated complement
        # was not recoverable from the published generation at all.
        # `scope_identity` lives in observations.py because the IR executor and
        # the SQL gate scope frontier disclose through the same helper.
        # `scoped_edge_ids` is the exact edge set the
        # dangling / narrowing / cardinality families evaluated; `dirty_node_ids`
        # is the exact node set node_subtype evaluated. Both are what the reader
        # subtracts from the live graph to get the UNEVALUATED complement.
        scoped_edge_identity = scope_identity("scoped_edge", edge_ids)
        dirty_node_identity = scope_identity("dirty_node", dirty_node_ids)
        scope_identity_payload: dict[str, Any] = {
            **scoped_edge_identity,
            **scope_identity("dirty_edge", dirty_edge_ids),
            **dirty_node_identity,
            **scope_identity("topology_dirty_node", topology_dirty_node_ids),
            # The recipe SUPPRESSES itself when the id list is capped. Emitting
            # "live MINUS scoped_edge_ids" against a truncated list hands the
            # reader a subtraction that reports thousands of edges the gate DID
            # evaluate as unevaluated -- a wrong answer is worse than a refusal.
            "scope_complement_recipe": {
                "edge_families": scope_complement_recipe(
                    "scoped_edge",
                    scoped_edge_identity,
                    universe="live edge_ids at this generation",
                    ids_path="details.scope_identity.scoped_edge_ids",
                ),
                "node_subtype": scope_complement_recipe(
                    "dirty_node",
                    dirty_node_identity,
                    universe="live node_ids at this generation",
                    ids_path="details.scope_identity.dirty_node_ids",
                ),
            },
        }
        # The delta argument: an earlier generation already passed this gate over
        # the FULL graph, and the catalog is unchanged, so nothing outside the
        # dirty scope can have changed its verdict. That claim is recorded
        # EXPLICITLY here -- it is NOT smuggled in as coverage_complete=true,
        # which asserts the different (and false) thing that this run enumerated
        # the whole candidate universe -- and it is MEASURED, not asserted: see
        # _measure_induction_basis for which conjuncts are observable and which
        # single residual is named instead of being folded into a true.
        induction = self._measure_induction_basis(
            conn,
            check_id=_transform_gate_definition(deployment_name).check_id,
            gate_name=self.name,
            target_generation_id=persist_generation_id,
            previous_catalog_version_id=previous_catalog_version_id,
            catalog_equivalence=catalog_equivalence,
        )
        scoped_coverage = CandidateCoverageReport(
            universe_key=(
                f"transform_gate:{target_generation_id}:scoped"
            ),
            # Exact over the derived scope (a pure graph antijoin, no
            # sampling) -> scoped-exact, which the gate algebra reads as
            # HEALTHY rather than blocked_incomplete.
            coverage_basis="exact_postgres",
            coverage_complete=False,
            scope_restricted=True,
            # One scalar column for a two-part scope (edges for the edge
            # families, nodes for node_subtype). The scalar is the sum; the
            # two sets are separable by IDENTITY in the payload, which is
            # what a reader actually needs.
            scope_subject_count=len(edge_ids) + len(dirty_node_ids),
            eligible_input_count=len(edge_ids) + len(dirty_node_ids),
            candidate_count=offending,
            evaluator_invocation_count=0,
            payload={
                "runtime": "chronos_scoped_sql",
                "scope_mode": "dirty_edges+incident_dirty_nodes",
                "scope_identity": scope_identity_payload,
                **induction,
                **_family_disclosure(activations),
            },
        )
        # Scoped-EXACT, disclosed on the GATE row. coverage_complete=false alone
        # cannot distinguish "exact over a delta" from "gave up partway", so a
        # reader that never joins to consistency_candidate_coverage (the
        # differential-replay proof reads gate summaries only) would have to
        # treat this healthy run as unproven coverage. Same predicate the gate
        # algebra applies to the persisted row, evaluated on the report that is
        # about to be written.
        scope_coverage_disclosure = {
            "scoped": True,
            "scoped_exact": coverage_is_scoped_exact(scoped_coverage),
            "scope_coverage_basis": scoped_coverage.coverage_basis,
        }
        fields = _GateFields(
            result="evaluated",
            blocks_publish=bool(offending),
            reason=(
                "transform_consistency_violation" if offending else "no_violations"
            ),
            open_observation_count=offending,
            # A scoped run is exact over its scope but did NOT enumerate the
            # universe, so it may not claim coverage_complete -- the same rule
            # the IR executor already follows and the same rule the
            # consistency_candidate_coverage CHECK enforces. The stronger,
            # honest claim (the verdict is universe-valid by induction) is
            # carried by details.induction below, where it is checkable.
            coverage_complete=False,
            coverage_basis=_coverage_basis_for_families(
                "chronos_dirty_edges+incident_dirty_nodes", activations
            ),
            elapsed_ms=elapsed_ms,
            coverage=scoped_coverage,
            preflight={
                "catalog_version_id": catalog_version_id,
                "previous_catalog_version_id": previous_catalog_version_id,
                "catalog_equivalence": catalog_equivalence,
                "delta_counts": dict(delta_counts or {}),
                "dirty_edge_count": len(dirty_edge_ids),
                "dirty_node_count": len(dirty_node_ids),
                "topology_dirty_node_count": len(topology_dirty_node_ids),
                "scoped_edge_count": len(edge_ids),
                # Identity rides on the gate row too: finding (v) of the audit
                # is that consistency_candidate_coverage is read by no CLI or
                # publisher surface, so identity landing only there would not
                # reach a reader. consistency_gate_results IS what
                # `okg generation describe` reads.
                "scope_identity": scope_identity_payload,
                **scope_coverage_disclosure,
            },
            details={
                **_family_disclosure(activations),
                "derived_node_cleanup": cleanup,
                "scope_identity": scope_identity_payload,
                **scope_coverage_disclosure,
                **induction,
                "published_induction_basis": published_basis,
                "retraction_marker_validation": retraction_markers,
                "catalog_equivalence": catalog_equivalence,
                "dangling_edge_ids": dangling,
                "pruned_dangling_edge_ids": pruned[:_EDGE_ID_SAMPLE_LIMIT],
                "pruned_dangling_edge_count": len(pruned),
                "pruned_dangling_edge_ids_truncated": (
                    len(pruned) > _EDGE_ID_SAMPLE_LIMIT
                ),
                "pruned_dangling_edge_attribution": prune_attribution,
                "orphan_retracted_derived_edge_ids": (
                    orphans_retracted[:_EDGE_ID_SAMPLE_LIMIT]
                ),
                "orphan_retracted_derived_edge_count": len(orphans_retracted),
                "orphan_retracted_derived_edge_ids_truncated": (
                    len(orphans_retracted) > _EDGE_ID_SAMPLE_LIMIT
                ),
                "narrowing_violation_edge_ids": narrowing,
                "node_subtype_violation_node_ids": node_subtype,
                "cardinality_violation_edge_ids": cardinality,
                # #3624: each missing pattern by name, with its count and the
                # library module that declares it, so the block says what to
                # add to deployment.yaml rather than only listing row ids.
                **patterns,
                "runtime": "chronos_scoped_sql",
                "dangling_policy": self.dangling_policy,
                "scope_mode": "dirty_edges+incident_dirty_nodes",
                # Edge checks scope to incident dirty nodes; the node-subtype antijoin
                # runs over the FULL dirty node set (a pure re-type / no-edge node is
                # covered), so record its scope distinctly from scope_mode.
                "node_subtype_scope": "all_dirty_nodes",
            },
        )
        return self._persist(
            conn,
            deployment_name=deployment_name,
            target_generation_id=persist_generation_id,
            trigger_kind="dirty",
            fields=fields,
        )

    def _persist(
        self,
        conn: psycopg.Connection[Any],
        *,
        deployment_name: str,
        target_generation_id: int,
        trigger_kind: str,
        fields: _GateFields,
    ) -> ConsistencyPublishGateEvaluation:
        """Bind the precomputed verdict to a real run and persist it durably.

        task.transformgate.persist: the always-on Phase B gate now lands a real
        ``derived_artifact_runs`` row (via ``start_consistency_run``, which also
        registers the synthetic check definition) and a
        ``consistency_gate_results`` row (via ``persist_precomputed_gate_evaluation``,
        which copies the block decision verbatim) instead of fabricating a discarded
        ``uuid4`` run id. Runs on EVERY publish, so the persistence cost is measured
        (``persist_ms`` in the run metrics). No silent fallback: a failed persist
        marks the run failed and re-raises, surfacing to the publisher's gate loop
        (which blocks the generation with ``gate_exception``).
        """
        definition = _transform_gate_definition(deployment_name)
        # Already resolved by the evaluate/evaluate_incremental entry points
        # via _side_table_generation_token — never parse a generation
        # reference here (a chronos TEXT ref is not an integer).
        gen = target_generation_id
        persist_t0 = time.monotonic()
        run_id = start_consistency_run(
            conn,
            definition,
            target_generation_id=gen,
            trigger_kind=trigger_kind,
            metrics={
                "gate_name": self.name,
                "gate_type": "transform_ir",
                "execution_phase": "phase_b",
                "blocks_publish": fields.blocks_publish,
                "trigger_kind": trigger_kind,
            },
        )
        try:
            if fields.coverage is not None:
                # Route the scoped run through the coverage table so
                # CHECK (NOT (coverage_complete AND scoped)) applies to it.
                # record_candidate_coverage additionally re-checks the same
                # invariant at the write boundary, so a future edit that pairs
                # scoped with complete fails LOUDLY here instead of silently
                # skipping the row as this path used to.
                record_candidate_coverage(
                    conn,
                    definition,
                    target_generation_id=gen,
                    run_id=run_id,
                    report=fields.coverage,
                )
            evaluation = ConsistencyPublishGateEvaluation(
                run_id=run_id,
                check_id=definition.check_id,
                target_generation_id=gen,
                gate_name=self.name,
                result=fields.result,
                blocks_publish=fields.blocks_publish,
                reason=fields.reason,
                open_observation_count=fields.open_observation_count,
                coverage_complete=fields.coverage_complete,
                coverage_basis=fields.coverage_basis,
                budget_ms=None,
                elapsed_ms=fields.elapsed_ms,
                preflight=dict(fields.preflight),
                details=dict(fields.details),
            )
            persist_precomputed_gate_evaluation(conn, evaluation)
            persist_ms = int((time.monotonic() - persist_t0) * 1000)
            complete_consistency_run(
                conn,
                run_id=run_id,
                metrics={
                    "gate_name": self.name,
                    "gate_type": "transform_ir",
                    "gate_result": fields.result,
                    "blocks_publish": fields.blocks_publish,
                    "offending_row_count": fields.open_observation_count,
                    "trigger_kind": trigger_kind,
                    "persist_ms": persist_ms,
                },
            )
            return evaluation
        except Exception as exc:
            _fail_run(conn, run_id, exc)
            raise

    def _prune_dangling(
        self,
        conn: psycopg.Connection[Any],
        edge_ids: list[int],
        *,
        branch_name: str = "default",
    ) -> tuple[list[int], dict[str, Any]]:
        """Drop dangling edges from the branch being published at publish time.

        Prunes on ``branch_name`` -- a fork publish must prune the fork's
        continuation segment, NOT default's. (``prune_edges`` is itself
        segment-scoped to the named branch's current segment, so this both targets
        the right branch and cannot touch a parent branch's inherited rows.)

        The receipt is captured before deletion, while producer/edge/endpoint data
        is still queryable, and reports only ids proven absent after the prune.
        """
        from okg.substrate.graph_store import ChronosGraphStore

        store = ChronosGraphStore.from_connection(conn, ensure_schema=False)
        before = store.checkout(branch=branch_name)
        predicate, params = _any_predicate(
            "e.edge_id",
            edge_ids,
            prefix="prune_edge_id",
        )
        rows = before.query(
            f"""
            SELECT e.edge_id,
                   e.edge_type,
                   e.source,
                   ns.subtype AS src_subtype,
                   nd.subtype AS dst_subtype
              FROM okg.graph_edges e
              LEFT JOIN okg.graph_nodes ns ON ns.node_id = e.src
              LEFT JOIN okg.graph_nodes nd ON nd.node_id = e.dst
             WHERE {predicate}
            """,
            params,
        )
        present_ids = {int(row["edge_id"]) for row in rows}
        store.prune_edges(edge_ids, branch=branch_name)
        after = store.checkout(branch=branch_name)
        remaining_ids = {
            int(row["edge_id"])
            for row in after.query(
                f"SELECT e.edge_id FROM okg.graph_edges e WHERE {predicate}",
                params,
            )
        }
        pruned = sorted(present_ids - remaining_ids)
        pruned_set = set(pruned)
        attribution = self._prune_attribution(
            [row for row in rows if int(row["edge_id"]) in pruned_set],
        )
        return pruned, attribution

    @staticmethod
    def _empty_prune_attribution() -> dict[str, Any]:
        return {
            "schema": "okg.transform_prune_attribution.v1",
            "total": 0,
            "attributed": 0,
            "unattributed": 0,
            "by_source": {"bucket_count": 0, "truncated": False, "buckets": []},
            "by_classification": {
                "bucket_count": 0,
                "truncated": False,
                "buckets": [],
            },
        }

    @classmethod
    def _prune_attribution(cls, rows: list[Mapping[str, Any]]) -> dict[str, Any]:
        source_counts: Counter[tuple[str]] = Counter()
        classification_counts: Counter[tuple[str, str, str, str, str]] = Counter()
        attributed = 0
        for row in rows:
            raw_source = row.get("source")
            source = (
                str(raw_source).strip()
                if raw_source is not None and str(raw_source).strip()
                else "__unattributed__"
            )
            if source != "__unattributed__":
                attributed += 1
            src_subtype = (
                str(row["src_subtype"])
                if row.get("src_subtype") is not None
                else "__missing__"
            )
            dst_subtype = (
                str(row["dst_subtype"])
                if row.get("dst_subtype") is not None
                else "__missing__"
            )
            if src_subtype == "__missing__" and dst_subtype == "__missing__":
                missing_endpoint = "both"
            elif src_subtype == "__missing__":
                missing_endpoint = "src"
            elif dst_subtype == "__missing__":
                missing_endpoint = "dst"
            else:
                missing_endpoint = "none"
            source_counts[(source,)] += 1
            classification_counts[(
                source,
                str(row.get("edge_type") or "__unknown__"),
                src_subtype,
                dst_subtype,
                missing_endpoint,
            )] += 1

        total = len(rows)
        return {
            "schema": "okg.transform_prune_attribution.v1",
            "total": total,
            "attributed": attributed,
            "unattributed": total - attributed,
            "by_source": cls._bounded_counter_receipt(
                source_counts,
                fields=("source",),
                limit=_SOURCE_BUCKET_LIMIT,
            ),
            "by_classification": cls._bounded_counter_receipt(
                classification_counts,
                fields=(
                    "source",
                    "edge_type",
                    "src_subtype",
                    "dst_subtype",
                    "missing_endpoint",
                ),
                limit=_CLASSIFICATION_BUCKET_LIMIT,
            ),
        }

    @staticmethod
    def _bounded_counter_receipt(
        counts: Counter[tuple[str, ...]],
        *,
        fields: tuple[str, ...],
        limit: int,
    ) -> dict[str, Any]:
        ordered = sorted(counts.items(), key=lambda item: (-item[1], item[0]))
        truncated = len(ordered) > limit
        visible = ordered if not truncated else ordered[: max(0, limit - 1)]
        buckets = [
            {**dict(zip(fields, key, strict=True)), "count": int(count)}
            for key, count in visible
        ]
        if truncated:
            omitted = ordered[len(visible):]
            buckets.append({
                **{field: "__attributed_overflow__" for field in fields},
                "count": sum(int(count) for _, count in omitted),
                "lineage_status": "attributed",
                "omitted_bucket_count": len(omitted),
                "omitted_key_sample": [list(key) for key, _ in omitted[:5]],
                "action": (
                    "inspect the bounded pruned edge-id sample or rerun the "
                    "diagnostic with a higher bucket limit"
                ),
            })
        return {
            "bucket_count": len(ordered),
            "truncated": truncated,
            "buckets": buckets,
        }

    def _violation_patterns(
        self,
        conn: psycopg.Connection[Any],
        gen: int | str,
        *,
        branch_name: str,
        narrowing: list[int],
        node_subtype: list[str],
    ) -> dict[str, Any]:
        """Name each narrowing / node-subtype violation pattern (#3624).

        Reads the same Chronos surface the gate read (the generation checkpoint,
        else the head of the branch being published), and only when there is a
        violation: a clean run opens no session and runs no query.

        Naming is a diagnostic on top of a verdict already reached. If the
        library index or the grouped read fails, the gate still blocks on its
        violation ids and persists its row; the failure is recorded in
        ``violation_patterns_error`` and logged, never dropped. The read runs
        inside its own transaction block (a savepoint when one is open), so a
        SQL error cannot leave the gate's connection aborted.
        """
        if not narrowing and not node_subtype:
            return empty_pattern_details()
        from okg.substrate.errors import SubstrateError
        from okg.substrate.graph_store import ChronosGraphStore

        # autocommit=False: the store must not COMMIT inside the savepoint
        # below (psycopg forbids an explicit commit in a transaction block).
        store = ChronosGraphStore.from_connection(
            conn, ensure_schema=False, autocommit=False,
        )
        try:
            session = store.checkout_generation(gen)
        except Exception:  # noqa: BLE001 - pending publish evaluates branch head
            session = store.checkout(branch=branch_name)
        try:
            with conn.transaction():
                return violation_pattern_details(
                    session,
                    narrowing_edge_ids=narrowing,
                    node_subtype_node_ids=node_subtype,
                )
        except (psycopg.Error, SubstrateError, OSError, yaml.YAMLError) as exc:
            log.warning(
                "transform_consistency_gate: could not name the %d narrowing / "
                "%d node-subtype violation pattern(s): %s: %s",
                len(narrowing), len(node_subtype), type(exc).__name__, exc,
            )
            return empty_pattern_details(
                {"type": type(exc).__name__, "message": str(exc)[:500]},
            )

    @staticmethod
    def _lowered_programs(registries: Registries, catalog_version_id: int):
        dangling_sql, dparams = lower_query(no_dangling_edges_program(), registries)
        narrowing_sql, nparams = lower_query(
            narrowing_conformance_program(catalog_version_id), registries
        )
        node_sql, node_params = lower_query(
            node_subtype_conformance_program(catalog_version_id), registries
        )
        return (
            (dangling_sql, dparams),
            (narrowing_sql, nparams),
            (node_sql, node_params),
        )

    def _eval_chronos(
        self, conn: psycopg.Connection[Any], gen: int, catalog_version_id: int,
        *, branch_name: str = "default",
        activations: tuple[_FamilyActivation, ...],
    ) -> tuple[list[int], list[int], list[str], list[int]]:
        """Lower the consistency programs over the Chronos surfaces and run them
        through a checkpoint-pinned Chronos session over okg.graph_nodes/graph_edges.

        Returns (dangling edge ids, narrowing-violating edge ids, node-subtype-
        violating node ids, cardinality-violating edge ids)."""
        from okg.substrate.graph_store import ChronosGraphStore

        reg = chronos_surface_registry()
        narrowing_active = _is_active(activations, "narrowing")
        if narrowing_active:
            reg.register(
                Surface(
                    "narrowing_allowed",
                    SurfaceKind.CATALOG_POLICY,
                    narrowing_allowed_binding(catalog_version_id),
                    pinned=False,
                )
            )
        reg.register(
            Surface(
                "catalog_subtypes",
                SurfaceKind.CATALOG_POLICY,
                catalog_subtypes_binding(catalog_version_id),
                pinned=False,
            )
        )
        registries = Registries(surfaces=reg, functions=FunctionRegistry())
        if narrowing_active:
            (d_sql, d_p), (n_sql, n_p), (ns_sql, ns_p) = self._lowered_programs(
                registries, catalog_version_id
            )
        else:
            d_sql, d_p = lower_query(no_dangling_edges_program(), registries)
            ns_sql, ns_p = lower_query(
                node_subtype_conformance_program(catalog_version_id),
                registries,
            )
        store = ChronosGraphStore.from_connection(conn, ensure_schema=False)
        # NB: checkout_generation takes no branch= on purpose -- its lookup is keyed
        # by checkpoint id (the branch param is cosmetic), and on the live publish
        # path `gen` is a synthetic token that always raises, so the branch-scoped
        # fallback below is what actually runs. Same for the other two _eval helpers.
        try:
            session = store.checkout_generation(gen)
        except Exception:  # noqa: BLE001 — gen not yet checkpointed pre-publish; fall back to current head (see below)
            # Publish-time gate: the to-be-published generation is not yet a Chronos
            # checkpoint (the publisher runs Phase B before checkpoint_generation, with
            # the compat int gen), so evaluate the CURRENT head of the branch being
            # published -- the exact state about to be checkpointed (default for a
            # default publish, the fork head for a fork publish).
            session = store.checkout(branch=branch_name)
        dangling = sorted(
            int(r["edge_id"]) for r in session.query(to_chronos_sql(d_sql), d_p)
        )
        narrowing = sorted(
            int(r["edge_id"]) for r in session.query(to_chronos_sql(n_sql), n_p)
        ) if narrowing_active else []
        node_subtype = sorted(
            str(r["node_id"]) for r in session.query(to_chronos_sql(ns_sql), ns_p)
        )
        # #710: at-most-one-target cardinality over the FULL live graph. The SQL
        # is already chronos-shaped (references okg.graph_edges/graph_nodes
        # directly), so it runs raw through the session like the scoped checks.
        cardinality = (
            sorted(
                int(r["edge_id"])
                for r in session.query(
                    _cardinality_violation_sql(catalog_version_id), {}
                )
            )
            if _is_active(activations, "cardinality")
            else []
        )
        return dangling, narrowing, node_subtype, cardinality

    @staticmethod
    def _measure_induction_basis(
        conn: psycopg.Connection[Any],
        *,
        check_id: str,
        gate_name: str,
        target_generation_id: int,
        previous_catalog_version_id: int | None,
        catalog_equivalence: Mapping[str, Any],
    ) -> dict[str, Any]:
        """MEASURE the inductive premise instead of asserting it.

        Pre-change this disclosure hardcoded ``True`` next to the prose "the
        previous published generation passed this gate over the full graph".
        Nothing checked that prose, and it is not always true: the previous
        generation may itself have been SCOPED (it swept a delta, not the graph).
        Every conjunct that is observable is therefore measured here, against
        ``okg.consistency_gate_results``:

        * ``induction_guard`` -- the applied catalog content identity is
          unchanged. (Also enforced by ``_incremental_fallback_reason``;
          measured again here so the recorded claim does not depend on a caller.)
        * ``induction_basis_generation_id`` -- the most recent EARLIER row for
          this check with ``result='evaluated'`` and ``coverage_complete = true``,
          i.e. a real FULL sweep. A scoped row cannot masquerade as the basis: a
          scoped run is ``coverage_complete = false`` by the schema CHECK.
        * ``induction_chain_unverified_generation_ids`` -- every gate row between
          that sweep and this run must itself have recorded a VERIFIED induction.
          One that did not breaks the chain.

        One conjunct is NOT observable from this table and so is disclosed as a
        residual rather than folded into a ``True``: a publish that skips the
        gate outright writes no row at all (the publisher's
        ``result="skipped_reused_previous"`` path), and
        ``target_generation_id`` on the Chronos-native path is a compat
        timestamp token rather than an ``okg.graph_generations`` key, so
        generation history cannot enumerate the publishes either. Reading an
        empty gap list off an unenumerable history would be absence dressed as
        agreement, so the residual is named in
        ``induction_ungated_publish_residual`` and left visible.

        No conjunct defaults to true: an absent basis or an unverified
        intermediate run yields ``False`` plus the reason.
        """

        guard_holds = bool(catalog_equivalence.get("equivalent"))
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT max(target_generation_id)
                  FROM okg.consistency_gate_results
                 WHERE check_id = %(check_id)s
                   AND gate_name = %(gate_name)s
                   AND target_generation_id < %(gen)s
                   AND result = 'evaluated'
                   AND coverage_complete IS TRUE
                   AND blocks_publish IS FALSE
                """,
                {
                    "check_id": check_id,
                    "gate_name": gate_name,
                    # Resolved to the BIGINT side-table token by the caller
                    # (_side_table_generation_token) — this `<` comparison is
                    # an ordinal-domain ORDER operation (D2 §3.2.1).
                    "gen": target_generation_id,
                },
            )
            row = cur.fetchone()
            basis_generation_id = None if row is None else row[0]
            unverified_chain: list[int] = []
            if basis_generation_id is not None:
                cur.execute(
                    """
                    SELECT DISTINCT target_generation_id
                      FROM okg.consistency_gate_results
                     WHERE check_id = %(check_id)s
                       AND gate_name = %(gate_name)s
                       AND target_generation_id > %(basis)s
                       AND target_generation_id < %(gen)s
                       AND COALESCE(
                             details->>'verdict_universe_valid_by_induction',
                             'false'
                           ) <> 'true'
                     ORDER BY target_generation_id
                    """,
                    {
                        "check_id": check_id,
                        "gate_name": gate_name,
                        "basis": int(basis_generation_id),
                        "gen": target_generation_id,
                    },
                )
                unverified_chain = [int(r[0]) for r in cur.fetchall()]

        if not guard_holds:
            reason: str | None = "catalog_version_guard_does_not_hold"
        elif basis_generation_id is None:
            reason = "no_earlier_full_sweep_gate_row_for_this_check"
        elif unverified_chain:
            reason = "intermediate_scoped_run_did_not_verify_its_own_induction"
        else:
            reason = None

        return {
            "verdict_universe_valid_by_induction": reason is None,
            "induction_basis_measured": True,
            "induction_guard": (
                "previous and current catalogs have the same applied "
                "content identity"
            ),
            "induction_previous_catalog_version_id": previous_catalog_version_id,
            "induction_catalog_equivalence": dict(catalog_equivalence),
            "induction_basis": (
                "MEASURED: an earlier generation passed THIS gate over the full "
                "graph (a consistency_gate_results row with "
                "result=evaluated AND coverage_complete=true AND "
                "blocks_publish=false, recorded as "
                "induction_basis_generation_id), every gate row since it "
                "recorded a verified induction of its own, and the applied "
                "catalog content identity is unchanged -- so only the dirty "
                "scope can change a verdict"
            ),
            "induction_basis_generation_id": (
                None if basis_generation_id is None else int(basis_generation_id)
            ),
            "induction_chain_unverified_generation_ids": unverified_chain,
            "induction_ungated_publish_residual": (
                "NOT MEASURED: a publish that skips this gate entirely writes no "
                "consistency_gate_results row (publisher "
                "result=skipped_reused_previous), and target_generation_id is a "
                "compat token rather than an okg.graph_generations key, so an "
                "ungated publish between induction_basis_generation_id and this "
                "run is not observable from this table"
            ),
            "induction_unverified_reason": reason,
        }

    @staticmethod
    def _incremental_fallback_reason(
        *,
        previous_catalog_version_id: int | None,
        catalog_equivalent: bool,
        delta_counts: Mapping[str, Any] | None,
        dirty_scope: tuple[dict[str, list[str]], dict[str, list[int]], bool] | None,
        published_basis_verified: bool = True,
        retraction_markers_verified: bool = True,
    ) -> str | None:
        if previous_catalog_version_id is None:
            return "no_previous_generation"
        if not catalog_equivalent:
            return "catalog_version_changed"
        if delta_counts is None:
            return "delta_counts_unavailable"
        if dirty_scope is None:
            return "dirty_scope_unavailable"
        if not dirty_scope[2]:
            return "dirty_edge_scope_unavailable"
        if not published_basis_verified:
            return "no_full_induction_basis"
        if not retraction_markers_verified:
            return "dirty_node_retraction_scope_mismatch"
        dirty_node_count = sum(len(node_ids) for node_ids in dirty_scope[0].values())
        dirty_edge_count = sum(len(edge_ids) for edge_ids in dirty_scope[1].values())
        changed_nodes = sum(
            int(delta_counts.get(key, 0) or 0)
            for key in ("nodes_added", "nodes_retracted", "nodes_updated")
        )
        changed_edges = sum(
            int(delta_counts.get(key, 0) or 0)
            for key in ("edges_added", "edges_retracted", "edges_updated")
        )
        if changed_nodes and dirty_node_count == 0:
            return "dirty_node_scope_mismatch"
        if changed_edges and dirty_edge_count == 0:
            return "dirty_edge_scope_mismatch"
        return None

    def _candidate_edge_ids(
        self,
        conn: psycopg.Connection[Any],
        gen: int,
        *,
        dirty_edge_ids: list[int],
        dirty_node_ids: list[str],
        branch_name: str = "default",
    ) -> list[int]:
        """Return live edges whose consistency result could have changed."""
        from okg.substrate.graph_store import ChronosGraphStore

        if not dirty_edge_ids and not dirty_node_ids:
            return []
        store = ChronosGraphStore.from_connection(conn, ensure_schema=False)
        try:
            session = store.checkout_generation(gen)
        except Exception:  # noqa: BLE001 - pending publish evaluates branch head
            session = store.checkout(branch=branch_name)
        candidates: set[int] = set()
        if dirty_edge_ids:
            edge_predicate, edge_params = _any_predicate(
                "edge_id",
                dirty_edge_ids,
                prefix="edge_id",
            )
            candidates.update(
                int(r["edge_id"])
                for r in session.query(
                    f"""
                    SELECT edge_id
                      FROM okg.graph_edges
                     WHERE {edge_predicate}
                    """,
                    edge_params,
                )
            )
        if dirty_node_ids:
            src_predicate, src_params = _any_predicate(
                "src",
                dirty_node_ids,
                prefix="src_node_id",
            )
            candidates.update(
                int(r["edge_id"])
                for r in session.query(
                    f"""
                    SELECT edge_id
                      FROM okg.graph_edges
                     WHERE {src_predicate}
                    """,
                    src_params,
                )
            )
            dst_predicate, dst_params = _any_predicate(
                "dst",
                dirty_node_ids,
                prefix="dst_node_id",
            )
            candidates.update(
                int(r["edge_id"])
                for r in session.query(
                    f"""
                    SELECT edge_id
                      FROM okg.graph_edges
                     WHERE {dst_predicate}
                    """,
                    dst_params,
                )
            )
        return sorted(candidates)

    def _eval_chronos_scoped_edges(
        self,
        conn: psycopg.Connection[Any],
        gen: int,
        catalog_version_id: int,
        *,
        edge_ids: list[int],
        branch_name: str = "default",
        activations: tuple[_FamilyActivation, ...],
    ) -> tuple[list[int], list[int]]:
        """Run the same consistency checks restricted to known-dirty edge ids."""
        from okg.substrate.graph_store import ChronosGraphStore

        if not edge_ids:
            return [], []
        store = ChronosGraphStore.from_connection(conn, ensure_schema=False)
        try:
            session = store.checkout_generation(gen)
        except Exception:  # noqa: BLE001 - pending publish evaluates branch head
            session = store.checkout(branch=branch_name)
        edge_predicate, edge_params = _any_predicate(
            "e.edge_id",
            edge_ids,
            prefix="eval_edge_id",
        )
        dangling = sorted(
            int(r["edge_id"])
            for r in session.query(
                f"""
                SELECT e.edge_id
                  FROM okg.graph_edges e
                  LEFT JOIN okg.graph_nodes ns
                    ON ns.node_id = e.src
                  LEFT JOIN okg.graph_nodes nd
                    ON nd.node_id = e.dst
                 WHERE {edge_predicate}
                   AND (ns.node_id IS NULL OR nd.node_id IS NULL)
                """,
                edge_params,
            )
        )
        narrowing: list[int] = []
        if _is_active(activations, "narrowing"):
            narrowing_sql = f"""
                SELECT e.edge_id
                  FROM okg.graph_edges e
                  JOIN okg.graph_nodes ns
                    ON ns.node_id = e.src
                  JOIN okg.graph_nodes nd
                    ON nd.node_id = e.dst
                  LEFT JOIN {narrowing_allowed_binding(catalog_version_id)} al
                    ON al.edge_type = e.edge_type
                   AND al.src_subtype = ns.subtype
                   AND al.dst_subtype = nd.subtype
                 WHERE {edge_predicate}
                   AND al.edge_type IS NULL
            """
            narrowing = sorted(
                int(r["edge_id"])
                for r in session.query(narrowing_sql, edge_params)
            )
        return dangling, narrowing

    def _eval_cardinality_scoped(
        self,
        conn: psycopg.Connection[Any],
        gen: int,
        catalog_version_id: int,
        *,
        edge_ids: list[int],
        node_ids: list[str],
        branch_name: str = "default",
        activations: tuple[_FamilyActivation, ...],
    ) -> list[int]:
        """At-most-one-target cardinality (#710) restricted to the dirty scope.

        A newly added edge to a src that already has one makes a >1 group. The
        relevant sources are the srcs of the candidate edges (the added edge's
        src is often a PRE-EXISTING, non-dirty node, so it is NOT in the dirty
        node set) plus the dirty nodes themselves (a re-typed node can newly
        fall under a constrained parent). Scoping the grp CTE to those sources
        keeps the incremental path O(delta); the DISTINCT-dst count still sees
        every live edge of each such src, so a >1 group is never missed."""
        if not _is_active(activations, "cardinality"):
            return []
        from okg.substrate.graph_store import ChronosGraphStore

        store = ChronosGraphStore.from_connection(conn, ensure_schema=False)
        try:
            session = store.checkout_generation(gen)
        except Exception:  # noqa: BLE001 - pending publish evaluates branch head
            session = store.checkout(branch=branch_name)
        src_ids: set[str] = set(node_ids)
        if edge_ids:
            edge_predicate, edge_params = _any_predicate(
                "edge_id", edge_ids, prefix="card_edge",
            )
            src_ids.update(
                str(r["src"])
                for r in session.query(
                    f"SELECT src FROM okg.graph_edges WHERE {edge_predicate}",
                    edge_params,
                )
            )
        if not src_ids:
            return []
        src_predicate, src_params = _any_predicate(
            "e.src", sorted(src_ids), prefix="card_src",
        )
        sql = _cardinality_violation_sql(
            catalog_version_id, grp_src_predicate=src_predicate,
        )
        return sorted(
            int(r["edge_id"]) for r in session.query(sql, src_params)
        )

    def _eval_node_subtype_scoped(
        self,
        conn: psycopg.Connection[Any],
        gen: int,
        catalog_version_id: int,
        *,
        node_ids: list[str],
        branch_name: str = "default",
    ) -> list[str]:
        """Node-subtype conformance restricted to known-dirty node ids: dirty nodes
        whose subtype is not in the active catalog (an antijoin against the
        catalog's subtype set), the incremental analogue of the full antijoin."""
        from okg.substrate.graph_store import ChronosGraphStore

        if not node_ids:
            return []
        store = ChronosGraphStore.from_connection(conn, ensure_schema=False)
        try:
            session = store.checkout_generation(gen)
        except Exception:  # noqa: BLE001 - pending publish evaluates branch head
            session = store.checkout(branch=branch_name)
        node_predicate, node_params = _any_predicate(
            "n.node_id",
            node_ids,
            prefix="eval_node_id",
        )
        node_subtype_sql = f"""
            SELECT n.node_id
              FROM okg.graph_nodes n
              LEFT JOIN {catalog_subtypes_binding(catalog_version_id)} cs
                ON cs.subtype_name = n.subtype
             WHERE {node_predicate}
               AND cs.subtype_name IS NULL
        """
        return sorted(
            str(r["node_id"])
            for r in session.query(node_subtype_sql, node_params)
        )


__all__ = ["TransformConsistencyGate"]
