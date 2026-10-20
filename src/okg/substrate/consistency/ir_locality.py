"""Derived locality for IR consistency checks — the frontier from the program, never declared.

PACT ``consistency-checks-as-ir``, task.ir.derived-locality
(req.ir.derived-locality); renamed to the frontier vocabulary by
``consistency-notation-alignment`` (the ratified notation map binds this
module's frontier plan to the canonical spec's detection frontier F(Delta) —
docs/develop/architecture/consistency.md, Section 9 intro). The engine COMPUTES
an IR check's incremental evaluation frontier from the program's read set plus
the publish delta — there is no user-declared frontier, so the
declared-frontier unsoundness (a subject's finding changing without the
subject entering the frontier) is impossible by construction, not merely
lintable.

The analysis is deliberately conservative: it recognizes two sound shapes and
falls back to FULL scope (with a recorded reason) for everything else.

Recognized shapes (single entry block, base ``FROM nodes_live``):

- **subject_local** — no other relations read: a node's result depends only on
  its own row, so the scope is the dirty node ids.
- **node_with_incident_edges** — joins touch only ``edges_live``: a node's
  result can also change when an incident edge changes, so the scope widens to
  the dirty node ids PLUS the endpoints of dirty edges (fetched from the
  current head AND the previous checkpoint — a REMOVED edge's endpoints only
  exist at the previous head, and that removal is exactly what must pull its
  endpoints into scope).

Everything else — node-to-node joins, deployment-registered surfaces,
aggregation (a row's group can be changed by any other row), LIMIT (a top-N
over the scope is not a top-N over the corpus), multiple blocks — runs FULL
with the reason recorded in coverage/preflight.

Scoping is applied by a pure program rewrite: the subject column gets an
``eq_any`` conjunct against one array parameter. The rewrite is an EXECUTION
artifact only — check identity stays the original program's checksum.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Iterable, Literal, Mapping

from okg.substrate.transform import OpKind, TransformProgram
from okg.substrate.transform.expr import and_, eq_any

SCOPE_PARAM = "okg_ir_scope_subject_ids"

# Surfaces whose rows ARE the graph subjects/edges under Chronos execution.
_NODE_SURFACE = "nodes_live"
_EDGE_SURFACE = "edges_live"

# The specific FULL reason recorded when a check reads the claim-state surface
# (effective claims / canonical-contested-superseded projection). The default
# deny already forces FULL for any non-``nodes_live`` base relation; this names
# WHY for the claim plane rather than the generic ``base_surface_not_nodes_live``
# so the fallback is auditable and points at the follow-on that would lift it.
# Bucket-scoped soundness over the claim plane is deliberately NOT invented here
# — it is task.binding.differential of consistency-claim-graph-binding. Until
# that lands, a claim-state check runs FULL, never silently scoped against a
# partial claim plane.
CLAIM_STATE_FULL_REASON = "claim_state_surface_bucket_locality_pending"

FrontierMode = Literal["subject_local", "node_with_incident_edges", "full"]


@dataclass(frozen=True)
class FrontierPlan:
    """The derived detection frontier (locality) of one IR check program."""

    mode: FrontierMode
    reason: str
    subject_expr: dict | None = None   # {"col": "<base_alias>.<column>"}
    base_alias: str | None = None

    @property
    def scoped(self) -> bool:
        return self.mode != "full"


def derive_frontier_plan(
    program: TransformProgram,
    *,
    check_attrs: Mapping[str, Any],
    claim_state_surfaces: frozenset[str] = frozenset(),
) -> FrontierPlan:
    """Analyze the program's read set and classify its locality. Pure.

    ``claim_state_surfaces`` is the set of the check's FROM-surface NAMES whose
    binding reads the claim-state kept tables (``okg.claim_effective`` /
    ``okg.claim_projection``); the executor supplies it from
    :meth:`IrConsistencyCheckSpec.claim_state_surface_names`. Any such surface in
    the read set forces FULL with :data:`CLAIM_STATE_FULL_REASON` (bucket
    locality over the claim plane is task.binding.differential). The default
    empty set leaves the graph-surface classification untouched.
    """
    if len(program.blocks) != 1:
        return FrontierPlan("full", "multi_block_program")

    block = program.blocks[0]
    from_surfaces: list[tuple[str, str]] = []  # (surface, alias)
    has_group = False
    has_limit = False
    where_count = 0
    projection: Mapping[str, Any] | None = None
    for binding in block.bindings:
        op = binding.op
        if op.kind is OpKind.FROM:
            surface = str(op.args.get("surface"))
            alias = str(op.args.get("alias", surface))
            from_surfaces.append((surface, alias))
        elif op.kind is OpKind.GROUP:
            has_group = True
        elif op.kind is OpKind.LIMIT:
            has_limit = True
        elif op.kind is OpKind.WHERE:
            where_count += 1
        elif op.kind is OpKind.SELECT:
            projection = op.args.get("project")
        elif op.kind is OpKind.UNNEST:
            # Row-local: multiplies rows of the SAME subject (lateral array
            # explosion), reads nothing across subjects — locality-transparent.
            continue
        elif op.kind is OpKind.SEARCH:
            return FrontierPlan("full", "search_op_reads_whole_index")
        elif op.kind is OpKind.RANK:
            # A row's rank/basis depends on the OTHER rows of its partition —
            # scoping to dirty subjects would rank against a partial bucket.
            # Bucket-keyed locality is future work (graph-derivation design §4);
            # until then RANK always runs full.
            return FrontierPlan("full", "rank_over_other_rows")
        elif op.kind in (OpKind.JOIN, OpKind.ORDER, OpKind.EMIT):
            # JOIN locality is derived from the FROM surfaces above; ORDER and
            # EMIT do not change which subjects participate.
            continue
        else:
            # Default-deny: an op this analyzer has not been taught is NOT
            # assumed locality-transparent (the silent fall-through here is
            # exactly how an unsound scope would ship with the next op).
            return FrontierPlan("full", f"unrecognized_op:{op.kind.value}")

    if not from_surfaces:
        return FrontierPlan("full", "no_from_surface")
    if claim_state_surfaces and any(
        surface in claim_state_surfaces for surface, _alias in from_surfaces
    ):
        # A read touching the claim-state plane runs FULL with the specific
        # reason — never scoped against a partial claim plane, and never the
        # generic base_surface reason. Checked before the group/limit/base
        # branches so a claim-state check reports the claim reason regardless of
        # its other shape.
        return FrontierPlan("full", CLAIM_STATE_FULL_REASON)
    if has_group:
        return FrontierPlan("full", "aggregation_over_other_rows")
    if has_limit:
        return FrontierPlan("full", "limit_changes_result_set")
    if where_count > 1:
        # The canonical SQL lowering keeps only the LAST WHERE binding; a
        # scope conjunct injected into any other one would be silently
        # DROPPED — the run would execute full while recorded as scoped.
        # Refuse to scope rather than lie about coverage.
        return FrontierPlan("full", "multiple_where_bindings")

    base_surface, base_alias = from_surfaces[0]
    if base_surface != _NODE_SURFACE:
        return FrontierPlan("full", f"base_surface_not_{_NODE_SURFACE}:{base_surface}")

    other = [s for s, _a in from_surfaces[1:]]
    non_edge = sorted({s for s in other if s != _EDGE_SURFACE})
    if non_edge:
        return FrontierPlan("full", f"non_local_join:{','.join(non_edge)}")

    subject_expr, subject_reason = _subject_column(
        check_attrs, projection, base_alias=base_alias
    )
    if subject_expr is None:
        return FrontierPlan("full", subject_reason)

    if other:  # only edges_live joins remain
        return FrontierPlan(
            "node_with_incident_edges",
            "base_nodes+edges_live_joins",
            subject_expr=subject_expr,
            base_alias=base_alias,
        )
    return FrontierPlan(
        "subject_local",
        "single_node_surface_no_joins",
        subject_expr=subject_expr,
        base_alias=base_alias,
    )


def scoped_program(
    program: TransformProgram,
    plan: FrontierPlan,
    *,
    param_name: str = SCOPE_PARAM,
) -> TransformProgram:
    """Rewrite the program to evaluate only the scoped subjects. Pure.

    Conjoins ``subject = ANY(:param)`` into the entry block's WHERE (or adds a
    WHERE if none exists, before the SELECT). Execution artifact only — the
    check's identity remains the ORIGINAL program's checksum.
    """
    if not plan.scoped or plan.subject_expr is None:
        raise ValueError("scoped_program requires a scoped FrontierPlan")
    scope_pred = eq_any(plan.subject_expr, param_name)

    data = program.to_dict()
    block = data["blocks"][0]
    # Conjoin into the LAST WHERE — the canonical lowering keeps only the
    # last one, so any other position would silently drop the scope.
    last_where = None
    for binding in block["bindings"]:
        if binding["op"]["kind"] == OpKind.WHERE.value:
            last_where = binding
    where_found = last_where is not None
    if where_found:
        args = last_where["op"]["args"]
        args["expr"] = and_(args["expr"], scope_pred)
    if not where_found:
        # insert a WHERE over the base symbol just before the SELECT
        base_symbol = None
        insert_at = len(block["bindings"])
        for i, binding in enumerate(block["bindings"]):
            if binding["op"]["kind"] == OpKind.FROM.value and base_symbol is None:
                base_symbol = binding["output"]
            if binding["op"]["kind"] == OpKind.SELECT.value:
                insert_at = i
                break
        block["bindings"].insert(insert_at, {
            "output": "okg_ir_scoped",
            "op": {"kind": OpKind.WHERE.value, "args": {"expr": scope_pred}},
            "inputs": [base_symbol] if base_symbol else [],
        })
    data["params"] = sorted({*data.get("params", ()), param_name})
    return TransformProgram.from_dict(data)


def scope_subjects_from_delta(
    dirty_scope: tuple | None,
    plan: FrontierPlan,
    *,
    session_current: Any,
    session_previous: Any | None,
) -> tuple[set[str] | None, str]:
    """Compute the scoped subject ids from the publish delta, or (None, reason)
    for a full run.

    ``dirty_scope`` is the publisher's chronos dirty scope tuple
    (nodes_by_subtype, edge_ids_by_type, edge_evidence_flag, ...). For the
    incident-edges mode the endpoints of dirty edges are fetched from BOTH the
    current head and the previous checkpoint — a removed edge exists only at
    the previous one, and its endpoints must enter the scope (the case the
    declared-frontier design missed).
    """
    if not plan.scoped:
        return None, plan.reason
    if dirty_scope is None:
        return None, "dirty_scope_unavailable"
    if len(dirty_scope) < 3 or not dirty_scope[2]:
        return None, "dirty_edge_scope_unavailable"

    subjects: set[str] = set()
    for node_ids in (dirty_scope[0] or {}).values():
        subjects.update(str(n) for n in node_ids)

    if plan.mode == "node_with_incident_edges":
        edge_ids = sorted({
            int(e)
            for edge_ids in (dirty_scope[1] or {}).values()
            for e in edge_ids
        })
        if edge_ids:
            if session_previous is None:
                # A removed edge's endpoints are only visible at the previous
                # checkpoint; without it the widened scope would be unsound.
                return None, "previous_checkpoint_unavailable_for_dirty_edges"
            subjects.update(_edge_endpoints(session_current, edge_ids))
            subjects.update(_edge_endpoints(session_previous, edge_ids))
    return subjects, plan.reason


def _edge_endpoints(session: Any, edge_ids: list[int]) -> set[str]:
    rows = session.query(
        "SELECT src, dst FROM okg.graph_edges WHERE edge_id = ANY(:okg_ir_dirty_edge_ids)",
        {"okg_ir_dirty_edge_ids": edge_ids},
    )
    out: set[str] = set()
    for row in rows:
        out.add(str(row["src"]))
        out.add(str(row["dst"]))
    return out


def _subject_column(
    check_attrs: Mapping[str, Any],
    projection: Mapping[str, Any] | None,
    *,
    base_alias: str,
) -> tuple[dict | None, str]:
    """Resolve the subject's SOURCE column expr on the base relation."""
    roles = check_attrs.get("participant_roles")
    if not isinstance(roles, Mapping) or not roles:
        return None, "no_participant_roles"
    subject_role = check_attrs.get("subject_role")
    if subject_role is None:
        if len(roles) == 1:
            subject_role = next(iter(roles))
        else:
            return None, "ambiguous_subject_role"
    column = roles.get(subject_role)
    if not isinstance(column, str):
        return None, f"unknown_subject_role:{subject_role}"
    if not isinstance(projection, Mapping) or column not in projection:
        return None, f"subject_column_not_projected:{column}"
    expr = projection[column]
    if (
        not isinstance(expr, Mapping)
        or set(expr) != {"col"}
        or not str(expr["col"]).startswith(f"{base_alias}.")
    ):
        return None, "subject_not_a_base_relation_column"
    return dict(expr), "ok"


def _iter_str(values: Iterable[Any]) -> list[str]:
    return [str(v) for v in values]


__all__ = [
    "CLAIM_STATE_FULL_REASON",
    "SCOPE_PARAM",
    "FrontierPlan",
    "derive_frontier_plan",
    "scope_subjects_from_delta",
    "scoped_program",
]
