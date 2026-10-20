"""Tier-1 effective claims (``C_eff = derived ⊕ corrections``) as a maintained derivation.

PACT ``consistency-claim-model-core``, task.core.decision-consumption: operator
decisions (claim corrections, precedents) are consumed by the publish cycle
through the substrate's maintained-derivation machinery (#444 kit + registry),
with the relational dataflow authored as **transform-IR programs** over the
graph surfaces and the pure library
:func:`okg.substrate.consistency.claim_model.effective.merge_effective_claims`
registered as the :class:`CallableCorrectnessOracle` ground truth. The library
IS the spec; any divergence between the IR encoding and the library is a bug
in the encoding. HONESTY NOTE: the parity oracle (``parity_check``) runs at
TEST time only today — no production publish path invokes it. Production
correctness rests on the IR encoding itself plus the validation pre-pass
below; the oracle is the differential-test harness, not a runtime guard.

Representation note (design decision D2 + the task's D2 note)
-------------------------------------------------------------
The graph-derivation design (designs/graph-derivation-model.md, D2) represents
tier-1 state as *status attrs on claims*. The #444 kit writes exactly one kept
side table per derivation and has **no attrs-on-nodes write path**, so this
kept table — ``okg.claim_effective``, pk ``(row_kind, row_key)`` — IS the
tier-1 status-attr surface: one ``effective_claim`` row per claim in C_eff
(row_key = the claim's node_id) carrying the effective content, provenance
(the library's ``derived`` / ``corrected`` / ``operator_asserted`` tokens),
the applied correction, and the ``stale_correction_review`` flag; one
``expired_correction`` row per correction naming an absent/vanished claim
(row_key = the correction node's node_id — its graph identity; the
deployment-supplied ``correction_id`` attr is provenance, not the key); and
one ``precedent`` row per claim_precedent node (row-presence pass-through for
tier 3 — precedents are READ and surfaced here, their ≻-semantics land with
task.core.projection-derivation).

IR-vs-Python split (requirement: the relational dataflow is transform-IR)
-------------------------------------------------------------------------
ALL relational dataflow — the latest-correction-per-target selection (the
LEFT-JOIN greater-sequence **anti-join**, the twiki reduction; per-field so
overrides naming different fields accumulate exactly like the library's
sequential ``replace``), action dispatch (``case_`` exprs), reset (retract /
assert) interaction, expiry classification, and the expiry *reason* — is
authored in the six ``TransformProgram``\\ s below, lowered via
``lower_query`` and executed against the generation-pinned Chronos checkout
session. The residual Python is deliberately **row-local** (never a join,
filter, or aggregation):

* ``observed_content`` JSON comparison for the stale flag — the snapshot is a
  JSON *string* attr whose field-wise comparison against the claim's current
  attrs is not IR-expressible (no JSON-parse-of-text expr);
* choosing the single ``applied_correction_id`` when several per-field
  overrides accumulated on one claim — a max over at most three columns of
  the row itself;
* mapping program output rows onto kept-table column dicts.

Surface bindings (the ``ir_check.registries()`` pattern): the programs read
``okg.graph_nodes`` through *registered surfaces* whose bindings are
column-renaming subquery projections (the ``narrowing_allowed_binding``
precedent). The rename-only subqueries exist because the reference
interpreter's JOIN merges rows by bare column name, so self-join arms need
disjoint column names; every condition and projection beyond
rename/extraction lives in the IR and participates in the program checksums.

Stale-correction flag (operator retraction decision 2026-07-06, spec §2)
------------------------------------------------------------------------
A correction whose target claim's CURRENT derived content (the claim node's
``predicate`` / ``normalized_key`` / ``value`` / ``scope`` attrs) equals its
``observed_content`` snapshot stays applied and its effective row carries
``stale_correction_review = true`` — never silently permanent, never silently
retracted. Only the per-field WINNING overrides are consulted (spec §2's
"stays applied but is flagged" reads winners-only): a superseded override —
one whose field was later overridden again — no longer applies, so its
snapshot cannot flag the row. Asserted rows never carry the flag (no derived
target exists). ``scope_state`` is deliberately EXCLUDED from the comparison:
the snapshot schema records the four content coordinates (predicate /
normalized_key / value / scope); scope_state is derivation metadata, not
observed content.

Expiry (spec: "a vanished claim expires its decisions")
-------------------------------------------------------
A correction naming an absent claim is recorded as an ``expired_correction``
row, never silently applied. The reason discriminator is checkout-computable:

* ``vanished_target`` — the correction follows a retract of its target (the
  target vanished from the effective set), OR it snapshots observed content
  (review F1: the snapshot is the witness that a target was observed at
  decision time and has since vanished);
* ``target_never_existed`` — no retract precedes it and no observed-content
  snapshot exists (the correction never observed a target).

Malformed decisions fail LOUD (validation pre-pass + GATING failure mode)
--------------------------------------------------------------------------
The library fails LOUD on malformed corrections (unknown action, OVERRIDE
changing no field, ASSERT of a present claim / missing coordinates,
half-specified scope, missing ``sequence`` / ``target_claim_id``). The IR
programs themselves cannot raise, so the derivation runs
:func:`validate_correction_nodes` — a pre-pass over the correction rows read
from the checkout that mirrors the library's raise conditions exactly and
raises :class:`SubstrateError` NAMING the offending correction node ids
(missing ``sequence`` included — same SubstrateError, never a TypeError). The
pre-pass is backstopped by running ``merge_effective_claims`` itself: any
library raise the mirror missed is re-raised as SubstrateError. Combined with
the row's ``FailureMode.GATING`` (a tier-1 correctness surface must never be
silently wrong: ``maintain_one_row`` re-raises instead of swallowing), a
malformed decision BLOCKS the publish loudly — the no-silent-fallback
posture.

Incremental maintenance (bucket-scoped, spec §9.8)
--------------------------------------------------
First publish → full recompute (kit dispatch, cause ``first_publish``).
Afterwards the incremental callback classifies this cycle's
``changed_node_ids`` into their (predicate, normalized_key) BUCKET reach and
recomputes ONLY those buckets' ``effective_claim`` rows — the projection
tier's mechanics, mirrored (``claim_projection_derivation``). A changed
``consistency_claim`` dirties its raw + prior-effective bucket (the moved-key
case); a changed ``claim_correction`` maps to its target claim's bucket (and
its own coordinate bucket for the ASSERT-resurrection / absent-target case).
Untouched buckets are carried forward by Chronos interval semantics; deletes
are restricted to the dirty buckets. The ``expired_correction`` and
``precedent`` rows are keyed by correction / precedent node_id (NOT by a
bucket), so they are re-derived in full every scoped cycle; a
``claim_precedent`` change therefore dirties zero effective buckets but marks
the cycle non-empty so the pass-through precedent rows update.

Batch is a RECORDED fallback, never the steady state: a changed id absent at
the checkout (a deletion — how a vanished claim is detected, which also
re-expires corrections targeting it) falls back to a full recompute carrying
the recorded cause ``deletion_shape`` (surfaced as ``summary['full_reason']``);
the kit dispatch records ``forced`` / ``scope_unavailable`` /
``first_publish`` / ``periodic_self_heal``. A full recompute with no recorded
cause is a defect. A cycle touching none of the three subtypes is a no-op.
``changed_edge_ids`` are ignored: tier 1 joins on the ``target_claim_id``
attr (review H1), not on edges — hence ``DeclaredDeps(kinds={"graph_node"})``.
"""
from __future__ import annotations

import json
import logging
from typing import Any, Callable, Iterable, Mapping

from okg.substrate.consistency.claim_model.effective import (
    _ACTION_ORDER,
    _ACTIONS,
    ACTION_ASSERT,
    ACTION_OVERRIDE,
    ACTION_RETRACT,
    PROV_ASSERTED,
    PROV_CORRECTED,
    PROV_DERIVED,
    Correction,
    EffectiveClaimError,
    merge_effective_claims,
)
from okg.substrate.consistency.claim_model.views import (
    CLAIM_SUBTYPE,
    ClaimView,
    ElementRef,
    claim_semantics_from_attr,
    element_ref_from_attr,
)
from okg.substrate.errors import SubstrateError
from okg.substrate.transform import (
    Binding,
    Block,
    Effect,
    FunctionRegistry,
    Op,
    OpKind,
    Registries,
    Surface,
    SurfaceKind,
    Terminator,
    TerminatorKind,
    TransformProgram,
    and_,
    chronos_surface_registry,
    col,
    eq,
    func,
    gt,
    in_,
    is_null,
    lit,
    lower_query,
    not_,
    or_,
)
from okg.substrate.transform.expr import case_, concat, eq_any
from okg.substrate.transform.operators import to_chronos_sql

logger = logging.getLogger(__name__)

DERIVATION_ID = "claim_effective"
CLAIM_EFFECTIVE_TABLE = "okg.claim_effective"

# CLAIM_SUBTYPE is imported from claim_model.views (its home is the pure
# claim-model layer; see the cycle note there) and re-exported here for the
# derivation-side importers.
CORRECTION_SUBTYPE = "claim_correction"
PRECEDENT_SUBTYPE = "claim_precedent"

ROW_KIND_EFFECTIVE = "effective_claim"
ROW_KIND_EXPIRED = "expired_correction"
ROW_KIND_PRECEDENT = "precedent"

REASON_VANISHED = "vanished_target"
REASON_NEVER_EXISTED = "target_never_existed"

# Bucket-scoped incremental maintenance (spec §9.8, mirroring tier-2/3's
# claim_projection_derivation). A claim-plane bucket is a
# (predicate, normalized_key) pair; an effective_claim row belongs to the
# bucket its EFFECTIVE (post-override) coordinates name. The dirty-bucket
# token concatenates the two with a separator that never appears in a key.
BUCKET_SEP = "\x1f"
_DIRTY_BUCKETS_PARAM = "dirty_buckets"
# A dirty-bucket token that matches NO real (predicate, key) bucket — a real
# token carries exactly one separator, this one carries two. Used when an
# incremental cycle must re-derive the NON-bucketed rows (expired_correction /
# precedent, always full) while recomputing zero effective_claim buckets (a
# precedent-only or retract-of-absent-target cycle), so the scoped SQL always
# binds a non-empty ANY() array. Not a silent fallback: dirty_bucket_count stays
# the real count (0), the recompute is loud and complete for the row kinds it owns.
_NO_BUCKET_TOKEN = f"{BUCKET_SEP}{BUCKET_SEP}"

KEY_COLUMNS = ("row_kind", "row_key")
VALUE_COLUMNS = (
    "predicate",
    "normalized_key",
    "value",
    "scope",
    "scope_state",
    "claim_semantics",
    "source_id",
    "provenance",
    "applied_correction_id",
    "stale_correction_review",
    "reason",
)
CADENCE_COLUMN = "item_cadence"

CREATE_TABLE_SQL = f"""
CREATE TABLE IF NOT EXISTS {CLAIM_EFFECTIVE_TABLE} (
  row_kind                text NOT NULL,
  row_key                 text NOT NULL,
  predicate               text,
  normalized_key          text,
  value                   text,
  scope                   text,
  scope_state             text,
  claim_semantics         text,
  source_id               text,
  provenance              text,
  applied_correction_id   text,
  stale_correction_review boolean,
  reason                  text,
  item_cadence            bigint,
  PRIMARY KEY (row_kind, row_key)
)
"""

UPGRADE_TABLE_SQL = f"""
ALTER TABLE {CLAIM_EFFECTIVE_TABLE}
  ADD COLUMN IF NOT EXISTS claim_semantics text
"""

# The correction-arm prefixes registered as surfaces. Each self-join arm of a
# program needs its own prefix so the interpreter's bare-column-name join
# cannot collide (see module docstring).
_CORRECTION_PREFIXES = (
    "a", "b", "d", "e", "f", "g", "h", "i", "j", "k"
)
# node_id projections are pinned to COLLATE "C" (byte order) so the SQL
# executor's equal-(sequence, action) node_id tiebreak (`_later`) agrees with
# the Python-ordinal comparison the oracle and the reference interpreter use.
# Under a non-C database default collation (en_US/ICU), text `>` would order
# 'corr:B' AFTER 'corr:a' and silently pick a different winner. The bindings
# are plain SQL strings substituted verbatim into the lowered query
# (lower_query -> to_chronos_sql only rewrites bind-parameter style), so the
# COLLATE survives the round-trip; UTF-8 byte order equals code-point order,
# matching Python.
_CORRECTION_FIELDS = (
    ("node_id", 'node_id COLLATE "C"', None),
    ("target", "attrs->>'target_claim_id'", None),
    ("action", "attrs->>'action'", None),
    ("sequence", "(attrs->>'sequence')::bigint", None),
    ("value", "attrs->>'value'", None),
    ("normalized_key", "attrs->>'normalized_key'", None),
    ("scope", "attrs->>'scope'", None),
    ("scope_state", "attrs->>'scope_state'", None),
    ("semantics", "attrs->>'claim_semantics'", None),
    ("predicate", "attrs->>'predicate'", None),
    ("source_id", "attrs->>'source_id'", None),
    ("observed", "attrs->>'observed_content'", None),
)


# --------------------------------------------------------------------------- #
# Surface bindings — column-renaming projections only (no logic; see docstring) #
# --------------------------------------------------------------------------- #
def _claims_binding() -> str:
    return (
        '(SELECT node_id COLLATE "C" AS c_claim_id, '
        "attrs->>'predicate' AS c_predicate, "
        "attrs->>'normalized_key' AS c_normalized_key, "
        "attrs->>'value' AS c_value, "
        "attrs->>'scope' AS c_scope, "
        "attrs->>'scope_state' AS c_scope_state, "
        "attrs->>'claim_semantics' AS c_semantics, "
        "attrs->>'source_id' AS c_source_id "
        f"FROM okg.graph_nodes WHERE subtype = '{CLAIM_SUBTYPE}')"
    )


def _corrections_binding(prefix: str) -> str:
    cols = ", ".join(
        f"{sql} AS {prefix}_{name}" for name, sql, _ in _CORRECTION_FIELDS
    )
    return (
        f"(SELECT {cols} FROM okg.graph_nodes "
        f"WHERE subtype = '{CORRECTION_SUBTYPE}')"
    )


def _precedents_binding() -> str:
    return (
        '(SELECT node_id COLLATE "C" AS p_node_id, '
        "attrs->>'precedent_id' AS p_precedent_id, "
        "attrs->>'prefer_source' AS p_prefer_source, "
        "attrs->>'scope' AS p_scope, "
        "(attrs->>'sequence')::bigint AS p_sequence "
        f"FROM okg.graph_nodes WHERE subtype = '{PRECEDENT_SUBTYPE}')"
    )


def registries() -> Registries:
    """Chronos surface registry + the tier-1 claim-model surfaces."""
    registry = chronos_surface_registry()
    registry.register(Surface("cons_claims", SurfaceKind.LIVE, _claims_binding()))
    for prefix in _CORRECTION_PREFIXES:
        registry.register(
            Surface(
                f"cons_corr_{prefix}",
                SurfaceKind.LIVE,
                _corrections_binding(prefix),
            )
        )
    registry.register(
        Surface("cons_precedents", SurfaceKind.LIVE, _precedents_binding())
    )
    return Registries(surfaces=registry, functions=FunctionRegistry())


# --------------------------------------------------------------------------- #
# Interpreter-side surface rows (the executable denotation of the bindings).  #
# Unit tests feed these to ``transform.evaluate``; they MUST mirror the       #
# binding projections above field-for-field.                                  #
# --------------------------------------------------------------------------- #
def claims_surface_rows(nodes: Iterable[Mapping[str, Any]]) -> list[dict[str, Any]]:
    out = []
    for n in nodes:
        attrs = dict(n.get("attrs") or {})
        out.append({
            "c_claim_id": str(n["node_id"]),
            "c_predicate": attrs.get("predicate"),
            "c_normalized_key": attrs.get("normalized_key"),
            "c_value": attrs.get("value"),
            "c_scope": attrs.get("scope"),
            "c_scope_state": attrs.get("scope_state"),
            "c_semantics": attrs.get("claim_semantics"),
            "c_source_id": attrs.get("source_id"),
        })
    return out


def corrections_surface_rows(
    prefix: str, nodes: Iterable[Mapping[str, Any]]
) -> list[dict[str, Any]]:
    out = []
    for n in nodes:
        attrs = dict(n.get("attrs") or {})
        seq = attrs.get("sequence")
        out.append({
            f"{prefix}_node_id": str(n["node_id"]),
            f"{prefix}_target": attrs.get("target_claim_id"),
            f"{prefix}_action": attrs.get("action"),
            f"{prefix}_sequence": int(seq) if seq is not None else None,
            f"{prefix}_value": attrs.get("value"),
            f"{prefix}_normalized_key": attrs.get("normalized_key"),
            f"{prefix}_scope": attrs.get("scope"),
            f"{prefix}_scope_state": attrs.get("scope_state"),
            f"{prefix}_semantics": attrs.get("claim_semantics"),
            f"{prefix}_predicate": attrs.get("predicate"),
            f"{prefix}_source_id": attrs.get("source_id"),
            f"{prefix}_observed": attrs.get("observed_content"),
        })
    return out


def precedents_surface_rows(
    nodes: Iterable[Mapping[str, Any]]
) -> list[dict[str, Any]]:
    out = []
    for n in nodes:
        attrs = dict(n.get("attrs") or {})
        seq = attrs.get("sequence")
        out.append({
            "p_node_id": str(n["node_id"]),
            "p_precedent_id": attrs.get("precedent_id"),
            "p_prefer_source": attrs.get("prefer_source"),
            "p_scope": attrs.get("scope"),
            "p_sequence": int(seq) if seq is not None else None,
        })
    return out


def interpreter_surfaces(
    claim_nodes: Iterable[Mapping[str, Any]],
    correction_nodes: Iterable[Mapping[str, Any]],
    precedent_nodes: Iterable[Mapping[str, Any]] = (),
) -> dict[str, list[dict[str, Any]]]:
    """In-memory surfaces for ``transform.evaluate`` mirroring ``registries()``."""
    correction_nodes = list(correction_nodes)
    surfaces: dict[str, list[dict[str, Any]]] = {
        "cons_claims": claims_surface_rows(claim_nodes),
        "cons_precedents": precedents_surface_rows(precedent_nodes),
    }
    for prefix in _CORRECTION_PREFIXES:
        surfaces[f"cons_corr_{prefix}"] = corrections_surface_rows(
            prefix, correction_nodes
        )
    return surfaces


# --------------------------------------------------------------------------- #
# IR expression helpers                                                       #
# --------------------------------------------------------------------------- #
def _action_ord(prefix: str) -> dict:
    """The library's equal-sequence action order (_ACTION_ORDER) as a case_ expr."""
    action = col(f"{prefix}_action")
    return case_(
        [
            (eq(action, lit(ACTION_RETRACT)), lit(_ACTION_ORDER[ACTION_RETRACT])),
            (eq(action, lit(ACTION_OVERRIDE)), lit(_ACTION_ORDER[ACTION_OVERRIDE])),
            (eq(action, lit(ACTION_ASSERT)), lit(_ACTION_ORDER[ACTION_ASSERT])),
        ],
        else_=lit(99),
    )


def _later(px: str, py: str) -> dict:
    """Total decision order: (sequence, action order, node_id) — px after py."""
    sx, sy = col(f"{px}_sequence"), col(f"{py}_sequence")
    ox, oy = _action_ord(px), _action_ord(py)
    return or_(
        gt(sx, sy),
        and_(eq(sx, sy), gt(ox, oy)),
        and_(
            eq(sx, sy),
            eq(ox, oy),
            gt(col(f"{px}_node_id"), col(f"{py}_node_id")),
        ),
    )


def _override_with_field(prefix: str, target: dict, field: str) -> dict:
    return and_(
        eq(col(f"{prefix}_target"), target),
        eq(col(f"{prefix}_action"), lit(ACTION_OVERRIDE)),
        not_(is_null(col(f"{prefix}_{field}"))),
    )


def _reset_arm(prefix: str, target: dict) -> dict:
    return and_(
        eq(col(f"{prefix}_target"), target),
        in_(col(f"{prefix}_action"), [ACTION_RETRACT, ACTION_ASSERT]),
    )


def bucket_token(predicate: str, normalized_key: str) -> str:
    """The dirty-bucket token the scoped programs' eq_any filters match on
    (mirrors claim_projection_derivation.bucket_token)."""
    return f"{predicate}{BUCKET_SEP}{normalized_key}"


def _scoped_bucket_filter(predicate: dict, effective_key: dict) -> dict:
    """Filter output rows to the dirty (predicate, EFFECTIVE-key) buckets — the
    projection tier's ``_scoped_filter`` idiom, but on the effective key (post
    correction-override) so a key override that MOVES a claim's bucket lands its
    row in the new bucket, never leaks a row outside the dirty set."""
    return eq_any(
        concat(predicate, lit(BUCKET_SEP), effective_key), _DIRTY_BUCKETS_PARAM
    )


def _entry(*bindings: Binding, returns: tuple[str, ...]) -> Block:
    return Block(
        block_id="entry",
        bindings=tuple(bindings),
        terminator=Terminator(TerminatorKind.RETURN, {"values": list(returns)}),
    )


def _from(sym: str, surface: str) -> Binding:
    return Binding(
        Op(OpKind.FROM, {"surface": surface, "alias": sym}), output=sym
    )


def _corr_arm(sym: str) -> Binding:
    return _from(sym, f"cons_corr_{sym}")


def _ljoin(out: str, left: str, right: str, on: dict) -> Binding:
    return Binding(
        Op(OpKind.JOIN, {"how": "left", "on": on}), output=out, inputs=(left, right)
    )


def _ijoin(out: str, left: str, right: str, on: dict) -> Binding:
    return Binding(
        Op(OpKind.JOIN, {"how": "inner", "on": on}), output=out, inputs=(left, right)
    )


def _program(program_id: str, *bindings: Binding) -> TransformProgram:
    return TransformProgram(
        program_id=program_id,
        effect=Effect.READ_ONLY,
        outputs=("rows",),
        entry_block="entry",
        attrs={"derivation": DERIVATION_ID},
        blocks=(_entry(*bindings, returns=("rows",)),),
    )


# --------------------------------------------------------------------------- #
# The six tier-1 programs                                                     #
# --------------------------------------------------------------------------- #
def derived_effective_program(scoped: bool = False) -> TransformProgram:
    """Effective rows whose base is a DERIVED claim (no retract/assert on it).

    Per field family (value / normalized_key / scope) the latest override is
    selected by the greater-sequence anti-join; families are independent, so
    overrides naming different fields accumulate — matching the library's
    sequential per-field ``replace`` semantics. Claims with ANY reset
    (retract/assert) correction are excluded here (a retract terminates the
    claim; an assert base is handled by :func:`asserted_effective_program`).

    ``scoped`` adds the dirty-bucket eq_any filter on the EFFECTIVE
    (predicate, coalesce(override_key, raw_key)) coordinates so an incremental
    cycle recomputes only the dirty buckets (spec §9.8).
    """
    target = col("c_claim_id")
    eff_key = func("coalesce", col("d_normalized_key"), col("c_normalized_key"))
    kept_expr = [
        is_null(col("b_node_id")),
        is_null(col("e_node_id")),
        is_null(col("g_node_id")),
        is_null(col("i_node_id")),
        is_null(col("j_node_id")),
    ]
    if scoped:
        kept_expr.append(_scoped_bucket_filter(col("c_predicate"), eff_key))
    return _program(
        f"claim_effective.derived{'.scoped' if scoped else ''}",
        _from("c", "cons_claims"),
        _corr_arm("a"),
        _ljoin("j1", "c", "a", _override_with_field("a", target, "value")),
        _corr_arm("b"),
        _ljoin("j2", "j1", "b", and_(
            _override_with_field("b", target, "value"), _later("b", "a"))),
        _corr_arm("d"),
        _ljoin("j3", "j2", "d", _override_with_field("d", target, "normalized_key")),
        _corr_arm("e"),
        _ljoin("j4", "j3", "e", and_(
            _override_with_field("e", target, "normalized_key"), _later("e", "d"))),
        _corr_arm("f"),
        _ljoin("j5", "j4", "f", _override_with_field("f", target, "scope")),
        _corr_arm("g"),
        _ljoin("j6", "j5", "g", and_(
            _override_with_field("g", target, "scope"), _later("g", "f"))),
        _corr_arm("h"),
        _ljoin("j7", "j6", "h", _override_with_field("h", target, "semantics")),
        _corr_arm("i"),
        _ljoin("j8", "j7", "i", and_(
            _override_with_field("i", target, "semantics"), _later("i", "h"))),
        _corr_arm("j"),
        _ljoin("j9", "j8", "j", _reset_arm("j", target)),
        Binding(Op(OpKind.WHERE, {"expr": and_(*kept_expr)}),
                output="kept", inputs=("j9",)),
        Binding(Op(OpKind.SELECT, {"project": {
            "claim_id": col("c_claim_id"),
            "predicate": col("c_predicate"),
            "value": func("coalesce", col("a_value"), col("c_value")),
            "normalized_key": func(
                "coalesce", col("d_normalized_key"), col("c_normalized_key")),
            "scope": case_(
                [(not_(is_null(col("f_node_id"))), col("f_scope"))],
                else_=func("coalesce", col("c_scope"), lit("")),
            ),
            "scope_state": case_(
                [(not_(is_null(col("f_node_id"))), col("f_scope_state"))],
                else_=func("coalesce", col("c_scope_state"), lit("unknown")),
            ),
            "claim_semantics": case_(
                [(not_(is_null(col("h_node_id"))), col("h_semantics"))],
                else_=col("c_semantics"),
            ),
            "source_id": col("c_source_id"),
            "corrected": or_(
                not_(is_null(col("a_node_id"))),
                not_(is_null(col("d_node_id"))),
                not_(is_null(col("f_node_id"))),
                not_(is_null(col("h_node_id"))),
            ),
            "cur_normalized_key": col("c_normalized_key"),
            "cur_value": col("c_value"),
            "cur_scope": func("coalesce", col("c_scope"), lit("")),
            "cur_semantics": col("c_semantics"),
            "ov_value_id": col("a_node_id"),
            "ov_value_seq": col("a_sequence"),
            "ov_value_observed": col("a_observed"),
            "ov_key_id": col("d_node_id"),
            "ov_key_seq": col("d_sequence"),
            "ov_key_observed": col("d_observed"),
            "ov_scope_id": col("f_node_id"),
            "ov_scope_seq": col("f_sequence"),
            "ov_scope_observed": col("f_observed"),
            "ov_semantics_id": col("h_node_id"),
            "ov_semantics_seq": col("h_sequence"),
            "ov_semantics_observed": col("h_observed"),
        }}), output="projected", inputs=("kept",)),
        Binding(Op(OpKind.ORDER, {"expr": col("claim_id")}),
                output="rows", inputs=("projected",)),
    )


def asserted_effective_program(scoped: bool = False) -> TransformProgram:
    """Effective rows whose base is an operator ASSERT (the last reset on the id).

    ``a`` is an assert correction with no later reset (anti-join ``b``);
    overrides later than the assert overlay per field, latest-per-field via
    the same anti-join idiom. Matches the library: an assert (after a retract,
    or on a never-derived id) resurrects the id as operator-authored, with
    ``scope or ''`` / ``scope_state or 'unknown'`` base defaults.

    ``scoped`` adds the dirty-bucket eq_any filter on the EFFECTIVE
    (predicate, coalesce(override_key, assert_key)) coordinates (spec §9.8).
    """
    target = col("a_target")
    eff_key = func("coalesce", col("f_normalized_key"), col("a_normalized_key"))
    kept_expr = [
        eq(col("a_action"), lit(ACTION_ASSERT)),
        is_null(col("b_node_id")),
        is_null(col("e_node_id")),
        is_null(col("g_node_id")),
        is_null(col("i_node_id")),
        is_null(col("k_node_id")),
    ]
    if scoped:
        kept_expr.append(_scoped_bucket_filter(col("a_predicate"), eff_key))
    return _program(
        f"claim_effective.asserted{'.scoped' if scoped else ''}",
        _corr_arm("a"),
        _corr_arm("b"),
        _ljoin("j1", "a", "b", and_(_reset_arm("b", target), _later("b", "a"))),
        _corr_arm("d"),
        _ljoin("j2", "j1", "d", and_(
            _override_with_field("d", target, "value"), _later("d", "a"))),
        _corr_arm("e"),
        _ljoin("j3", "j2", "e", and_(
            _override_with_field("e", target, "value"), _later("e", "d"))),
        _corr_arm("f"),
        _ljoin("j4", "j3", "f", and_(
            _override_with_field("f", target, "normalized_key"), _later("f", "a"))),
        _corr_arm("g"),
        _ljoin("j5", "j4", "g", and_(
            _override_with_field("g", target, "normalized_key"), _later("g", "f"))),
        _corr_arm("h"),
        _ljoin("j6", "j5", "h", and_(
            _override_with_field("h", target, "scope"), _later("h", "a"))),
        _corr_arm("i"),
        _ljoin("j7", "j6", "i", and_(
            _override_with_field("i", target, "scope"), _later("i", "h"))),
        _corr_arm("j"),
        _ljoin("j8", "j7", "j", and_(
            _override_with_field("j", target, "semantics"), _later("j", "a"))),
        _corr_arm("k"),
        _ljoin("j9", "j8", "k", and_(
            _override_with_field("k", target, "semantics"), _later("k", "j"))),
        Binding(Op(OpKind.WHERE, {"expr": and_(*kept_expr)}),
                output="kept", inputs=("j9",)),
        Binding(Op(OpKind.SELECT, {"project": {
            "claim_id": col("a_target"),
            "predicate": col("a_predicate"),
            "value": func("coalesce", col("d_value"), col("a_value")),
            "normalized_key": func(
                "coalesce", col("f_normalized_key"), col("a_normalized_key")),
            "scope": case_(
                [(not_(is_null(col("h_node_id"))), col("h_scope"))],
                else_=func("coalesce", col("a_scope"), lit("")),
            ),
            "scope_state": case_(
                [(not_(is_null(col("h_node_id"))), col("h_scope_state"))],
                else_=func("coalesce", col("a_scope_state"), lit("unknown")),
            ),
            "claim_semantics": case_(
                [(not_(is_null(col("j_node_id"))), col("j_semantics"))],
                else_=col("a_semantics"),
            ),
            "source_id": col("a_source_id"),
            "assert_id": col("a_node_id"),
            "assert_seq": col("a_sequence"),
            "ov_value_id": col("d_node_id"),
            "ov_value_seq": col("d_sequence"),
            "ov_key_id": col("f_node_id"),
            "ov_key_seq": col("f_sequence"),
            "ov_scope_id": col("h_node_id"),
            "ov_scope_seq": col("h_sequence"),
            "ov_semantics_id": col("j_node_id"),
            "ov_semantics_seq": col("j_sequence"),
        }}), output="projected", inputs=("kept",)),
        Binding(Op(OpKind.ORDER, {"expr": col("claim_id")}),
                output="rows", inputs=("projected",)),
    )


def _observed_reason() -> dict:
    """Reason for expiry with no preceding retract: the observed-content
    snapshot is the witness that a target was observed at decision time."""
    return case_(
        [(not_(is_null(col("a_observed"))), lit(REASON_VANISHED))],
        else_=lit(REASON_NEVER_EXISTED),
    )


def expired_retract_program() -> TransformProgram:
    """RETRACTs naming a claim that does not exist and was never asserted."""
    target = col("a_target")
    return _program(
        "claim_effective.expired_retract",
        _corr_arm("a"),
        _from("c", "cons_claims"),
        _ljoin("j1", "a", "c", eq(col("c_claim_id"), target)),
        _corr_arm("b"),
        _ljoin("j2", "j1", "b", and_(
            eq(col("b_target"), target),
            eq(col("b_action"), lit(ACTION_ASSERT)),
            _later("a", "b"),
        )),
        Binding(Op(OpKind.WHERE, {"expr": and_(
            eq(col("a_action"), lit(ACTION_RETRACT)),
            is_null(col("c_claim_id")),
            is_null(col("b_node_id")),
        )}), output="expired", inputs=("j2",)),
        Binding(Op(OpKind.SELECT, {"project": {
            "correction_id": col("a_node_id"),
            "target_claim_id": col("a_target"),
            "reason": _observed_reason(),
        }}), output="projected", inputs=("expired",)),
        Binding(Op(OpKind.ORDER, {"expr": col("correction_id")}),
                output="rows", inputs=("projected",)),
    )


def expired_override_orphan_program() -> TransformProgram:
    """OVERRIDEs naming a claim that does not exist, with no reset before them."""
    target = col("a_target")
    return _program(
        "claim_effective.expired_override_orphan",
        _corr_arm("a"),
        _from("c", "cons_claims"),
        _ljoin("j1", "a", "c", eq(col("c_claim_id"), target)),
        _corr_arm("b"),
        _ljoin("j2", "j1", "b", and_(_reset_arm("b", target), _later("a", "b"))),
        Binding(Op(OpKind.WHERE, {"expr": and_(
            eq(col("a_action"), lit(ACTION_OVERRIDE)),
            is_null(col("c_claim_id")),
            is_null(col("b_node_id")),
        )}), output="expired", inputs=("j2",)),
        Binding(Op(OpKind.SELECT, {"project": {
            "correction_id": col("a_node_id"),
            "target_claim_id": col("a_target"),
            "reason": _observed_reason(),
        }}), output="projected", inputs=("expired",)),
        Binding(Op(OpKind.ORDER, {"expr": col("correction_id")}),
                output="rows", inputs=("projected",)),
    )


def expired_override_after_retract_program() -> TransformProgram:
    """OVERRIDEs whose last preceding reset is a RETRACT — the target vanished
    from the effective set; the override is expired, never applied."""
    target = col("a_target")
    return _program(
        "claim_effective.expired_override_after_retract",
        _corr_arm("a"),
        _corr_arm("b"),
        _ijoin("j1", "a", "b", and_(
            eq(col("b_target"), target),
            eq(col("b_action"), lit(ACTION_RETRACT)),
            _later("a", "b"),
        )),
        _corr_arm("d"),
        _ljoin("j2", "j1", "d", and_(
            _reset_arm("d", target), _later("a", "d"), _later("d", "b"))),
        Binding(Op(OpKind.WHERE, {"expr": and_(
            eq(col("a_action"), lit(ACTION_OVERRIDE)),
            is_null(col("d_node_id")),
        )}), output="expired", inputs=("j2",)),
        Binding(Op(OpKind.SELECT, {"project": {
            "correction_id": col("a_node_id"),
            "target_claim_id": col("a_target"),
            "reason": lit(REASON_VANISHED),
        }}), output="projected", inputs=("expired",)),
        Binding(Op(OpKind.ORDER, {"expr": col("correction_id")}),
                output="rows", inputs=("projected",)),
    )


def precedents_program() -> TransformProgram:
    """Precedent row-presence pass-through (≻ consumption is tier 3 work)."""
    return _program(
        "claim_effective.precedents",
        _from("p", "cons_precedents"),
        Binding(Op(OpKind.SELECT, {"project": {
            "node_id": col("p_node_id"),
            "prefer_source": col("p_prefer_source"),
            "scope": col("p_scope"),
        }}), output="projected", inputs=("p",)),
        Binding(Op(OpKind.ORDER, {"expr": col("node_id")}),
                output="rows", inputs=("projected",)),
    )


def tier1_programs(scoped: bool = False) -> tuple[TransformProgram, ...]:
    """The six tier-1 programs. ``scoped`` selects the dirty-bucket variants of
    the two effective_claim programs (derived / asserted); the expired-correction
    and precedent programs are NEVER bucket-scoped — their rows are keyed by
    correction / precedent node_id, not by a (predicate, key) bucket, so they are
    re-derived in full on every incremental cycle (they are operator-decision
    rows, few in number). The bucket-scoped win is on the effective_claim rows
    (the many derived claims), which is where §9.8 locality matters."""
    return (
        derived_effective_program(scoped),
        asserted_effective_program(scoped),
        expired_retract_program(),
        expired_override_orphan_program(),
        expired_override_after_retract_program(),
        precedents_program(),
    )


# --------------------------------------------------------------------------- #
# Row-local Python: observed-content comparison + kept-row assembly           #
# --------------------------------------------------------------------------- #
def _as_text(value: Any) -> str | None:
    """Normalize a snapshot/attr value to the text rendering postgres
    ``attrs->>`` yields (numbers/booleans as their JSON text; strings as-is;
    None stays None) so a JSON-number snapshot still matches the text the SQL
    path produced for the same content."""
    if value is None or isinstance(value, str):
        return value
    return json.dumps(value)


def observed_matches_current(
    observed: str | None,
    *,
    predicate: Any,
    normalized_key: Any,
    value: Any,
    scope: Any,
    claim_semantics: Any = None,
) -> bool:
    """Field-wise comparison of an ``observed_content`` JSON snapshot against a
    claim's CURRENT derived content (spec §2, the stale-flag comparison).

    Both sides are normalized to TEXT before comparing: the postgres executor
    reads claim content through ``attrs->>`` (always text), while the snapshot
    (and the interpreter path) may carry raw JSON scalars — a JSON-number
    snapshot must still match. ``scope_state`` is deliberately EXCLUDED: the
    snapshot schema records the four observed content coordinates only
    (predicate / normalized_key / value / scope); scope_state is derivation
    metadata, not observed content.

    A present-but-unparseable snapshot fails LOUD — a silent no-match would
    quietly suppress the operator-review flag.
    """
    if observed is None:
        return False
    try:
        parsed = json.loads(observed)
    except (TypeError, ValueError) as exc:
        raise SubstrateError(
            "claim_effective: unparseable observed_content snapshot",
            data={"observed_content": str(observed)[:200]},
        ) from exc
    if not isinstance(parsed, Mapping):
        raise SubstrateError(
            "claim_effective: observed_content must be a JSON object",
            data={"observed_content": str(observed)[:200]},
        )
    return (
        _as_text(parsed.get("predicate")) == _as_text(predicate)
        and _as_text(parsed.get("normalized_key")) == _as_text(normalized_key)
        and _as_text(parsed.get("value")) == _as_text(value)
        and (_as_text(parsed.get("scope")) or "") == (_as_text(scope) or "")
        and _canonical_semantics(parsed.get("claim_semantics"))
        == _canonical_semantics(claim_semantics)
    )


def _canonical_semantics(value: Any) -> str | None:
    """Return canonical persisted text for a typed semantic payload."""
    if value is None:
        return None
    try:
        parsed = claim_semantics_from_attr(value)
    except (TypeError, ValueError) as exc:
        raise SubstrateError(
            "claim_effective: malformed claim_semantics",
            data={"claim_semantics": str(value)[:200]},
        ) from exc
    return parsed.to_attr() if parsed is not None else None


def _empty_value_row() -> dict[str, Any]:
    return {column: None for column in VALUE_COLUMNS}


def _latest_candidate(
    candidates: list[tuple[int, str]]
) -> str | None:
    """The (sequence, node_id)-max correction id among the row's own applied
    corrections — row-local (never a join; at most four candidates)."""
    if not candidates:
        return None
    return max(candidates)[1]


def assemble_rows(
    derived_rows: Iterable[Mapping[str, Any]],
    asserted_rows: Iterable[Mapping[str, Any]],
    expired_rows: Iterable[Mapping[str, Any]],
    precedent_rows: Iterable[Mapping[str, Any]],
) -> dict[tuple[str, str], dict[str, Any]]:
    """Map the programs' output rows onto kept-table rows keyed by (kind, key).

    Row-local only: applied_correction_id (max over the row's own override
    columns), the stale flag (observed-content JSON comparison), provenance
    token mapping. Duplicate keys fail loud.
    """
    out: dict[tuple[str, str], dict[str, Any]] = {}

    def _put(kind: str, key: str, row: dict[str, Any]) -> None:
        if key is None or key == "":
            raise SubstrateError(
                f"claim_effective: {kind} row with empty row_key", data={"kind": kind}
            )
        k = (kind, str(key))
        if k in out:
            raise SubstrateError(
                "claim_effective: duplicate kept-table key",
                data={"row_kind": kind, "row_key": str(key)},
            )
        out[k] = row

    for r in derived_rows:
        families = (
            (r.get("ov_value_seq"), r.get("ov_value_id"), r.get("ov_value_observed")),
            (r.get("ov_key_seq"), r.get("ov_key_id"), r.get("ov_key_observed")),
            (r.get("ov_scope_seq"), r.get("ov_scope_id"), r.get("ov_scope_observed")),
            (
                r.get("ov_semantics_seq"),
                r.get("ov_semantics_id"),
                r.get("ov_semantics_observed"),
            ),
        )
        applied: dict[str, tuple[int, str, Any]] = {}
        for seq, nid, observed in families:
            if nid is not None:
                applied[str(nid)] = (int(seq), str(nid), observed)
        corrected = bool(r.get("corrected"))
        if corrected != bool(applied):
            raise SubstrateError(
                "claim_effective: corrected flag / applied-override mismatch",
                data={"claim_id": r.get("claim_id")},
            )
        stale = any(
            observed_matches_current(
                observed,
                predicate=r.get("predicate"),
                normalized_key=r.get("cur_normalized_key"),
                value=r.get("cur_value"),
                scope=r.get("cur_scope"),
                claim_semantics=r.get("cur_semantics"),
            )
            for (_seq, _nid, observed) in applied.values()
        )
        row = _empty_value_row()
        row.update({
            "predicate": r.get("predicate"),
            "normalized_key": r.get("normalized_key"),
            "value": r.get("value"),
            "scope": r.get("scope"),
            "scope_state": r.get("scope_state"),
            "claim_semantics": _canonical_semantics(r.get("claim_semantics")),
            "source_id": r.get("source_id"),
            "provenance": PROV_CORRECTED if applied else PROV_DERIVED,
            "applied_correction_id": _latest_candidate(
                [(seq, nid) for (seq, nid, _o) in applied.values()]
            ),
            "stale_correction_review": stale,
        })
        _put(ROW_KIND_EFFECTIVE, r["claim_id"], row)

    for r in asserted_rows:
        candidates = [(int(r["assert_seq"]), str(r["assert_id"]))]
        for seq_key, id_key in (
            ("ov_value_seq", "ov_value_id"),
            ("ov_key_seq", "ov_key_id"),
            ("ov_scope_seq", "ov_scope_id"),
            ("ov_semantics_seq", "ov_semantics_id"),
        ):
            if r.get(id_key) is not None:
                candidates.append((int(r[seq_key]), str(r[id_key])))
        row = _empty_value_row()
        row.update({
            "predicate": r.get("predicate"),
            "normalized_key": r.get("normalized_key"),
            "value": r.get("value"),
            "scope": r.get("scope"),
            "scope_state": r.get("scope_state"),
            "claim_semantics": _canonical_semantics(r.get("claim_semantics")),
            "source_id": r.get("source_id"),
            "provenance": PROV_ASSERTED,
            "applied_correction_id": _latest_candidate(candidates),
            # No derived target exists behind an asserted claim, so the
            # stale comparison has no CURRENT derived content to run against.
            "stale_correction_review": False,
        })
        _put(ROW_KIND_EFFECTIVE, r["claim_id"], row)

    for r in expired_rows:
        row = _empty_value_row()
        row["reason"] = r["reason"]
        _put(ROW_KIND_EXPIRED, r["correction_id"], row)

    for r in precedent_rows:
        row = _empty_value_row()
        row.update({"value": r.get("prefer_source"), "scope": r.get("scope")})
        _put(ROW_KIND_PRECEDENT, r["node_id"], row)

    return out


# --------------------------------------------------------------------------- #
# Validation pre-pass: malformed corrections fail LOUD before any IR runs     #
# --------------------------------------------------------------------------- #
def validate_correction_nodes(
    claim_nodes: Iterable[Mapping[str, Any]],
    correction_nodes: Iterable[Mapping[str, Any]],
) -> None:
    """Reject malformed correction rows LOUDLY, naming the correction node ids.

    Mirrors the library's raise conditions exactly (:mod:`...effective`):
    missing ``target_claim_id`` / ``sequence``; unknown action; an APPLIED
    OVERRIDE with a half-specified scope pair or changing no field; an ASSERT
    of an already-present claim, with missing coordinates, or with a
    half-specified scope pair. Context-dependent rules replay presence the way
    the library does — an OVERRIDE naming an absent target is EXPIRED, never
    validated (the library only validates a correction it applies).

    Backstop: after the mirror passes, ``merge_effective_claims`` itself runs;
    an :class:`EffectiveClaimError` the mirror missed is re-raised as
    :class:`SubstrateError` — the derivation can never silently apply a shape
    the library rejects. With the row's ``FailureMode.GATING`` this blocks the
    publish.
    """
    claim_nodes = list(claim_nodes)
    correction_nodes = list(correction_nodes)

    def _raise(message: str, node_id: str, **data: Any) -> None:
        raise SubstrateError(
            f"claim_effective: malformed correction {node_id!r}: {message}",
            data={"node_id": node_id, **data},
        )

    ordered: list[tuple[int, int, str, str, str, Mapping[str, Any]]] = []
    for n in correction_nodes:
        attrs = n["attrs"]
        nid = str(n["node_id"])
        target = attrs.get("target_claim_id")
        if not target:
            _raise("correction without target_claim_id", nid)
        sequence = attrs.get("sequence")
        if sequence is None:
            _raise("correction without sequence", nid)
        action = str(attrs.get("action"))
        ordered.append(
            (int(sequence), _ACTION_ORDER.get(action, 99), nid,
             str(target), action, attrs)
        )
    # The oracle replay's total decision order (sequence, action, node_id);
    # per-target relative order matches the library's sort.
    ordered.sort(key=lambda t: t[:3])

    present: dict[str, bool] = {str(n["node_id"]): True for n in claim_nodes}
    for _seq, _ord, nid, target, action, attrs in ordered:
        if action not in _ACTIONS:
            _raise(f"unknown action {action!r}", nid, action=action)
        is_present = present.get(target, False)
        half_scope = (attrs.get("scope") is None) != (attrs.get("scope_state") is None)
        if action == ACTION_RETRACT:
            present[target] = False
            continue
        if action == ACTION_OVERRIDE:
            if not is_present:
                continue  # expired, never applied -> the library never validates it
            if half_scope:
                _raise("scope and scope_state must be set together", nid)
            if all(
                attrs.get(field) is None
                for field in ("value", "normalized_key", "scope", "claim_semantics")
            ):
                _raise("OVERRIDE changes no field", nid)
            continue
        # ASSERT
        if is_present:
            _raise(
                "ASSERT of an already-present claim "
                "(use OVERRIDE to change a derived claim)", nid,
            )
        missing = [
            field for field in ("source_id", "predicate", "normalized_key", "value")
            if attrs.get(field) is None
        ]
        if missing:
            _raise(f"ASSERT missing coordinates {missing}", nid, missing=missing)
        if half_scope:
            _raise("scope and scope_state must be set together", nid)
        present[target] = True

    # Backstop: the library IS the spec — run it.
    try:
        merge_effective_claims(
            [_claim_view(n) for n in claim_nodes],
            sorted(
                (_correction(n) for n in correction_nodes),
                key=lambda c: (
                    c.sequence, _ACTION_ORDER.get(c.action, 99), c.decision_id
                ),
            ),
        )
    except EffectiveClaimError as exc:
        raise SubstrateError(
            f"claim_effective: malformed correction (library): {exc}",
        ) from exc


# --------------------------------------------------------------------------- #
# Executors: lowered SQL against a Chronos session / the interpreter          #
# --------------------------------------------------------------------------- #
def compute_rows(
    session: Any,
    *,
    dirty_buckets: frozenset[str] | set[str] | None = None,
    run_program: Callable[[TransformProgram], list[dict[str, Any]]] | None = None,
) -> dict[tuple[str, str], dict[str, Any]]:
    """Derive the kept rows at ``session`` by lowering + executing the tier-1
    IR programs through the (generation-pinned) Chronos checkout session.

    When ``session`` is a real checkout (the production path), the malformed-
    correction validation pre-pass runs FIRST over the raw correction rows;
    the interpreted path validates in :func:`compute_rows_interpreted`.

    ``dirty_buckets`` (bucket tokens) selects the SCOPED effective_claim program
    variants: only those (predicate, key) buckets' effective_claim rows are
    derived; the caller carries every other effective_claim row forward. The
    expired_correction / precedent rows are ALWAYS derived in full (they are not
    bucket-keyed — tier1_programs docstring). Mirrors
    claim_projection_derivation.compute_rows."""
    if session is not None:
        validate_correction_nodes(
            _read_nodes(session, CLAIM_SUBTYPE),
            _read_nodes(session, CORRECTION_SUBTYPE),
        )
    scoped = dirty_buckets is not None
    regs = registries()
    extra_params = (
        {_DIRTY_BUCKETS_PARAM: sorted(dirty_buckets)} if scoped else {}
    )

    def _run(program: TransformProgram) -> list[dict[str, Any]]:
        sql, params = lower_query(program, regs)
        params.update(extra_params)
        return [dict(r) for r in session.query(to_chronos_sql(sql), params)]

    run = run_program or _run
    derived, asserted, exp_r, exp_o, exp_ar, precedents = (
        run(p) for p in tier1_programs(scoped)
    )
    return assemble_rows(derived, asserted, [*exp_r, *exp_o, *exp_ar], precedents)


def compute_rows_interpreted(
    claim_nodes: Iterable[Mapping[str, Any]],
    correction_nodes: Iterable[Mapping[str, Any]],
    precedent_nodes: Iterable[Mapping[str, Any]] = (),
    *,
    dirty_buckets: frozenset[str] | set[str] | None = None,
) -> dict[tuple[str, str], dict[str, Any]]:
    """The SAME programs + assembly, executed by the reference interpreter over
    in-memory surfaces (unit-test denotation; no DB). Runs the same
    malformed-correction validation pre-pass as the production path.

    ``dirty_buckets`` selects the scoped effective_claim variants (as
    :func:`compute_rows`), the bucket token param threaded into ``evaluate``."""
    from okg.substrate.transform import evaluate

    claim_nodes = list(claim_nodes)
    correction_nodes = list(correction_nodes)
    validate_correction_nodes(claim_nodes, correction_nodes)
    surfaces = interpreter_surfaces(claim_nodes, correction_nodes, precedent_nodes)
    params = (
        {_DIRTY_BUCKETS_PARAM: sorted(dirty_buckets)}
        if dirty_buckets is not None else {}
    )

    def _run(program: TransformProgram) -> list[dict[str, Any]]:
        return list(
            evaluate(program, surfaces, kernels={}, params=params).values["rows"]
        )

    return compute_rows(None, dirty_buckets=dirty_buckets, run_program=_run)


def rows_as_truth(
    rows: Mapping[tuple[str, str], Mapping[str, Any]]
) -> dict[tuple[str, str], tuple[Any, ...]]:
    """Flatten assembled rows to the oracle's {key tuple -> value tuple} shape."""
    return {
        key: tuple(row[column] for column in VALUE_COLUMNS)
        for key, row in rows.items()
    }


# --------------------------------------------------------------------------- #
# The library-as-oracle: read raw nodes, run merge_effective_claims, flatten. #
# --------------------------------------------------------------------------- #
_NODES_BY_SUBTYPE_SQL = (
    "SELECT node_id, attrs FROM okg.graph_nodes WHERE subtype = :s ORDER BY node_id"
)


def _read_nodes(checkout: Any, subtype: str) -> list[dict[str, Any]]:
    return [
        {"node_id": str(r["node_id"]), "attrs": dict(r.get("attrs") or {})}
        for r in checkout.query(_NODES_BY_SUBTYPE_SQL, {"s": subtype})
    ]


def _parse_element_ref(payload: Any, node_id: str) -> ElementRef | None:
    """Parse the optional ``element_ref`` attr into an :class:`ElementRef`.

    ``None`` for a corpus-level claim; a malformed payload fails LOUD as
    :class:`SubstrateError` naming the claim node (the derivation's GATING
    failure mode) — never silently dropped to ``None``. Resolving the
    reference is a later task; this only threads the honest shape through."""
    try:
        return element_ref_from_attr(payload)
    except (ValueError, TypeError) as exc:
        raise SubstrateError(
            f"claim_effective: malformed element_ref on claim {node_id!r}: {exc}",
            data={"node_id": node_id},
        ) from exc


def _claim_view(node: Mapping[str, Any]) -> ClaimView:
    attrs = node["attrs"]
    node_id = str(node["node_id"])
    try:
        semantics = claim_semantics_from_attr(attrs.get("claim_semantics"))
        return ClaimView(
            node_id=node_id,
            source_id=attrs.get("source_id"),
            predicate=attrs.get("predicate"),
            normalized_key=attrs.get("normalized_key"),
            value=attrs.get("value"),
            scope=attrs.get("scope") or "",
            scope_state=attrs.get("scope_state") or "unknown",
            element_ref=_parse_element_ref(attrs.get("element_ref"), node_id),
            semantics=semantics,
        )
    except (TypeError, ValueError) as exc:
        raise SubstrateError(
            f"claim_effective: malformed claim_semantics on claim {node_id!r}: {exc}",
            data={"node_id": node_id},
        ) from exc


def _correction(node: Mapping[str, Any]) -> Correction:
    attrs = node["attrs"]
    target = attrs.get("target_claim_id")
    if not target:
        raise SubstrateError(
            "claim_effective oracle: correction without target_claim_id",
            data={"node_id": node["node_id"]},
        )
    sequence = attrs.get("sequence")
    if sequence is None:
        raise SubstrateError(
            "claim_effective oracle: correction without sequence",
            data={"node_id": node["node_id"]},
        )
    try:
        semantics = claim_semantics_from_attr(attrs.get("claim_semantics"))
    except (TypeError, ValueError) as exc:
        raise SubstrateError(
            "claim_effective oracle: malformed correction claim_semantics "
            f"on {node['node_id']!r}: {exc}",
            data={"node_id": node["node_id"]},
        ) from exc
    return Correction(
        claim_id=str(target),
        action=str(attrs.get("action")),
        sequence=int(sequence),
        decision_id=str(node["node_id"]),
        value=attrs.get("value"),
        normalized_key=attrs.get("normalized_key"),
        # Raw pass-through: a half-specified scope pair is malformed and the
        # library raises LOUD when the correction applies (never repaired here).
        scope=attrs.get("scope"),
        scope_state=attrs.get("scope_state"),
        source_id=attrs.get("source_id"),
        predicate=attrs.get("predicate"),
        semantics=semantics,
    )


def library_truth_rows(
    claim_nodes: Iterable[Mapping[str, Any]],
    correction_nodes: Iterable[Mapping[str, Any]],
    precedent_nodes: Iterable[Mapping[str, Any]] = (),
) -> dict[tuple[str, str], tuple[Any, ...]]:
    """Ground truth from the pure library: ``merge_effective_claims`` decides
    the effective set / provenance / retraction / expiry, and a sequential
    replay (validated against the merge result — any disagreement raises)
    refines it with the per-correction bookkeeping the kept table needs:
    WHICH correction applied last, WHICH corrections expired and why, and the
    stale flag. The replay never overrides the library — it only refines
    library outputs to per-correction grain, and the cross-checks pin it to
    the library on every oracle run.

    Stale flag is WINNERS-ONLY (spec §2): only the per-field winning
    overrides' ``observed_content`` snapshots are compared against the
    claim's current derived content — a superseded override no longer
    applies, so its snapshot cannot flag the row. This matches the IR path,
    which carries observed snapshots for the per-field anti-join winners
    only.
    """
    claim_nodes = list(claim_nodes)
    correction_nodes = list(correction_nodes)
    claims_by_id = {str(n["node_id"]): n["attrs"] for n in claim_nodes}
    views = [_claim_view(n) for n in claim_nodes]
    corrections = [_correction(n) for n in correction_nodes]
    observed_by_decision = {
        str(n["node_id"]): (n["attrs"].get("observed_content"))
        for n in correction_nodes
    }
    ordered = sorted(
        corrections,
        key=lambda c: (c.sequence, _ACTION_ORDER.get(c.action, 99), c.decision_id),
    )

    merge = merge_effective_claims(views, ordered)

    # Sequential replay for per-correction bookkeeping (see docstring).
    present: dict[str, bool] = {v.node_id: True for v in views}
    ever: set[str] = set(present)
    asserted_origin: set[str] = set()
    # Per claim, per field family, the WINNING override's (seq, id, observed)
    # — later applied overrides on the same field replace earlier ones, so
    # only winners' snapshots feed the stale flag (see docstring).
    winning_overrides: dict[str, dict[str, tuple[int, str, Any]]] = {}
    last_applied: dict[str, str] = {}
    last_reset_corr: dict[str, str] = {}
    expired: dict[str, str] = {}  # correction node_id -> reason
    corrected_cids: set[str] = set()
    asserted_cids: set[str] = set()

    for c in ordered:
        cid = c.claim_id
        observed = observed_by_decision[c.decision_id]
        if c.action == ACTION_RETRACT:
            if present.get(cid):
                present[cid] = False
                winning_overrides[cid] = {}
                last_applied.pop(cid, None)
            elif cid not in ever:
                expired[c.decision_id] = (
                    REASON_VANISHED if observed is not None else REASON_NEVER_EXISTED
                )
            last_reset_corr[cid] = ACTION_RETRACT
            continue
        if c.action == ACTION_OVERRIDE:
            if present.get(cid):
                winners = winning_overrides.setdefault(cid, {})
                for field in ("value", "normalized_key", "scope", "semantics"):
                    if getattr(c, field) is not None:
                        winners[field] = (c.sequence, c.decision_id, observed)
                last_applied[cid] = c.decision_id
                # Mirrors the library's `touched` set: an applied override
                # marks the id corrected-bookkeeping-wise even when the base
                # is asserted (provenance stays operator_asserted).
                corrected_cids.add(cid)
            elif last_reset_corr.get(cid) == ACTION_RETRACT:
                expired[c.decision_id] = REASON_VANISHED
            else:
                expired[c.decision_id] = (
                    REASON_VANISHED if observed is not None else REASON_NEVER_EXISTED
                )
            continue
        # ASSERT (merge_effective_claims already raised on a present target)
        present[cid] = True
        ever.add(cid)
        asserted_origin.add(cid)
        asserted_cids.add(cid)
        winning_overrides[cid] = {}
        last_applied[cid] = c.decision_id
        last_reset_corr[cid] = ACTION_ASSERT

    # The replay is a REFINEMENT of the library result — pin it, loudly.
    replay_expired_targets = {
        _correction_target(correction_nodes, nid) for nid in expired
    }
    if replay_expired_targets != set(merge.expired):
        raise SubstrateError(
            "claim_effective oracle: replay expired set diverged from the library",
            data={"replay": sorted(replay_expired_targets),
                  "library": sorted(merge.expired)},
        )
    if {c for c in corrected_cids if present.get(c)} != set(merge.corrected):
        raise SubstrateError(
            "claim_effective oracle: replay corrected set diverged from the library",
            data={"library": sorted(merge.corrected)},
        )
    if {c for c in asserted_cids if present.get(c)} != set(merge.asserted):
        raise SubstrateError(
            "claim_effective oracle: replay asserted set diverged from the library",
            data={"library": sorted(merge.asserted)},
        )

    out: dict[tuple[str, str], tuple[Any, ...]] = {}
    for cv in merge.effective:
        cid = cv.node_id
        provenance = merge.provenance[cid]
        applied_id = last_applied.get(cid) if provenance != PROV_DERIVED else None
        stale = False
        if provenance == PROV_CORRECTED:
            attrs = claims_by_id[cid]
            stale = any(
                observed_matches_current(
                    observed,
                    predicate=attrs.get("predicate"),
                    normalized_key=attrs.get("normalized_key"),
                    value=attrs.get("value"),
                    scope=attrs.get("scope") or "",
                    claim_semantics=attrs.get("claim_semantics"),
                )
                for (_seq, _nid, observed) in winning_overrides.get(cid, {}).values()
            )
        out[(ROW_KIND_EFFECTIVE, cid)] = (
            cv.predicate, cv.normalized_key, cv.value, cv.scope, cv.scope_state,
            cv.semantics.to_attr() if cv.semantics is not None else None,
            cv.source_id, provenance, applied_id, stale, None,
        )
    for nid, reason in expired.items():
        out[(ROW_KIND_EXPIRED, nid)] = (
            None, None, None, None, None, None, None, None, None, None, reason,
        )
    for n in precedent_nodes:
        attrs = n["attrs"]
        out[(ROW_KIND_PRECEDENT, str(n["node_id"]))] = (
            None, None, attrs.get("prefer_source"), attrs.get("scope"),
            None, None, None, None, None, None, None,
        )
    return out


def _correction_target(
    correction_nodes: list[Mapping[str, Any]], node_id: str
) -> str:
    for n in correction_nodes:
        if str(n["node_id"]) == node_id:
            return str(n["attrs"].get("target_claim_id"))
    raise SubstrateError(
        "claim_effective oracle: unknown correction node id",
        data={"node_id": node_id},
    )


def claim_effective_truth(checkout: Any) -> dict[tuple[str, str], tuple[Any, ...]]:
    """The CallableCorrectnessOracle recompute: raw node reads + the library."""
    return library_truth_rows(
        _read_nodes(checkout, CLAIM_SUBTYPE),
        _read_nodes(checkout, CORRECTION_SUBTYPE),
        _read_nodes(checkout, PRECEDENT_SUBTYPE),
    )


# --------------------------------------------------------------------------- #
# Registry row (#444 kit), following the inline _row() shape in               #
# tests/substrate/test_deployment_maintained_derivation_consumers.py          #
# --------------------------------------------------------------------------- #
def claim_effective_derivation_row() -> Any:
    from okg.substrate.derived_artifacts.derivation_registry import (
        DeclaredDeps,
        DerivationRow,
        DeterminismClass,
        ExecutionPhase,
        FailureMode,
        GovernanceProfile,
        Materialization,
        PublishCycleDeltas,
    )
    from okg.substrate.derived_artifacts.maintained_derivation import (
        FULL_REASON_DELETION_SHAPE,
        FULL_REASON_SCOPE_UNAVAILABLE,
        CallableCorrectnessOracle,
        DerivationSpec,
        ForceInputs,
        GenericChronosKeptTable,
        MaintainContext,
        MaintainResult,
        SentinelSpec,
        read_cadence_counter,
    )
    from okg.substrate.derived_artifacts.maintained_derivation_constants import (
        SENTINEL_TOKEN,
    )

    sentinel = SentinelSpec(pk_columns=list(KEY_COLUMNS), cadence_column=CADENCE_COLUMN)
    oracle = CallableCorrectnessOracle(
        claim_effective_truth,
        key_columns=list(KEY_COLUMNS),
        value_columns=list(VALUE_COLUMNS),
        label=DERIVATION_ID,
    )

    def _existing_rows(checkout: Any) -> dict[tuple[str, str], dict[str, Any]]:
        """Existing kept rows (sans sentinel) with the columns the incremental
        delete-restriction needs: an effective_claim row's (predicate,
        normalized_key) IS its bucket."""
        rows = checkout.query(
            f"SELECT row_kind, row_key, predicate, normalized_key "
            f"FROM {CLAIM_EFFECTIVE_TABLE} "
            f"WHERE NOT (row_kind = :s AND row_key = :s)",
            {"s": SENTINEL_TOKEN},
        )
        return {
            (str(r["row_kind"]), str(r["row_key"])): dict(r) for r in rows
        }

    def _row_bucket_token(kind: str, row: Mapping[str, Any]) -> str | None:
        """The bucket an EXISTING kept row belongs to; only effective_claim rows
        are bucketed (expired_correction / precedent rows are not)."""
        if kind != ROW_KIND_EFFECTIVE:
            return None
        predicate, key = row.get("predicate"), row.get("normalized_key")
        if predicate is None or key is None:
            return None
        return bucket_token(str(predicate), str(key))

    def _upserts(
        rows: Mapping[tuple[str, str], Mapping[str, Any]]
    ) -> list[dict[str, Any]]:
        return [
            {"row_kind": k[0], "row_key": k[1], **rows[k]}
            for k in sorted(rows)
        ]

    def _full(ctx: MaintainContext) -> MaintainResult:
        """Kit-dispatched full recompute (first publish / forced / self-heal).
        The KIT records the cause via ``mode.reason`` → ``summary['full_reason']``;
        no callback ``full_reason`` needed here."""
        rows = compute_rows(ctx.checkout)
        existing = _existing_rows(ctx.checkout)
        deletes = [
            {"row_kind": k[0], "row_key": k[1]}
            for k in sorted(existing) if k not in rows
        ]
        return MaintainResult(
            upserts=_upserts(rows), deletes=deletes,
            cadence_counter=0, mode="full_recount", dirty_bucket_count=None,
        )

    def _full_under_incremental(ctx: MaintainContext, cause: str) -> MaintainResult:
        """A full recompute the incremental callback fell back to (mode stays
        ``incremental``); ``cause`` is recorded (spec §9.8 recorded-cause
        fallback, surfaced as ``summary['full_reason']``). Batch is a recorded
        fallback, never the steady state."""
        logger.info(
            "claim_effective: full recompute under incremental dispatch "
            "(cause=%s)", cause,
        )
        rows = compute_rows(ctx.checkout)
        existing = _existing_rows(ctx.checkout)
        deletes = [
            {"row_kind": k[0], "row_key": k[1]}
            for k in sorted(existing) if k not in rows
        ]
        return MaintainResult(
            upserts=_upserts(rows), deletes=deletes,
            cadence_counter=_next_cadence(ctx), mode="incremental",
            dirty_bucket_count=None, full_reason=cause,
        )

    def _noop(ctx: MaintainContext) -> MaintainResult:
        # A no-write cycle carries prior rows forward (Chronos interval
        # semantics); only the sentinel is rewritten.
        return MaintainResult(
            upserts=[], deletes=[], cadence_counter=_next_cadence(ctx),
            dirty_bucket_count=0, mode="incremental",
        )

    def _next_cadence(ctx: MaintainContext) -> int:
        elapsed = read_cadence_counter(
            ctx.checkout, table=CLAIM_EFFECTIVE_TABLE, spec=sentinel
        )
        return (elapsed or 0) + 1

    def _classify_changes(
        checkout: Any,
        changed: tuple[str, ...],
        existing: Mapping[tuple[str, str], Mapping[str, Any]],
    ) -> tuple[str | None, set[str], bool]:
        """Classify this cycle's changed nodes into their (predicate, key) bucket
        reach. Returns ``(full_cause | None, dirty_bucket_tokens, touched_nonbucket)``.

        * A changed node ABSENT at the checkout is a DELETION we cannot subtype
          (and a vanished claim re-expires corrections targeting it) → recorded
          FULL cause ``deletion_shape``.
        * A changed ``consistency_claim`` dirties its RAW bucket and its PRIOR
          effective bucket (the moved-key case).
        * A changed ``claim_correction`` maps to its TARGET claim's bucket (raw +
          prior effective + any override-key bucket); a correction whose target
          is absent still dirties the bucket its own coordinates name (the ASSERT
          resurrection case). ``touched_nonbucket`` is set so the (unscoped)
          expired-correction rows are re-derived.
        * A changed ``claim_precedent`` sets ``touched_nonbucket`` (precedent rows
          are pass-through, keyed by node_id, not bucketed — their tier-1 bucket
          reach is empty; they are re-derived in full)."""
        node_rows = checkout.query(
            "SELECT node_id, subtype, attrs FROM okg.graph_nodes "
            "WHERE node_id = ANY(:ids)",
            {"ids": [str(n) for n in changed]},
        )
        by_id = {str(r["node_id"]): r for r in node_rows}
        prior_effective_bucket: dict[str, str] = {}
        for (kind, key), row in existing.items():
            if kind == ROW_KIND_EFFECTIVE:
                tok = _row_bucket_token(kind, row)
                if tok is not None:
                    prior_effective_bucket[key] = tok

        # Batch-read the raw claim node of every correction target (targets are
        # rarely in `changed` themselves).
        target_ids = {
            str(dict(by_id[str(n)].get("attrs") or {}).get("target_claim_id"))
            for n in changed
            if str(n) in by_id
            and str(by_id[str(n)]["subtype"]) == CORRECTION_SUBTYPE
            and dict(by_id[str(n)].get("attrs") or {}).get("target_claim_id")
        }
        target_claim: dict[str, dict[str, Any]] = {}
        if target_ids:
            for r in checkout.query(
                "SELECT node_id, attrs FROM okg.graph_nodes "
                "WHERE subtype = :s AND node_id = ANY(:ids)",
                {"s": CLAIM_SUBTYPE, "ids": sorted(target_ids)},
            ):
                target_claim[str(r["node_id"])] = dict(r.get("attrs") or {})

        tokens: set[str] = set()
        touched_nonbucket = False
        for node_id in changed:
            row = by_id.get(str(node_id))
            if row is None:
                return FULL_REASON_DELETION_SHAPE, set(), False
            subtype = str(row["subtype"])
            attrs = dict(row.get("attrs") or {})
            if subtype == CLAIM_SUBTYPE:
                pred, key = attrs.get("predicate"), attrs.get("normalized_key")
                if pred is not None and key is not None:
                    tokens.add(bucket_token(str(pred), str(key)))
                prior = prior_effective_bucket.get(str(node_id))
                if prior is not None:
                    tokens.add(prior)
            elif subtype == CORRECTION_SUBTYPE:
                touched_nonbucket = True
                target = attrs.get("target_claim_id")
                if target:
                    target = str(target)
                    prior = prior_effective_bucket.get(target)
                    if prior is not None:
                        tokens.add(prior)
                    t_attrs = target_claim.get(target)
                    if t_attrs is not None:
                        t_pred = t_attrs.get("predicate")
                        t_key = t_attrs.get("normalized_key")
                        if t_pred is not None and t_key is not None:
                            tokens.add(bucket_token(str(t_pred), str(t_key)))
                        # A key OVERRIDE moves the target to a new bucket.
                        ov_key = attrs.get("normalized_key")
                        if t_pred is not None and ov_key is not None:
                            tokens.add(bucket_token(str(t_pred), str(ov_key)))
                # A correction's OWN coordinates name a bucket (ASSERT
                # resurrection, or an override targeting an absent claim).
                c_pred, c_key = attrs.get("predicate"), attrs.get("normalized_key")
                if c_pred is not None and c_key is not None:
                    tokens.add(bucket_token(str(c_pred), str(c_key)))
            elif subtype == PRECEDENT_SUBTYPE:
                touched_nonbucket = True
            # other subtypes are not tier-1 inputs: no bucket, no non-bucket work
        return None, tokens, touched_nonbucket

    def _incremental(ctx: MaintainContext) -> MaintainResult:
        cycle = ctx.change_inputs
        scope_unavailable = (
            cycle is None
            or getattr(cycle, "dirty_scope", None) is None
            or getattr(cycle, "changed_node_ids", None) is None
        )
        if scope_unavailable:
            # The kit routes force_full / scope_available=False to _full; this
            # is the residual case (changed_node_ids is None but dirty_scope is
            # present) — no scope to work from, recompute in full, recorded.
            return _full_under_incremental(ctx, FULL_REASON_SCOPE_UNAVAILABLE)
        changed = tuple(getattr(cycle, "changed_node_ids", None) or ())
        if not changed:
            return _noop(ctx)

        existing = _existing_rows(ctx.checkout)
        cause, tokens, touched_nonbucket = _classify_changes(
            ctx.checkout, changed, existing
        )
        if cause is not None:
            return _full_under_incremental(ctx, cause)
        if not tokens and not touched_nonbucket:
            # Changed nodes exist but none touch claims/corrections/precedents.
            return _noop(ctx)

        # Bucket-scoped recompute. The effective_claim rows are recomputed for
        # exactly `tokens`; the expired_correction / precedent rows (not
        # bucketed) are re-derived in full every scoped cycle. When only
        # non-bucketed rows changed (precedent-only / retract-of-absent), a
        # never-matching sentinel keeps the scoped SQL's ANY() non-empty while
        # recomputing zero effective buckets.
        dirty = tokens if tokens else {_NO_BUCKET_TOKEN}
        rows = compute_rows(ctx.checkout, dirty_buckets=dirty)
        # Guard: every recomputed effective_claim row lands in a dirty bucket.
        for (kind, key), row in rows.items():
            if kind != ROW_KIND_EFFECTIVE:
                continue
            tok = bucket_token(str(row["predicate"]), str(row["normalized_key"]))
            if tok not in dirty:
                raise SubstrateError(
                    "claim_effective: scoped recompute produced an "
                    "effective_claim row outside the dirty buckets",
                    data={"row_key": key, "bucket": tok},
                )
        deletes: list[dict[str, Any]] = []
        for (kind, key), row in sorted(existing.items()):
            if (kind, key) in rows:
                continue
            if kind == ROW_KIND_EFFECTIVE:
                # effective_claim deletes are restricted to the dirty buckets;
                # rows in untouched buckets are carried forward.
                if _row_bucket_token(kind, row) in dirty:
                    deletes.append({"row_kind": kind, "row_key": key})
            else:
                # expired_correction / precedent rows are re-derived in full,
                # so their deletes reconcile against the full computed set.
                deletes.append({"row_kind": kind, "row_key": key})
        return MaintainResult(
            upserts=_upserts(rows), deletes=deletes,
            cadence_counter=_next_cadence(ctx),
            dirty_bucket_count=len(tokens), mode="incremental",
        )

    def _make_inputs(cycle: "PublishCycleDeltas") -> ForceInputs:
        return ForceInputs(
            change_inputs=cycle,
            force_full=cycle.force_full,
            scope_available=cycle.dirty_scope is not None,
        )

    spec = DerivationSpec(
        derivation_id=DERIVATION_ID,
        sentinel=sentinel,
        oracle=oracle,
        force_full_predicates=[],
        full_recount=_full,
        incremental=_incremental,
        self_heal_every=25,
    )
    return DerivationRow(
        spec=spec,
        declared_deps=DeclaredDeps(kinds=frozenset({"graph_node"})),
        materializations=[
            Materialization(
                kind="side_table",
                storage=GenericChronosKeptTable(
                    table=CLAIM_EFFECTIVE_TABLE,
                    primary_key=list(KEY_COLUMNS),
                    columns=["row_kind", "row_key", *VALUE_COLUMNS, CADENCE_COLUMN],
                    create_table_sql=CREATE_TABLE_SQL,
                    upgrade_table_sql=UPGRADE_TABLE_SQL,
                ),
                sentinel=sentinel,
            )
        ],
        governance=GovernanceProfile(determinism=DeterminismClass.DETERMINISTIC),
        execution_phase=ExecutionPhase.PRE_CHECKPOINT,
        # Tier-1 effective claims are a CORRECTNESS surface, not read
        # acceleration: a failed maintenance (e.g. the malformed-correction
        # validation pre-pass raising) must BLOCK the publish, never leave a
        # silently stale/mis-applied decision table (maintain_one_row
        # re-raises under GATING).
        failure_mode=FailureMode.GATING,
        timing_key=DERIVATION_ID,
        make_cycle_inputs=_make_inputs,
    )


def register_claim_effective_derivation(registry: Any) -> Any:
    """Register the tier-1 derivation on a ``DerivationRegistry``; returns it."""
    registry.register(claim_effective_derivation_row())
    return registry


__all__ = [
    "BUCKET_SEP",
    "CLAIM_EFFECTIVE_TABLE",
    "DERIVATION_ID",
    "KEY_COLUMNS",
    "REASON_NEVER_EXISTED",
    "REASON_VANISHED",
    "ROW_KIND_EFFECTIVE",
    "ROW_KIND_EXPIRED",
    "ROW_KIND_PRECEDENT",
    "VALUE_COLUMNS",
    "assemble_rows",
    "bucket_token",
    "claim_effective_derivation_row",
    "claim_effective_truth",
    "compute_rows",
    "compute_rows_interpreted",
    "interpreter_surfaces",
    "library_truth_rows",
    "observed_matches_current",
    "register_claim_effective_derivation",
    "registries",
    "rows_as_truth",
    "tier1_programs",
    "validate_correction_nodes",
]
