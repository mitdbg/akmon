"""Tiers 2+3 of the claim model — conflict findings + ≻ resolution — as ONE
maintained derivation (``claim_projection``) over the tier-1 output.

PACT ``consistency-claim-model-core``, task.core.projection-derivation: the
canonical/contested projection registers on the #444 kit (registry row +
``CallableCorrectnessOracle`` + GATING failure mode) and materializes the
kept table ``okg.claim_projection`` — per-pair conflict findings (tier 2)
and per-conflicted-bucket ≻ resolutions with the deciding signal class
(tier 3), generation-versioned through Chronos. The pure library
(:func:`...conflicts.derive_conflict_findings` +
:func:`...precedence.resolve_bucket`) IS the spec; the oracle recomputes
through it over the same inputs read via plain SQL, and any divergence from
the IR/RANK encoding is a bug in the encoding. HONESTY NOTE (as tier-1): the
parity oracle runs at TEST time; production correctness rests on the
encoding plus the validation pre-pass below.

Inputs (and the tier-1 dependency)
----------------------------------
Tier 2 reads the EFFECTIVE claims from the tier-1 kept table
``okg.claim_effective`` (``row_kind = 'effective_claim'``) — the kept-table
surface, never the raw claim nodes (a correction must flow through C_eff
before it can resolve a conflict). If the tier-1 sentinel is absent at the
checkout the derivation raises :class:`SubstrateError` LOUDLY — falling back
to graph reads is forbidden. Σ policies (``predicate_policy`` nodes),
≻ precedents (``claim_precedent`` nodes), and the per-claim signal attrs
(below) are read from ``okg.graph_nodes``.

ORDERING MECHANISM (tier-1 before tier-2 in one cycle): the publisher drives
``DerivationRegistry.rows()``, which iterates in SORTED ``derivation_id``
order (derivation_registry.py — "load-bearing: the publish graph_hash ...
depend[s] on a stable maintenance order"), and ``maintain()`` opens a fresh
branch-head checkout per row, so a later row sees an earlier row's kept
writes. ``"claim_effective" < "claim_projection"`` in that order, so tier 1
always maintains first; the ordering is pinned by
``test_registry_orders_tier1_before_tier2`` and proven end-to-end by the
one-cycle chaining test (a correction recorded at generation g re-buckets
AND re-resolves at g+1 through both rows in one ``Publisher.cycle()``).

Σ as graph data (v1 conventions, all validated loudly)
------------------------------------------------------
``predicate_policy`` rows (design review F6 shapes), ``enabled='true'`` only:

* ``functional`` — any two distinct values of the predicate conflict. At
  most one enabled functional row per predicate; a predicate may not carry
  both functional and incompatible_pair policies (the library names ONE
  constraint per predicate via ``constraint_for``).
* ``incompatible_pair`` — the (value_a, value_b) pair conflicts, matched in
  BOTH orientations. All enabled rows for one predicate must share one
  ``policy_id`` (they are the pair rows of one constraint); the shared id is
  the constraint name.
* ``linked_pair`` — cross-predicate policy: a source-predicate claim links
  to target-predicate claims via ``link_key_fn`` and conflicts when the
  target's value is in ``violates_values``. v1 registers exactly ONE link
  kernel, ``normalize_identity`` (the link key IS the source claim's value);
  any other ``link_key_fn`` fails validation. ``violates_values`` is a JSON
  list of values.
* ``unresolved_key`` — the named key cannot be bucketed: multi-member
  buckets on it are NOT-COMPARABLE, never a forced verdict. Matching the
  library (``ConflictRules.unresolved_keys`` is one global set), the
  unresolved set is GLOBAL — ``""`` is always unresolved, the row's
  ``predicate`` attr is advisory provenance in v1.

Disabled rows are SKIPPED, not dropped silently: the validation pre-pass
logs the skipped node ids. Malformed rows (unknown ``policy_kind``,
incompatible_pair missing values, linked_pair missing target/link fields or
an unregistered kernel, group-shape violations) raise SubstrateError NAMING
the node ids; with ``FailureMode.GATING`` a malformed policy BLOCKS the
publish.

Deployment-supplied signal surface (v1 convention)
--------------------------------------------------
Claims carry no core-mandated ≻ signal attrs. v1 convention: a
``consistency_claim`` node's attrs MAY carry the seven signal columns
(``source_class``, ``is_superseded``, ``effective_date``,
``source_authority``, ``independent_support``, ``document_recency``,
``maintenance_rank``), read via ``attrs->>`` with NULL → default
(``""`` / false / 0). A claim without them (including every
operator-ASSERTED effective row, which has no node) ranks on defaults.
Non-integer numeric signals and non-boolean ``is_superseded`` values fail
the validation pre-pass loudly (a silently mis-typed signal would silently
re-rank a bucket). The claim's SCOPE for precedent matching is the
EFFECTIVE scope (tier-1 output), not the raw node attr — a scope correction
must move precedent applicability.

One further attr on the same convention, and NOT a ≻ signal:
``provenance_group`` (:data:`DECLARED_ORIGIN_ATTR`) declares the claim's
ORIGIN. Corroboration counts distinct ORIGIN, and a claim that declares none
is its own origin — its document identity (see :func:`_support_binding`). It
is read as a declaration, never ranked directly, and a corpus that declares
nothing scores exactly what it scored before the attr existed.

IR-vs-Python split (operator decision D1 + this task's mandate)
---------------------------------------------------------------
Relational assembly is transform-IR lowered through the generation-pinned
Chronos checkout session:

* the effective-claims read (+ the per-claim signal join and the
  latest-applicable-precedent anti-join — the ``_later`` idiom from tier 1),
* the tier-2 bucket SELF-JOINS: functional candidate pairs, incompatible_pair
  rows matched in both orientations, the unresolved-key member scan, and pair
  locality. Candidate applicability is deliberately not encoded in IR: the
  residual calls the one authoritative typed operator from
  ``claim_model.applicability`` so heading, bounds, time, subject, activity,
  and modality cannot drift into a second SQL algebra,
* the tier-3 ranking, which MUST and does use the native RANK op
  (decision D1): per-(predicate, key) partitions ordered by the seven ≻
  signal components DESC with a ``claim_id`` DESC tiebreak, emitting the
  rank and the leader's separating-component basis. Ranking is never
  reimplemented in Python on the production path; the oracle uses
  ``resolve_bucket``.

The Python residuals, each with its reason:

* **deployment incompatibility + typed applicability classification + finding
  assembly** (outcomes, ids, member lists, detail strings) — incompatibility
  reuses the pure engine's typed-or-v1 helper and applicability reuses the pure
  authoritative operator; finding ids
  are content hashes and member lists are aggregates; the IR has no
  hash/array-agg. Row-local over IR-produced pair/member rows.
* **unresolved-key grouping** (members per bucket, ≥2 threshold) — needs
  the full member id LIST per bucket; no array-agg in the IR. The member
  SCAN (which claims sit on an unresolved key under a conflict predicate)
  is IR.
* **linked_pair findings** — ``violates_values`` membership is a dynamic
  JSON-array test against a policy-row column (``jsonb_array_any`` takes a
  static pattern only) and the link key goes through a NAMED kernel; both
  are outside the expr layer. The residual is row-local over the
  IR-produced effective rows + policy rows and reuses the library's
  ``scope_relation`` (library-exact by construction).
* **conflict-component membership, outcome mapping + kept-row shaping** for
  tier 3 — the RANK program receives only claims participating in the one
  connected conflict component. Multiple disconnected components fail loudly
  pending #745; decisive-vs-ranking
  basis classification over the RANK output's leader row (a set-membership
  test on one row), and mapping rows onto kept-table column dicts.
* **dirty-bucket bookkeeping** in the incremental callback (subtype lookup,
  membership scan) — maintenance plumbing, mirroring tier-1's plain-SQL
  ``_relevant_change_reason``.

Persisted rows (pk ``(row_kind, row_key)``)
-------------------------------------------
* ``finding`` — one row per library finding. ``row_key`` mirrors the
  library id shape ``conflict:{constraint}:{sha256(members)[:12]}`` (stable
  hash of constraint+members; the members determine the bucket). Columns:
  constraint id/kind, predicate, bucket key, outcome
  (definite/possible/not_comparable), locality (``internal`` /
  ``cross_document`` — the ontology vocabulary; the library's ``cross_doc``
  token maps on flatten), sorted member claim ids (JSON), detail.
* ``bucket_resolution`` — one row per CONFLICTED bucket: a
  (predicate, normalized_key) bucket with at least one same-bucket pairwise
  finding (functional / incompatible_pair). NOT-COMPARABLE buckets are
  deliberately not resolved (that would force the verdict the library
  refuses) and linked_pair findings are policy violations, not
  pick-a-winner conflicts (spec §3), so neither triggers a resolution.
  ``row_key = json.dumps([predicate, key])`` (unambiguous under ``:`` in
  either coordinate). Columns: outcome (canonical/contested), winner
  (``""`` when contested — the library token), basis (the deciding signal
  class, ``tie``, or ``sole_claim``), ranked member ids ≻-descending
  (JSON), superseded ids (rank>1 members of a canonical bucket; ``[]``
  when contested) — full provenance.

Incremental maintenance (bucket locality, design §4 v1 + decision D4)
---------------------------------------------------------------------
First publish → full. Afterwards the incremental callback classifies this
cycle's ``changed_node_ids`` at the checkout:

* any changed ``predicate_policy`` or ``claim_precedent`` → conservative
  FULL (a Σ edit re-classifies every bucket; a precedent write's blast
  radius is every bucket its scope touches — D4's design leaning), reason
  logged;
* any changed id ABSENT at the checkout (a deletion we cannot subtype) →
  conservative FULL;
* enabled ``linked_pair`` policies present AND claims/corrections dirty →
  conservative FULL (design §4: linked pairs are cross-bucket dirtiness
  sources; bucket-pair dirtiness is a later analyzer stage), reason logged;
* otherwise dirty claims (changed ``consistency_claim`` nodes plus the
  ``target_claim_id`` of changed ``claim_correction`` nodes) map to their
  (predicate, normalized_key) buckets — the CURRENT effective bucket
  (bucket keys are claim-local, decision D3) plus every bucket whose
  existing kept rows mention the claim (the moved-bucket / removed-member
  case) — and ONLY those buckets are recomputed: the IR programs take an
  ``eq_any`` bucket-token filter, rows outside the dirty buckets are
  carried forward by Chronos interval semantics, and deletes are restricted
  to dirty buckets. A cycle touching none of the four subtypes is a no-op.
``changed_edge_ids`` are ignored: every join here is on attrs/kept columns
(review H1) — hence ``DeclaredDeps(kinds={"graph_node"})``.
"""
from __future__ import annotations

import hashlib
import json
import logging
from typing import Any, Callable, Iterable, Mapping, Sequence

from okg.substrate.consistency.claim_model.conflicts import (
    CONSTRAINT_AMBIGUOUS,
    ConflictRules,
    CrossPredicatePolicy,
    LOCALITY_CROSS_DOC,
    LOCALITY_INTERNAL,
    OUTCOME_DEFINITE,
    OUTCOME_ALIGNMENT_UNCERTAIN,
    OUTCOME_APPLICABILITY_UNCERTAIN,
    OUTCOME_CONFLICT,
    OUTCOME_NOT_COMPARABLE,
    OUTCOME_POSSIBLE,
    TypedOutcome,
    ALIGNMENT_DIMENSIONS,
    APPLICABILITY_DIMENSIONS,
    _dimension_evidence,
    claims_incompatible,
    classify_outcome,
    derive_conflict_findings,
)
from okg.substrate.consistency.claim_model.precedence import (
    _DECISIVE,
    OUTCOME_CANONICAL,
    OUTCOME_CONTESTED,
    ClaimSignals,
    Precedent,
    SIGNAL_DOCUMENT_RECENCY,
    SIGNAL_EFFECTIVE_DATE,
    SIGNAL_INDEPENDENT_SUPPORT,
    SIGNAL_MAINTENANCE,
    SIGNAL_PRECEDENT,
    SIGNAL_SOURCE_AUTHORITY,
    SIGNAL_SUPERSESSION,
    resolve_bucket,
)
from okg.substrate.consistency.claim_model.selection import ConflictBucket
from okg.substrate.consistency.claim_model.views import (
    ClaimView,
    ElementRef,
    REL_DISJOINT,
    REL_OVERLAP,
    SCOPE_PATH_SEP,
    applicability_relation,
    claim_semantics_from_attr,
    element_ref_from_attr,
    scope_relation,
)
from okg.substrate.derived_artifacts.claim_effective_derivation import (
    CADENCE_COLUMN as _T1_CADENCE_COLUMN,
    CLAIM_EFFECTIVE_TABLE,
    CLAIM_SUBTYPE,
    CORRECTION_SUBTYPE,
    KEY_COLUMNS as _T1_KEY_COLUMNS,
    PRECEDENT_SUBTYPE,
    ROW_KIND_EFFECTIVE,
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
from okg.substrate.transform.expr import case_, concat, eq_any, starts_with
from okg.substrate.transform.operators import to_chronos_sql

logger = logging.getLogger(__name__)

DERIVATION_ID = "claim_projection"
CLAIM_PROJECTION_TABLE = "okg.claim_projection"
# The graph-node table the per-claim DECLARED attrs are read from. Named so the
# SQL/interpreter differential can retarget the support binding at a fixture
# table; every other binding in this file spells it inline.
GRAPH_NODES_TABLE = "okg.graph_nodes"

# The claim's DECLARED ORIGIN (PACT consistency-independent-support). One more
# deployment-supplied claim attr on the v1 convention, read via ``attrs->>``
# exactly like the ≻ signal attrs — but NOT a ranking signal: it names the
# origin whose distinct count IS corroboration (see _support_binding). A claim
# that does not declare it is its own origin, so a corpus declaring nothing
# scores exactly what it scored before.
DECLARED_ORIGIN_ATTR = "provenance_group"

POLICY_SUBTYPE = "predicate_policy"
_CONFLICTED_CLAIMS_PARAM = "conflicted_claim_ids"
_NO_CLAIM_TOKEN = "\x1fno-conflicted-claim\x1f"

ROW_KIND_FINDING = "finding"
ROW_KIND_RESOLUTION = "bucket_resolution"

KIND_FUNCTIONAL = "functional"
KIND_INCOMPATIBLE = "incompatible_pair"
KIND_LINKED = "linked_pair"
KIND_UNRESOLVED = "unresolved_key"
_POLICY_KINDS = frozenset(
    {KIND_FUNCTIONAL, KIND_INCOMPATIBLE, KIND_LINKED, KIND_UNRESOLVED}
)

# v1 link-kernel registry (linked_pair policies name one of these). The only
# registered kernel is normalize_identity: the link key IS the source claim's
# value (cern-twiki's normalize_entity_value collapses to identity once
# canonicalization moves to extraction time, decision D3).
LINK_KERNELS: dict[str, Callable[[str], str]] = {
    "normalize_identity": lambda value: value,
}

# Ontology vocabulary for the kept locality column (schema.yaml ConflictFinding
# declares internal|cross_document); the library token cross_doc maps here.
LOCALITY_CROSS_DOCUMENT = "cross_document"
_LOCALITY_TOKEN = {
    LOCALITY_INTERNAL: LOCALITY_INTERNAL,
    LOCALITY_CROSS_DOC: LOCALITY_CROSS_DOCUMENT,
}

BASIS_SOLE = "sole_claim"  # the library's sole-claim basis token
BASIS_TIE = "tie"

# The unresolved-key detail string, byte-identical to the library's.
_AMBIGUOUS_DETAIL = (
    "claims share an unresolved (unknown) key; alignment unconfirmed"
)

BUCKET_SEP = "\x1f"  # dirty-bucket token separator (never appears in keys)
_DIRTY_BUCKETS_PARAM = "dirty_buckets"

KEY_COLUMNS = ("row_kind", "row_key")
VALUE_COLUMNS = (
    "predicate",
    "bucket_key",
    "constraint_id",
    "constraint_kind",
    "outcome",
    "locality",
    "member_claim_ids",
    "detail",
    "winner_claim_id",
    "basis",
    "ranked_claim_ids",
    "superseded_claim_ids",
    "typed_category",
    "actionable_conflict",
    "alignment_established",
    "alignment_unknown",
    "applicability_established",
    "applicability_unknown",
    "legacy_marker",
)
CADENCE_COLUMN = "item_cadence"

CREATE_TABLE_SQL = f"""
CREATE TABLE IF NOT EXISTS {CLAIM_PROJECTION_TABLE} (
  row_kind             text NOT NULL,
  row_key              text NOT NULL,
  predicate            text,
  bucket_key           text,
  constraint_id        text,
  constraint_kind      text,
  outcome              text,
  locality             text,
  member_claim_ids     text,
  detail               text,
  winner_claim_id      text,
  basis                text,
  ranked_claim_ids     text,
  superseded_claim_ids text,
  typed_category       text,
  actionable_conflict  boolean,
  alignment_established text,
  alignment_unknown    text,
  applicability_established text,
  applicability_unknown text,
  legacy_marker        text,
  item_cadence         bigint,
  PRIMARY KEY (row_kind, row_key)
)
"""

UPGRADE_TABLE_SQL = f"""
ALTER TABLE {CLAIM_PROJECTION_TABLE}
  ADD COLUMN IF NOT EXISTS typed_category text;
ALTER TABLE {CLAIM_PROJECTION_TABLE}
  ADD COLUMN IF NOT EXISTS actionable_conflict boolean;
ALTER TABLE {CLAIM_PROJECTION_TABLE}
  ADD COLUMN IF NOT EXISTS alignment_established text;
ALTER TABLE {CLAIM_PROJECTION_TABLE}
  ADD COLUMN IF NOT EXISTS alignment_unknown text;
ALTER TABLE {CLAIM_PROJECTION_TABLE}
  ADD COLUMN IF NOT EXISTS applicability_established text;
ALTER TABLE {CLAIM_PROJECTION_TABLE}
  ADD COLUMN IF NOT EXISTS applicability_unknown text;
ALTER TABLE {CLAIM_PROJECTION_TABLE}
  ADD COLUMN IF NOT EXISTS legacy_marker text
"""

# The seven ≻ signal components, strongest first, DESC, claim_id DESC tiebreak
# — the RANK-op shape task.core.window-op proved against resolve_bucket.
_RANK_COMPONENTS = [
    {"name": SIGNAL_PRECEDENT, "expr": col("precedent_rank"), "desc": True},
    {"name": SIGNAL_SUPERSESSION, "expr": col("not_superseded"), "desc": True},
    {"name": SIGNAL_EFFECTIVE_DATE, "expr": col("effective_date"), "desc": True},
    {"name": SIGNAL_SOURCE_AUTHORITY, "expr": col("source_authority"), "desc": True},
    {"name": SIGNAL_INDEPENDENT_SUPPORT,
     "expr": col("independent_support"), "desc": True},
    {"name": SIGNAL_DOCUMENT_RECENCY, "expr": col("document_recency"), "desc": True},
    {"name": SIGNAL_MAINTENANCE, "expr": col("maintenance_rank"), "desc": True},
]


# --------------------------------------------------------------------------- #
# Key/flatten helpers — the SHARED row encoding (key derivation + column      #
# layout only; the semantics stay differential: IR/RANK vs the library).      #
# --------------------------------------------------------------------------- #
def finding_row_key(constraint: str, member_ids: Iterable[str]) -> str:
    """The library's deterministic finding id shape (conflicts._finding)."""
    members = sorted(member_ids)
    digest = hashlib.sha256("|".join(members).encode("utf-8")).hexdigest()[:12]
    return f"conflict:{constraint}:{digest}"


def bucket_row_key(predicate: str, bucket_key: str) -> str:
    """Unambiguous bucket_resolution row key (predicates may contain ':')."""
    return json.dumps([predicate, bucket_key], separators=(",", ":"))


def bucket_token(predicate: str, bucket_key: str) -> str:
    """The dirty-bucket token the eq_any filters match on."""
    return f"{predicate}{BUCKET_SEP}{bucket_key}"


def _ids_json(ids: Iterable[str]) -> str:
    return json.dumps(list(ids), separators=(",", ":"))


def _empty_value_row() -> dict[str, Any]:
    return {column: None for column in VALUE_COLUMNS}


def _linked_detail(policy_id: str, source_value: str, target_value: str) -> str:
    return (
        f"{policy_id}: source value {source_value!r} conflicts with linked "
        f"target value {target_value!r}"
    )


def _pairwise_detail(predicate: str, value_a: str, value_b: str) -> str:
    """Byte-identical to the library's pairwise finding detail."""
    return f"incompatible {predicate} values {value_a!r} vs {value_b!r}"


def _finding_value_row(
    *,
    predicate: str,
    bucket_key: str,
    constraint_id: str,
    constraint_kind: str,
    outcome: str,
    locality: str,
    member_ids: Iterable[str],
    detail: str,
    typed: TypedOutcome | None = None,
) -> dict[str, Any]:
    typed = typed or TypedOutcome(
        OUTCOME_CONFLICT if outcome == OUTCOME_DEFINITE
        else OUTCOME_NOT_COMPARABLE if outcome == OUTCOME_NOT_COMPARABLE
        else OUTCOME_APPLICABILITY_UNCERTAIN
    )
    row = _empty_value_row()
    row.update({
        "predicate": predicate,
        "bucket_key": bucket_key,
        "constraint_id": constraint_id,
        "constraint_kind": constraint_kind,
        "outcome": outcome,
        "locality": _LOCALITY_TOKEN[locality],
        "member_claim_ids": _ids_json(sorted(member_ids)),
        "detail": detail,
        "typed_category": typed.category,
        "actionable_conflict": typed.actionable_conflict,
        "alignment_established": _ids_json(typed.alignment_established),
        "alignment_unknown": _ids_json(typed.alignment_unknown),
        "applicability_established": _ids_json(typed.applicability_established),
        "applicability_unknown": _ids_json(typed.applicability_unknown),
        "legacy_marker": (
            "legacy_possible" if typed.category in {
                OUTCOME_ALIGNMENT_UNCERTAIN, OUTCOME_APPLICABILITY_UNCERTAIN
            } else ""
        ),
    })
    return row


def _resolution_value_row(
    *,
    predicate: str,
    bucket_key: str,
    outcome: str,
    winner: str,
    basis: str,
    ranked_ids: Iterable[str],
    superseded_ids: Iterable[str],
) -> dict[str, Any]:
    ranked = list(ranked_ids)
    row = _empty_value_row()
    row.update({
        "predicate": predicate,
        "bucket_key": bucket_key,
        "outcome": outcome,
        "winner_claim_id": winner,
        "basis": basis,
        "ranked_claim_ids": _ids_json(ranked),
        # The claims the winner BEAT — those asserting a different value — not
        # every claim below rank 1. A claim restating the winner's value
        # corroborates it; recording it as superseded is factually wrong and
        # makes the repair layer retract the evidence that settled the bucket.
        "superseded_claim_ids": _ids_json(
            list(superseded_ids) if outcome == OUTCOME_CANONICAL else []
        ),
    })
    return row


def _attr_text(value: Any) -> str | None:
    """The text rendering postgres ``attrs->>`` yields (tier-1's convention)."""
    if value is None or isinstance(value, str):
        return value
    return json.dumps(value)


def _attr_int(value: Any) -> int:
    """Mirror the SQL ``(attrs->>'x')::bigint`` cast: text-parse, loud on junk."""
    return int(str(value))


def _attr_flag(value: Any) -> bool:
    """Mirror the SQL ``attrs->>'x' = 'true'`` test exactly."""
    return value is True or value == "true"


# --------------------------------------------------------------------------- #
# Surface bindings — column-renaming projections only (tier-1 discipline:     #
# COLLATE "C" on every id used in a comparison; every condition beyond        #
# rename/extraction lives in the IR and participates in program checksums).   #
# --------------------------------------------------------------------------- #
_EFF_PREFIXES = ("ea", "eb")
_POLICY_PREFIXES = ("pf", "pi", "pu", "pw", "pq", "pv", "pz")
_PRECEDENT_PREFIXES = ("pa", "pb")


def _effective_binding(prefix: str) -> str:
    return (
        f'(SELECT row_key COLLATE "C" AS {prefix}_claim_id, '
        # NOT coalesced, unlike scope/scope_state/source_id below. These three
        # are join keys against the corroboration surface, where `NULL = NULL`
        # is NULL in SQL but True for the interpreter's `==` — but a NULL here
        # is unreachable: validate_projection_inputs raises on a missing
        # predicate/normalized_key/value and runs on BOTH entry points (the
        # Chronos session path and compute_rows_interpreted) before any program
        # executes. Witness: test_corroboration.py
        # ::test_null_coordinate_raises_before_any_join_can_miss.
        # Coalescing anyway is not free: this binding feeds every finding
        # program, so folding NULL to '' there would change conflict detection,
        # not just the support join.
        f"predicate AS {prefix}_predicate, "
        f"normalized_key AS {prefix}_key, "
        f"value AS {prefix}_value, "
        # Coerce scope/scope_state to match the oracle's `x or default`
        # (claim_projection_derivation.py oracle: scope or "", scope_state
        # or "unknown"). Without this, a NULL scope_state makes the SQL
        # not_(eq(scope_state,'known')) evaluate to NULL — the pair is
        # silently DROPPED, losing a real conflict, while the interpreter's
        # Python `==` keeps it (transform/expr.py delegates NULL-eq parity
        # to the consumer). NULLIF folds '' into the default too, exactly
        # like `or`. Tier-1 guarantees non-null today, but the projection
        # must not rely on an invariant it does not enforce.
        f"COALESCE(scope, '') AS {prefix}_scope, "
        f"COALESCE(NULLIF(scope_state, ''), 'unknown') AS {prefix}_scope_state, "
        f"claim_semantics AS {prefix}_semantics, "
        f"COALESCE(source_id, '') AS {prefix}_source "
        f"FROM {CLAIM_EFFECTIVE_TABLE} "
        f"WHERE row_kind = '{ROW_KIND_EFFECTIVE}')"
    )


def _support_binding() -> str:
    """Bucket-level corroboration: how many DISTINCT ORIGINS assert a value.

    This is the one ≻ signal no producer can supply. The other six are
    properties of a single claim or its document; corroboration is a
    property of the BUCKET, which a producer emitting one claim cannot see —
    and bucketing happens downstream of extraction, over C_eff (post
    correction-merge), so any producer-side count is stale by construction.
    Only the engine holds the bucket, and until now nothing computed it, so
    the derivation read `COALESCE(attrs->>'independent_support', 0)` and got
    0 in every production bucket: the corroboration tier of ≻ could never
    settle anything.

    ORIGIN, NOT DOCUMENT (PACT consistency-independent-support). This counted
    distinct ``source_id`` — distinct DOCUMENT PATHS — so a value echoed into
    three documents from ONE origin scored 3, identical to the same value
    asserted by three genuinely independent sources. Copies are evidence of
    copying, not of truth, and the ranking could not tell the difference:
    measured, every preserved majority whose corrupted renderings shared one
    origin settled on the wrong value, on basis dependency_adjusted_support.

    The rule, stated once: a claim's origin is its DECLARED origin
    (``attrs->>'provenance_group'`` on the claim node, the v1 deployment-supplied
    convention every other claim-level signal already uses); a claim with no
    declared origin IS ITS OWN ORIGIN — its origin is its document identity.
    That is a DECLARED identity rule, not a silent default: it is what makes
    this behaviour-preserving for every corpus that declares nothing (which is
    every corpus in production today — with no declarations the count is
    literally COUNT(DISTINCT COALESCE(source_id, '')) again, row for row).
    The alternative, refusing to count support where origin is undeclared, is
    rejected: it would zero the corroboration tier for every existing
    deployment and reintroduce the defect this binding was written to fix.

    Declared origins and document identities share ONE namespace deliberately,
    because a document IS an origin: a corpus that declares
    ``provenance_group = <some document path>`` is saying "this claim
    originates from that document", and it must merge with the claims that
    document contributes without declaring anything.

    Computed over the EFFECTIVE claims (not graph_nodes) so an operator
    override or retraction is reflected; the claim node is joined for the
    DECLARATION only, which is a property of how the claim was asserted rather
    than of its effective value. A plain GROUP BY subquery, so the count is
    exact rather than the window-function approximation COUNT(DISTINCT ...)
    OVER cannot express. The join is LEFT and the aggregate is
    COUNT(DISTINCT ...), so a claim with no node (every operator-ASSERTED
    effective row) keeps its document identity and a duplicated node row
    cannot inflate the count.
    """
    # The grouping keys are NOT coalesced, matching _effective_binding so the
    # join compares like with like. A NULL coordinate would miss the join in
    # postgres only (`NULL = NULL` is NULL) and COALESCE(sp_support, 0) would
    # then silently report support 0 — but that is unreachable:
    # validate_projection_inputs raises on a missing predicate/normalized_key/
    # value on BOTH entry points before any program runs. source_id IS
    # coalesced because an empty source is legal outside a conflicted bucket.
    #
    # NULLIF folds a declared-but-EMPTY origin back to the document identity,
    # so `provenance_group = ''` cannot collapse a whole bucket to support 1 —
    # an empty declaration declares nothing.
    return (
        "(SELECT e.predicate AS sp_predicate, "
        "e.normalized_key AS sp_key, "
        "e.value AS sp_value, "
        "COUNT(DISTINCT COALESCE("
        f"NULLIF(n.attrs->>'{DECLARED_ORIGIN_ATTR}', ''), e.source_id, ''"
        ")) AS sp_support "
        f"FROM {CLAIM_EFFECTIVE_TABLE} e "
        f"LEFT JOIN {GRAPH_NODES_TABLE} n "
        'ON n.node_id COLLATE "C" = e.row_key COLLATE "C" '
        f"AND n.subtype = '{CLAIM_SUBTYPE}' "
        f"WHERE e.row_kind = '{ROW_KIND_EFFECTIVE}' "
        "GROUP BY e.predicate, e.normalized_key, e.value)"
    )


def support_surface_rows(
    effective_rows: Iterable[Mapping[str, Any]],
    claim_nodes: Iterable[Mapping[str, Any]] = (),
) -> list[dict[str, Any]]:
    """Interpreter mirror of ``_support_binding``.

    Deliberately an INDEPENDENT restatement rather than a shared helper: the
    parity oracle exists to catch the SQL and the reference implementation
    disagreeing, and a shared helper would make it agree with itself exactly
    where the new logic lives. ``_attr_text`` is the module's one rendering of
    postgres ``attrs->>`` (every mirror in this file uses it) — the RULE below
    is restated, not borrowed.

    ``claim_nodes`` supplies the DECLARED origin. Omitting them is the
    declare-nothing corpus, which is exactly the pre-change count.
    """
    declared: dict[str, str] = {}
    for n in claim_nodes:
        origin = _attr_text(dict(n.get("attrs") or {}).get(DECLARED_ORIGIN_ATTR))
        # `if origin` mirrors NULLIF(..., ''): an empty declaration declares
        # nothing and falls back to the document identity.
        if origin:
            declared[str(n["node_id"])] = origin
    counts: dict[tuple[Any, Any, Any], set[str]] = {}
    for r in effective_rows:
        # Coordinates are used raw, mirroring the binding's un-coalesced GROUP
        # BY; only the origin is coalesced, exactly as the SQL does.
        bucket = (r.get("predicate"), r.get("normalized_key"), r.get("value"))
        origin = declared.get(str(r.get("claim_id") or ""))
        if not origin:
            # the declared identity rule: no declared origin means the claim IS
            # its own origin, and its identity is the document it came from.
            origin = str(r.get("source_id") or "")
        counts.setdefault(bucket, set()).add(origin)
    return [
        {
            "sp_predicate": predicate,
            "sp_key": key,
            "sp_value": value,
            "sp_support": len(origins),
        }
        for (predicate, key, value), origins in sorted(
            counts.items(), key=lambda kv: tuple(str(x) for x in kv[0])
        )
    ]


def _policy_binding(prefix: str) -> str:
    return (
        f'(SELECT node_id COLLATE "C" AS {prefix}_node_id, '
        f"attrs->>'policy_id' AS {prefix}_policy_id, "
        f"attrs->>'policy_kind' AS {prefix}_kind, "
        f"attrs->>'predicate' AS {prefix}_predicate, "
        f"attrs->>'value_a' AS {prefix}_value_a, "
        f"attrs->>'value_b' AS {prefix}_value_b, "
        f"attrs->>'target_predicate' AS {prefix}_target_predicate, "
        f"attrs->>'link_key_fn' AS {prefix}_link_key_fn, "
        f"attrs->>'unresolved_key' AS {prefix}_unresolved_key, "
        f"attrs->>'violates_values' AS {prefix}_violates_values, "
        f"COALESCE(attrs->>'enabled', '') AS {prefix}_enabled "
        f"FROM okg.graph_nodes WHERE subtype = '{POLICY_SUBTYPE}')"
    )


def _precedent_binding(prefix: str) -> str:
    return (
        f'(SELECT node_id COLLATE "C" AS {prefix}_node_id, '
        f"attrs->>'prefer_source' AS {prefix}_prefer, "
        f"COALESCE(attrs->>'scope', '') AS {prefix}_scope, "
        f"(attrs->>'sequence')::bigint AS {prefix}_sequence "
        f"FROM okg.graph_nodes WHERE subtype = '{PRECEDENT_SUBTYPE}')"
    )


def _signal_binding() -> str:
    # The deployment-supplied signal surface (module docstring): NULL→default
    # folded HERE so the L1 projection only has to cover the missing-node case.
    return (
        '(SELECT node_id COLLATE "C" AS cs_claim_id, '
        "COALESCE(attrs->>'source_class', '') AS cs_source_class, "
        "CASE WHEN attrs->>'is_superseded' = 'true' THEN 0 ELSE 1 END "
        "AS cs_not_superseded, "
        "COALESCE(attrs->>'effective_date', '') AS cs_effective_date, "
        "COALESCE((attrs->>'source_authority')::bigint, 0) AS cs_source_authority, "
        # No cs_independent_support: corroboration is computed by the engine
        # from the bucket (see _support_binding), never read from a claim attr.
        # Emitting the dead column would invite a future author to wire the
        # unpopulatable attr back in.
        "COALESCE(attrs->>'document_recency', '') AS cs_document_recency, "
        "COALESCE((attrs->>'maintenance_rank')::bigint, 0) AS cs_maintenance_rank "
        f"FROM okg.graph_nodes WHERE subtype = '{CLAIM_SUBTYPE}')"
    )


def registries() -> Registries:
    """Chronos surface registry + the tier-2/3 claim-projection surfaces."""
    registry = chronos_surface_registry()
    for prefix in _EFF_PREFIXES:
        registry.register(
            Surface(f"cons_proj_eff_{prefix}", SurfaceKind.LIVE,
                    _effective_binding(prefix))
        )
    for prefix in _POLICY_PREFIXES:
        registry.register(
            Surface(f"cons_proj_pol_{prefix}", SurfaceKind.LIVE,
                    _policy_binding(prefix))
        )
    for prefix in _PRECEDENT_PREFIXES:
        registry.register(
            Surface(f"cons_proj_prec_{prefix}", SurfaceKind.LIVE,
                    _precedent_binding(prefix))
        )
    registry.register(Surface("cons_proj_sig", SurfaceKind.LIVE, _signal_binding()))
    registry.register(
        Surface("cons_proj_support", SurfaceKind.LIVE, _support_binding())
    )
    return Registries(surfaces=registry, functions=FunctionRegistry())


# --------------------------------------------------------------------------- #
# Interpreter-side surface rows (the executable denotation of the bindings;   #
# MUST mirror the projections above field-for-field).                         #
# --------------------------------------------------------------------------- #
def effective_surface_rows(
    prefix: str, effective_rows: Iterable[Mapping[str, Any]]
) -> list[dict[str, Any]]:
    out = []
    for r in effective_rows:
        source = r.get("source_id")
        out.append({
            f"{prefix}_claim_id": str(r["claim_id"]),
            f"{prefix}_predicate": r.get("predicate"),
            f"{prefix}_key": r.get("normalized_key"),
            f"{prefix}_value": r.get("value"),
            # Same coercion as the SQL binding + the oracle (x or default),
            # so all three paths agree on NULL/'' scope inputs.
            f"{prefix}_scope": r.get("scope") or "",
            f"{prefix}_scope_state": r.get("scope_state") or "unknown",
            f"{prefix}_semantics": r.get("claim_semantics"),
            f"{prefix}_source": source if source is not None else "",
        })
    return out


def policy_surface_rows(
    prefix: str, nodes: Iterable[Mapping[str, Any]]
) -> list[dict[str, Any]]:
    out = []
    for n in nodes:
        attrs = dict(n.get("attrs") or {})
        enabled = _attr_text(attrs.get("enabled"))
        out.append({
            f"{prefix}_node_id": str(n["node_id"]),
            f"{prefix}_policy_id": _attr_text(attrs.get("policy_id")),
            f"{prefix}_kind": _attr_text(attrs.get("policy_kind")),
            f"{prefix}_predicate": _attr_text(attrs.get("predicate")),
            f"{prefix}_value_a": _attr_text(attrs.get("value_a")),
            f"{prefix}_value_b": _attr_text(attrs.get("value_b")),
            f"{prefix}_target_predicate": _attr_text(attrs.get("target_predicate")),
            f"{prefix}_link_key_fn": _attr_text(attrs.get("link_key_fn")),
            f"{prefix}_unresolved_key": _attr_text(attrs.get("unresolved_key")),
            f"{prefix}_violates_values": _attr_text(attrs.get("violates_values")),
            f"{prefix}_enabled": enabled if enabled is not None else "",
        })
    return out


def precedent_surface_rows(
    prefix: str, nodes: Iterable[Mapping[str, Any]]
) -> list[dict[str, Any]]:
    out = []
    for n in nodes:
        attrs = dict(n.get("attrs") or {})
        seq = attrs.get("sequence")
        scope = _attr_text(attrs.get("scope"))
        out.append({
            f"{prefix}_node_id": str(n["node_id"]),
            f"{prefix}_prefer": _attr_text(attrs.get("prefer_source")),
            f"{prefix}_scope": scope if scope is not None else "",
            f"{prefix}_sequence": _attr_int(seq) if seq is not None else None,
        })
    return out


def signal_surface_rows(
    nodes: Iterable[Mapping[str, Any]]
) -> list[dict[str, Any]]:
    out = []
    for n in nodes:
        attrs = dict(n.get("attrs") or {})

        def _text(name: str) -> str:
            value = _attr_text(attrs.get(name))
            return value if value is not None else ""

        def _num(name: str) -> int:
            value = attrs.get(name)
            return _attr_int(value) if value is not None else 0

        out.append({
            "cs_claim_id": str(n["node_id"]),
            "cs_source_class": _text("source_class"),
            "cs_not_superseded": 0 if _attr_flag(attrs.get("is_superseded")) else 1,
            "cs_effective_date": _text("effective_date"),
            "cs_source_authority": _num("source_authority"),
            # no cs_independent_support — see _signal_binding
            "cs_document_recency": _text("document_recency"),
            "cs_maintenance_rank": _num("maintenance_rank"),
        })
    return out


def interpreter_surfaces(
    effective_rows: Iterable[Mapping[str, Any]],
    claim_nodes: Iterable[Mapping[str, Any]],
    policy_nodes: Iterable[Mapping[str, Any]],
    precedent_nodes: Iterable[Mapping[str, Any]] = (),
) -> dict[str, list[dict[str, Any]]]:
    """In-memory surfaces for ``transform.evaluate`` mirroring ``registries()``."""
    effective_rows = list(effective_rows)
    # materialized because TWO surfaces read it now (the signal surface and the
    # support surface's declared-origin join); a one-shot iterator would leave
    # the second one silently empty — i.e. every origin undeclared.
    claim_nodes = list(claim_nodes)
    policy_nodes = list(policy_nodes)
    precedent_nodes = list(precedent_nodes)
    surfaces: dict[str, list[dict[str, Any]]] = {
        "cons_proj_sig": signal_surface_rows(claim_nodes),
        "cons_proj_support": support_surface_rows(effective_rows, claim_nodes),
    }
    for prefix in _EFF_PREFIXES:
        surfaces[f"cons_proj_eff_{prefix}"] = effective_surface_rows(
            prefix, effective_rows
        )
    for prefix in _POLICY_PREFIXES:
        surfaces[f"cons_proj_pol_{prefix}"] = policy_surface_rows(
            prefix, policy_nodes
        )
    for prefix in _PRECEDENT_PREFIXES:
        surfaces[f"cons_proj_prec_{prefix}"] = precedent_surface_rows(
            prefix, precedent_nodes
        )
    return surfaces


# --------------------------------------------------------------------------- #
# IR expression helpers                                                       #
# --------------------------------------------------------------------------- #
def _enabled(prefix: str) -> dict:
    return eq(col(f"{prefix}_enabled"), lit("true"))


def _unresolved_match(prefix: str, key_expr: dict) -> dict:
    """A key matches an enabled unresolved_key row (GLOBAL set — docstring)."""
    return and_(
        _enabled(prefix),
        eq(col(f"{prefix}_kind"), lit(KIND_UNRESOLVED)),
        eq(col(f"{prefix}_unresolved_key"), key_expr),
    )


def _pair_join(a: str, b: str) -> dict:
    """Same-(predicate, key) bucket, canonical pair order (a < b, ordinal via
    the bindings' COLLATE "C")."""
    return and_(
        eq(col(f"{b}_predicate"), col(f"{a}_predicate")),
        eq(col(f"{b}_key"), col(f"{a}_key")),
        gt(col(f"{b}_claim_id"), col(f"{a}_claim_id")),
    )


def _pair_locality(a: str, b: str) -> dict:
    return case_(
        [(eq(col(f"{a}_source"), col(f"{b}_source")), lit(LOCALITY_INTERNAL))],
        else_=lit(LOCALITY_CROSS_DOCUMENT),
    )


def _scoped_filter(prefix: str) -> dict:
    return eq_any(
        concat(col(f"{prefix}_predicate"), lit(BUCKET_SEP), col(f"{prefix}_key")),
        _DIRTY_BUCKETS_PARAM,
    )


def _applicable(p: str) -> dict:
    """Precedent applicability (precedence._scope_applies): global scope, or a
    non-empty claim scope equal to / subsuming / subsumed by the precedent's."""
    ps, cs = col(f"{p}_scope"), col("ea_scope")
    return or_(
        eq(ps, lit("")),
        and_(
            not_(eq(cs, lit(""))),
            or_(
                eq(ps, cs),
                starts_with(cs, concat(ps, lit(SCOPE_PATH_SEP))),
                starts_with(ps, concat(cs, lit(SCOPE_PATH_SEP))),
            ),
        ),
    )


def _later_precedent(px: str, py: str) -> dict:
    """px supersedes py as the latest applicable precedent: greater sequence,
    or equal sequence and SMALLER node_id (the oracle's first-of-max under a
    node_id-sorted list; COLLATE "C" keeps the SQL comparison ordinal)."""
    return or_(
        gt(col(f"{px}_sequence"), col(f"{py}_sequence")),
        and_(
            eq(col(f"{px}_sequence"), col(f"{py}_sequence")),
            gt(col(f"{py}_node_id"), col(f"{px}_node_id")),
        ),
    )


def _entry(*bindings: Binding, returns: tuple[str, ...]) -> Block:
    return Block(
        block_id="entry",
        bindings=tuple(bindings),
        terminator=Terminator(TerminatorKind.RETURN, {"values": list(returns)}),
    )


def _from(sym: str, surface: str) -> Binding:
    return Binding(Op(OpKind.FROM, {"surface": surface, "alias": sym}), output=sym)


def _ljoin(out: str, left: str, right: str, on: dict) -> Binding:
    return Binding(
        Op(OpKind.JOIN, {"how": "left", "on": on}), output=out, inputs=(left, right)
    )


def _ijoin(out: str, left: str, right: str, on: dict) -> Binding:
    return Binding(
        Op(OpKind.JOIN, {"how": "inner", "on": on}), output=out, inputs=(left, right)
    )


def _where(out: str, src: str, expr: dict) -> Binding:
    return Binding(Op(OpKind.WHERE, {"expr": expr}), output=out, inputs=(src,))


def _program(program_id: str, *bindings: Binding) -> TransformProgram:
    return TransformProgram(
        program_id=program_id,
        effect=Effect.READ_ONLY,
        outputs=("rows",),
        entry_block="entry",
        attrs={"derivation": DERIVATION_ID},
        blocks=(_entry(*bindings, returns=("rows",)),),
    )


def _with_scope(exprs: list[dict], scoped: bool, prefix: str = "ea") -> dict:
    if scoped:
        exprs = [*exprs, _scoped_filter(prefix)]
    return and_(*exprs) if len(exprs) > 1 else exprs[0]


# --------------------------------------------------------------------------- #
# The tier-2/3 programs                                                       #
# --------------------------------------------------------------------------- #
def pairwise_functional_program(
    scoped: bool = False, *, typed_callback: bool = False
) -> TransformProgram:
    """Tier-2 functional candidates: every same-bucket pair under an enabled
    functional policy. The shared typed-or-v1 incompatibility decision and
    applicability operator classify them in the documented residual."""
    pair_projection = {
        "constraint": col("pf_policy_id"),
        "predicate": col("ea_predicate"),
        "bucket_key": col("ea_key"),
        "claim_a": col("ea_claim_id"),
        "claim_b": col("eb_claim_id"),
        "value_a": col("ea_value"),
        "value_b": col("eb_value"),
        "scope_a": col("ea_scope"),
        "scope_state_a": col("ea_scope_state"),
        "semantics_a": col("ea_semantics"),
        "source_a": col("ea_source"),
        "scope_b": col("eb_scope"),
        "scope_state_b": col("eb_scope_state"),
        "semantics_b": col("eb_semantics"),
        "source_b": col("eb_source"),
        "locality": _pair_locality("ea", "eb"),
    }
    pair_join = _pair_join("ea", "eb")
    if not typed_callback:
        pair_join = and_(
            pair_join, not_(eq(col("eb_value"), col("ea_value")))
        )
    return _program(
        f"claim_projection.pairwise_functional"
        f"{'.scoped' if scoped else ''}"
        f"{'.typed' if typed_callback else ''}",
        _from("ea", "cons_proj_eff_ea"),
        _from("pf", "cons_proj_pol_pf"),
        _ijoin("j1", "ea", "pf", and_(
            _enabled("pf"),
            eq(col("pf_kind"), lit(KIND_FUNCTIONAL)),
            eq(col("pf_predicate"), col("ea_predicate")),
        )),
        _from("eb", "cons_proj_eff_eb"),
        _ijoin("j2", "j1", "eb", pair_join),
        _from("pu", "cons_proj_pol_pu"),
        _ljoin("j3", "j2", "pu", _unresolved_match("pu", col("ea_key"))),
        _where("kept", "j3", _with_scope([
            is_null(col("pu_node_id")),
            not_(eq(col("ea_key"), lit(""))),
        ], scoped)),
        Binding(Op(OpKind.SELECT, {"project": pair_projection}),
                output="projected", inputs=("kept",)),
        Binding(Op(OpKind.ORDER, {"keys": [
            {"expr": col("claim_a")}, {"expr": col("claim_b")},
        ]}), output="rows", inputs=("projected",)),
    )


def pairwise_incompatible_program(
    scoped: bool = False, *, typed_callback: bool = False
) -> TransformProgram:
    """Tier-2 incompatible candidates under an enabled predicate policy.

    DISTINCT collapses the policy's value-pair rows to one candidate per claim
    pair. The shared incompatibility helper applies those values for v1 claims
    or the registered typed callback when either claim is typed.
    """
    pair_projection = {
        "constraint": col("pi_policy_id"),
        "predicate": col("ea_predicate"),
        "bucket_key": col("ea_key"),
        "claim_a": col("ea_claim_id"),
        "claim_b": col("eb_claim_id"),
        "value_a": col("ea_value"),
        "value_b": col("eb_value"),
        "scope_a": col("ea_scope"),
        "scope_state_a": col("ea_scope_state"),
        "semantics_a": col("ea_semantics"),
        "source_a": col("ea_source"),
        "scope_b": col("eb_scope"),
        "scope_state_b": col("eb_scope_state"),
        "semantics_b": col("eb_semantics"),
        "source_b": col("eb_source"),
        "locality": _pair_locality("ea", "eb"),
    }
    policy_join = [
        _enabled("pi"),
        eq(col("pi_kind"), lit(KIND_INCOMPATIBLE)),
        eq(col("pi_predicate"), col("ea_predicate")),
    ]
    if not typed_callback:
        policy_join.append(or_(
            and_(eq(col("ea_value"), col("pi_value_a")),
                 eq(col("eb_value"), col("pi_value_b"))),
            and_(eq(col("ea_value"), col("pi_value_b")),
                 eq(col("eb_value"), col("pi_value_a"))),
        ))
    return _program(
        f"claim_projection.pairwise_incompatible"
        f"{'.scoped' if scoped else ''}"
        f"{'.typed' if typed_callback else ''}",
        _from("ea", "cons_proj_eff_ea"),
        _from("eb", "cons_proj_eff_eb"),
        _ijoin("j1", "ea", "eb", _pair_join("ea", "eb")),
        _from("pi", "cons_proj_pol_pi"),
        _ijoin("j2", "j1", "pi", and_(*policy_join)),
        _from("pw", "cons_proj_pol_pw"),
        _ljoin("j3", "j2", "pw", _unresolved_match("pw", col("ea_key"))),
        _where("kept", "j3", _with_scope([
            is_null(col("pw_node_id")),
            not_(eq(col("ea_key"), lit(""))),
        ], scoped)),
        Binding(Op(OpKind.SELECT, {"project": pair_projection, "distinct": True}),
                output="projected", inputs=("kept",)),
        Binding(Op(OpKind.ORDER, {"keys": [
            {"expr": col("claim_a")}, {"expr": col("claim_b")},
        ]}), output="rows", inputs=("projected",)),
    )


def ambiguous_members_program(scoped: bool = False) -> TransformProgram:
    """Tier-2 unresolved-key member scan: claims of a conflict predicate whose
    bucket key is unresolved ("" or an enabled unresolved_key row). The ≥2
    grouping into NOT-COMPARABLE findings is the documented Python residual."""
    return _program(
        f"claim_projection.ambiguous_members{'.scoped' if scoped else ''}",
        _from("ea", "cons_proj_eff_ea"),
        _from("pq", "cons_proj_pol_pq"),
        _ijoin("j1", "ea", "pq", and_(
            _enabled("pq"),
            in_(col("pq_kind"), [KIND_FUNCTIONAL, KIND_INCOMPATIBLE]),
            eq(col("pq_predicate"), col("ea_predicate")),
        )),
        _from("pv", "cons_proj_pol_pv"),
        _ljoin("j2", "j1", "pv", _unresolved_match("pv", col("ea_key"))),
        _where("kept", "j2", _with_scope([
            or_(eq(col("ea_key"), lit("")), not_(is_null(col("pv_node_id")))),
        ], scoped)),
        Binding(Op(OpKind.SELECT, {"project": {
            "claim_id": col("ea_claim_id"),
            "predicate": col("ea_predicate"),
            "bucket_key": col("ea_key"),
            "source_id": col("ea_source"),
            "value": col("ea_value"),
            "scope": col("ea_scope"),
            "scope_state": col("ea_scope_state"),
            "claim_semantics": col("ea_semantics"),
        }, "distinct": True}), output="projected", inputs=("kept",)),
        Binding(Op(OpKind.ORDER, {"expr": col("claim_id")}),
                output="rows", inputs=("projected",)),
    )


def effective_program() -> TransformProgram:
    """All effective rows — the input relation of the linked_pair residual."""
    return _program(
        "claim_projection.effective",
        _from("ea", "cons_proj_eff_ea"),
        Binding(Op(OpKind.SELECT, {"project": {
            "claim_id": col("ea_claim_id"),
            "predicate": col("ea_predicate"),
            "normalized_key": col("ea_key"),
            "value": col("ea_value"),
            "scope": col("ea_scope"),
            "scope_state": col("ea_scope_state"),
            "claim_semantics": col("ea_semantics"),
            "source_id": col("ea_source"),
        }}), output="projected", inputs=("ea",)),
        Binding(Op(OpKind.ORDER, {"expr": col("claim_id")}),
                output="rows", inputs=("projected",)),
    )


def policies_program() -> TransformProgram:
    """All Σ policy rows (enabled AND disabled — the assembly skips disabled
    rows loudly rather than never seeing them)."""
    return _program(
        "claim_projection.policies",
        _from("pz", "cons_proj_pol_pz"),
        Binding(Op(OpKind.SELECT, {"project": {
            "node_id": col("pz_node_id"),
            "policy_id": col("pz_policy_id"),
            "policy_kind": col("pz_kind"),
            "predicate": col("pz_predicate"),
            "value_a": col("pz_value_a"),
            "value_b": col("pz_value_b"),
            "target_predicate": col("pz_target_predicate"),
            "link_key_fn": col("pz_link_key_fn"),
            "unresolved_key": col("pz_unresolved_key"),
            "violates_values": col("pz_violates_values"),
            "enabled": col("pz_enabled"),
        }}), output="projected", inputs=("pz",)),
        Binding(Op(OpKind.ORDER, {"expr": col("node_id")}),
                output="rows", inputs=("projected",)),
    )


def rank_program(scoped: bool = False) -> TransformProgram:
    """Tier-3 ranking on the native RANK op (decision D1): effective claims
    joined to their signal attrs and the latest applicable precedent
    (greater-sequence anti-join), partitioned by (predicate, key), ordered by
    the seven ≻ components DESC with a claim_id DESC tiebreak; the leader row
    carries the separating-component basis."""
    source_class = func("coalesce", col("cs_source_class"), lit(""))
    return _program(
        f"claim_projection.rank{'.scoped' if scoped else ''}",
        _from("ea", "cons_proj_eff_ea"),
        _from("cs", "cons_proj_sig"),
        _ljoin("j1", "ea", "cs", eq(col("cs_claim_id"), col("ea_claim_id"))),
        _from("pa", "cons_proj_prec_pa"),
        _ljoin("j2", "j1", "pa", _applicable("pa")),
        _from("pb", "cons_proj_prec_pb"),
        _ljoin("j3", "j2", "pb", and_(
            _applicable("pb"), _later_precedent("pb", "pa"))),
        # Corroboration joins UPSTREAM of the SELECT that feeds RANK, because
        # RANK consumes independent_support as a ranking component. Joining it
        # to the ranked output instead could not change a ranking that has
        # already happened.
        #
        # The support surface aggregates over ALL effective claims, not just
        # the conflict participants filtered below: a value's corroboration is
        # how many distinct sources assert it, which is a property of the
        # bucket. Restricting the COUNT to participants would undercount it.
        _from("sp", "cons_proj_support"),
        _ljoin("j4", "j3", "sp", and_(
            eq(col("sp_predicate"), col("ea_predicate")),
            eq(col("sp_key"), col("ea_key")),
            eq(col("sp_value"), col("ea_value")),
        )),
        _where("kept", "j4", _with_scope([
            is_null(col("pb_node_id")),
            eq_any(col("ea_claim_id"), _CONFLICTED_CLAIMS_PARAM),
        ], scoped)),
        Binding(Op(OpKind.SELECT, {"project": {
            "bucket_p": col("ea_predicate"),
            "bucket_k": col("ea_key"),
            "claim_id": col("ea_claim_id"),
            # The peer group for the basis comparison. Claims asserting the
            # SAME value restate each other; ranking them against one another
            # answers no question the bucket asks.
            #
            # EXCEPT under typed semantics, where a registered callback — not
            # the data — decides incompatibility, so two claims can conflict
            # despite identical values. SQL cannot consult the callback, so a
            # typed claim is its OWN peer group: every peer is a rival, which
            # is exactly the pre-corroboration comparison. The library mirrors
            # this in _signals_for; the condition must stay identical on both
            # sides or the GATING parity oracle diverges.
            "claim_value": case_(
                [(is_null(col("ea_semantics")), col("ea_value"))],
                else_=col("ea_claim_id"),
            ),
            "precedent_rank": case_(
                [(and_(
                    not_(is_null(col("pa_node_id"))),
                    not_(eq(source_class, lit(""))),
                    eq(col("pa_prefer"), source_class),
                ), lit(1))],
                else_=lit(0),
            ),
            "not_superseded": func("coalesce", col("cs_not_superseded"), lit(1)),
            "effective_date": func("coalesce", col("cs_effective_date"), lit("")),
            "source_authority": func(
                "coalesce", col("cs_source_authority"), lit(0)),
            # COMPUTED corroboration, not the deployment-supplied attr: the
            # attr is unpopulatable by any producer (see _support_binding) and
            # was therefore always 0. A missing join row means the claim's
            # value is asserted by nobody, which cannot happen for a claim in
            # its own bucket — coalesce to 0 keeps it fail-safe rather than
            # NULL-poisoning the rank.
            "independent_support": func(
                "coalesce", col("sp_support"), lit(0)),
            "document_recency": func(
                "coalesce", col("cs_document_recency"), lit("")),
            "maintenance_rank": func(
                "coalesce", col("cs_maintenance_rank"), lit(0)),
        }}), output="projected", inputs=("kept",)),
        Binding(Op(OpKind.RANK, {
            "partition": [col("bucket_p"), col("bucket_k")],
            "components": _RANK_COMPONENTS,
            "tiebreak": {"expr": col("claim_id"), "desc": True},
            "rank_as": "rnk",
            "basis_as": "basis",
            # Compare the leader against the best claim asserting a DIFFERENT
            # value. Without this the basis compares two claims stating the
            # same value, ties, and reports the bucket contested — which is
            # what made corroboration unable to settle anything: a value with
            # support s is carried by s mutually-tied claims that occupy both
            # top slots, so the rival value was never reached.
            "peer": col("claim_value"),
        }), output="rows", inputs=("projected",)),
    )


def tier23_programs(scoped: bool = False) -> tuple[TransformProgram, ...]:
    return (
        pairwise_functional_program(scoped),
        pairwise_incompatible_program(scoped),
        ambiguous_members_program(scoped),
        policies_program(),
        effective_program(),
        rank_program(scoped),
    )


# --------------------------------------------------------------------------- #
# Validation pre-pass: malformed Σ / precedent / signal rows fail LOUD        #
# --------------------------------------------------------------------------- #
def validate_projection_inputs(
    effective_rows: Iterable[Mapping[str, Any]],
    claim_nodes: Iterable[Mapping[str, Any]],
    policy_nodes: Iterable[Mapping[str, Any]],
    precedent_nodes: Iterable[Mapping[str, Any]],
    *,
    typed_incompatible: Callable[[ClaimView, ClaimView], bool] | None = None,
) -> None:
    """Reject malformed inputs LOUDLY, naming node ids (module docstring for
    the per-kind rules). Disabled policy rows are skipped WITH a log line —
    never dropped silently. With ``FailureMode.GATING`` a raise here blocks
    the publish."""

    def _raise(message: str, node_id: str, **data: Any) -> None:
        raise SubstrateError(
            f"claim_projection: {message} ({node_id!r})",
            data={"node_id": node_id, **data},
        )

    buckets: dict[tuple[Any, Any], set[Any]] = {}
    unsourced: dict[tuple[Any, Any], str] = {}
    for r in effective_rows:
        cid = str(r["claim_id"])
        for field in ("predicate", "normalized_key", "value"):
            if r.get(field) is None:
                _raise(
                    f"effective claim missing required coordinate {field!r}", cid
                )
        bucket = (r.get("predicate"), r.get("normalized_key"))
        buckets.setdefault(bucket, set()).add(r.get("value"))
        if not str(r.get("source_id") or "").strip():
            unsourced.setdefault(bucket, cid)
        view = _view_of(r)
        if (
            typed_incompatible is None
            and view.semantics is not None
            and view.semantics.requires_typed_rules()
        ):
            _raise(
                "typed callback-owned semantics require a registered maintained "
                "typed incompatibility callback",
                cid,
            )

    # source_id is the distinctness key for corroboration (design decision 8).
    # An unset source_id inside a CONFLICTED bucket is not a degraded count, it
    # is a wrong one: every unsourced claim folds to the same empty key, so a
    # value asserted by three unsourced documents scores 1 and corroboration is
    # silently disabled exactly where it was meant to decide. Capping at 1
    # quietly would be a default-value-as-fallback, so it is a loud failure.
    # Unconflicted buckets are untouched — corroboration decides nothing there,
    # so deployments that never populate source_id keep publishing.
    for bucket, cid in unsourced.items():
        if len(buckets.get(bucket, ())) > 1:
            _raise(
                "effective claim has no source_id inside a conflicted bucket, "
                "so bucket-level corroboration cannot count distinct sources",
                cid,
                predicate=bucket[0],
                normalized_key=bucket[1],
            )

    skipped: list[str] = []
    pairwise_groups: dict[str, dict[str, set[str]]] = {}
    seen_policy_groups: dict[str, tuple[str, str]] = {}
    for n in policy_nodes:
        attrs = dict(n.get("attrs") or {})
        nid = str(n["node_id"])
        kind = attrs.get("policy_kind")
        if kind not in _POLICY_KINDS:
            _raise(f"unknown policy_kind {kind!r}", nid, policy_kind=kind)
        enabled = attrs.get("enabled")
        if not (_attr_flag(enabled) or enabled in (False, "false")):
            _raise(
                f"policy enabled must be 'true' or 'false', got {enabled!r}", nid
            )
        if not _attr_flag(enabled):
            skipped.append(nid)
            continue
        policy_id = attrs.get("policy_id")
        if not policy_id:
            _raise("policy without policy_id", nid)
        predicate = attrs.get("predicate")
        if not predicate:
            _raise("policy without predicate", nid)
        policy_id, predicate = str(policy_id), str(predicate)
        if kind in (KIND_FUNCTIONAL, KIND_INCOMPATIBLE):
            group = pairwise_groups.setdefault(
                predicate, {KIND_FUNCTIONAL: set(), KIND_INCOMPATIBLE: set()}
            )
            group[kind].add(policy_id)
            if kind == KIND_INCOMPATIBLE:
                if attrs.get("value_a") is None or attrs.get("value_b") is None:
                    _raise("incompatible_pair policy missing value_a/value_b", nid)
        if kind == KIND_LINKED:
            for field in ("target_predicate", "link_key_fn", "violates_values"):
                if attrs.get(field) is None:
                    _raise(f"linked_pair policy missing {field!r}", nid)
            kernel = str(attrs.get("link_key_fn"))
            if kernel not in LINK_KERNELS:
                _raise(
                    f"linked_pair policy names unregistered link kernel "
                    f"{kernel!r} (registered: {sorted(LINK_KERNELS)})", nid,
                )
            try:
                values = _violates_list(attrs.get("violates_values"))
            except (TypeError, ValueError) as exc:
                raise SubstrateError(
                    f"claim_projection: linked_pair violates_values is not a "
                    f"JSON list ({nid!r})",
                    data={"node_id": nid},
                ) from exc
            if not values:
                _raise("linked_pair policy with empty violates_values", nid)
        if kind == KIND_UNRESOLVED and attrs.get("unresolved_key") is None:
            _raise("unresolved_key policy missing unresolved_key", nid)
        # One policy_id belongs to exactly one (kind, predicate) group — the
        # constraint-name discipline (module docstring).
        group_key = (str(kind), predicate if kind != KIND_UNRESOLVED else "")
        prior = seen_policy_groups.get(policy_id)
        if prior is not None and prior != group_key:
            _raise(
                f"policy_id {policy_id!r} reused across policy groups "
                f"{prior!r} and {group_key!r}", nid,
            )
        seen_policy_groups[policy_id] = group_key
    for predicate, group in pairwise_groups.items():
        functional, incompatible = group[KIND_FUNCTIONAL], group[KIND_INCOMPATIBLE]
        if functional and incompatible:
            raise SubstrateError(
                "claim_projection: predicate carries BOTH functional and "
                "incompatible_pair policies (the library names one constraint "
                "per predicate)",
                data={"predicate": predicate},
            )
        if len(functional) > 1 or len(incompatible) > 1:
            raise SubstrateError(
                "claim_projection: enabled pairwise policies for one predicate "
                "must share a single policy_id",
                data={"predicate": predicate,
                      "policy_ids": sorted(functional | incompatible)},
            )
    if skipped:
        logger.info(
            "claim_projection: skipping %d disabled predicate_policy rows: %s",
            len(skipped), skipped,
        )

    for n in precedent_nodes:
        attrs = dict(n.get("attrs") or {})
        nid = str(n["node_id"])
        if attrs.get("prefer_source") is None:
            _raise("precedent without prefer_source", nid)
        seq = attrs.get("sequence")
        if seq is None:
            _raise("precedent without sequence", nid)
        try:
            _attr_int(seq)
        except (TypeError, ValueError):
            _raise(f"precedent sequence {seq!r} is not an integer", nid)

    for n in claim_nodes:
        attrs = dict(n.get("attrs") or {})
        nid = str(n["node_id"])
        for field in ("source_authority", "independent_support", "maintenance_rank"):
            value = attrs.get(field)
            if value is None:
                continue
            try:
                _attr_int(value)
            except (TypeError, ValueError):
                _raise(
                    f"claim signal {field!r} = {value!r} is not an integer", nid
                )
        flag = attrs.get("is_superseded")
        if flag is not None and flag not in (True, False, "true", "false"):
            _raise(
                f"claim signal is_superseded = {flag!r} is not a boolean", nid
            )


def _violates_list(raw: Any) -> list[str]:
    """Normalize a violates_values attr (JSON list, or its text) to a list."""
    if isinstance(raw, str):
        raw = json.loads(raw)
    if not isinstance(raw, list):
        raise TypeError(f"violates_values must be a list, got {type(raw).__name__}")
    return [str(v) for v in raw]


# --------------------------------------------------------------------------- #
# Python residuals over IR-produced rows (each documented in the module       #
# docstring): linked_pair findings + unresolved grouping + assembly.          #
# --------------------------------------------------------------------------- #
def _enabled_linked_policies(
    policy_rows: Iterable[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    out = []
    for r in policy_rows:
        if r.get("policy_kind") == KIND_LINKED and r.get("enabled") == "true":
            out.append(dict(r))
    out.sort(key=lambda r: str(r["node_id"]))
    return out


def _parse_element_ref(payload: Any, claim_id: str) -> ElementRef | None:
    """Parse an effective row's optional ``element_ref`` into an
    :class:`ElementRef` (``None`` for a corpus-level claim). Passthrough only —
    tier 1 does not yet persist the handle, so today this is ``None``; a
    malformed payload still fails LOUD as :class:`SubstrateError` (the GATING
    failure mode), never silently dropped to ``None``."""
    try:
        return element_ref_from_attr(payload)
    except (ValueError, TypeError) as exc:
        raise SubstrateError(
            f"claim_projection: malformed element_ref on claim {claim_id!r}: "
            f"{exc}",
            data={"claim_id": claim_id},
        ) from exc


def _view_of(row: Mapping[str, Any]) -> ClaimView:
    claim_id = str(row["claim_id"])
    try:
        semantics = claim_semantics_from_attr(row.get("claim_semantics"))
        return ClaimView(
            node_id=claim_id,
            source_id=row.get("source_id") or "",
            predicate=row["predicate"],
            normalized_key=row["normalized_key"],
            value=row["value"],
            scope=row.get("scope") or "",
            scope_state=row.get("scope_state") or "unknown",
            element_ref=_parse_element_ref(row.get("element_ref"), claim_id),
            semantics=semantics,
        )
    except (TypeError, ValueError) as exc:
        raise SubstrateError(
            f"claim_projection: malformed claim_semantics on claim {claim_id!r}: {exc}",
            data={"claim_id": claim_id},
        ) from exc


def _pair_view(row: Mapping[str, Any], suffix: str) -> ClaimView:
    return _view_of({
        "claim_id": row[f"claim_{suffix}"],
        "source_id": row.get(f"source_{suffix}"),
        "predicate": row["predicate"],
        "normalized_key": row["bucket_key"],
        "value": row[f"value_{suffix}"],
        "scope": row.get(f"scope_{suffix}"),
        "scope_state": row.get(f"scope_state_{suffix}"),
        "claim_semantics": row.get(f"semantics_{suffix}"),
    })


def _classify_pair_candidates(
    rows: Iterable[Mapping[str, Any]],
    rules: ConflictRules,
) -> list[dict[str, Any]]:
    """Apply the shared incompatibility and typed applicability operators."""
    kept: list[dict[str, Any]] = []
    for raw in rows:
        row = dict(raw)
        left, right = _pair_view(row, "a"), _pair_view(row, "b")
        if not claims_incompatible(rules, str(row["predicate"]), left, right):
            continue
        relation = applicability_relation(
            left.resolved_applicability(), right.resolved_applicability(),
        )
        typed = classify_outcome(
            left, right, incompatible=True, applicability=relation,
        )
        if relation == REL_DISJOINT and not typed.alignment_unknown:
            continue
        row["outcome"] = (
            OUTCOME_DEFINITE if relation == REL_OVERLAP else OUTCOME_POSSIBLE
        )
        row["typed"] = typed
        kept.append(row)
    return kept


def _component_members(
    edges_by_bucket: Mapping[tuple[str, str], set[tuple[str, str]]],
) -> dict[tuple[str, str], frozenset[str]]:
    """Return the one conflict component per bucket; fail loud on many (#745)."""
    result: dict[tuple[str, str], frozenset[str]] = {}
    for bucket, edges in edges_by_bucket.items():
        adjacency: dict[str, set[str]] = {}
        for left, right in edges:
            adjacency.setdefault(left, set()).add(right)
            adjacency.setdefault(right, set()).add(left)
        components: list[set[str]] = []
        remaining = set(adjacency)
        while remaining:
            todo = [remaining.pop()]
            component: set[str] = set()
            while todo:
                node = todo.pop()
                component.add(node)
                for neighbor in adjacency[node] - component:
                    remaining.discard(neighbor)
                    todo.append(neighbor)
            components.append(component)
        if len(components) > 1:
            raise SubstrateError(
                "claim_projection: multiple disconnected conflict components in "
                "one bucket need per-component row keys, which this version does "
                "not support",
                data={
                    "predicate": bucket[0],
                    "bucket_key": bucket[1],
                    "components": [sorted(c) for c in components],
                },
            )
        result[bucket] = frozenset(components[0])
    return result


def derive_linked_findings(
    effective_rows: Iterable[Mapping[str, Any]],
    linked_policies: Iterable[Mapping[str, Any]],
    rules: ConflictRules,
) -> list[dict[str, Any]]:
    """The linked_pair Python residual (reason: dynamic violates_values array
    membership + named link kernel are not expr-layer expressible). Mirrors
    conflicts.py's cross-policy pass over IR-produced rows; scope gating goes
    through the library's ``scope_relation`` (library-exact)."""
    rows = [dict(r) for r in effective_rows]
    findings: list[dict[str, Any]] = []
    for policy in linked_policies:
        policy_id = str(policy["policy_id"])
        source_predicate = str(policy["predicate"])
        target_predicate = str(policy["target_predicate"])
        kernel = LINK_KERNELS[str(policy["link_key_fn"])]
        violates = frozenset(_violates_list(policy["violates_values"]))
        targets_by_key: dict[str, list[dict[str, Any]]] = {}
        for r in rows:
            if r["predicate"] == target_predicate:
                targets_by_key.setdefault(r["normalized_key"], []).append(r)
        for c in sorted(rows, key=lambda r: str(r["claim_id"])):
            if c["predicate"] != source_predicate:
                continue
            link = kernel(c["value"])
            for t in sorted(
                targets_by_key.get(link, ()), key=lambda r: str(r["claim_id"])
            ):
                cv, tv = _view_of(c), _view_of(t)
                if not claims_incompatible(
                    rules,
                    source_predicate,
                    cv,
                    tv,
                    legacy_incompatible=lambda _source, target: (
                        target.value in violates
                    ),
                ):
                    continue
                rel = scope_relation(cv, tv)
                typed = classify_outcome(
                    cv, tv, incompatible=True, applicability=rel
                )
                if rel == REL_DISJOINT and not typed.alignment_unknown:
                    continue
                members = (str(c["claim_id"]), str(t["claim_id"]))
                sources = {cv.source_id, tv.source_id}
                findings.append({
                    "constraint": policy_id,
                    "predicate": source_predicate,
                    "bucket_key": link,
                    "members": members,
                    "outcome": (
                        OUTCOME_DEFINITE if rel == REL_OVERLAP else OUTCOME_POSSIBLE
                    ),
                    "typed": typed,
                    "locality": (
                        LOCALITY_INTERNAL if len(sources) <= 1
                        else LOCALITY_CROSS_DOC
                    ),
                    "detail": _linked_detail(policy_id, c["value"], t["value"]),
                })
    return findings


def _basis_token(raw: Any) -> str:
    """Map the RANK op's leader basis to the library vocabulary."""
    if raw == "sole":
        return BASIS_SOLE
    return str(raw)


def _resolution_outcome(basis: str) -> str:
    if basis == BASIS_SOLE or basis in _DECISIVE:
        return OUTCOME_CANONICAL
    return OUTCOME_CONTESTED  # tie, or a ranking-only lead


def assemble_rows(
    functional_pairs: Iterable[Mapping[str, Any]],
    incompatible_pairs: Iterable[Mapping[str, Any]],
    ambiguous_members: Iterable[Mapping[str, Any]],
    linked_findings: Iterable[Mapping[str, Any]],
    ranked_rows: Iterable[Mapping[str, Any]],
) -> dict[tuple[str, str], dict[str, Any]]:
    """Map program outputs (+ the linked residual) onto kept-table rows.

    Row-local / per-bucket only: finding-id hashing, member-list aggregation,
    detail formatting, decisive-basis outcome mapping. Duplicate keys fail
    loud. Ranking itself comes exclusively from the RANK program's output —
    never recomputed here (decision D1)."""
    out: dict[tuple[str, str], dict[str, Any]] = {}

    def _put(kind: str, key: str, row: dict[str, Any]) -> None:
        k = (kind, str(key))
        if k in out:
            raise SubstrateError(
                "claim_projection: duplicate kept-table key",
                data={"row_kind": kind, "row_key": str(key)},
            )
        out[k] = row

    conflicted_edges: dict[tuple[str, str], set[tuple[str, str]]] = {}
    for constraint_kind, pair_rows in (
        (KIND_FUNCTIONAL, functional_pairs),
        (KIND_INCOMPATIBLE, incompatible_pairs),
    ):
        for r in pair_rows:
            members = (str(r["claim_a"]), str(r["claim_b"]))
            constraint = str(r["constraint"])
            bucket = (str(r["predicate"]), str(r["bucket_key"]))
            left, right = _pair_view(r, "a"), _pair_view(r, "b")
            typed = classify_outcome(
                left,
                right,
                incompatible=True,
                applicability=applicability_relation(
                    left.resolved_applicability(), right.resolved_applicability()
                ),
            )
            conflicted_edges.setdefault(bucket, set()).add(members)
            _put(
                ROW_KIND_FINDING,
                finding_row_key(constraint, members),
                _finding_value_row(
                    predicate=str(r["predicate"]),
                    bucket_key=str(r["bucket_key"]),
                    constraint_id=constraint,
                    constraint_kind=constraint_kind,
                    outcome=str(r["outcome"]),
                    # The pair programs emit ontology-vocabulary locality
                    # directly; normalize through the shared map regardless.
                    locality=(
                        LOCALITY_INTERNAL
                        if r["locality"] == LOCALITY_INTERNAL
                        else LOCALITY_CROSS_DOC
                    ),
                    member_ids=members,
                    detail=_pairwise_detail(
                        str(r["predicate"]), r["value_a"], r["value_b"]
                    ),
                    typed=typed,
                ),
            )

    buckets: dict[tuple[str, str], list[dict[str, Any]]] = {}
    for r in ambiguous_members:
        buckets.setdefault(
            (str(r["predicate"]), str(r["bucket_key"])), []
        ).append(dict(r))
    for (predicate, key), members in sorted(buckets.items()):
        if len(members) < 2:
            continue  # a sole claim on an unresolved key is not a finding
        member_ids = sorted(str(m["claim_id"]) for m in members)
        sources = {m.get("source_id") for m in members}
        views = [_view_of({
            "claim_id": m["claim_id"],
            "predicate": m["predicate"],
            "normalized_key": m["bucket_key"],
            "value": m.get("value", ""),
            "scope": m.get("scope"),
            "scope_state": m.get("scope_state"),
            "claim_semantics": m.get("claim_semantics"),
            "source_id": m.get("source_id"),
        }) for m in members]
        evidence = [
            _dimension_evidence(views[0], view) for view in views[1:]
        ]
        ae = tuple(dim for dim in ALIGNMENT_DIMENSIONS
                   if all(dim in item[0] for item in evidence))
        au = tuple(dim for dim in ALIGNMENT_DIMENSIONS if dim not in ae)
        pe = tuple(dim for dim in APPLICABILITY_DIMENSIONS
                   if all(dim in item[2] for item in evidence))
        pu = tuple(dim for dim in APPLICABILITY_DIMENSIONS if dim not in pe)
        typed = TypedOutcome(
            OUTCOME_NOT_COMPARABLE,
            tuple(dim for dim in ae if dim != "subject"),
            tuple(dict.fromkeys((*au, "subject"))),
            pe,
            pu,
        )
        _put(
            ROW_KIND_FINDING,
            finding_row_key(CONSTRAINT_AMBIGUOUS, member_ids),
            _finding_value_row(
                predicate=predicate,
                bucket_key=key,
                constraint_id=CONSTRAINT_AMBIGUOUS,
                constraint_kind=KIND_UNRESOLVED,
                outcome=OUTCOME_NOT_COMPARABLE,
                locality=(
                    LOCALITY_INTERNAL if len(sources) <= 1 else LOCALITY_CROSS_DOC
                ),
                member_ids=member_ids,
                detail=_AMBIGUOUS_DETAIL,
                typed=typed,
            ),
        )

    for f in linked_findings:
        _put(
            ROW_KIND_FINDING,
            finding_row_key(str(f["constraint"]), f["members"]),
            _finding_value_row(
                predicate=str(f["predicate"]),
                bucket_key=str(f["bucket_key"]),
                constraint_id=str(f["constraint"]),
                constraint_kind=KIND_LINKED,
                outcome=str(f["outcome"]),
                locality=str(f["locality"]),
                member_ids=f["members"],
                detail=str(f["detail"]),
                typed=f.get("typed"),
            ),
        )

    component_members = _component_members(conflicted_edges)
    ranked_by_bucket: dict[tuple[str, str], list[dict[str, Any]]] = {}
    for r in ranked_rows:
        ranked_by_bucket.setdefault(
            (str(r["bucket_p"]), str(r["bucket_k"])), []
        ).append(dict(r))
    for predicate, key in sorted(component_members):
        participant_ids = component_members[(predicate, key)]
        members = [
            row for row in ranked_by_bucket.get((predicate, key), [])
            if str(row["claim_id"]) in participant_ids
        ]
        if not members:
            raise SubstrateError(
                "claim_projection: conflicted bucket missing from the RANK "
                "output (tier-3 input drift)",
                data={"predicate": predicate, "bucket_key": key},
            )
        members.sort(key=lambda r: int(r["rnk"]))
        ranked_ids = [str(m["claim_id"]) for m in members]
        basis = _basis_token(members[0]["basis"])
        outcome = _resolution_outcome(basis)
        # Only claims asserting a DIFFERENT value were beaten; same-value
        # claims corroborate the winner (mirrors resolve_bucket's set, which
        # the GATING oracle compares against).
        winner_value = members[0].get("claim_value")
        superseded_ids = [
            str(m["claim_id"]) for m in members[1:]
            if m.get("claim_value") != winner_value
        ]
        _put(
            ROW_KIND_RESOLUTION,
            bucket_row_key(predicate, key),
            _resolution_value_row(
                predicate=predicate,
                bucket_key=key,
                outcome=outcome,
                winner=ranked_ids[0] if outcome == OUTCOME_CANONICAL else "",
                basis=basis,
                ranked_ids=ranked_ids,
                superseded_ids=superseded_ids,
            ),
        )
    return out


# --------------------------------------------------------------------------- #
# Executors: lowered SQL against a Chronos session / the interpreter          #
# --------------------------------------------------------------------------- #
_T1_SENTINEL_GUARD = (
    "claim_projection: tier-1 sentinel absent in okg.claim_effective — the "
    "claim_effective derivation must be registered and run FIRST in the same "
    "cycle (DerivationRegistry sorted order guarantees this when both rows "
    "are registered); reading raw claim nodes instead is forbidden"
)


def _require_tier1_sentinel(checkout: Any) -> None:
    from okg.substrate.derived_artifacts.maintained_derivation import (
        SentinelSpec,
        has_prior_sentinel,
    )

    spec = SentinelSpec(
        pk_columns=list(_T1_KEY_COLUMNS), cadence_column=_T1_CADENCE_COLUMN
    )
    try:
        present = has_prior_sentinel(
            checkout, table=CLAIM_EFFECTIVE_TABLE, spec=spec
        )
    except Exception as exc:  # table unregistered => same loud failure
        raise SubstrateError(
            _T1_SENTINEL_GUARD, data={"reason": "table_unavailable"}
        ) from exc
    if not present:
        raise SubstrateError(_T1_SENTINEL_GUARD, data={"reason": "no_sentinel"})


_EFFECTIVE_ROWS_SQL = (
    "SELECT row_key AS claim_id, predicate, normalized_key, value, scope, "
    f"scope_state, claim_semantics, source_id FROM {CLAIM_EFFECTIVE_TABLE} "
    "WHERE row_kind = :k ORDER BY row_key"
)
_NODES_BY_SUBTYPE_SQL = (
    "SELECT node_id, attrs FROM okg.graph_nodes WHERE subtype = :s ORDER BY node_id"
)


def _read_effective_rows(checkout: Any) -> list[dict[str, Any]]:
    return [
        dict(r) for r in checkout.query(
            _EFFECTIVE_ROWS_SQL, {"k": ROW_KIND_EFFECTIVE}
        )
    ]


def _read_nodes(checkout: Any, subtype: str) -> list[dict[str, Any]]:
    return [
        {"node_id": str(r["node_id"]), "attrs": dict(r.get("attrs") or {})}
        for r in checkout.query(_NODES_BY_SUBTYPE_SQL, {"s": subtype})
    ]


def compute_rows(
    session: Any,
    *,
    dirty_buckets: frozenset[str] | set[str] | None = None,
    policy_nodes: Iterable[Mapping[str, Any]] | None = None,
    typed_incompatible: Callable[[ClaimView, ClaimView], bool] | None = None,
    run_program: Callable[
        [TransformProgram, Mapping[str, Any]], list[dict[str, Any]]
    ] | None = None,
) -> dict[tuple[str, str], dict[str, Any]]:
    """Derive the kept rows at ``session`` by lowering + executing the tier-2/3
    IR programs through the (generation-pinned) Chronos checkout session.

    ``dirty_buckets`` (bucket tokens) selects the SCOPED program variants:
    only those (predicate, key) buckets are read/derived; the caller owns
    carrying every other row forward. Bucket-scoped mode requires NO enabled
    linked_pair policies (design §4 v1 — the incremental callback classifies
    that case as a full recompute); violating that here raises loudly.
    """
    if session is not None:
        _require_tier1_sentinel(session)
        effective_rows = _read_effective_rows(session)
        policy_nodes = _read_nodes(session, POLICY_SUBTYPE)
        validate_projection_inputs(
            effective_rows,
            _read_nodes(session, CLAIM_SUBTYPE),
            policy_nodes,
            _read_nodes(session, PRECEDENT_SUBTYPE),
            typed_incompatible=typed_incompatible,
        )
    if policy_nodes is None:
        raise ValueError("claim_projection: policy_nodes are required without a session")
    policy_nodes = list(policy_nodes)
    rules, _ = _compile_rules(
        policy_nodes, typed_incompatible=typed_incompatible
    )
    scoped = dirty_buckets is not None
    regs = registries()
    extra_params = (
        {_DIRTY_BUCKETS_PARAM: sorted(dirty_buckets)} if scoped else {}
    )
    extra_params[_CONFLICTED_CLAIMS_PARAM] = [_NO_CLAIM_TOKEN]

    def _run(
        program: TransformProgram, dynamic_params: Mapping[str, Any]
    ) -> list[dict[str, Any]]:
        sql, params = lower_query(program, regs)
        params.update(dynamic_params)
        return [dict(r) for r in session.query(to_chronos_sql(sql), params)]

    run = run_program or _run
    func_pairs = _classify_pair_candidates(
        run(pairwise_functional_program(
            scoped, typed_callback=typed_incompatible is not None
        ), extra_params), rules
    )
    incomp_pairs = _classify_pair_candidates(
        run(pairwise_incompatible_program(
            scoped, typed_callback=typed_incompatible is not None
        ), extra_params), rules
    )
    ambiguous = run(ambiguous_members_program(scoped), extra_params)
    policies = run(policies_program(), extra_params)
    effective = run(effective_program(), extra_params)
    participant_ids = sorted({
        str(pair[field])
        for pair in (*func_pairs, *incomp_pairs)
        for field in ("claim_a", "claim_b")
    })
    extra_params[_CONFLICTED_CLAIMS_PARAM] = participant_ids or [_NO_CLAIM_TOKEN]
    ranked = run(rank_program(scoped), extra_params)
    linked_policies = _enabled_linked_policies(policies)
    if scoped and linked_policies:
        raise SubstrateError(
            "claim_projection: bucket-scoped recompute with enabled "
            "linked_pair policies (cross-bucket dirtiness) — the incremental "
            "classifier must have forced a full recompute",
            data={"policies": [str(p["node_id"]) for p in linked_policies]},
        )
    linked = (
        derive_linked_findings(effective, linked_policies, rules)
        if linked_policies else []
    )
    return assemble_rows(func_pairs, incomp_pairs, ambiguous, linked, ranked)


def compute_rows_interpreted(
    effective_rows: Iterable[Mapping[str, Any]],
    claim_nodes: Iterable[Mapping[str, Any]],
    policy_nodes: Iterable[Mapping[str, Any]],
    precedent_nodes: Iterable[Mapping[str, Any]] = (),
    *,
    dirty_buckets: frozenset[str] | set[str] | None = None,
    typed_incompatible: Callable[[ClaimView, ClaimView], bool] | None = None,
) -> dict[tuple[str, str], dict[str, Any]]:
    """The SAME programs + residuals, executed by the reference interpreter
    over in-memory surfaces (unit-test denotation; no DB). Runs the same
    validation pre-pass as the production path."""
    from okg.substrate.transform import evaluate

    effective_rows = list(effective_rows)
    claim_nodes = list(claim_nodes)
    policy_nodes = list(policy_nodes)
    precedent_nodes = list(precedent_nodes)
    validate_projection_inputs(
        effective_rows, claim_nodes, policy_nodes, precedent_nodes,
        typed_incompatible=typed_incompatible,
    )
    surfaces = interpreter_surfaces(
        effective_rows, claim_nodes, policy_nodes, precedent_nodes
    )
    def _run(
        program: TransformProgram, dynamic_params: Mapping[str, Any]
    ) -> list[dict[str, Any]]:
        result = evaluate(program, surfaces, kernels={}, params=dict(dynamic_params))
        return list(result.values["rows"])

    return compute_rows(
        None,
        dirty_buckets=dirty_buckets,
        policy_nodes=policy_nodes,
        typed_incompatible=typed_incompatible,
        run_program=_run,
    )


def rows_as_truth(
    rows: Mapping[tuple[str, str], Mapping[str, Any]]
) -> dict[tuple[str, str], tuple[Any, ...]]:
    """Flatten assembled rows to the oracle's {key tuple -> value tuple} shape."""
    return {
        key: tuple(row[column] for column in VALUE_COLUMNS)
        for key, row in rows.items()
    }


# --------------------------------------------------------------------------- #
# The library-as-oracle: plain-SQL reads -> ConflictRules compilation ->      #
# derive_conflict_findings + resolve_bucket -> identical flatten.             #
# --------------------------------------------------------------------------- #
def _compile_rules(
    policy_nodes: Iterable[Mapping[str, Any]],
    *,
    typed_incompatible: Callable[[ClaimView, ClaimView], bool] | None = None,
) -> tuple[ConflictRules, dict[str, str]]:
    """Compile enabled Σ policy nodes into the library's ConflictRules value.

    Returns (rules, constraint_kind_by_id) — the latter maps each pairwise /
    linked constraint name to its policy kind for the flatten."""
    functional_by_pred: dict[str, str] = {}
    pairs_by_pred: dict[str, set[tuple[str, str]]] = {}
    pair_constraint_by_pred: dict[str, str] = {}
    unresolved: set[str] = {""}
    linked: list[dict[str, Any]] = []
    for n in sorted(policy_nodes, key=lambda n: str(n["node_id"])):
        attrs = dict(n.get("attrs") or {})
        if not _attr_flag(attrs.get("enabled")):
            continue
        kind = attrs.get("policy_kind")
        predicate = str(attrs.get("predicate"))
        policy_id = str(attrs.get("policy_id"))
        if kind == KIND_FUNCTIONAL:
            functional_by_pred[predicate] = policy_id
        elif kind == KIND_INCOMPATIBLE:
            pairs_by_pred.setdefault(predicate, set()).add(
                (str(attrs.get("value_a")), str(attrs.get("value_b")))
            )
            pair_constraint_by_pred[predicate] = policy_id
        elif kind == KIND_UNRESOLVED:
            unresolved.add(str(attrs.get("unresolved_key")))
        elif kind == KIND_LINKED:
            linked.append({
                "policy_id": policy_id,
                "predicate": predicate,
                "target_predicate": str(attrs.get("target_predicate")),
                "kernel": LINK_KERNELS[str(attrs.get("link_key_fn"))],
                "violates": frozenset(_violates_list(attrs.get("violates_values"))),
            })

    def _incompatible(predicate: str, va: str, vb: str) -> bool:
        if predicate in functional_by_pred:
            return va != vb
        pairs = pairs_by_pred.get(predicate, set())
        return (va, vb) in pairs or (vb, va) in pairs

    def _constraint_for(predicate: str) -> str:
        if predicate in functional_by_pred:
            return functional_by_pred[predicate]
        return pair_constraint_by_pred[predicate]

    cross = tuple(
        CrossPredicatePolicy(
            constraint=p["policy_id"],
            source_predicate=p["predicate"],
            target_predicate=p["target_predicate"],
            link_key=(lambda c, kern=p["kernel"]: kern(c.value)),
            violates=(lambda c, t, vs=p["violates"]: t.value in vs),
            detail=(
                lambda c, t, pid=p["policy_id"]:
                _linked_detail(pid, c.value, t.value)
            ),
        )
        for p in linked
    )
    rules = ConflictRules(
        conflict_predicates=frozenset(functional_by_pred) | frozenset(pairs_by_pred),
        incompatible=_incompatible,
        constraint_for=_constraint_for,
        canonical_key=None,  # decision D3: keys canonicalize at extraction time
        unresolved_keys=frozenset(unresolved),
        cross_policies=cross,
        typed_incompatible=typed_incompatible,
    )
    kind_by_constraint = {pid: KIND_FUNCTIONAL for pid in functional_by_pred.values()}
    kind_by_constraint.update(
        {pid: KIND_INCOMPATIBLE for pid in pair_constraint_by_pred.values()}
    )
    kind_by_constraint.update({p["policy_id"]: KIND_LINKED for p in linked})
    return rules, kind_by_constraint


def computed_support(
    views: Sequence[ClaimView],
    claim_attrs: Mapping[str, Mapping[str, Any]] | None = None,
) -> dict[str, int]:
    """claim_id -> bucket-level corroboration, restated from the views.

    A THIRD, deliberately independent statement of the rule the SQL surface
    (``_support_binding``) and the interpreter mirror (``support_surface_rows``)
    each express. The parity oracle exists to catch those two disagreeing, so
    it must not reach for either of them: sharing a helper would make the
    oracle agree with the derivation by construction, going vacuous exactly
    where the new logic lives. The DECLARED-ORIGIN rule is restated here in
    full for that reason; only ``_attr_text`` (the module's single rendering of
    postgres ``attrs->>``) is common, as it already is for every signal.

    ``claim_attrs`` is claim_id -> that claim's node attrs. Absent, every claim
    is its own origin — the declare-nothing corpus, whose count is unchanged
    from before this rule existed.
    """
    attrs_by_id = claim_attrs or {}

    def _origin(view: ClaimView) -> str:
        declared = _attr_text(
            attrs_by_id.get(view.node_id, {}).get(DECLARED_ORIGIN_ATTR)
        )
        # An empty declaration declares nothing (SQL: NULLIF(..., '')), so the
        # claim falls back to being its own origin = its document identity.
        return declared if declared else (view.source_id or "")

    origins: dict[tuple[str, str, str], set[str]] = {}
    for v in views:
        origins.setdefault(
            (v.predicate, v.normalized_key, v.value), set()
        ).add(_origin(v))
    return {
        v.node_id: len(origins[(v.predicate, v.normalized_key, v.value)])
        for v in views
    }


def _signals_for(
    view: ClaimView, attrs: Mapping[str, Any], *, support: int
) -> ClaimSignals:
    """The deployment-supplied signal surface, mirrored to the SQL text/casts
    (module docstring): NULL→default, ``attrs->>`` text rendering, bigint
    parse. Scope comes from the EFFECTIVE row, not the raw node attr.

    ``support`` is the ENGINE-COMPUTED corroboration and overrides any
    supplied ``independent_support`` attr, which no producer can compute
    correctly (see ``_support_binding``) and which the derivation no longer
    reads."""

    def _text(name: str) -> str:
        value = _attr_text(attrs.get(name))
        return value if value is not None else ""

    def _num(name: str) -> int:
        value = attrs.get(name)
        return _attr_int(value) if value is not None else 0

    return ClaimSignals(
        claim_id=view.node_id,
        source_class=_text("source_class"),
        scope=view.scope,
        is_superseded=_attr_flag(attrs.get("is_superseded")),
        effective_date=_text("effective_date"),
        source_authority=_num("source_authority"),
        independent_support=support,
        # Mirrors the RANK op's `peer` EXACTLY (see rank_program's claim_value
        # projection): the basis compares the leader against the best claim
        # asserting a DIFFERENT value. Omitting it would leave the oracle on
        # the legacy claim-adjacent comparison and diverge on every
        # corroborated bucket — and the oracle is GATING, so that blocks
        # publishes rather than failing quietly.
        #
        # A typed claim is its own peer group: a registered callback, not the
        # data, decides its incompatibility, so equal values do NOT imply
        # restatement there.
        value=view.node_id if view.semantics is not None else view.value,
        # ...and that node_id is a SURROGATE, not an asserted value: it ranks,
        # but nothing may write it back into a claim (precedence.ClaimSignals).
        value_is_surrogate=view.semantics is not None,
        document_recency=_text("document_recency"),
        maintenance_rank=_num("maintenance_rank"),
    )


def library_truth_rows(
    effective_rows: Iterable[Mapping[str, Any]],
    claim_nodes: Iterable[Mapping[str, Any]],
    policy_nodes: Iterable[Mapping[str, Any]],
    precedent_nodes: Iterable[Mapping[str, Any]] = (),
    *,
    typed_incompatible: Callable[[ClaimView, ClaimView], bool] | None = None,
) -> dict[tuple[str, str], tuple[Any, ...]]:
    """Ground truth from the pure library over the same inputs: compile the
    policy rows into ``ConflictRules``, run ``derive_conflict_findings`` for
    tier 2, then ``resolve_bucket`` per conflicted bucket (same-bucket
    pairwise findings only — module docstring) for tier 3, flattened onto the
    identical kept-row encoding."""
    effective_rows = list(effective_rows)
    claim_nodes = list(claim_nodes)
    rules, kind_by_constraint = _compile_rules(
        policy_nodes, typed_incompatible=typed_incompatible
    )
    views = [_view_of(r) for r in effective_rows]
    views_by_id = {v.node_id: v for v in views}
    attrs_by_id = {str(n["node_id"]): dict(n.get("attrs") or {}) for n in claim_nodes}
    # engine-computed corroboration, restated independently of both the
    # SQL surface and the interpreter mirror (see computed_support). The attrs
    # carry the DECLARED origin the count is over.
    support_by_claim = computed_support(views, attrs_by_id)

    findings = derive_conflict_findings(views, rules)

    out: dict[tuple[str, str], tuple[Any, ...]] = {}
    conflicted_edges: dict[tuple[str, str], set[tuple[str, str]]] = {}
    for f in findings:
        if f.constraint == CONSTRAINT_AMBIGUOUS:
            constraint_kind = KIND_UNRESOLVED
        else:
            constraint_kind = kind_by_constraint[f.constraint]
        first = views_by_id[f.member_claim_ids[0]]
        if constraint_kind == KIND_LINKED:
            # The source member carries the policy's source predicate; the
            # bucket key is the link key derived from its value.
            source_policy = next(
                p for p in rules.cross_policies if p.constraint == f.constraint
            )
            source_view = next(
                views_by_id[m] for m in f.member_claim_ids
                if views_by_id[m].predicate == source_policy.source_predicate
            )
            predicate = source_policy.source_predicate
            bucket_key = source_policy.link_key(source_view)
        else:
            predicate, bucket_key = first.predicate, first.normalized_key
            if constraint_kind in (KIND_FUNCTIONAL, KIND_INCOMPATIBLE):
                conflicted_edges.setdefault((predicate, bucket_key), set()).add(
                    tuple(f.member_claim_ids)
                )
        row = _finding_value_row(
            predicate=predicate,
            bucket_key=bucket_key,
            constraint_id=f.constraint,
            constraint_kind=constraint_kind,
            outcome=f.outcome,
            locality=f.locality,
            member_ids=f.member_claim_ids,
            detail=f.detail,
            typed=TypedOutcome(
                f.typed_category,
                f.alignment_established,
                f.alignment_unknown,
                f.applicability_established,
                f.applicability_unknown,
            ),
        )
        out[(ROW_KIND_FINDING, f.node_id)] = tuple(
            row[column] for column in VALUE_COLUMNS
        )

    precedents = [
        Precedent(
            prefer_source=str(n["attrs"].get("prefer_source")),
            scope=(_attr_text(n["attrs"].get("scope")) or ""),
            sequence=_attr_int(n["attrs"].get("sequence")),
        )
        for n in sorted(precedent_nodes, key=lambda n: str(n["node_id"]))
    ]
    component_members = _component_members(conflicted_edges)
    for predicate, key in sorted(component_members):
        members = [
            v for v in views
            if v.node_id in component_members[(predicate, key)]
        ]
        resolution = resolve_bucket(
            [
                _signals_for(
                    v, attrs_by_id.get(v.node_id, {}), support=support_by_claim[v.node_id]
                )
                for v in members
            ],
            precedents,
        )
        row = _resolution_value_row(
            predicate=predicate,
            bucket_key=key,
            outcome=resolution.outcome,
            winner=resolution.canonical_claim_id,
            basis=resolution.basis,
            ranked_ids=resolution.ranked_claim_ids,
            superseded_ids=resolution.superseded_claim_ids,
        )
        out[(ROW_KIND_RESOLUTION, bucket_row_key(predicate, key))] = tuple(
            row[column] for column in VALUE_COLUMNS
        )
    return out


def claim_projection_truth(
    checkout: Any,
    *,
    typed_incompatible: Callable[[ClaimView, ClaimView], bool] | None = None,
) -> dict[tuple[str, str], tuple[Any, ...]]:
    """The CallableCorrectnessOracle recompute: plain-SQL reads + the library."""
    _require_tier1_sentinel(checkout)
    return library_truth_rows(
        _read_effective_rows(checkout),
        _read_nodes(checkout, CLAIM_SUBTYPE),
        _read_nodes(checkout, POLICY_SUBTYPE),
        _read_nodes(checkout, PRECEDENT_SUBTYPE),
        typed_incompatible=typed_incompatible,
    )


# --------------------------------------------------------------------------- #
# Repair-selection inputs — the conflicted buckets + precedents the pure       #
# selection layer (claim_model.selection.select_repairs) consumes. Factored    #
# from library_truth_rows' conflicted-bucket detection so the buckets are the  #
# SAME (predicate, key) set the kept bucket_resolution rows resolve; each      #
# bucket carries its members' ClaimSignals so selection can consult the        #
# precedence resolution WITHOUT re-ranking (the resolution is resolve_bucket's #
# alone). Used by derived_artifacts.repair_materialization.                    #
# --------------------------------------------------------------------------- #
def read_projection_inputs(
    checkout: Any,
) -> tuple[
    list[dict[str, Any]],
    list[dict[str, Any]],
    list[dict[str, Any]],
    list[dict[str, Any]],
]:
    """Read the tier-2/3 inputs at ``checkout``: (effective rows, claim nodes,
    policy nodes, precedent nodes) — the same relations ``compute_rows``
    validates, exposed so repair materialization reads them once more from the
    generation-pinned checkout (full recompute only)."""
    return (
        _read_effective_rows(checkout),
        _read_nodes(checkout, CLAIM_SUBTYPE),
        _read_nodes(checkout, POLICY_SUBTYPE),
        _read_nodes(checkout, PRECEDENT_SUBTYPE),
    )


def build_conflict_buckets(
    effective_rows: Iterable[Mapping[str, Any]],
    claim_nodes: Iterable[Mapping[str, Any]],
    policy_nodes: Iterable[Mapping[str, Any]],
    precedent_nodes: Iterable[Mapping[str, Any]] = (),
    *,
    typed_incompatible: Callable[[ClaimView, ClaimView], bool] | None = None,
) -> tuple[tuple[ConflictBucket, ...], tuple[Precedent, ...]]:
    """Reconstruct the conflicted (predicate, key) buckets + applicable
    precedents for repair selection.

    Mirrors :func:`library_truth_rows`' conflicted-bucket detection EXACTLY
    (only functional / incompatible_pair pairwise findings drive a resolution;
    not-comparable and linked_pair findings do not — spec §3), so the buckets
    returned are the same set the kept ``bucket_resolution`` rows carry. Each
    :class:`ConflictBucket` is keyed by :func:`bucket_row_key` (the kept row
    key, so selection rows join the projection) and carries its members'
    :class:`ClaimSignals` in a deterministic (claim-id-sorted) order, so the
    downstream ``select_repairs`` output is byte-stable under input shuffling.
    Pure: no DB, no mutation."""
    effective_rows = list(effective_rows)
    claim_nodes = list(claim_nodes)
    rules, kind_by_constraint = _compile_rules(
        policy_nodes, typed_incompatible=typed_incompatible
    )
    views = [_view_of(r) for r in effective_rows]
    views_by_id = {v.node_id: v for v in views}
    attrs_by_id = {
        str(n["node_id"]): dict(n.get("attrs") or {}) for n in claim_nodes
    }
    # engine-computed corroboration, restated independently of both the
    # SQL surface and the interpreter mirror (see computed_support). The attrs
    # carry the DECLARED origin the count is over.
    support_by_claim = computed_support(views, attrs_by_id)

    findings = derive_conflict_findings(views, rules)
    conflicted_edges: dict[tuple[str, str], set[tuple[str, str]]] = {}
    for f in findings:
        if f.constraint == CONSTRAINT_AMBIGUOUS:
            continue
        if kind_by_constraint[f.constraint] not in (
            KIND_FUNCTIONAL, KIND_INCOMPATIBLE
        ):
            continue
        first = views_by_id[f.member_claim_ids[0]]
        conflicted_edges.setdefault(
            (first.predicate, first.normalized_key), set()
        ).add(tuple(f.member_claim_ids))

    buckets: list[ConflictBucket] = []
    component_members = _component_members(conflicted_edges)
    for predicate, key in sorted(component_members):
        members = sorted(
            (v for v in views if v.node_id in component_members[(predicate, key)]),
            key=lambda v: v.node_id,
        )
        signals = tuple(
            _signals_for(
                v, attrs_by_id.get(v.node_id, {}), support=support_by_claim[v.node_id]
            )
            for v in members
        )
        buckets.append(
            ConflictBucket(bucket_key=bucket_row_key(predicate, key), signals=signals)
        )

    precedents = tuple(
        Precedent(
            prefer_source=str(n["attrs"].get("prefer_source")),
            scope=(_attr_text(n["attrs"].get("scope")) or ""),
            sequence=_attr_int(n["attrs"].get("sequence")),
        )
        for n in sorted(precedent_nodes, key=lambda n: str(n["node_id"]))
    )
    return tuple(buckets), precedents


# --------------------------------------------------------------------------- #
# Registry row (#444 kit), tier-1's shape with the bucket-scoped incremental. #
# --------------------------------------------------------------------------- #
def claim_projection_derivation_row(
    deployment: str | None = None,
    *,
    typed_incompatible: Callable[[ClaimView, ClaimView], bool] | None = None,
) -> Any:
    """Build the ``claim_projection`` derivation row.

    ``typed_incompatible`` registers the deployment callback used by the pure
    library, transform residual, parity oracle, and repair-selection rebuild.
    Callback-owned typed semantics fail loudly when it is absent.

    ``deployment`` opts the deployment into repair-selection materialization
    (PACT consistency-repair-objective, task.repair.materialize +
    task.repair.differential): when set, EVERY generation records the complete
    selected repair (r*) into ``okg.repair_selections`` under that deployment
    label — a FULL recompute via
    :func:`...repair_materialization.materialize_selection_for_generation`, a
    bucket-scoped incremental via
    :func:`...repair_materialization.materialize_selection_differential` (dirty
    buckets re-selected through the pure layer, untouched buckets carried forward
    from the prior generation, so the incremental and from-scratch paths select
    identically, spec §9.8). Left ``None`` (the default, and every existing test
    caller) the derivation is unchanged — findings + resolutions only, no
    selection sink.
    """
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
        FULL_REASON_LINKED_PAIR,
        FULL_REASON_POLICY_CHANGE,
        FULL_REASON_PRECEDENT_SCOPE_UNPROVABLE,
        FULL_REASON_SCOPE_UNAVAILABLE,
        FULL_REASON_SELECTION_SCHEMA_UPGRADE,
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

    sentinel = SentinelSpec(
        pk_columns=list(KEY_COLUMNS), cadence_column=CADENCE_COLUMN
    )
    oracle = CallableCorrectnessOracle(
        lambda checkout: claim_projection_truth(
            checkout, typed_incompatible=typed_incompatible
        ),
        key_columns=list(KEY_COLUMNS),
        value_columns=list(VALUE_COLUMNS),
        label=DERIVATION_ID,
    )

    def _existing_rows(checkout: Any) -> dict[tuple[str, str], dict[str, Any]]:
        columns = ", ".join((*KEY_COLUMNS, *VALUE_COLUMNS))
        rows = checkout.query(
            f"SELECT {columns} FROM {CLAIM_PROJECTION_TABLE} "
            f"WHERE NOT (row_kind = :s AND row_key = :s)",
            {"s": SENTINEL_TOKEN},
        )
        return {
            (str(r["row_kind"]), str(r["row_key"])): dict(r) for r in rows
        }

    def _upserts(
        rows: Mapping[tuple[str, str], Mapping[str, Any]]
    ) -> list[dict[str, Any]]:
        return [
            {"row_kind": k[0], "row_key": k[1], **rows[k]} for k in sorted(rows)
        ]

    def _next_cadence(ctx: MaintainContext) -> int:
        elapsed = read_cadence_counter(
            ctx.checkout, table=CLAIM_PROJECTION_TABLE, spec=sentinel
        )
        return (elapsed or 0) + 1

    def _full(
        ctx: MaintainContext, *, cadence_counter: int = 0,
        mode: str = "full_recount", dirty_bucket_count: int | None = None,
        full_reason: str | None = None,
    ) -> MaintainResult:
        rows = compute_rows(
            ctx.checkout, typed_incompatible=typed_incompatible
        )
        existing = _existing_rows(ctx.checkout)
        deletes = [
            {"row_kind": k[0], "row_key": k[1]}
            for k in sorted(existing) if k not in rows
        ]
        # Materialize the selected repair (r*) for the complete resolved state
        # (task.repair.materialize). Full recompute: the buckets `rows` holds ARE
        # the complete generation, so the recorded selection is complete. A
        # bucket-scoped incremental cycle materializes it DIFFERENTIALLY instead
        # (see `_incremental` — dirty re-selected, untouched carried forward). The
        # pure layer decides r*; this only records it, in the same connection as
        # (immediately before) the kept-row write.
        if deployment is not None:
            from okg.substrate.derived_artifacts.repair_materialization import (
                materialize_selection_for_generation,
            )

            materialize_selection_for_generation(
                ctx.checkout,
                ctx.store.context.db,
                deployment=deployment,
                generation_id=ctx.generation_id,
                kept_rows=rows,
                typed_incompatible=typed_incompatible,
            )
        return MaintainResult(
            upserts=_upserts(rows),
            deletes=deletes,
            cadence_counter=cadence_counter,
            dirty_bucket_count=dirty_bucket_count,
            mode=mode,
            full_reason=full_reason,
        )

    def _full_under_incremental(ctx: MaintainContext, cause: str,
                                dirty_count: int, detail: str = "") -> MaintainResult:
        """A full recompute the incremental callback fell back to (mode stays
        ``incremental``). ``cause`` is a member of the recorded-cause taxonomy
        (:data:`FULL_RECOMPUTE_CAUSES`) surfaced as ``summary['full_reason']`` —
        batch is a recorded fallback, never the steady state (spec §9.8). A full
        recompute with no recorded cause is a defect."""
        logger.info(
            "claim_projection: full recompute under incremental dispatch "
            "(cause=%s%s)", cause, f": {detail}" if detail else "",
        )
        return _full(
            ctx, cadence_counter=_next_cadence(ctx), mode="incremental",
            dirty_bucket_count=dirty_count, full_reason=cause,
        )

    def _noop(ctx: MaintainContext) -> MaintainResult:
        return MaintainResult(
            upserts=[], deletes=[], cadence_counter=_next_cadence(ctx),
            dirty_bucket_count=0, mode="incremental",
        )

    def _row_bucket_token(row: Mapping[str, Any]) -> str | None:
        """The bucket a kept row belongs to; linked rows have none (their
        bucket_key is a link key, not a bucket coordinate)."""
        if row.get("constraint_kind") == KIND_LINKED:
            return None
        return bucket_token(str(row["predicate"]), str(row["bucket_key"]))

    def _classify_changes(
        checkout: Any, changed: tuple[str, ...]
    ) -> tuple[str | None, str, set[str]]:
        """(full-cause | None, detail, dirty claim ids) for this cycle's changes.

        The cause is a member of the recorded-cause taxonomy
        (:data:`FULL_RECOMPUTE_CAUSES`); ``detail`` is the free-text log rider.
        A full recompute with no recorded cause is a defect (spec §9.8)."""
        rows = checkout.query(
            "SELECT node_id, subtype, attrs FROM okg.graph_nodes "
            "WHERE node_id = ANY(:ids)",
            {"ids": [str(n) for n in changed]},
        )
        by_id = {str(r["node_id"]): r for r in rows}
        dirty_claims: set[str] = set()
        for node_id in changed:
            row = by_id.get(str(node_id))
            if row is None:
                return (
                    FULL_REASON_DELETION_SHAPE,
                    f"changed node {node_id!r} absent at checkout (deletion)",
                    dirty_claims,
                )
            subtype = str(row["subtype"])
            if subtype == POLICY_SUBTYPE:
                # A Σ edit re-classifies every bucket.
                return (
                    FULL_REASON_POLICY_CHANGE,
                    f"dirty predicate_policy node ({node_id!r})",
                    dirty_claims,
                )
            if subtype == PRECEDENT_SUBTYPE:
                # A precedent's heading-path scope may govern many buckets; v1
                # does not prove the governed bucket set (design D4 leaning), so
                # it falls back to full with a recorded cause.
                return (
                    FULL_REASON_PRECEDENT_SCOPE_UNPROVABLE,
                    f"dirty claim_precedent node ({node_id!r}) — governed "
                    "bucket set not proven in v1",
                    dirty_claims,
                )
            if subtype == CLAIM_SUBTYPE:
                dirty_claims.add(str(node_id))
            elif subtype == CORRECTION_SUBTYPE:
                target = dict(row.get("attrs") or {}).get("target_claim_id")
                if target:
                    dirty_claims.add(str(target))
        return None, "", dirty_claims

    def _linked_policies_enabled(checkout: Any) -> bool:
        rows = checkout.query(
            "SELECT 1 FROM okg.graph_nodes WHERE subtype = :s "
            "AND attrs->>'policy_kind' = :k AND attrs->>'enabled' = 'true' "
            "LIMIT 1",
            {"s": POLICY_SUBTYPE, "k": KIND_LINKED},
        )
        return bool(rows)

    def _incremental(ctx: MaintainContext) -> MaintainResult:
        cycle = ctx.change_inputs
        changed = tuple(getattr(cycle, "changed_node_ids", None) or ())
        force_full = bool(getattr(cycle, "force_full", False))
        scope_unavailable = (
            cycle is None
            or getattr(cycle, "dirty_scope", None) is None
            or getattr(cycle, "changed_node_ids", None) is None
        )
        if force_full or scope_unavailable:
            # force_full is routed to _full by the kit dispatch; this defends the
            # residual (changed_node_ids None while dirty_scope present) case.
            return _full_under_incremental(
                ctx, FULL_REASON_SCOPE_UNAVAILABLE, len(changed),
                "force_full" if force_full else "changed_node_ids unavailable",
            )
        if not changed:
            return _noop(ctx)
        cause, detail, dirty_claims = _classify_changes(ctx.checkout, changed)
        if cause is not None:
            return _full_under_incremental(ctx, cause, len(changed), detail)
        if not dirty_claims:
            # Changed nodes exist but none touch the four input subtypes.
            return _noop(ctx)
        if _linked_policies_enabled(ctx.checkout):
            return _full_under_incremental(
                ctx,
                FULL_REASON_LINKED_PAIR,
                len(changed),
                "enabled linked_pair policies present (cross-bucket "
                "dirtiness, design §4 v1)",
            )
        if deployment is not None:
            # The selection-carry UPGRADE FENCE. A bucket-scoped cycle copies
            # every untouched bucket's prior selection row forward verbatim;
            # that is only sound when the prior rows were written under the
            # RUNNING selection schema version. A deployment that upgraded
            # across a change to the recorded selection content (the override
            # candidate, PACT concord-second-repair-generator) holds rows whose
            # override_claim_ids is a DDL default, not a measurement — carrying
            # them would produce a candidate set missing the override candidate
            # and a whole-generation digest that no longer equals a from-scratch
            # recompute's, with nothing else detecting it. So the first cycle
            # after such an upgrade recomputes the whole selection set.
            from okg.substrate.derived_artifacts.repair_materialization import (
                stale_carry_schema_version,
            )

            stale_version = stale_carry_schema_version(
                ctx.store.context.db,
                deployment=deployment,
                current_generation_id=str(ctx.generation_id),
            )
            if stale_version is not None:
                return _full_under_incremental(
                    ctx,
                    FULL_REASON_SELECTION_SCHEMA_UPGRADE,
                    len(changed),
                    "prior repair_selections written under selection schema "
                    f"{stale_version!r}; the differential carry is not licensed",
                )

        # Bucket-scoped recompute (bucket keys are claim-local, decision D3):
        # the dirty claims' CURRENT buckets plus every bucket whose existing
        # rows mention them (the moved-bucket / removed-member case).
        existing = _existing_rows(ctx.checkout)
        tokens: set[str] = set()
        current = ctx.checkout.query(
            f"SELECT predicate, normalized_key FROM {CLAIM_EFFECTIVE_TABLE} "
            "WHERE row_kind = :k AND row_key = ANY(:ids)",
            {"k": ROW_KIND_EFFECTIVE, "ids": sorted(dirty_claims)},
        )
        for r in current:
            tokens.add(bucket_token(str(r["predicate"]), str(r["normalized_key"])))
        for key, row in existing.items():
            mentioned = set(
                json.loads(row.get("member_claim_ids") or "[]")
            ) | set(json.loads(row.get("ranked_claim_ids") or "[]"))
            if mentioned & dirty_claims:
                token = _row_bucket_token(row)
                if token is None:
                    raise SubstrateError(
                        "claim_projection: linked_pair kept row present under "
                        "bucket-scoped dispatch — invariant breach (linked "
                        "policies force full recomputes)",
                        data={"row_key": key[1]},
                    )
                tokens.add(token)
        if not tokens:
            return _noop(ctx)

        rows = compute_rows(
            ctx.checkout,
            dirty_buckets=tokens,
            typed_incompatible=typed_incompatible,
        )
        for (kind, key), row in rows.items():
            token = _row_bucket_token(row)
            if token is None or token not in tokens:
                raise SubstrateError(
                    "claim_projection: scoped recompute produced a row "
                    "outside the dirty buckets",
                    data={"row_kind": kind, "row_key": key},
                )
        deletes = [
            {"row_kind": k[0], "row_key": k[1]}
            for k in sorted(existing)
            if k not in rows and _row_bucket_token(existing[k]) in tokens
        ]
        if deployment is not None:
            # Differential selection materialization (task.repair.differential,
            # spec §9.8): re-select ONLY the dirty buckets through the pure layer
            # and carry every untouched bucket's selection row forward from the
            # prior generation, so this generation carries a COMPLETE selection
            # set byte-identical to a from-scratch recompute. Same connection as
            # (and immediately before) the kept-row write, exactly like `_full`.
            from okg.substrate.derived_artifacts.repair_materialization import (
                materialize_selection_differential,
            )

            materialize_selection_differential(
                ctx.checkout,
                ctx.store.context.db,
                deployment=deployment,
                generation_id=ctx.generation_id,
                kept_rows=rows,
                dirty_bucket_keys={
                    bucket_row_key(*tok.split(BUCKET_SEP, 1)) for tok in tokens
                },
                typed_incompatible=typed_incompatible,
            )
        return MaintainResult(
            upserts=_upserts(rows),
            deletes=deletes,
            cadence_counter=_next_cadence(ctx),
            dirty_bucket_count=len(tokens),
            mode="incremental",
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
                    table=CLAIM_PROJECTION_TABLE,
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
        # Tier-2/3 findings + resolutions are a CORRECTNESS surface: a failed
        # maintenance (malformed Σ row, absent tier-1 sentinel) must BLOCK the
        # publish, never leave silently stale/mis-classified conflict state.
        failure_mode=FailureMode.GATING,
        timing_key=DERIVATION_ID,
        make_cycle_inputs=_make_inputs,
    )


def register_claim_projection_derivation(
    registry: Any,
    *,
    deployment: str | None = None,
    typed_incompatible: Callable[[ClaimView, ClaimView], bool] | None = None,
) -> Any:
    """Register the tier-2/3 derivation on a ``DerivationRegistry``; returns it.

    The tier-1 ``claim_effective`` row must be registered alongside: the
    registry's sorted-``derivation_id`` iteration runs it first (module
    docstring, ORDERING MECHANISM). ``deployment`` opts into repair-selection
    materialization; ``typed_incompatible`` registers the same typed callback
    across maintained derivation and oracle (see
    :func:`claim_projection_derivation_row`)."""
    registry.register(claim_projection_derivation_row(
        deployment=deployment, typed_incompatible=typed_incompatible
    ))
    return registry


__all__ = [
    "BASIS_SOLE",
    "BASIS_TIE",
    "BUCKET_SEP",
    "CLAIM_PROJECTION_TABLE",
    "DECLARED_ORIGIN_ATTR",
    "DERIVATION_ID",
    "GRAPH_NODES_TABLE",
    "KEY_COLUMNS",
    "KIND_FUNCTIONAL",
    "KIND_INCOMPATIBLE",
    "KIND_LINKED",
    "KIND_UNRESOLVED",
    "LINK_KERNELS",
    "ROW_KIND_FINDING",
    "ROW_KIND_RESOLUTION",
    "VALUE_COLUMNS",
    "assemble_rows",
    "bucket_row_key",
    "bucket_token",
    "build_conflict_buckets",
    "claim_projection_derivation_row",
    "claim_projection_truth",
    "compute_rows",
    "compute_rows_interpreted",
    "derive_linked_findings",
    "effective_program",
    "read_projection_inputs",
    "finding_row_key",
    "interpreter_surfaces",
    "library_truth_rows",
    "pairwise_functional_program",
    "pairwise_incompatible_program",
    "ambiguous_members_program",
    "policies_program",
    "rank_program",
    "register_claim_projection_derivation",
    "registries",
    "rows_as_truth",
    "tier23_programs",
    "validate_projection_inputs",
]
