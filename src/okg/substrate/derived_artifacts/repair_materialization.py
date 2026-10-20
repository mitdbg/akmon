"""Materialize the selected repair (r*) into ``okg.repair_selections``.

PACT ``consistency-repair-objective``, task.repair.materialize
(req.repair.executed-cleaned-state). The pure selection layer
(:mod:`okg.substrate.consistency.claim_model.selection`) decides r* — a set of
corrections with pairwise-disjoint footprints chosen from declared generators,
recorded with the winning candidate, its generator identity, and the deciding
basis; contested buckets abstain (the empty repair, violation retained). This
module is the **recording** step: it runs the pure layer over a generation's
resolved buckets and writes one ``okg.repair_selections`` row per bucket, plus a
byte-stable whole-selection digest.

Selection semantics are inherited from the pure layer ONLY. This module never
re-ranks and never re-derives a repair: it reconstructs the conflicted buckets
(``build_conflict_buckets`` — the SAME (predicate, key) set the kept
``bucket_resolution`` rows carry), runs :func:`select_repairs`, and then
**cross-checks** the pure output against the kept rows that the projection
already computed for the generation (the IR/RANK resolution). Any divergence
between the persisted selection and ``select_repairs`` output is a defect: it
raises :class:`SubstrateError` LOUDLY (the derivation's GATING failure mode
blocks the publish), never a silent tolerance.

Source preservation is the projection's semantics, not this module's: the losers
stay present in ``okg.claim_projection`` as ``superseded`` (never deleted); these
rows only RECORD which claims the selected repair distrusts (``dist(r)``) and
which it asserts (``r(G)∖G`` — empty while declaration order keeps the
deletion-shaped retraction generator selected). The rows also carry
``override_claim_ids``: the claims the second declared generator
(``override-surviving-value``) would have rewritten to the winner's value had
that repair been chosen — candidate-set content, not the selected edit, so a
reader can tell what the generation chose BETWEEN and not only what it chose.

Differential materialization (task.repair.materialize + task.repair.differential;
spec §9.8). The selection is materialized on EVERY generation, complete:

* a FULL recompute of the ``claim_projection`` derivation (``_full`` — first
  publish, policy/precedent change, unprovable scope, periodic self-heal) reads
  the complete resolved state and writes the complete selection set via
  :func:`materialize_selection_for_generation`;
* a bucket-scoped INCREMENTAL cycle re-selects ONLY the dirty buckets through the
  pure layer (:func:`compute_dirty_selection` — ``select_repairs`` sees the dirty
  buckets and nothing else, cross-checked against the cycle's recomputed
  resolutions), copies every untouched bucket's selection row forward from the
  prior generation (:func:`read_prior_selection_rows`, recording the carry
  provenance in ``carried_from_generation``), and persists the union via
  :func:`materialize_selection_differential`.

Both paths write a COMPLETE per-generation set with the same whole-generation
digest, so the incremental and from-scratch paths produce IDENTICAL selections
(the §9.8 arbiter): re-selection over the dirty subset equals the from-scratch
selection restricted to those buckets (pure-layer bucket-locality), and the
carried rows are byte-identical to the buckets a from-scratch recompute would
emit. There is no deferral — an incremental cycle never leaves a generation with
a partial selection.
"""
from __future__ import annotations

import hashlib
import json
import logging
from typing import Any, Callable, Iterable, Mapping

from okg.substrate.consistency.claim_model.effective import (
    ACTION_OVERRIDE,
    ACTION_RETRACT,
)
from okg.substrate.consistency.claim_model.views import ClaimView
from okg.substrate.consistency.claim_model.precedence import (
    OUTCOME_CANONICAL,
    OUTCOME_CONTESTED,
)
from okg.substrate.consistency.claim_model.selection import (
    EMPTY_REPAIR_BUCKET_KEY,
    EMPTY_REPAIR_GENERATOR_ID,
    GENERATOR_VERSION_V1,
    OVERRIDE_SURVIVING_VALUE_GENERATOR_ID,
    PRECEDENCE_RETRACTION_GENERATOR_ID,
    SELECTION_SCHEMA_VERSION,
    SelectedRepair,
    select_repairs,
)
from okg.substrate.derived_artifacts.claim_projection_derivation import (
    ROW_KIND_RESOLUTION,
    bucket_row_key,
    build_conflict_buckets,
    read_projection_inputs,
)
from okg.substrate.errors import SubstrateError

logger = logging.getLogger(__name__)

REPAIR_SELECTIONS_TABLE = "okg.repair_selections"

# The persisted per-bucket outcome tokens (schema column ``outcome``): a
# decisively-resolved bucket is ``selected`` (its repair executed — losers
# distrusted); a contested bucket is ``unresolved`` (the empty repair, violation
# retained). These are the recording vocabulary; the pure layer's per-bucket
# ``resolve_bucket`` outcome (canonical / contested) maps onto them 1:1.
OUTCOME_SELECTED = "selected"
OUTCOME_UNRESOLVED = "unresolved"

_INSERT_SQL = f"""
INSERT INTO {REPAIR_SELECTIONS_TABLE}
  (deployment_name, generation_id, bucket_key, outcome, winner_claim_id,
   generator_id, generator_version, basis, distrusted_claim_ids,
   asserted_claim_ids, override_claim_ids, unresolved_reason,
   carried_from_generation, selection_schema_version, selection_digest)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb, %s::jsonb, %s::jsonb, %s,
        %s, %s, %s)
"""

_DELETE_SQL = (
    f"DELETE FROM {REPAIR_SELECTIONS_TABLE} "
    "WHERE deployment_name = %s AND generation_id = %s"
)


def selection_digest(selected: SelectedRepair) -> str:
    """Byte-stable sha256 of the whole-generation SelectedRepair.

    Over ``select_repairs``'s canonical, order-independent ``to_json_dict``
    (the pure layer sorts every list and the candidate set), so shuffling the
    input claim order yields an identical digest — footprint-disjoint
    corrections commute. Two generations whose selections coincide share this
    digest; it is stored on every row so a generation's r* is comparable
    byte-stably and recoverable from its rows.
    """
    canonical = json.dumps(
        selected.to_json_dict(),
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
    )
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


# The persisted-row → resolve_bucket outcome inverse: the recording vocabulary
# ('selected' / 'unresolved') maps back to the pure layer's per-bucket outcome
# ('canonical' / 'contested') 1:1 (the inverse of ``_cross_check``'s ``_expected``).
_ROW_OUTCOME_TO_RESOLUTION = {
    OUTCOME_SELECTED: OUTCOME_CANONICAL,
    OUTCOME_UNRESOLVED: OUTCOME_CONTESTED,
}


def _selected_repair_json_from_rows(rows: Iterable[Mapping[str, Any]]) -> dict:
    """Reconstruct ``SelectedRepair.to_json_dict()`` from persisted selection rows.

    The differential path assembles a generation's complete selection as dirty
    freshly-selected rows plus untouched rows copied forward — WITHOUT re-invoking
    the pure layer over the carried buckets. To stamp the whole-generation digest
    (which every row of a generation shares) this rebuilds the ``SelectedRepair``
    JSON shape from those rows alone: the shaping is the exact inverse of
    :func:`_shape_selection_rows` plus the candidate-set reconstruction
    (:func:`...selection.enumerate_candidates`'s deterministic output — the empty
    repair, one precedence-retraction candidate per decisive bucket with losers,
    and one override candidate per decisive bucket the override generator did
    NOT refuse). The override candidate is reconstructable only because the row
    records it: ``override_claim_ids`` gives the targets and ``winner_claim_id``
    the surviving claim they copy from, which is exactly the pair the pure
    candidate carries. It is byte-equal to
    ``select_repairs(all_buckets).to_json_dict()``, LOCKED by
    ``test_selection_digest_from_rows_matches_pure_layer``; that witness is the
    guard against this inverse drifting from the forward shaping."""
    selected_json: list[dict] = []
    unresolved_json: list[dict] = []
    distrusted: set[str] = set()
    candidates: list[dict] = [{
        "bucket_key": EMPTY_REPAIR_BUCKET_KEY,
        "generator_id": EMPTY_REPAIR_GENERATOR_ID,
        "generator_version": GENERATOR_VERSION_V1,
        "corrections": [],
    }]
    for r in rows:
        bucket_key = r["bucket_key"]
        distrusted_ids = list(r.get("distrusted_claim_ids") or [])
        corrections = [
            {"action": ACTION_RETRACT, "claim_id": cid} for cid in distrusted_ids
        ]
        selected_json.append({
            "bucket_key": bucket_key,
            "generator_id": r["generator_id"],
            "generator_version": r["generator_version"],
            "basis": r["basis"],
            "outcome": _ROW_OUTCOME_TO_RESOLUTION[r["outcome"]],
            "corrections": corrections,
        })
        if r["outcome"] == OUTCOME_SELECTED and distrusted_ids:
            candidates.append({
                "bucket_key": bucket_key,
                "generator_id": PRECEDENCE_RETRACTION_GENERATOR_ID,
                "generator_version": GENERATOR_VERSION_V1,
                "corrections": corrections,
            })
            distrusted.update(distrusted_ids)
        override_ids = sorted(r.get("override_claim_ids") or [])
        if override_ids:
            candidates.append({
                "bucket_key": bucket_key,
                "generator_id": OVERRIDE_SURVIVING_VALUE_GENERATOR_ID,
                "generator_version": GENERATOR_VERSION_V1,
                "corrections": [
                    {
                        "action": ACTION_OVERRIDE,
                        "claim_id": cid,
                        "source_claim_id": r["winner_claim_id"],
                    }
                    for cid in override_ids
                ],
            })
        if r["outcome"] == OUTCOME_UNRESOLVED:
            unresolved_json.append({
                "bucket_key": bucket_key,
                "reason": r.get("unresolved_reason"),
                "basis": r["basis"],
            })
    selected_json.sort(key=lambda s: s["bucket_key"])
    unresolved_json.sort(key=lambda u: u["bucket_key"])
    # Mirrors selection._candidate_sort_key EXACTLY (including the correction
    # tuple's source_claim_id slot) — the digest is over this order.
    candidates.sort(key=lambda c: (
        c["bucket_key"], c["generator_id"], c["generator_version"],
        tuple(
            (rc["action"], rc["claim_id"], rc.get("source_claim_id") or "")
            for rc in c["corrections"]
        ),
    ))
    return {
        "schema_version": SELECTION_SCHEMA_VERSION,
        "selected": selected_json,
        "distrusted_claim_ids": sorted(distrusted),
        "asserted_claim_ids": [],
        "unresolved": unresolved_json,
        "candidate_set": {
            "schema_version": SELECTION_SCHEMA_VERSION,
            "candidates": candidates,
        },
    }


def selection_digest_from_rows(rows: Iterable[Mapping[str, Any]]) -> str:
    """The whole-generation digest computed from persisted selection rows.

    Byte-equal to :func:`selection_digest` over the ``SelectedRepair`` those rows
    encode (locked by ``test_selection_digest_from_rows_matches_pure_layer``), so
    a differential cycle — which assembles rows from a dirty re-selection plus
    carried-forward rows and CANNOT rebuild the pure ``SelectedRepair`` without
    re-ranking the untouched buckets — stamps the SAME digest a from-scratch
    recompute would. Order-independent (every list is sorted)."""
    canonical = json.dumps(
        _selected_repair_json_from_rows(rows),
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
    )
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def _kept_resolutions(
    kept_rows: Mapping[tuple[str, str], Mapping[str, Any]],
) -> dict[str, Mapping[str, Any]]:
    """The kept ``bucket_resolution`` rows keyed by their bucket row_key."""
    return {
        key[1]: row
        for key, row in kept_rows.items()
        if key[0] == ROW_KIND_RESOLUTION
    }


def _cross_check(
    selected: SelectedRepair,
    kept_resolutions: Mapping[str, Mapping[str, Any]],
) -> None:
    """Prove the pure selection agrees with the kept resolution rows.

    The kept rows are the projection's own IR/RANK resolution for this
    generation; ``select_repairs`` is the library resolution over the same
    signals + precedents. They MUST agree bucket-for-bucket (the parity oracle
    proves IR == library; this is the production-time guard). Every raise here
    is a real defect — a divergence between what will be persisted as the
    projection and what the pure selection layer decided.
    """
    selection_buckets = {sc.bucket_key for sc in selected.selected}
    kept_buckets = set(kept_resolutions)
    if selection_buckets != kept_buckets:
        raise SubstrateError(
            "repair_materialization: selection buckets diverge from the kept "
            "bucket_resolution rows (the pure selection and the projection "
            "disagree on the conflicted-bucket set)",
            data={
                "only_in_selection": sorted(selection_buckets - kept_buckets),
                "only_in_kept": sorted(kept_buckets - selection_buckets),
            },
        )

    _expected = {
        OUTCOME_CANONICAL: OUTCOME_CANONICAL,
        OUTCOME_CONTESTED: OUTCOME_CONTESTED,
    }
    for sc in selected.selected:
        kept = kept_resolutions[sc.bucket_key]
        kept_outcome = str(kept.get("outcome"))
        if kept_outcome != _expected.get(sc.outcome, sc.outcome):
            raise SubstrateError(
                "repair_materialization: bucket resolution outcome diverges "
                "between the pure selection and the kept row",
                data={
                    "bucket_key": sc.bucket_key,
                    "selection_outcome": sc.outcome,
                    "kept_outcome": kept_outcome,
                },
            )
        if sc.outcome == OUTCOME_CANONICAL:
            distrusted_here = sorted(c.claim_id for c in sc.corrections)
            kept_superseded = sorted(
                json.loads(kept.get("superseded_claim_ids") or "[]")
            )
            if distrusted_here != kept_superseded:
                raise SubstrateError(
                    "repair_materialization: distrusted set diverges from the "
                    "kept superseded set for a decisive bucket",
                    data={
                        "bucket_key": sc.bucket_key,
                        "distrusted": distrusted_here,
                        "kept_superseded": kept_superseded,
                    },
                )


def _shape_selection_rows(
    selected: SelectedRepair,
    kept_resolutions: Mapping[str, Mapping[str, Any]],
) -> list[dict[str, Any]]:
    """Shape ``okg.repair_selections`` rows from a (cross-checked) SelectedRepair.

    One row per selected bucket: ``selected`` with the distrusted set + winner for
    a decisive bucket, ``unresolved`` with reason contested for an abstained one.
    ``carried_from_generation`` is ``None`` — these rows were freshly selected;
    the differential carry path stamps the prior generation on the rows it copies.

    ``override_claim_ids`` records the SECOND repair the bucket had available but
    did not take: the claims the override generator would have rewritten to the
    winner's value (empty when it refused — a contested bucket, a value-less
    bucket, or a typed-surrogate one). It is candidate-set content, not the
    selected edit, and it is what makes "which repairs did this generation
    choose between" answerable from the published rows without re-deriving.
    Shared by :func:`compute_repair_selection` (full) and
    :func:`compute_dirty_selection` (the dirty subset)."""
    unresolved_reason_by_bucket = {
        u.bucket_key: u.reason for u in selected.unresolved
    }
    override_by_bucket = {
        c.bucket_key: c
        for c in selected.candidate_set.candidates
        if c.generator_id == OVERRIDE_SURVIVING_VALUE_GENERATOR_ID
    }
    rows: list[dict[str, Any]] = []
    for sc in selected.selected:
        if sc.outcome == OUTCOME_CANONICAL:
            kept = kept_resolutions[sc.bucket_key]
            winner = kept.get("winner_claim_id") or None
            override = override_by_bucket.get(sc.bucket_key)
            override_ids: list[str] = []
            if override is not None:
                sources = {c.source_claim_id for c in override.corrections}
                # The override copies the WINNER's value; the row stores only the
                # targets, so the source must be the winner the row already
                # carries or the recorded candidate would be unreconstructable.
                # Checked, never assumed.
                if sources != {winner}:
                    raise SubstrateError(
                        "repair_materialization: override candidate sources its "
                        "value from a claim other than the recorded winner",
                        data={
                            "bucket_key": sc.bucket_key,
                            "winner_claim_id": winner,
                            "override_sources": sorted(
                                s or "" for s in sources
                            ),
                        },
                    )
                override_ids = sorted(c.claim_id for c in override.corrections)
            # The v1 selected repair is deletion-shaped: declaration order keeps
            # the retraction generator first, so a selected repair that emitted
            # anything but retractions means the tie-break changed under this
            # shaping without it — surface that rather than mis-record it as
            # distrust.
            non_retractions = sorted(
                {c.action for c in sc.corrections} - {ACTION_RETRACT}
            )
            if non_retractions:
                raise SubstrateError(
                    "repair_materialization: the selected repair emitted "
                    "non-retraction corrections, which this row shaping cannot "
                    "record as distrust",
                    data={
                        "bucket_key": sc.bucket_key,
                        "generator_id": sc.generator_id,
                        "actions": non_retractions,
                    },
                )
            rows.append({
                "bucket_key": sc.bucket_key,
                "outcome": OUTCOME_SELECTED,
                "winner_claim_id": winner,
                "generator_id": sc.generator_id,
                "generator_version": sc.generator_version,
                "basis": sc.basis,
                "distrusted_claim_ids": [c.claim_id for c in sc.corrections],
                "asserted_claim_ids": [],
                "override_claim_ids": override_ids,
                "unresolved_reason": None,
                "carried_from_generation": None,
            })
        else:
            rows.append({
                "bucket_key": sc.bucket_key,
                "outcome": OUTCOME_UNRESOLVED,
                "winner_claim_id": None,
                "generator_id": sc.generator_id,
                "generator_version": sc.generator_version,
                "basis": sc.basis,
                "distrusted_claim_ids": [],
                "asserted_claim_ids": [],
                "override_claim_ids": [],
                "unresolved_reason": unresolved_reason_by_bucket.get(
                    sc.bucket_key, OUTCOME_CONTESTED
                ),
                "carried_from_generation": None,
            })
    rows.sort(key=lambda r: r["bucket_key"])
    return rows


def compute_repair_selection(
    effective_rows: Iterable[Mapping[str, Any]],
    claim_nodes: Iterable[Mapping[str, Any]],
    policy_nodes: Iterable[Mapping[str, Any]],
    precedent_nodes: Iterable[Mapping[str, Any]],
    kept_rows: Mapping[tuple[str, str], Mapping[str, Any]],
    *,
    typed_incompatible: Callable[[ClaimView, ClaimView], bool] | None = None,
) -> tuple[list[dict[str, Any]], str]:
    """Run the pure selection layer and shape ``okg.repair_selections`` rows.

    Returns ``(rows, digest)``: one row per conflicted bucket
    (``selected`` with the distrusted set for a decisive bucket, ``unresolved``
    with reason contested for an abstained one), and the whole-selection digest.
    Cross-checks the pure output against ``kept_rows`` (the projection's own
    resolution) — divergence raises. Pure: no DB, no mutation.
    """
    buckets, precedents = build_conflict_buckets(
        effective_rows, claim_nodes, policy_nodes, precedent_nodes,
        typed_incompatible=typed_incompatible,
    )
    selected = select_repairs(buckets, precedents)
    kept_resolutions = _kept_resolutions(kept_rows)
    _cross_check(selected, kept_resolutions)
    rows = _shape_selection_rows(selected, kept_resolutions)
    return rows, selection_digest(selected)


def compute_dirty_selection(
    effective_rows: Iterable[Mapping[str, Any]],
    claim_nodes: Iterable[Mapping[str, Any]],
    policy_nodes: Iterable[Mapping[str, Any]],
    precedent_nodes: Iterable[Mapping[str, Any]],
    kept_rows: Mapping[tuple[str, str], Mapping[str, Any]],
    *,
    dirty_bucket_keys: set[str],
    typed_incompatible: Callable[[ClaimView, ClaimView], bool] | None = None,
) -> list[dict[str, Any]]:
    """Re-select ONLY the dirty buckets through the pure layer. Pure: no DB.

    The differential half of the §9.8 arbiter. The effective rows are restricted
    to the dirty buckets BEFORE :func:`build_conflict_buckets`, so
    ``select_repairs`` is invoked over the dirty conflicted buckets and NOTHING
    else (the "pure layer invoked only for dirty buckets" witness) — untouched
    buckets are never re-ranked. The GATING cross-check is preserved on the dirty
    subset: the pure re-selection must agree with the cycle's recomputed kept
    resolutions (``kept_rows`` from ``compute_rows(dirty_buckets=…)``), or it
    raises. Returns the fresh dirty rows (``carried_from_generation`` is
    ``None``); the whole-generation digest is stamped by the caller once the
    carried rows are assembled. A dirty bucket that is no longer conflicted
    produces no row (correctly dropped from the new generation)."""
    dirty_effective = [
        r for r in effective_rows
        if bucket_row_key(r["predicate"], r["normalized_key"]) in dirty_bucket_keys
    ]
    buckets, precedents = build_conflict_buckets(
        dirty_effective, claim_nodes, policy_nodes, precedent_nodes,
        typed_incompatible=typed_incompatible,
    )
    selected = select_repairs(buckets, precedents)
    kept_resolutions = {
        key: row
        for key, row in _kept_resolutions(kept_rows).items()
        if key in dirty_bucket_keys
    }
    _cross_check(selected, kept_resolutions)
    return _shape_selection_rows(selected, kept_resolutions)


def persist_repair_selections(
    db: Any,
    *,
    deployment: str,
    generation_id: str,
    rows: list[dict[str, Any]],
    digest: str,
) -> int:
    """Write the generation's complete selection set atomically.

    Delete-then-insert under one transaction on the publisher's connection
    (``db`` is the Chronos ``PostgresDatabaseAdapter``; its ``raw_connection``
    is the psycopg connection the kept-row write also rides). ``okg.repair_
    selections`` is a plain recording table (like ``okg.model_outcomes``), NOT a
    Chronos interval table — it does not participate in the #444 kit's kept-row
    transaction, so this is the same CONNECTION, an immediately-preceding commit.
    ``generation_id`` is fresh per cycle, so a re-publish under the same id
    (self-heal) replaces its own prior rows and nothing else. Returns the count.
    """
    if not deployment:
        raise SubstrateError(
            "repair_materialization: a deployment label is required to record "
            "repair selections"
        )
    conn = db.raw_connection
    with conn.transaction(), conn.cursor() as cur:
        cur.execute(_DELETE_SQL, (deployment, generation_id))
        if rows:
            cur.executemany(
                _INSERT_SQL,
                [
                    (
                        deployment,
                        generation_id,
                        r["bucket_key"],
                        r["outcome"],
                        r["winner_claim_id"],
                        r["generator_id"],
                        r["generator_version"],
                        r["basis"],
                        json.dumps(r["distrusted_claim_ids"], separators=(",", ":")),
                        json.dumps(r["asserted_claim_ids"], separators=(",", ":")),
                        json.dumps(r["override_claim_ids"], separators=(",", ":")),
                        r["unresolved_reason"],
                        r.get("carried_from_generation"),
                        # Stamped from the RUNNING constant, never carried from
                        # the prior row: a row this process writes was assembled
                        # under this version, and a carried row is only ever
                        # carried when its version already equals it (the fence
                        # in `materialize_selection_differential`).
                        SELECTION_SCHEMA_VERSION,
                        digest,
                    )
                    for r in rows
                ],
            )
    return len(rows)


def materialize_selection_for_generation(
    checkout: Any,
    db: Any,
    *,
    deployment: str,
    generation_id: int | str,
    kept_rows: Mapping[tuple[str, str], Mapping[str, Any]],
    typed_incompatible: Callable[[ClaimView, ClaimView], bool] | None = None,
) -> dict[str, Any]:
    """Read the resolved state at ``checkout``, run the pure selection layer,
    and persist the generation's complete selection set. Called from the
    ``claim_projection`` derivation's full recompute; returns a summary."""
    effective_rows, claim_nodes, policy_nodes, precedent_nodes = (
        read_projection_inputs(checkout)
    )
    rows, digest = compute_repair_selection(
        effective_rows,
        claim_nodes,
        policy_nodes,
        precedent_nodes,
        kept_rows,
        typed_incompatible=typed_incompatible,
    )
    n = persist_repair_selections(
        db,
        deployment=deployment,
        generation_id=str(generation_id),
        rows=rows,
        digest=digest,
    )
    selected_count = sum(1 for r in rows if r["outcome"] == OUTCOME_SELECTED)
    unresolved_count = n - selected_count
    logger.info(
        "repair_materialization: recorded %d repair selections "
        "(%d selected, %d unresolved) deployment=%s gen=%s digest=%s",
        n, selected_count, unresolved_count, deployment, generation_id, digest[:12],
    )
    return {
        "deployment": deployment,
        "generation_id": str(generation_id),
        "selection_rows": n,
        "selected": selected_count,
        "unresolved": unresolved_count,
        "selection_digest": digest,
    }


_PRIOR_GENERATION_SQL = f"""
SELECT generation_id, bucket_key, outcome, winner_claim_id, generator_id,
       generator_version, basis, distrusted_claim_ids, asserted_claim_ids,
       override_claim_ids, unresolved_reason, selection_schema_version
  FROM {REPAIR_SELECTIONS_TABLE}
 WHERE deployment_name = %s AND generation_id = (
   SELECT generation_id FROM {REPAIR_SELECTIONS_TABLE}
    WHERE deployment_name = %s AND generation_id <> %s
    ORDER BY recorded_at DESC, generation_id DESC
    LIMIT 1
 )
"""


def read_prior_selection_rows(
    db: Any, *, deployment: str, current_generation_id: str
) -> tuple[str | None, list[dict[str, Any]]]:
    """The most-recent prior generation's selection rows, for carry-forward.

    Returns ``(prior_generation_id, rows)``. ``okg.repair_selections`` is not a
    Chronos interval table, so a differential cycle copies the untouched buckets
    forward EXPLICITLY: the "prior generation" is the deployment's most recently
    RECORDED generation other than the current one (``recorded_at`` DESC — each
    publish cycle is a distinct transaction, so it is monotone across generations;
    a ``generation_id`` tie-break covers same-instant pathological ties). No cast
    on the opaque ``generation_id`` token. ``(None, [])`` when there is no prior
    generation (a first publish never takes the incremental path)."""
    conn = db.raw_connection
    with conn.cursor() as cur:
        cur.execute(
            _PRIOR_GENERATION_SQL,
            (deployment, deployment, str(current_generation_id)),
        )
        fetched = cur.fetchall()
    rows: list[dict[str, Any]] = []
    prior_generation: str | None = None
    for (
        gen_id, bucket_key, outcome, winner, generator_id, generator_version,
        basis, distrusted, asserted, overrides, unresolved_reason,
        schema_version,
    ) in fetched:
        prior_generation = str(gen_id)
        rows.append({
            "bucket_key": str(bucket_key),
            "outcome": outcome,
            "winner_claim_id": winner,
            "generator_id": generator_id,
            "generator_version": generator_version,
            "basis": basis,
            "distrusted_claim_ids": list(distrusted or []),
            "asserted_claim_ids": list(asserted or []),
            "override_claim_ids": list(overrides or []),
            "unresolved_reason": unresolved_reason,
            "selection_schema_version": schema_version,
        })
    return prior_generation, rows


def stale_carry_schema_version(
    db: Any, *, deployment: str, current_generation_id: str
) -> str | None:
    """The prior generation's selection schema version, when it is NOT carryable.

    ``None`` means the carry is licensed: either there is no prior generation
    (nothing to carry) or every prior row was written under the running
    :data:`SELECTION_SCHEMA_VERSION`. A returned string is the offending
    version — the differential carry MUST NOT run, because a row written under
    a different selection schema encodes different selection content.

    The concrete case this exists for: a deployment upgraded ACROSS the PACT
    that added the override candidate. Its rows predate ``override_claim_ids``,
    so the upgrade's DDL default gave them ``'[]'`` — indistinguishable in the
    column from the override generator legitimately refusing. Carrying such a
    row forward would assemble a generation whose candidate set is missing the
    override candidate for every untouched bucket, and whose whole-generation
    ``selection_digest`` therefore no longer equals a from-scratch recompute's
    (an ``equivalence_differ`` release-blocking mismatch), with NOTHING else
    detecting it: the carried row keeps the selected generator's id and version.
    Absence is a DISTINCT state here, never a value that compares equal to a
    measured empty — so the version, not the column, is what is checked."""
    _, rows = read_prior_selection_rows(
        db, deployment=deployment, current_generation_id=current_generation_id
    )
    for row in rows:
        version = row.get("selection_schema_version")
        if version != SELECTION_SCHEMA_VERSION:
            return str(version)
    return None


def materialize_selection_differential(
    checkout: Any,
    db: Any,
    *,
    deployment: str,
    generation_id: int | str,
    kept_rows: Mapping[tuple[str, str], Mapping[str, Any]],
    dirty_bucket_keys: set[str],
    typed_incompatible: Callable[[ClaimView, ClaimView], bool] | None = None,
) -> dict[str, Any]:
    """Materialize the selection for a bucket-scoped incremental cycle.

    The §9.8 carry: re-select the DIRTY buckets through the pure layer
    (:func:`compute_dirty_selection`, GATING cross-check preserved), copy every
    untouched bucket's row forward from the prior generation with its carry
    provenance, assemble the COMPLETE set, and persist it under
    ``generation_id`` with the whole-generation digest (recomputed from the
    assembled rows so it equals the from-scratch digest). Called from the
    ``claim_projection`` derivation's incremental callback; returns a summary.

    The carry is FENCED on the selection schema version: a prior row written
    under a version other than the running :data:`SELECTION_SCHEMA_VERSION` is
    not carryable (see :func:`stale_carry_schema_version`) and this REFUSES
    rather than assembling a divergent generation. The derivation checks the
    same condition first and falls back to a full recompute, so the refusal is
    a backstop, not the upgrade path."""
    effective_rows, claim_nodes, policy_nodes, precedent_nodes = (
        read_projection_inputs(checkout)
    )
    dirty_rows = compute_dirty_selection(
        effective_rows, claim_nodes, policy_nodes, precedent_nodes, kept_rows,
        dirty_bucket_keys=dirty_bucket_keys,
        typed_incompatible=typed_incompatible,
    )
    prior_generation, prior_rows = read_prior_selection_rows(
        db, deployment=deployment, current_generation_id=str(generation_id)
    )
    # The upgrade fence, fail-closed. The claim_projection derivation checks
    # the same condition BEFORE dispatching here and falls back to a full
    # recompute (cause 'selection_schema_upgrade'), so this refusal should be
    # unreachable; it is here so a future caller that skips that check gets a
    # loud failure instead of a silently divergent generation.
    stale = {
        str(r.get("selection_schema_version"))
        for r in prior_rows
        if r.get("selection_schema_version") != SELECTION_SCHEMA_VERSION
    }
    if stale:
        raise SubstrateError(
            "repair_materialization: refusing to carry selection rows written "
            "under a different selection schema version — the caller must force "
            "a full recompute for this cycle",
            data={
                "deployment": deployment,
                "generation_id": str(generation_id),
                "prior_generation": prior_generation,
                "running_schema_version": SELECTION_SCHEMA_VERSION,
                "prior_schema_versions": sorted(stale),
            },
        )
    carried_rows = [
        {**r, "carried_from_generation": prior_generation}
        for r in prior_rows
        if r["bucket_key"] not in dirty_bucket_keys
    ]
    all_rows = sorted(
        [*dirty_rows, *carried_rows], key=lambda r: r["bucket_key"]
    )
    digest = selection_digest_from_rows(all_rows)
    n = persist_repair_selections(
        db,
        deployment=deployment,
        generation_id=str(generation_id),
        rows=all_rows,
        digest=digest,
    )
    selected_count = sum(1 for r in all_rows if r["outcome"] == OUTCOME_SELECTED)
    unresolved_count = n - selected_count
    logger.info(
        "repair_materialization: differential selection — recorded %d rows "
        "(%d selected, %d unresolved; %d dirty re-selected, %d carried from "
        "gen=%s) deployment=%s gen=%s digest=%s",
        n, selected_count, unresolved_count, len(dirty_rows), len(carried_rows),
        prior_generation, deployment, generation_id, digest[:12],
    )
    return {
        "deployment": deployment,
        "generation_id": str(generation_id),
        "selection_rows": n,
        "selected": selected_count,
        "unresolved": unresolved_count,
        "dirty_reselected": len(dirty_rows),
        "carried_forward": len(carried_rows),
        "carried_from_generation": prior_generation,
        "selection_digest": digest,
    }


__all__ = [
    "OUTCOME_SELECTED",
    "OUTCOME_UNRESOLVED",
    "REPAIR_SELECTIONS_TABLE",
    "compute_dirty_selection",
    "compute_repair_selection",
    "materialize_selection_differential",
    "materialize_selection_for_generation",
    "persist_repair_selections",
    "read_prior_selection_rows",
    "selection_digest",
    "selection_digest_from_rows",
    "stale_carry_schema_version",
]
