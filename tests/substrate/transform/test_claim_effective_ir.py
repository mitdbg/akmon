"""Interpreter unit tests for the tier-1 effective-claims IR programs.

task.core.decision-consumption (consistency-claim-model-core): the six
transform-IR programs in ``claim_effective_derivation`` are executed by the
reference interpreter (``transform.evaluate``) over in-memory surfaces and
compared row-for-row against the pure library
(``merge_effective_claims`` + the validated replay in ``library_truth_rows``)
— the library is the spec; the interpreter differential proves the IR
encoding without a database. Programs also validate against the registered
surfaces and lower to SQL (smoke).
"""
from __future__ import annotations

import json
from typing import Any

import pytest

from okg.substrate.derived_artifacts.claim_effective_derivation import (
    REASON_NEVER_EXISTED,
    REASON_VANISHED,
    ROW_KIND_EFFECTIVE,
    ROW_KIND_EXPIRED,
    ROW_KIND_PRECEDENT,
    VALUE_COLUMNS,
    compute_rows_interpreted,
    library_truth_rows,
    registries,
    rows_as_truth,
    tier1_programs,
)
from okg.substrate.errors import SubstrateError
from okg.substrate.transform import lower_query


def _claim(node_id: str, value: str = "v1", **over: Any) -> dict[str, Any]:
    attrs = {
        "predicate": "preferred_target",
        "normalized_key": f"key|{node_id}",
        "value": value,
        "scope": "HCAL/Run3",
        "scope_state": "known",
        "claim_provenance": "derived",
        "source_id": "page:1",
    }
    attrs.update(over)
    return {"node_id": node_id, "attrs": attrs}


def _correction(
    node_id: str,
    target: str,
    action: str,
    sequence: int,
    **payload: Any,
) -> dict[str, Any]:
    attrs = {
        "correction_id": f"cid:{node_id}",
        "target_claim_id": target,
        "action": action,
        "sequence": sequence,
        "decided_by": "operator:test",
    }
    attrs.update(payload)
    return {"node_id": node_id, "attrs": attrs}


def _observed_of(claim: dict[str, Any]) -> str:
    a = claim["attrs"]
    return json.dumps({
        "predicate": a["predicate"],
        "normalized_key": a["normalized_key"],
        "value": a["value"],
        "scope": a.get("scope", ""),
    })


def _differential(claims, corrections, precedents=()):
    """Interpreter-executed IR rows == library truth rows, byte for byte."""
    derived = rows_as_truth(
        compute_rows_interpreted(claims, corrections, precedents)
    )
    truth = library_truth_rows(claims, corrections, precedents)
    assert derived == truth, {
        "only_derived": {k: v for k, v in derived.items() if truth.get(k) != v},
        "only_truth": {k: v for k, v in truth.items() if derived.get(k) != v},
    }
    return derived


def _col(row: tuple, name: str):
    return row[VALUE_COLUMNS.index(name)]


# --------------------------------------------------------------------------- #
# program hygiene                                                             #
# --------------------------------------------------------------------------- #
def test_programs_validate_and_lower() -> None:
    regs = registries()
    for program in tier1_programs():
        assert program.validate(regs) == [], program.program_id
        sql, _params = lower_query(program, regs)
        assert sql.strip().upper().startswith("SELECT"), program.program_id


# --------------------------------------------------------------------------- #
# derived base                                                                #
# --------------------------------------------------------------------------- #
def test_claims_alone_derive_effective_rows() -> None:
    claims = [_claim("claim:a"), _claim("claim:b", value="v2")]
    rows = _differential(claims, [])
    assert set(rows) == {
        (ROW_KIND_EFFECTIVE, "claim:a"),
        (ROW_KIND_EFFECTIVE, "claim:b"),
    }
    row = rows[(ROW_KIND_EFFECTIVE, "claim:a")]
    assert _col(row, "provenance") == "derived"
    assert _col(row, "value") == "v1"
    assert _col(row, "applied_correction_id") is None
    assert _col(row, "stale_correction_review") is False


def test_override_applies_with_provenance_corrected() -> None:
    claims = [_claim("claim:a")]
    corrections = [
        _correction(
            "corr:1", "claim:a", "override", 1,
            value="v-fixed",
            observed_content=json.dumps({
                "predicate": "preferred_target",
                "normalized_key": "key|claim:a",
                "value": "v-older",   # operator saw OLDER content -> not stale
                "scope": "HCAL/Run3",
            }),
        )
    ]
    rows = _differential(claims, corrections)
    row = rows[(ROW_KIND_EFFECTIVE, "claim:a")]
    assert _col(row, "value") == "v-fixed"
    assert _col(row, "provenance") == "corrected"
    assert _col(row, "applied_correction_id") == "corr:1"
    assert _col(row, "stale_correction_review") is False


def test_stale_flag_fires_when_observed_equals_current() -> None:
    claim = _claim("claim:a")
    corrections = [
        _correction(
            "corr:1", "claim:a", "override", 1,
            value="v-fixed", observed_content=_observed_of(claim),
        )
    ]
    rows = _differential([claim], corrections)
    row = rows[(ROW_KIND_EFFECTIVE, "claim:a")]
    assert _col(row, "value") == "v-fixed"          # correction STAYS applied
    assert _col(row, "stale_correction_review") is True


def test_superseded_override_snapshot_never_flags_stale() -> None:
    """Review F1 counterexample: ov1 (seq 1, value, observed == derived) is
    SUPERSEDED by ov2 (seq 2, value, no snapshot). Spec §2's "stays applied
    but is flagged" reads winners-only — the superseded override no longer
    applies, so its matching snapshot must NOT flag the row. Both the IR path
    and the oracle replay must agree: stale is False."""
    claim = _claim("claim:a", value="v0")
    corrections = [
        _correction("corr:1", "claim:a", "override", 1,
                    value="x1", observed_content=_observed_of(claim)),
        _correction("corr:2", "claim:a", "override", 2, value="x2"),
    ]
    rows = _differential([claim], corrections)
    row = rows[(ROW_KIND_EFFECTIVE, "claim:a")]
    assert _col(row, "value") == "x2"
    assert _col(row, "applied_correction_id") == "corr:2"
    assert _col(row, "stale_correction_review") is False


def test_equal_sequence_tiebreak_is_ordinal_on_case_mixed_node_ids() -> None:
    """Review F2: the equal-(sequence, action) tiebreak on node_id must be
    Python-ordinal ('corr:B' < 'corr:a', so corr:a wins) — the SQL executor
    pins node_id to COLLATE "C" to match (see the postgres-path test in
    tests/substrate/test_claim_effective_derivation.py)."""
    claims = [_claim("claim:x", value="v0")]
    corrections = [
        _correction("corr:B", "claim:x", "override", 5, value="FROM-B"),
        _correction("corr:a", "claim:x", "override", 5, value="FROM-a"),
    ]
    rows = _differential(claims, corrections)
    row = rows[(ROW_KIND_EFFECTIVE, "claim:x")]
    assert _col(row, "value") == "FROM-a"
    assert _col(row, "applied_correction_id") == "corr:a"


def test_last_writer_wins_per_field_by_sequence() -> None:
    claims = [_claim("claim:a")]
    corrections = [
        _correction("corr:1", "claim:a", "override", 1, value="first"),
        _correction("corr:2", "claim:a", "override", 5, value="last"),
    ]
    rows = _differential(claims, corrections)
    row = rows[(ROW_KIND_EFFECTIVE, "claim:a")]
    assert _col(row, "value") == "last"
    assert _col(row, "applied_correction_id") == "corr:2"


def test_overrides_naming_different_fields_accumulate() -> None:
    """The library's sequential per-field replace == per-field anti-joins."""
    claims = [_claim("claim:a")]
    corrections = [
        _correction("corr:1", "claim:a", "override", 1, value="v-new"),
        _correction("corr:2", "claim:a", "override", 2, normalized_key="k-new"),
        _correction(
            "corr:3", "claim:a", "override", 3,
            scope="HCAL/Run4", scope_state="known",
        ),
    ]
    rows = _differential(claims, corrections)
    row = rows[(ROW_KIND_EFFECTIVE, "claim:a")]
    assert _col(row, "value") == "v-new"
    assert _col(row, "normalized_key") == "k-new"
    assert _col(row, "scope") == "HCAL/Run4"
    assert _col(row, "applied_correction_id") == "corr:3"


def test_retract_removes_effective_row() -> None:
    claims = [_claim("claim:a"), _claim("claim:b")]
    corrections = [
        _correction(
            "corr:1", "claim:a", "retract", 1,
            observed_content=_observed_of(claims[0]),
        )
    ]
    rows = _differential(claims, corrections)
    assert set(rows) == {(ROW_KIND_EFFECTIVE, "claim:b")}


# --------------------------------------------------------------------------- #
# asserted base                                                               #
# --------------------------------------------------------------------------- #
def test_assert_creates_operator_authored_row() -> None:
    corrections = [
        _correction(
            "corr:1", "claim:new", "assert", 1,
            predicate="preferred_target", normalized_key="key|new",
            value="v-op", source_id="operator:src",
        )
    ]
    rows = _differential([], corrections)
    row = rows[(ROW_KIND_EFFECTIVE, "claim:new")]
    assert _col(row, "provenance") == "operator_asserted"
    assert _col(row, "value") == "v-op"
    assert _col(row, "scope") == ""            # library default
    assert _col(row, "scope_state") == "unknown"
    assert _col(row, "applied_correction_id") == "corr:1"


def test_retract_then_assert_resurrects_then_override_overlays() -> None:
    claims = [_claim("claim:a")]
    corrections = [
        _correction("corr:1", "claim:a", "retract", 1,
                    observed_content=_observed_of(claims[0])),
        _correction("corr:2", "claim:a", "assert", 2,
                    predicate="preferred_target", normalized_key="key|claim:a",
                    value="v-op", source_id="operator:src"),
        _correction("corr:3", "claim:a", "override", 3, value="v-op-2"),
    ]
    rows = _differential(claims, corrections)
    row = rows[(ROW_KIND_EFFECTIVE, "claim:a")]
    assert _col(row, "provenance") == "operator_asserted"
    assert _col(row, "value") == "v-op-2"
    assert _col(row, "applied_correction_id") == "corr:3"


# --------------------------------------------------------------------------- #
# expiry                                                                      #
# --------------------------------------------------------------------------- #
def test_retract_of_vanished_claim_expires_with_reason() -> None:
    # observed_content present = the operator SAW a claim that has vanished.
    corrections = [
        _correction(
            "corr:1", "claim:gone", "retract", 1,
            observed_content=json.dumps({
                "predicate": "p", "normalized_key": "k", "value": "v",
                "scope": "",
            }),
        )
    ]
    rows = _differential([], corrections)
    row = rows[(ROW_KIND_EXPIRED, "corr:1")]
    assert _col(row, "reason") == REASON_VANISHED


def test_retract_of_never_observed_claim_expires_never_existed() -> None:
    corrections = [_correction("corr:1", "claim:ghost", "retract", 1)]
    rows = _differential([], corrections)
    row = rows[(ROW_KIND_EXPIRED, "corr:1")]
    assert _col(row, "reason") == REASON_NEVER_EXISTED


def test_override_of_absent_claim_expires() -> None:
    corrections = [
        _correction("corr:1", "claim:ghost", "override", 1, value="v-x"),
    ]
    rows = _differential([], corrections)
    row = rows[(ROW_KIND_EXPIRED, "corr:1")]
    assert _col(row, "reason") == REASON_NEVER_EXISTED


def test_override_after_retract_expires_as_vanished() -> None:
    claims = [_claim("claim:a")]
    corrections = [
        _correction("corr:1", "claim:a", "retract", 1,
                    observed_content=_observed_of(claims[0])),
        _correction("corr:2", "claim:a", "override", 2, value="v-late"),
    ]
    rows = _differential(claims, corrections)
    assert (ROW_KIND_EFFECTIVE, "claim:a") not in rows
    row = rows[(ROW_KIND_EXPIRED, "corr:2")]
    assert _col(row, "reason") == REASON_VANISHED


def test_redundant_retract_is_not_expired() -> None:
    claims = [_claim("claim:a")]
    corrections = [
        _correction("corr:1", "claim:a", "retract", 1,
                    observed_content=_observed_of(claims[0])),
        _correction("corr:2", "claim:a", "retract", 2,
                    observed_content=_observed_of(claims[0])),
    ]
    rows = _differential(claims, corrections)
    assert not any(kind == ROW_KIND_EXPIRED for kind, _ in rows)


# --------------------------------------------------------------------------- #
# precedents (row-presence pass-through; ≻ semantics are tier-3 work)         #
# --------------------------------------------------------------------------- #
def test_precedent_rows_pass_through() -> None:
    precedents = [{
        "node_id": "prec:1",
        "attrs": {
            "precedent_id": "pid:1", "prefer_source": "release_notes",
            "scope": "HCAL/Run3", "sequence": 1, "decided_by": "operator:test",
        },
    }]
    rows = _differential([_claim("claim:a")], [], precedents)
    row = rows[(ROW_KIND_PRECEDENT, "prec:1")]
    assert _col(row, "value") == "release_notes"
    assert _col(row, "scope") == "HCAL/Run3"


# --------------------------------------------------------------------------- #
# a denser mixed corpus, still library-differential                           #
# --------------------------------------------------------------------------- #
def test_mixed_corpus_differential() -> None:
    claims = [
        _claim("claim:a"),
        _claim("claim:b", value="vb"),
        _claim("claim:c", value="vc"),
    ]
    corrections = [
        _correction("corr:1", "claim:a", "override", 1, value="va-fixed",
                    observed_content=_observed_of(claims[0])),
        _correction("corr:2", "claim:b", "retract", 2,
                    observed_content=_observed_of(claims[1])),
        _correction("corr:3", "claim:b", "override", 3, value="vb-late"),
        _correction("corr:4", "claim:ghost", "override", 4, value="vx"),
        _correction("corr:5", "claim:new", "assert", 5,
                    predicate="preferred_target", normalized_key="key|new",
                    value="v-op", source_id="operator:src"),
        _correction("corr:6", "claim:a", "override", 6, normalized_key="ka2"),
    ]
    precedents = [{
        "node_id": "prec:1",
        "attrs": {"precedent_id": "pid:1", "prefer_source": "docs",
                  "scope": "", "sequence": 1},
    }]
    rows = _differential(claims, corrections, precedents)
    assert set(rows) == {
        (ROW_KIND_EFFECTIVE, "claim:a"),
        (ROW_KIND_EFFECTIVE, "claim:c"),
        (ROW_KIND_EFFECTIVE, "claim:new"),
        (ROW_KIND_EXPIRED, "corr:3"),
        (ROW_KIND_EXPIRED, "corr:4"),
        (ROW_KIND_PRECEDENT, "prec:1"),
    }
    a = rows[(ROW_KIND_EFFECTIVE, "claim:a")]
    assert _col(a, "value") == "va-fixed"
    assert _col(a, "normalized_key") == "ka2"
    assert _col(a, "applied_correction_id") == "corr:6"
    # corr:1's snapshot matches current AND corr:1 still WINS the value family
    # (corr:6 touched only normalized_key), so the flag fires.
    assert _col(a, "stale_correction_review") is True


# --------------------------------------------------------------------------- #
# malformed corrections fail LOUD in the derivation itself (review F5)        #
# --------------------------------------------------------------------------- #
def _corr_raw(node_id: str, target: str, action: str,
              sequence: int | None = None, **payload: Any) -> dict[str, Any]:
    attrs: dict[str, Any] = {"target_claim_id": target, "action": action}
    if sequence is not None:
        attrs["sequence"] = sequence
    attrs.update(payload)
    return {"node_id": node_id, "attrs": attrs}


_MALFORMED_CASES = {
    # Shapes from .scratch/claim_eff_malformed.py: each previously produced
    # silently mis-applied rows (or a TypeError); now the validation pre-pass
    # raises SubstrateError naming the correction node id.
    "assert_of_present_derived_claim": (
        [_claim("claim:a")],
        [_corr_raw("corr:1", "claim:a", "assert", 1, predicate="p",
                   normalized_key="k", value="v-op", source_id="s")],
        "ASSERT of an already-present claim",
    ),
    "double_assert_no_retract": (
        [],
        [_corr_raw("corr:1", "claim:new", "assert", 1, predicate="p",
                   normalized_key="k", value="v1", source_id="s"),
         _corr_raw("corr:2", "claim:new", "assert", 2, predicate="p",
                   normalized_key="k", value="v2", source_id="s")],
        "ASSERT of an already-present claim",
    ),
    "override_changes_no_field": (
        [_claim("claim:a")],
        [_corr_raw("corr:1", "claim:a", "override", 1)],
        "OVERRIDE changes no field",
    ),
    "half_scope_scope_only": (
        [_claim("claim:a")],
        [_corr_raw("corr:1", "claim:a", "override", 1, scope="S2")],
        "scope and scope_state must be set together",
    ),
    "half_scope_state_only": (
        [_claim("claim:a")],
        [_corr_raw("corr:1", "claim:a", "override", 1, scope_state="known")],
        "scope and scope_state must be set together",
    ),
    "unknown_action": (
        [_claim("claim:a")],
        [_corr_raw("corr:1", "claim:a", "unfrobnicate", 1, value="x")],
        "unknown action",
    ),
    "missing_sequence_override": (
        [_claim("claim:a")],
        [_corr_raw("corr:1", "claim:a", "override", None, value="x")],
        "correction without sequence",
    ),
    "missing_sequence_two_overrides": (
        [_claim("claim:a")],
        [_corr_raw("corr:1", "claim:a", "override", None, value="x1"),
         _corr_raw("corr:2", "claim:a", "override", 2, value="x2")],
        "correction without sequence",
    ),
    "assert_missing_coordinates": (
        [],
        [_corr_raw("corr:1", "claim:new", "assert", 1, value="v")],
        "ASSERT missing coordinates",
    ),
    "missing_target_claim_id": (
        [_claim("claim:a")],
        [{"node_id": "corr:1", "attrs": {"action": "override",
                                         "sequence": 1, "value": "x"}}],
        "correction without target_claim_id",
    ),
}


@pytest.mark.parametrize("name", sorted(_MALFORMED_CASES))
def test_malformed_correction_raises_substrate_error_naming_node(
    name: str,
) -> None:
    claims, corrections, message = _MALFORMED_CASES[name]
    with pytest.raises(SubstrateError, match=message) as exc_info:
        compute_rows_interpreted(claims, corrections, ())
    # The raise NAMES the offending correction node id (never a TypeError).
    assert "corr:" in str(exc_info.value)


def test_malformed_observed_content_still_raises_in_assembly() -> None:
    """Snapshot malformation is caught at assembly (not shape validation):
    both an unparseable snapshot and a non-object snapshot fail loud."""
    for observed, message in (
        ("{not json", "unparseable observed_content"),
        (json.dumps(["p", "k", "v"]), "observed_content must be a JSON object"),
    ):
        with pytest.raises(SubstrateError, match=message):
            compute_rows_interpreted(
                [_claim("claim:a")],
                [_correction("corr:1", "claim:a", "override", 1, value="x",
                             observed_content=observed)],
                (),
            )
