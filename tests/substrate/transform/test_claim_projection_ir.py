"""Interpreter unit tests for the tier-2/3 claim-projection IR programs.

task.core.projection-derivation (consistency-claim-model-core): the
transform-IR programs + documented Python residuals in
``claim_projection_derivation`` are executed by the reference interpreter
(``transform.evaluate``) over in-memory surfaces and compared row-for-row
against the pure library (``derive_conflict_findings`` + ``resolve_bucket``
flattened by ``library_truth_rows``) — the library is the spec. Includes a
seeded fuzz over random corpora (findings AND resolutions), the
bucket-scoped filter semantics, the malformed-Σ validation pre-pass, and the
registry ordering mechanism that runs tier 1 before tier 2.
"""
from __future__ import annotations

import random
from typing import Any

import pytest

from okg.substrate.consistency.claim_model.precedence import (
    SIGNAL_DOCUMENT_RECENCY,
    SIGNAL_PRECEDENT,
    SIGNAL_SOURCE_AUTHORITY,
)
from okg.substrate.derived_artifacts.claim_projection_derivation import (
    BASIS_TIE,
    KIND_FUNCTIONAL,
    KIND_INCOMPATIBLE,
    KIND_LINKED,
    KIND_UNRESOLVED,
    ROW_KIND_FINDING,
    ROW_KIND_RESOLUTION,
    VALUE_COLUMNS,
    bucket_row_key,
    bucket_token,
    compute_rows_interpreted,
    library_truth_rows,
    registries,
    rows_as_truth,
    tier23_programs,
)
from okg.substrate.errors import SubstrateError
from okg.substrate.transform import lower_query


def _eff(cid: str, *, pred: str = "preferred_target", key: str = "k1",
         value: str = "v", scope: str = "A > B", state: str = "known",
         source: str = "page:1") -> dict[str, Any]:
    return {
        "claim_id": cid, "predicate": pred, "normalized_key": key,
        "value": value, "scope": scope, "scope_state": state,
        "source_id": source,
    }


def _claim_node(cid: str, **attrs: Any) -> dict[str, Any]:
    return {"node_id": cid, "attrs": attrs}


def _pol(nid: str, kind: str, pred: str, pid: str, enabled: str = "true",
         **extra: Any) -> dict[str, Any]:
    attrs = {"policy_id": pid, "policy_kind": kind, "predicate": pred,
             "enabled": enabled}
    attrs.update(extra)
    return {"node_id": nid, "attrs": attrs}


def _prec(nid: str, prefer: str, scope: str = "", sequence: int = 1) -> dict:
    return {"node_id": nid, "attrs": {
        "precedent_id": f"pid:{nid}", "prefer_source": prefer,
        "scope": scope, "sequence": sequence,
    }}


_FUNCTIONAL = [_pol("pol:f", "functional", "preferred_target", "unique_preferred")]


def _differential(effective, claims=(), policies=_FUNCTIONAL, precedents=()):
    """Interpreter-executed IR rows == library truth rows, byte for byte."""
    derived = rows_as_truth(
        compute_rows_interpreted(effective, claims, policies, precedents)
    )
    truth = library_truth_rows(effective, claims, policies, precedents)
    assert derived == truth, {
        "only_derived": {k: v for k, v in derived.items() if truth.get(k) != v},
        "only_truth": {k: v for k, v in truth.items() if derived.get(k) != v},
    }
    return derived


def _col(row: tuple, name: str):
    return row[VALUE_COLUMNS.index(name)]


def _findings(rows):
    return {k: v for k, v in rows.items() if k[0] == ROW_KIND_FINDING}


def _resolutions(rows):
    return {k: v for k, v in rows.items() if k[0] == ROW_KIND_RESOLUTION}


# --------------------------------------------------------------------------- #
# program hygiene + the ordering mechanism                                    #
# --------------------------------------------------------------------------- #
def test_programs_validate_and_lower() -> None:
    regs = registries()
    for scoped in (False, True):
        for program in tier23_programs(scoped):
            assert program.validate(regs) == [], program.program_id
            sql, _params = lower_query(program, regs)
            assert sql.strip().upper().startswith("SELECT"), program.program_id


def test_registry_orders_tier1_before_tier2() -> None:
    """The ordering mechanism (module docstring): DerivationRegistry.rows()
    iterates in sorted derivation_id order, and 'claim_effective' <
    'claim_projection', so tier 1 always maintains before tier 2 within one
    publish cycle — registration order is irrelevant."""
    from okg.substrate.derived_artifacts.claim_effective_derivation import (
        claim_effective_derivation_row,
    )
    from okg.substrate.derived_artifacts.claim_projection_derivation import (
        claim_projection_derivation_row,
    )
    from okg.substrate.derived_artifacts.derivation_registry import (
        DerivationRegistry,
    )

    registry = DerivationRegistry()
    registry.register(claim_projection_derivation_row())  # tier 2 FIRST
    registry.register(claim_effective_derivation_row())
    assert [r.spec.derivation_id for r in registry.rows()] == [
        "claim_effective", "claim_projection",
    ]


# --------------------------------------------------------------------------- #
# tier 2: functional pairs + scope classification (spec §4.1)                 #
# --------------------------------------------------------------------------- #
def test_functional_conflict_same_scope_is_definite() -> None:
    rows = _differential([
        _eff("claim:a", value="X"), _eff("claim:b", value="Y", source="page:2"),
    ])
    findings = _findings(rows)
    assert len(findings) == 1
    (row,) = findings.values()
    assert _col(row, "outcome") == "definite"
    assert _col(row, "constraint_kind") == KIND_FUNCTIONAL
    assert _col(row, "constraint_id") == "unique_preferred"
    assert _col(row, "locality") == "cross_document"
    assert _col(row, "member_claim_ids") == '["claim:a","claim:b"]'


def test_equal_values_are_not_a_functional_conflict() -> None:
    rows = _differential([_eff("claim:a", value="X"), _eff("claim:b", value="X")])
    assert rows == {}


def test_subsumed_scope_is_definite_both_directions() -> None:
    for sa, sb in (("A > B", "A > B > C"), ("A > B > C", "A > B")):
        rows = _differential([
            _eff("claim:a", value="X", scope=sa),
            _eff("claim:b", value="Y", scope=sb),
        ])
        (row,) = _findings(rows).values()
        assert _col(row, "outcome") == "definite", (sa, sb)


def test_disjoint_known_scopes_are_no_conflict() -> None:
    rows = _differential([
        _eff("claim:a", value="X", scope="A > B"),
        _eff("claim:b", value="Y", scope="A > C"),
    ])
    assert _findings(rows) == {}


def test_prefix_without_separator_is_disjoint() -> None:
    # "A > BC" is NOT subsumed by "A > B" — the ' > ' separator gates it.
    rows = _differential([
        _eff("claim:a", value="X", scope="A > B"),
        _eff("claim:b", value="Y", scope="A > BC"),
    ])
    assert _findings(rows) == {}


def test_unknown_scope_downgrades_to_possible() -> None:
    rows = _differential([
        _eff("claim:a", value="X"),
        _eff("claim:b", value="Y", scope="", state="unknown"),
    ])
    (row,) = _findings(rows).values()
    assert _col(row, "outcome") == "possible"


def test_null_and_empty_scope_state_match_oracle() -> None:
    # Adversarial-review regression: a NULL (or '') scope/scope_state must be
    # coerced (x or default) exactly like the oracle. The raw SQL binding once
    # read scope_state bare, so a NULL made not_(eq(scope_state,'known'))
    # evaluate to NULL and the WHERE silently DROPPED the conflict (a real
    # finding vanished); the interpreter's Python `==` accidentally matched the
    # oracle. Both paths now coerce. This pins the interpreter side; the SQL
    # coercion is pinned by test_effective_binding_sql_coerces_null_scope_state.
    for bad in (None, ""):
        rows = _differential([
            _eff("claim:a", value="X", scope=bad, state=bad),
            _eff("claim:b", value="Y", scope="A > B", state="known",
                 source="page:2"),
        ])
        # the conflict is NOT lost — unknown participation -> possible finding.
        assert _findings(rows), (bad, rows)


def test_internal_locality_single_source() -> None:
    rows = _differential([
        _eff("claim:a", value="X"), _eff("claim:b", value="Y"),
    ])
    (row,) = _findings(rows).values()
    assert _col(row, "locality") == "internal"


# --------------------------------------------------------------------------- #
# tier 2: incompatible_pair                                                   #
# --------------------------------------------------------------------------- #
def test_incompatible_pair_matches_both_orientations_once() -> None:
    policies = [
        # A symmetric duplicate pair row: the projection must still emit ONE
        # finding (the library incompatible() is a predicate, not a row count).
        _pol("pol:1", "incompatible_pair", "lifecycle_status", "life",
             value_a="current", value_b="deprecated"),
        _pol("pol:2", "incompatible_pair", "lifecycle_status", "life",
             value_a="deprecated", value_b="current"),
    ]
    for va, vb in (("current", "deprecated"), ("deprecated", "current")):
        rows = _differential(
            [
                _eff("claim:a", pred="lifecycle_status", value=va),
                _eff("claim:b", pred="lifecycle_status", value=vb),
            ],
            policies=policies,
        )
        findings = _findings(rows)
        assert len(findings) == 1, (va, vb)
        (row,) = findings.values()
        assert _col(row, "constraint_kind") == KIND_INCOMPATIBLE
        assert _col(row, "constraint_id") == "life"


def test_incompatible_pair_unlisted_values_do_not_conflict() -> None:
    policies = [_pol("pol:1", "incompatible_pair", "lifecycle_status", "life",
                     value_a="current", value_b="deprecated")]
    rows = _differential(
        [
            _eff("claim:a", pred="lifecycle_status", value="current"),
            _eff("claim:b", pred="lifecycle_status", value="experimental"),
        ],
        policies=policies,
    )
    assert _findings(rows) == {}


# --------------------------------------------------------------------------- #
# tier 2: unresolved keys — not comparable, never forced                      #
# --------------------------------------------------------------------------- #
def test_empty_key_bucket_is_not_comparable() -> None:
    rows = _differential([
        _eff("claim:a", key="", value="X"), _eff("claim:b", key="", value="Y"),
    ])
    (row,) = _findings(rows).values()
    assert _col(row, "outcome") == "not_comparable"
    assert _col(row, "constraint_kind") == KIND_UNRESOLVED
    assert _resolutions(rows) == {}  # never resolved — that would force a verdict


def test_unresolved_key_row_flags_named_key() -> None:
    policies = [
        *_FUNCTIONAL,
        _pol("pol:u", "unresolved_key", "preferred_target", "unres",
             unresolved_key="ACTIVITY_UNKNOWN"),
    ]
    rows = _differential(
        [
            _eff("claim:a", key="ACTIVITY_UNKNOWN", value="X"),
            _eff("claim:b", key="ACTIVITY_UNKNOWN", value="Y"),
        ],
        policies=policies,
    )
    (row,) = _findings(rows).values()
    assert _col(row, "outcome") == "not_comparable"


def test_sole_claim_on_unresolved_key_is_not_a_finding() -> None:
    rows = _differential([_eff("claim:a", key="", value="X")])
    assert rows == {}


# --------------------------------------------------------------------------- #
# tier 2: linked_pair (the documented Python residual)                        #
# --------------------------------------------------------------------------- #
_LINKED = [
    _pol("pol:l", "linked_pair", "recommends", "no_deprecated",
         target_predicate="lifecycle_status", link_key_fn="normalize_identity",
         violates_values=["deprecated", "retired"]),
]


def test_linked_pair_flags_violating_target() -> None:
    rows = _differential(
        [
            _eff("claim:src", pred="recommends", key="r1", value="modX"),
            _eff("claim:tgt", pred="lifecycle_status", key="modX",
                 value="deprecated", source="page:2"),
        ],
        policies=_LINKED,
    )
    (row,) = _findings(rows).values()
    assert _col(row, "constraint_kind") == KIND_LINKED
    assert _col(row, "outcome") == "definite"
    assert _col(row, "locality") == "cross_document"
    assert _col(row, "bucket_key") == "modX"  # normalize_identity link key
    assert _resolutions(rows) == {}  # policy violation, not a pick-a-winner


def test_linked_pair_non_violating_target_is_clean() -> None:
    rows = _differential(
        [
            _eff("claim:src", pred="recommends", key="r1", value="modX"),
            _eff("claim:tgt", pred="lifecycle_status", key="modX",
                 value="current"),
        ],
        policies=_LINKED,
    )
    assert rows == {}


def test_linked_pair_disjoint_scopes_are_gated() -> None:
    rows = _differential(
        [
            _eff("claim:src", pred="recommends", key="r1", value="modX",
                 scope="A > B"),
            _eff("claim:tgt", pred="lifecycle_status", key="modX",
                 value="deprecated", scope="D"),
        ],
        policies=_LINKED,
    )
    assert rows == {}


def test_disabled_policy_is_skipped_not_dropped_silently(caplog) -> None:
    import logging

    policies = [_pol("pol:f", "functional", "preferred_target", "unique",
                     enabled="false")]
    with caplog.at_level(
        logging.INFO,
        logger="okg.substrate.derived_artifacts.claim_projection_derivation",
    ):
        rows = _differential(
            [_eff("claim:a", value="X"), _eff("claim:b", value="Y")],
            policies=policies,
        )
    assert rows == {}
    assert any("disabled predicate_policy" in r.message for r in caplog.records)
    assert any("pol:f" in r.message for r in caplog.records)


# --------------------------------------------------------------------------- #
# tier 3: resolution outcomes (RANK op on the production path)                #
# --------------------------------------------------------------------------- #
def test_authority_settles_canonical_with_superseded_losers() -> None:
    rows = _differential(
        [_eff("claim:a", value="X"), _eff("claim:b", value="Y"),
         _eff("claim:c", value="Z")],
        claims=[_claim_node("claim:a", source_authority=5),
                _claim_node("claim:b", source_authority=3)],
    )
    row = rows[(ROW_KIND_RESOLUTION, bucket_row_key("preferred_target", "k1"))]
    assert _col(row, "outcome") == "canonical"
    assert _col(row, "winner_claim_id") == "claim:a"
    assert _col(row, "basis") == SIGNAL_SOURCE_AUTHORITY
    assert _col(row, "ranked_claim_ids") == '["claim:a","claim:b","claim:c"]'
    assert _col(row, "superseded_claim_ids") == '["claim:b","claim:c"]'


def test_recency_only_lead_is_contested() -> None:
    rows = _differential(
        [_eff("claim:a", value="X"), _eff("claim:b", value="Y")],
        claims=[_claim_node("claim:a", document_recency="2026-01-01")],
    )
    row = rows[(ROW_KIND_RESOLUTION, bucket_row_key("preferred_target", "k1"))]
    assert _col(row, "outcome") == "contested"
    assert _col(row, "winner_claim_id") == ""
    assert _col(row, "basis") == SIGNAL_DOCUMENT_RECENCY
    assert _col(row, "superseded_claim_ids") == "[]"


def test_full_tie_is_contested() -> None:
    rows = _differential(
        [_eff("claim:a", value="X"), _eff("claim:b", value="Y")],
    )
    row = rows[(ROW_KIND_RESOLUTION, bucket_row_key("preferred_target", "k1"))]
    assert _col(row, "outcome") == "contested"
    assert _col(row, "basis") == BASIS_TIE


def test_precedent_flips_contested_to_canonical_latest_wins() -> None:
    claims = [_claim_node("claim:a", source_class="release_notes"),
              _claim_node("claim:b", source_class="wiki")]
    effective = [_eff("claim:a", value="X"), _eff("claim:b", value="Y")]
    # Competing precedents: the LATEST by sequence governs (prefers wiki).
    precedents = [_prec("prec:1", "release_notes", sequence=1),
                  _prec("prec:2", "wiki", sequence=2)]
    rows = _differential(effective, claims=claims, precedents=precedents)
    row = rows[(ROW_KIND_RESOLUTION, bucket_row_key("preferred_target", "k1"))]
    assert _col(row, "outcome") == "canonical"
    assert _col(row, "winner_claim_id") == "claim:b"
    assert _col(row, "basis") == SIGNAL_PRECEDENT


def test_precedent_scope_applies_by_subsumption_either_direction() -> None:
    claims = [_claim_node("claim:a", source_class="release_notes")]
    effective = [_eff("claim:a", value="X", scope="A > B"),
                 _eff("claim:b", value="Y", scope="A > B")]
    for scope, applies in (
        ("A", True),            # precedent scope subsumes the claim scope
        ("A > B > C", True),    # claim scope subsumes the precedent scope
        ("A > B", True),        # equal
        ("D", False),           # unrelated — no effect
        ("", True),             # global
    ):
        rows = _differential(
            effective, claims=claims,
            precedents=[_prec("prec:1", "release_notes", scope=scope)],
        )
        row = rows[(ROW_KIND_RESOLUTION, bucket_row_key("preferred_target", "k1"))]
        expected = "canonical" if applies else "contested"
        assert _col(row, "outcome") == expected, scope


def test_case_mixed_claim_ids_tiebreak_matches_oracle() -> None:
    """Collation regression: 'claim:B2' vs 'claim:a1' in ONE bucket — the
    ordinal claim_id DESC tiebreak ranks 'claim:a1' first ('a' > 'B')."""
    rows = _differential(
        [_eff("claim:B2", value="X"), _eff("claim:a1", value="Y")],
    )
    row = rows[(ROW_KIND_RESOLUTION, bucket_row_key("preferred_target", "k1"))]
    assert _col(row, "ranked_claim_ids") == '["claim:a1","claim:B2"]'


# --------------------------------------------------------------------------- #
# bucket-scoped compute (the incremental path's filter semantics)             #
# --------------------------------------------------------------------------- #
def test_scoped_compute_matches_full_restricted_to_dirty_buckets() -> None:
    effective = [
        _eff("claim:a", key="k1", value="X"), _eff("claim:b", key="k1", value="Y"),
        _eff("claim:c", key="k2", value="X"), _eff("claim:d", key="k2", value="Y"),
    ]
    full = rows_as_truth(compute_rows_interpreted(effective, [], _FUNCTIONAL, []))
    token = bucket_token("preferred_target", "k1")
    scoped = rows_as_truth(compute_rows_interpreted(
        effective, [], _FUNCTIONAL, [], dirty_buckets={token},
    ))
    expected = {
        k: v for k, v in full.items()
        if bucket_token(_col(v, "predicate") or "", _col(v, "bucket_key") or "")
        == token
    }
    assert scoped == expected
    assert len(scoped) == 2  # one finding + one resolution for k1 only


def test_scoped_compute_with_enabled_linked_policies_raises() -> None:
    with pytest.raises(SubstrateError, match="bucket-scoped recompute"):
        compute_rows_interpreted(
            [_eff("claim:a", pred="recommends", key="r1", value="m")],
            [], _LINKED, [],
            dirty_buckets={bucket_token("recommends", "r1")},
        )


# --------------------------------------------------------------------------- #
# validation pre-pass: malformed Σ / precedent / signal rows fail LOUD        #
# --------------------------------------------------------------------------- #
_GOOD_EFF = [_eff("claim:a", value="X")]

_MALFORMED_CASES = {
    "unknown_policy_kind": (
        _GOOD_EFF, [],
        [_pol("pol:1", "frobnicate", "p", "c1")], [],
        "unknown policy_kind",
    ),
    "policy_enabled_junk": (
        _GOOD_EFF, [],
        [_pol("pol:1", "functional", "p", "c1", enabled="TRUE")], [],
        "enabled must be 'true' or 'false'",
    ),
    "policy_missing_policy_id": (
        _GOOD_EFF, [],
        [{"node_id": "pol:1", "attrs": {
            "policy_kind": "functional", "predicate": "p", "enabled": "true"}}],
        [],
        "policy without policy_id",
    ),
    "incompatible_missing_value_b": (
        _GOOD_EFF, [],
        [_pol("pol:1", "incompatible_pair", "p", "c1", value_a="x")], [],
        "missing value_a/value_b",
    ),
    "linked_missing_link_key_fn": (
        _GOOD_EFF, [],
        [_pol("pol:1", "linked_pair", "p", "c1", target_predicate="q",
              violates_values=["x"])],
        [],
        "missing 'link_key_fn'",
    ),
    "linked_unregistered_kernel": (
        _GOOD_EFF, [],
        [_pol("pol:1", "linked_pair", "p", "c1", target_predicate="q",
              link_key_fn="rapidfuzz_cluster", violates_values=["x"])],
        [],
        "unregistered link kernel",
    ),
    "linked_violates_not_a_list": (
        _GOOD_EFF, [],
        [_pol("pol:1", "linked_pair", "p", "c1", target_predicate="q",
              link_key_fn="normalize_identity", violates_values="deprecated")],
        [],
        "violates_values is not a JSON list",
    ),
    "linked_violates_empty": (
        _GOOD_EFF, [],
        [_pol("pol:1", "linked_pair", "p", "c1", target_predicate="q",
              link_key_fn="normalize_identity", violates_values=[])],
        [],
        "empty violates_values",
    ),
    "unresolved_missing_key": (
        _GOOD_EFF, [],
        [_pol("pol:1", "unresolved_key", "p", "c1")], [],
        "missing unresolved_key",
    ),
    "two_functional_policies_one_predicate": (
        _GOOD_EFF, [],
        [_pol("pol:1", "functional", "p", "c1"),
         _pol("pol:2", "functional", "p", "c2")],
        [],
        "single policy_id",
    ),
    "functional_and_incompatible_mixed": (
        _GOOD_EFF, [],
        [_pol("pol:1", "functional", "p", "c1"),
         _pol("pol:2", "incompatible_pair", "p", "c2",
              value_a="x", value_b="y")],
        [],
        "BOTH functional and incompatible_pair",
    ),
    "policy_id_reused_across_groups": (
        _GOOD_EFF, [],
        [_pol("pol:1", "functional", "p", "c1"),
         _pol("pol:2", "functional", "q", "c1")],
        [],
        "reused across policy groups",
    ),
    "precedent_missing_sequence": (
        _GOOD_EFF, [], list(_FUNCTIONAL),
        [{"node_id": "prec:1", "attrs": {"prefer_source": "x"}}],
        "precedent without sequence",
    ),
    "precedent_junk_sequence": (
        _GOOD_EFF, [], list(_FUNCTIONAL),
        [{"node_id": "prec:1", "attrs": {
            "prefer_source": "x", "sequence": "soon"}}],
        "is not an integer",
    ),
    "claim_signal_non_integer": (
        _GOOD_EFF, [_claim_node("claim:a", source_authority="high")],
        list(_FUNCTIONAL), [],
        "is not an integer",
    ),
    "claim_is_superseded_junk": (
        _GOOD_EFF, [_claim_node("claim:a", is_superseded="yes")],
        list(_FUNCTIONAL), [],
        "is not a boolean",
    ),
    "effective_row_missing_value": (
        [{"claim_id": "claim:a", "predicate": "p", "normalized_key": "k",
          "value": None, "scope": "", "scope_state": "unknown",
          "source_id": "s"}],
        [], list(_FUNCTIONAL), [],
        "missing required coordinate 'value'",
    ),
}


@pytest.mark.parametrize("name", sorted(_MALFORMED_CASES))
def test_malformed_inputs_raise_substrate_error_naming_node(name: str) -> None:
    effective, claims, policies, precedents, message = _MALFORMED_CASES[name]
    with pytest.raises(SubstrateError, match=message) as exc_info:
        compute_rows_interpreted(effective, claims, policies, precedents)
    text = str(exc_info.value)
    assert ("pol:" in text or "prec:" in text or "claim:" in text
            or "predicate" in text)


# --------------------------------------------------------------------------- #
# seeded fuzz: random corpora, findings + resolutions vs the library          #
# --------------------------------------------------------------------------- #
def _fuzz_corpus(rng: random.Random) -> tuple[list, list, list, list, bool]:
    scopes = ["", "A", "A > B", "A > B > C", "D", "D > E"]
    values = ["v1", "v2", "v3", "current", "deprecated", "retired"]
    sources = ["page:1", "page:2", "page:3"]
    source_classes = ["", "release_notes", "wiki"]
    dates = ["", "2020-01-01", "2024-06-01", "2026-01-01"]

    policies: list[dict] = []
    policies.append(_pol("pol:f1", "functional", "p_func", "c_func",
                         enabled=rng.choice(["true", "true", "false"])))
    if rng.random() < 0.8:
        pairs = rng.sample(
            [("current", "deprecated"), ("current", "retired"),
             ("deprecated", "current")],
            k=rng.randint(1, 3),
        )
        for i, (va, vb) in enumerate(pairs):
            policies.append(_pol(
                f"pol:i{i}", "incompatible_pair", "p_pair", "c_pair",
                value_a=va, value_b=vb,
            ))
    if rng.random() < 0.5:
        policies.append(_pol("pol:u1", "unresolved_key", "p_func", "c_unres",
                             unresolved_key="KX"))
    has_linked = rng.random() < 0.4
    if has_linked:
        policies.append(_pol(
            "pol:l1", "linked_pair", "p_src", "c_link",
            target_predicate="p_pair", link_key_fn="normalize_identity",
            violates_values=["deprecated", "retired"],
        ))

    n = rng.randint(2, 14)
    effective, claims = [], []
    for i in range(n):
        # Case-mixed ids in shared buckets — the collation surface.
        cid = f"claim:{rng.choice('aAbBcC')}{i}"
        pred = rng.choice(
            ["p_func", "p_func", "p_pair", "p_pair", "p_src", "p_other"])
        key = (rng.choice(["k1", "k1", "k2", "KX", ""])
               if pred != "p_src" else rng.choice(["r1", "r2"]))
        value = rng.choice(values) if pred != "p_src" else rng.choice(
            ["modA", "modB", "modC"])
        state = rng.choice(["known", "known", "unknown"])
        effective.append(_eff(
            cid, pred=pred, key=key, value=value,
            scope=rng.choice(scopes) if state == "known" else "",
            state=state, source=rng.choice(sources),
        ))
        if rng.random() < 0.6:
            claims.append(_claim_node(
                cid,
                source_class=rng.choice(source_classes),
                is_superseded=rng.choice([True, False, "true", "false"]),
                effective_date=rng.choice(dates),
                source_authority=rng.randint(0, 3),
                independent_support=rng.randint(0, 2),
                document_recency=rng.choice(dates),
                maintenance_rank=rng.randint(0, 2),
            ))
    precedents = [
        _prec(f"prec:{rng.choice('xY')}{i}",
              rng.choice(["release_notes", "wiki"]),
              scope=rng.choice(scopes),
              sequence=rng.randint(0, 2))  # equal sequences occur
        for i in range(rng.randint(0, 3))
    ]
    return effective, claims, policies, precedents, has_linked


def test_fuzz_differential_findings_and_resolutions() -> None:
    rng = random.Random(0x5EED_23)
    nonempty = 0
    for _ in range(200):
        effective, claims, policies, precedents, has_linked = _fuzz_corpus(rng)
        derived = rows_as_truth(compute_rows_interpreted(
            effective, claims, policies, precedents))
        truth = library_truth_rows(effective, claims, policies, precedents)
        assert derived == truth, {
            "only_derived": {
                k: v for k, v in derived.items() if truth.get(k) != v},
            "only_truth": {
                k: v for k, v in truth.items() if derived.get(k) != v},
        }
        if derived:
            nonempty += 1
        # Scoped == full restricted to the chosen buckets (linked-free only).
        enabled_linked = has_linked and any(
            n["attrs"].get("policy_kind") == "linked_pair"
            and n["attrs"].get("enabled") == "true" for n in policies
        )
        if not enabled_linked and derived:
            all_tokens = sorted({
                bucket_token(_col(v, "predicate") or "",
                             _col(v, "bucket_key") or "")
                for v in derived.values()
            })
            chosen = set(rng.sample(all_tokens, k=rng.randint(1, len(all_tokens))))
            scoped = rows_as_truth(compute_rows_interpreted(
                effective, claims, policies, precedents, dirty_buckets=chosen))
            expected = {
                k: v for k, v in derived.items()
                if bucket_token(_col(v, "predicate") or "",
                                _col(v, "bucket_key") or "") in chosen
            }
            assert scoped == expected
    # The corpus generator must actually exercise the machinery.
    assert nonempty > 60
