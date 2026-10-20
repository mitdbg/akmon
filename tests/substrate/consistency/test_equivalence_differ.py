"""The exact three-way equivalence differ (PACT consistency-complete-equivalence,
task.harness.differ; req.harness.exact-diff).

Witnesses, mapped to the task verification clause:

* EQUAL on identical states — byte-identical snapshots diff EQUAL, and two
  extractions of the SAME generation are byte-equal (determinism, live);
* a single seeded divergence in EACH compared family (claim value, resolution
  winner, repair distrusted set, finding normalized_values) is detected and
  NAMED in the artifact — object identity + per-field diff;
* a missing/deleted replay key turns the update into EXCLUDED with the key
  named, DOMINATING an otherwise-EQUAL pair, and structurally
  ``counted_as_agreement=False``;
* no code path renders a partial or tolerant verdict — the signature does not
  accept an epsilon / ignore-list / severity filter;
* the verdict + artifact round-trip to canonical JSON;
* a live end-to-end: snapshot the consistency-fixture publish, EQUAL against
  itself, MISMATCH after a surgical row edit.

Live seeding + publish wiring follow
tests/substrate/consistency/test_claim_plane_wire.py.
"""
from __future__ import annotations

import inspect
import json
import uuid
from pathlib import Path
from typing import Any

import pytest

from okg.substrate.consistency import equivalence_differ as ed
from okg.substrate.consistency.equivalence_differ import (
    FAMILY_EFFECTIVE,
    FAMILY_FINDING,
    FAMILY_RESOLUTION,
    FAMILY_SELECTION,
    VERDICT_EQUAL,
    VERDICT_EXCLUDED,
    VERDICT_MISMATCH,
    EquivalenceVerdict,
    ResolvedStateSnapshot,
    diff_resolved_states,
)
from okg.substrate.llm.outcomes import ReplayExclusion

_RK1 = json.dumps(["preferred_target", "k1"], separators=(",", ":"))
_RK2 = json.dumps(["preferred_target", "k2"], separators=(",", ":"))


# --------------------------------------------------------------------------- #
# Pure fixtures: a fully-populated resolved state over all four families.      #
# --------------------------------------------------------------------------- #
def _finding_identity(input_scope: str, predicate: str, nv: dict[str, Any]) -> str:
    return ed._canonical_json([input_scope, predicate, nv])


_BASE_FINDING_SCOPE = "ir:claim_bucket_contested:%s:abc" % _RK2
_BASE_FINDING_NV = {"row_key": _RK2, "outcome": "contested"}
_BASE_FINDING_ID = _finding_identity(
    _BASE_FINDING_SCOPE, "claim_bucket_contested", _BASE_FINDING_NV
)


def _base_state() -> dict[str, Any]:
    """Fresh, mutable per-family state each call (mutations never leak)."""
    return {
        "effective": {
            "claim:a": {
                "predicate": "preferred_target", "normalized_key": "k1",
                "value": "vX", "scope": "HCAL/Run3", "scope_state": "known",
                "source_id": "page:1", "provenance": "derived",
                "applied_correction_id": None, "stale_correction_review": False,
                "element_ref": None,
            },
            "claim:b": {
                "predicate": "preferred_target", "normalized_key": "k1",
                "value": "vY", "scope": "HCAL/Run3", "scope_state": "known",
                "source_id": "page:2", "provenance": "derived",
                "applied_correction_id": None, "stale_correction_review": False,
                "element_ref": None,
            },
        },
        "resolution": {
            _RK1: {
                "predicate": "preferred_target", "bucket_key": "k1",
                "outcome": "canonical", "winner_claim_id": "claim:a",
                "basis": "source_authority",
                "ranked_claim_ids": ["claim:a", "claim:b"],
                "superseded_claim_ids": ["claim:b"],
            },
        },
        "selections": {
            _RK1: {
                "outcome": "selected", "winner_claim_id": "claim:a",
                "generator_id": "precedence-retraction", "generator_version": "v1",
                "basis": "source_authority",
                "distrusted_claim_ids": ["claim:b"], "asserted_claim_ids": [],
                "unresolved_reason": None,
            },
        },
        "selection_digest": "digest-abcdef",
        "findings": {
            _BASE_FINDING_ID: {
                "input_scope": _BASE_FINDING_SCOPE,
                "normalized_predicate": "claim_bucket_contested",
                "normalized_values": dict(_BASE_FINDING_NV),
                "severity": "medium",
            },
        },
    }


def _snapshot(
    state: dict[str, Any],
    *,
    deployment: str = "dep",
    generation_id: str = "7",
    chronos_generation_id: str = "ckpt-7",
) -> ResolvedStateSnapshot:
    return ResolvedStateSnapshot(
        deployment=deployment,
        generation_id=generation_id,
        chronos_generation_id=chronos_generation_id,
        effective_claims=state["effective"],
        resolution_outcomes=state["resolution"],
        selections=state["selections"],
        selection_digest=state["selection_digest"],
        open_findings=state["findings"],
    )


# --------------------------------------------------------------------------- #
# EQUAL + determinism + byte-stability                                         #
# --------------------------------------------------------------------------- #
def test_equal_on_byte_identical_snapshots() -> None:
    a, b = _snapshot(_base_state()), _snapshot(_base_state())
    assert a.canonical_json() == b.canonical_json()
    verdict = diff_resolved_states(a, b)
    assert verdict.verdict == VERDICT_EQUAL
    assert verdict.counted_as_agreement is True
    assert verdict.artifact is None
    assert verdict.excluded_keys == ()


def test_equal_ignores_generation_labels() -> None:
    # The same-result contract is on the resolved STATE, not the generation the
    # two executions never share: identical families, different labels -> EQUAL.
    ref = _snapshot(_base_state(), generation_id="7", chronos_generation_id="ckpt-7")
    cand = _snapshot(
        _base_state(), generation_id="99", chronos_generation_id="ckpt-99"
    )
    assert ref.canonical_json() != cand.canonical_json()          # labels differ
    assert ref.state_canonical_json() == cand.state_canonical_json()  # state same
    assert diff_resolved_states(ref, cand).verdict == VERDICT_EQUAL


def test_snapshot_byte_stable_across_extraction_order() -> None:
    forward = _snapshot(_base_state())

    reordered = _base_state()
    # Re-insert every family (and one row's fields) in reversed order.
    reordered["effective"] = dict(reversed(list(reordered["effective"].items())))
    reordered["effective"]["claim:a"] = dict(
        reversed(list(reordered["effective"]["claim:a"].items()))
    )
    reordered["findings"] = dict(reversed(list(reordered["findings"].items())))
    backward = _snapshot(reordered)

    assert forward.canonical_json() == backward.canonical_json()
    assert forward.state_canonical_json() == backward.state_canonical_json()


# --------------------------------------------------------------------------- #
# One seeded divergence per family, named in the artifact                      #
# --------------------------------------------------------------------------- #
def test_divergence_effective_claim_value_is_named() -> None:
    ref = _snapshot(_base_state())
    state = _base_state()
    state["effective"]["claim:a"]["value"] = "vZ"
    cand = _snapshot(state)

    verdict = diff_resolved_states(ref, cand)
    assert verdict.verdict == VERDICT_MISMATCH
    assert verdict.counted_as_agreement is False
    assert set(verdict.artifact.families) == {FAMILY_EFFECTIVE}
    fd = verdict.artifact.families[FAMILY_EFFECTIVE]
    assert fd.added == () and fd.removed == ()
    assert fd.changed["claim:a"]["value"] == {
        "reference": "vX", "candidate": "vZ"
    }


def test_divergence_effective_claim_semantics_is_named() -> None:
    ref = _snapshot(_base_state())
    state = _base_state()
    state["effective"]["claim:a"]["claim_semantics"] = (
        '{"modality":"pending"}'
    )
    cand = _snapshot(state)

    verdict = diff_resolved_states(ref, cand)
    assert verdict.verdict == VERDICT_MISMATCH
    assert set(verdict.artifact.families) == {FAMILY_EFFECTIVE}
    changed = verdict.artifact.families[FAMILY_EFFECTIVE].changed["claim:a"]
    assert changed["claim_semantics"] == {
        "reference": None,
        "candidate": '{"modality":"pending"}',
    }


def test_divergence_resolution_winner_is_named() -> None:
    ref = _snapshot(_base_state())
    state = _base_state()
    state["resolution"][_RK1]["winner_claim_id"] = "claim:b"
    cand = _snapshot(state)

    verdict = diff_resolved_states(ref, cand)
    assert verdict.verdict == VERDICT_MISMATCH
    assert set(verdict.artifact.families) == {FAMILY_RESOLUTION}
    fd = verdict.artifact.families[FAMILY_RESOLUTION]
    assert fd.changed[_RK1]["winner_claim_id"] == {
        "reference": "claim:a", "candidate": "claim:b"
    }


def test_divergence_selection_distrusted_set_is_named() -> None:
    ref = _snapshot(_base_state())
    state = _base_state()
    state["selections"][_RK1]["distrusted_claim_ids"] = ["claim:c"]
    cand = _snapshot(state)

    verdict = diff_resolved_states(ref, cand)
    assert verdict.verdict == VERDICT_MISMATCH
    assert set(verdict.artifact.families) == {FAMILY_SELECTION}
    fd = verdict.artifact.families[FAMILY_SELECTION]
    assert fd.changed[_RK1]["distrusted_claim_ids"] == {
        "reference": ["claim:b"], "candidate": ["claim:c"]
    }


def test_divergence_selection_digest_scalar_is_named() -> None:
    ref = _snapshot(_base_state())
    state = _base_state()
    state["selection_digest"] = "digest-CHANGED"
    cand = _snapshot(state)

    verdict = diff_resolved_states(ref, cand)
    assert verdict.verdict == VERDICT_MISMATCH
    fd = verdict.artifact.families[FAMILY_SELECTION]
    assert fd.scalar_changed["selection_digest"] == {
        "reference": "digest-abcdef", "candidate": "digest-CHANGED"
    }


def test_divergence_finding_normalized_values_is_named() -> None:
    ref = _snapshot(_base_state())
    state = _base_state()
    # A changed assignment IS a different finding (per-assignment identity keys
    # on normalized_values): the old identity is removed, the new one added.
    del state["findings"][_BASE_FINDING_ID]
    new_nv = {"row_key": _RK2, "outcome": "canonical"}
    new_id = _finding_identity(
        _BASE_FINDING_SCOPE, "claim_bucket_contested", new_nv
    )
    state["findings"][new_id] = {
        "input_scope": _BASE_FINDING_SCOPE,
        "normalized_predicate": "claim_bucket_contested",
        "normalized_values": new_nv,
        "severity": "medium",
    }
    cand = _snapshot(state)

    verdict = diff_resolved_states(ref, cand)
    assert verdict.verdict == VERDICT_MISMATCH
    assert set(verdict.artifact.families) == {FAMILY_FINDING}
    fd = verdict.artifact.families[FAMILY_FINDING]
    assert _BASE_FINDING_ID in fd.removed   # names the old assignment
    assert new_id in fd.added               # names the new assignment
    assert fd.changed == {}
    # The mutated assignment data rides in the machine-readable artifact (the
    # added identity string embeds the new normalized_values verbatim).
    assert "canonical" in verdict.artifact.canonical_json()


def test_divergence_finding_severity_relabel_is_a_field_change() -> None:
    # severity is a LABEL, not identity: a relabel keeps the finding key and
    # shows up as a per-field diff (not add/remove).
    ref = _snapshot(_base_state())
    state = _base_state()
    state["findings"][_BASE_FINDING_ID]["severity"] = "high"
    cand = _snapshot(state)

    verdict = diff_resolved_states(ref, cand)
    assert verdict.verdict == VERDICT_MISMATCH
    fd = verdict.artifact.families[FAMILY_FINDING]
    assert fd.added == () and fd.removed == ()
    assert fd.changed[_BASE_FINDING_ID]["severity"] == {
        "reference": "medium", "candidate": "high"
    }


def test_multiple_family_divergences_all_named() -> None:
    ref = _snapshot(_base_state())
    state = _base_state()
    state["effective"]["claim:a"]["value"] = "vZ"
    state["resolution"][_RK1]["winner_claim_id"] = "claim:b"
    cand = _snapshot(state)

    verdict = diff_resolved_states(ref, cand)
    assert verdict.verdict == VERDICT_MISMATCH
    assert set(verdict.artifact.families) == {FAMILY_EFFECTIVE, FAMILY_RESOLUTION}


# --------------------------------------------------------------------------- #
# EXCLUDED dominates and names the missing keys (§9.7)                         #
# --------------------------------------------------------------------------- #
def test_excluded_dominates_equal_pair_and_names_keys() -> None:
    a, b = _snapshot(_base_state()), _snapshot(_base_state())
    # Byte-identical states — would be EQUAL — but a replay miss excludes it.
    exclusion = ReplayExclusion(
        step_id="enricher:claim_extractor",
        key_digest="deadbeefcafef00d",
        fragment_id="frag:42",
    )
    verdict = diff_resolved_states(a, b, exclusions=[exclusion, "bare-digest-key"])

    assert verdict.verdict == VERDICT_EXCLUDED
    assert verdict.counted_as_agreement is False       # structural, never agreement
    assert verdict.artifact is None
    named = {k["key_digest"] for k in verdict.excluded_keys}
    assert named == {"deadbeefcafef00d", "bare-digest-key"}
    # The ReplayExclusion is fully named (step + fragment), the bare key too.
    by_digest = {k["key_digest"]: k for k in verdict.excluded_keys}
    assert by_digest["deadbeefcafef00d"]["step_id"] == "enricher:claim_extractor"
    assert by_digest["deadbeefcafef00d"]["fragment_id"] == "frag:42"
    assert by_digest["bare-digest-key"]["step_id"] is None


def test_excluded_dominates_even_a_mismatching_pair() -> None:
    ref = _snapshot(_base_state())
    state = _base_state()
    state["effective"]["claim:a"]["value"] = "vZ"
    cand = _snapshot(state)
    # States diverge, but an unresolved key means we cannot MEASURE equality.
    verdict = diff_resolved_states(
        ref, cand, exclusions=[ReplayExclusion("step", "digest", "frag")]
    )
    assert verdict.verdict == VERDICT_EXCLUDED
    assert verdict.counted_as_agreement is False


# --------------------------------------------------------------------------- #
# No tolerance tier — structurally impossible to ask for one                   #
# --------------------------------------------------------------------------- #
def test_diff_signature_admits_no_tolerance_knob() -> None:
    # The signature admits exactly the two snapshots, the EXCLUDED-dominating
    # exclusions, and the §9.7 certified-differences PROOF (a valid-certificate
    # licence, not a tolerance). It accepts NO epsilon / ignore-list / severity
    # knob — a caller can never ask for a lenient comparison.
    params = inspect.signature(diff_resolved_states).parameters
    assert set(params) == {
        "reference", "candidate", "exclusions", "certified_differences"
    }
    assert params["exclusions"].kind is inspect.Parameter.KEYWORD_ONLY
    assert params["certified_differences"].kind is inspect.Parameter.KEYWORD_ONLY
    # certified_differences defaults to None -> byte-identical to the pre-facet
    # differ; it can only ever RESCUE a would-be mismatch behind a proof.
    assert params["certified_differences"].default is None
    banned = ("tol", "epsilon", "eps", "ignore", "atol", "rtol", "severity",
              "threshold", "approx", "fuzzy")
    for name in params:
        lowered = name.lower()
        assert not any(token in lowered for token in banned), name


def test_counted_as_agreement_is_structural() -> None:
    equal = diff_resolved_states(_snapshot(_base_state()), _snapshot(_base_state()))
    assert equal.counted_as_agreement is (equal.verdict == VERDICT_EQUAL) is True

    # The constructor refuses an inconsistent structural claim.
    with pytest.raises(ValueError, match="counted_as_agreement"):
        EquivalenceVerdict(verdict=VERDICT_EXCLUDED, counted_as_agreement=True,
                           excluded_keys=({"key_digest": "x"},))
    with pytest.raises(ValueError, match="counted_as_agreement"):
        EquivalenceVerdict(verdict=VERDICT_MISMATCH, counted_as_agreement=True)
    with pytest.raises(ValueError, match="name every missing key"):
        EquivalenceVerdict(verdict=VERDICT_EXCLUDED, counted_as_agreement=False)


# --------------------------------------------------------------------------- #
# Round-trip to canonical JSON (storage for the live-series task)              #
# --------------------------------------------------------------------------- #
def test_verdict_and_artifact_round_trip() -> None:
    ref = _snapshot(_base_state())
    state = _base_state()
    state["effective"]["claim:a"]["value"] = "vZ"
    state["selections"][_RK1]["distrusted_claim_ids"] = ["claim:c"]
    mismatch = diff_resolved_states(ref, _snapshot(state))
    assert mismatch.verdict == VERDICT_MISMATCH

    restored = EquivalenceVerdict.from_canonical_json(mismatch.canonical_json())
    assert restored.canonical_json() == mismatch.canonical_json()
    assert restored.artifact.canonical_json() == mismatch.artifact.canonical_json()

    excluded = diff_resolved_states(
        ref, ref, exclusions=[ReplayExclusion("s", "d", "f")]
    )
    assert (
        EquivalenceVerdict.from_canonical_json(excluded.canonical_json()).canonical_json()
        == excluded.canonical_json()
    )


def test_snapshot_round_trips() -> None:
    snap = _snapshot(_base_state())
    restored = ResolvedStateSnapshot.from_canonical_json(snap.canonical_json())
    assert restored.canonical_json() == snap.canonical_json()
    assert diff_resolved_states(snap, restored).verdict == VERDICT_EQUAL


# --------------------------------------------------------------------------- #
# Live end-to-end: extract from a real consistency-fixture publish.            #
# --------------------------------------------------------------------------- #
psycopg = pytest.importorskip("psycopg")

import yaml  # noqa: E402

from okg.substrate.catalog.modules_apply import apply_composed_catalog  # noqa: E402
from okg.substrate.catalog.modules_compose import compose_catalog  # noqa: E402
from okg.substrate.derived_artifacts.claim_projection_derivation import (  # noqa: E402
    bucket_row_key,
)
from okg.substrate.graph_store import ChronosGraphStore  # noqa: E402
from okg.substrate.library.sources.base import NodeFact  # noqa: E402
from okg.substrate.publisher import Publisher  # noqa: E402
from okg.substrate.publisher.deployment_stack import (  # noqa: E402
    build_deployment_publisher_stack,
)
from tests.pytest.catalog_fixture_builders import bind_test_catalog_writer  # noqa: E402

_DEPLOYMENT = "consistency-fixture"
_PREDICATE = "preferred_target"
_CONSTRAINT = "unique_preferred"
_SCOPE = "HCAL/Run3"
_DEP_DIR = Path(__file__).resolve().parents[3] / "deployments" / "consistency-fixture"


def _install_composed_catalog(dsn: str, tmp_path: Path) -> None:
    d = tmp_path / "scratch"
    d.mkdir()
    (d / "deployment.yaml").write_text(yaml.safe_dump({
        "name": "scratch", "schema_dir": "schemas",
        "modules": ["extraction", "consistency"],
    }))
    (d / "schemas").mkdir()
    composed = compose_catalog(d)
    with psycopg.connect(dsn, autocommit=True) as conn:
        apply_composed_catalog(conn, composed)


def _apply(dsn: str, facts: list[NodeFact]) -> None:
    with psycopg.connect(dsn, autocommit=True) as conn:
        store = ChronosGraphStore.from_connection(conn, ensure_schema=True)
        store.apply_facts(facts, source=_DEPLOYMENT, strict_endpoints=False)


def _claim_fact(node_id: str, value: str, *, key: str, source: str,
                source_class: str, **signals: Any) -> NodeFact:
    attrs = {
        "predicate": _PREDICATE, "normalized_key": key, "value": value,
        "scope": _SCOPE, "scope_state": "known",
        "claim_provenance": "derived", "source_id": source,
        "source_class": source_class,
    }
    attrs.update({k: str(v) for k, v in signals.items()})
    return NodeFact(node_id, "consistency_claim", attrs, {"id": node_id}, {"v": 1})


def _functional_policy(node_id: str) -> NodeFact:
    return NodeFact(
        node_id, "predicate_policy",
        {"enabled": "true", "policy_id": _CONSTRAINT,
         "policy_kind": "functional", "predicate": _PREDICATE},
        {"id": node_id}, {"v": 1},
    )


def _publish(dsn: str) -> dict[str, Any]:
    stack = build_deployment_publisher_stack(
        dsn=dsn, deployment_name=_DEPLOYMENT, deployment_dir=_DEP_DIR,
    )
    out = Publisher(dsn, _DEPLOYMENT, **stack.publisher_kwargs(), catalog_identity=bind_test_catalog_writer(dsn, _DEPLOYMENT)).cycle()
    assert out["status"] == "published", out
    return out


def _extract(dsn: str, *, chronos_gen: Any, compat_gen: str) -> ResolvedStateSnapshot:
    with psycopg.connect(dsn, autocommit=True) as conn:
        return ResolvedStateSnapshot.extract(
            conn,
            deployment=_DEPLOYMENT,
            generation_id=compat_gen,
            chronos_generation_id=chronos_gen,
        )


@pytest.mark.postgres
def test_live_snapshot_equal_determinism_and_mismatch(
    clean_runtime: str, tmp_path: Path
) -> None:
    _install_composed_catalog(clean_runtime, tmp_path)
    sfx = uuid.uuid4().hex[:8]
    a, b = f"claim:a:{sfx}", f"claim:b:{sfx}"
    c, d = f"claim:c:{sfx}", f"claim:d:{sfx}"
    key1, key2 = f"key|one:{sfx}", f"key|two:{sfx}"
    rk1, rk2 = bucket_row_key(_PREDICATE, key1), bucket_row_key(_PREDICATE, key2)

    # Decisive bucket key1 (A authority 10 beats B), contested bucket key2 (tie).
    _apply(clean_runtime, [
        _claim_fact(a, "vX", key=key1, source="page:1",
                    source_class="release_notes", source_authority=10),
        _claim_fact(b, "vY", key=key1, source="page:2",
                    source_class="wiki", source_authority=1),
        _claim_fact(c, "vP", key=key2, source="page:3", source_class="wiki"),
        _claim_fact(d, "vQ", key=key2, source="page:4", source_class="wiki"),
        _functional_policy(f"pol:{sfx}"),
    ])
    out = _publish(clean_runtime)
    chronos_gen = out["generation_id"]
    compat_gen = str(out["compat_generation_id"])
    print(f"\n[equiv-differ] published chronos_gen={chronos_gen} "
          f"compat_gen={compat_gen}")

    # ---- determinism: two extractions of the SAME generation are byte-equal.
    snap1 = _extract(clean_runtime, chronos_gen=chronos_gen, compat_gen=compat_gen)
    snap2 = _extract(clean_runtime, chronos_gen=chronos_gen, compat_gen=compat_gen)
    assert snap1.canonical_json() == snap2.canonical_json()

    # ---- every family populated (extraction reaches all four surfaces).
    print(f"[equiv-differ] effective={len(snap1.effective_claims)} "
          f"resolution={len(snap1.resolution_outcomes)} "
          f"selections={len(snap1.selections)} "
          f"findings={len(snap1.open_findings)} "
          f"digest={(snap1.selection_digest or '')[:12]}")
    assert len(snap1.effective_claims) >= 4          # a, b, c, d
    assert len(snap1.resolution_outcomes) >= 2       # key1 + key2 buckets
    assert set(snap1.selections) == {rk1, rk2}
    assert snap1.selection_digest
    assert len(snap1.open_findings) >= 1             # contested bucket finding
    # The recorded selection matches the wire contract (decisive vs contested).
    assert snap1.selections[rk1]["outcome"] == "selected"
    assert snap1.selections[rk1]["distrusted_claim_ids"] == [b]
    assert snap1.selections[rk2]["outcome"] == "unresolved"

    # ---- EQUAL against itself.
    assert diff_resolved_states(snap1, snap2).verdict == VERDICT_EQUAL

    # ---- EXCLUDED dominates the equal pair when a replay key is missing.
    excluded = diff_resolved_states(
        snap1, snap2,
        exclusions=[ReplayExclusion("enricher:claim_extractor", "missingkey", "f")],
    )
    assert excluded.verdict == VERDICT_EXCLUDED
    assert excluded.counted_as_agreement is False
    assert excluded.excluded_keys[0]["key_digest"] == "missingkey"

    # ---- surgical row edit -> MISMATCH naming the exact bucket + field.
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE okg.repair_selections "
                "SET distrusted_claim_ids = %s::jsonb "
                "WHERE deployment_name = %s AND generation_id = %s AND bucket_key = %s",
                (json.dumps(["claim:surgical"]), _DEPLOYMENT, compat_gen, rk1),
            )
            assert cur.rowcount == 1
    mutated = _extract(clean_runtime, chronos_gen=chronos_gen, compat_gen=compat_gen)

    verdict = diff_resolved_states(snap1, mutated)
    assert verdict.verdict == VERDICT_MISMATCH
    assert verdict.counted_as_agreement is False
    assert FAMILY_SELECTION in verdict.artifact.families
    fd = verdict.artifact.families[FAMILY_SELECTION]
    assert rk1 in fd.changed
    assert fd.changed[rk1]["distrusted_claim_ids"] == {
        "reference": [b], "candidate": ["claim:surgical"]
    }
    print(f"[equiv-differ] MISMATCH named bucket={rk1} "
          f"field=distrusted_claim_ids ref={[b]} cand=['claim:surgical']")
    # The digest also moved (whole-selection digest is not re-derived by the
    # surgical edit, so it stays byte-identical -> only the row diverges).
    assert verdict.artifact.canonical_json()  # round-trippable, non-empty
    assert (
        EquivalenceVerdict.from_canonical_json(verdict.canonical_json()).canonical_json()
        == verdict.canonical_json()
    )


# --------------------------------------------------------------------------- #
# As-of prerequisite (PACT consistency-resolution-aware-reuse, the live-series #
# seam): an open finding carried forward unchanged from an earlier generation  #
# is captured by the gen-N snapshot. The pre-fix `latest_generation_id = gen`  #
# filter dropped it (bounded event growth does not re-stamp an unchanged        #
# finding); the as-of `<= gen` filter (mirroring gates.py) keeps the series     #
# honest.                                                                       #
# --------------------------------------------------------------------------- #
def _finding_row_index(dsn: str) -> dict[str, dict[str, Any]]:
    """Map every deployment open/closed finding identity -> its current row
    (latest_generation_id + status), built exactly like the differ builds its
    finding identity so the two are comparable."""
    with psycopg.connect(dsn, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT oc.input_scope, oc.normalized_predicate, oc.normalized_values, "
            "       oc.latest_generation_id, oc.finding_status "
            "  FROM okg.observation_current oc "
            "  JOIN okg.consistency_check_definitions d "
            "    ON d.check_id = oc.check_id "
            " WHERE d.deployment_name = %s",
            (_DEPLOYMENT,),
        )
        rows = cur.fetchall()
    index: dict[str, dict[str, Any]] = {}
    for input_scope, npred, nvals, latest_gen, status in rows:
        identity = ed._canonical_json(
            [str(input_scope), str(npred), dict(nvals or {})]
        )
        index[identity] = {"latest_gen": int(latest_gen), "status": status}
    return index


@pytest.mark.postgres
def test_carried_forward_unchanged_finding_is_captured(
    clean_runtime: str, tmp_path: Path
) -> None:
    """A finding opened at gen1 and carried forward UNCHANGED to gen2 (which
    touches an unrelated bucket) is captured by the gen2 snapshot. Under the
    pre-fix `= gen` filter it was dropped (its latest_generation_id stays at
    gen1); the as-of `<= gen` filter keeps a steady-state series honest."""
    _install_composed_catalog(clean_runtime, tmp_path)
    sfx = uuid.uuid4().hex[:8]

    # ---- gen1: a contested bucket (tie) opens a finding. --------------------
    c, d = f"claim:c:{sfx}", f"claim:d:{sfx}"
    key1 = f"key|contested:{sfx}"
    _apply(clean_runtime, [
        _claim_fact(c, "vP", key=key1, source="page:c", source_class="wiki"),
        _claim_fact(d, "vQ", key=key1, source="page:d", source_class="wiki"),
        _functional_policy(f"pol:{sfx}"),
    ])
    out1 = _publish(clean_runtime)
    compat1 = str(out1["compat_generation_id"])
    snap1 = _extract(
        clean_runtime, chronos_gen=out1["generation_id"], compat_gen=compat1
    )
    carried_ids = set(snap1.open_findings)
    assert carried_ids, "gen1 must open at least one finding (contested bucket)"

    # ---- gen2: touch an UNRELATED bucket so the contested finding is carried
    #      forward unchanged (a single new claim -> a clean, finding-free bucket).
    e = f"claim:e:{sfx}"
    key2 = f"key|unrelated:{sfx}"
    _apply(clean_runtime, [
        _claim_fact(e, "vR", key=key2, source="page:e", source_class="wiki"),
    ])
    out2 = _publish(clean_runtime)
    compat2 = str(out2["compat_generation_id"])
    assert int(compat2) > int(compat1), (compat1, compat2)

    # ---- precondition (this is what makes the witness a real regression guard):
    #      the carried finding is STILL OPEN but its latest_generation_id is at
    #      gen1, NOT re-stamped at gen2. A `= gen2` filter therefore MISSES it.
    index = _finding_row_index(clean_runtime)
    for fid in carried_ids:
        row = index[fid]
        assert row["status"] == "open", (fid, row)
        assert row["latest_gen"] == int(compat1) < int(compat2), (fid, row)
    print(f"\n[carried-finding] compat1={compat1} compat2={compat2} "
          f"carried={len(carried_ids)} "
          f"latest_gens={sorted({index[f]['latest_gen'] for f in carried_ids})} "
          f"(stamped at gen1, NOT re-stamped at gen2 -> `= gen2` would drop it)")

    # ---- the fix: the gen2 snapshot CAPTURES the carried finding (as-of `<=`).
    snap2 = _extract(
        clean_runtime, chronos_gen=out2["generation_id"], compat_gen=compat2
    )
    assert carried_ids <= set(snap2.open_findings), (
        "as-of extraction must carry the unchanged open finding into the gen2 "
        "snapshot; missing="
        f"{sorted(carried_ids - set(snap2.open_findings))}"
    )
    print(f"[carried-finding] gen2 snapshot open_findings={len(snap2.open_findings)} "
          f"captured all {len(carried_ids)} carried finding(s)")
