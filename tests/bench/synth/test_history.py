"""task.hist.generator + task.hist.expected-diff witnesses, gold side (no DB).

Everything here is a property of the GENERATOR: byte reproducibility, the
touched-document contract, the four live-proof shapes, the tamper-evident digest
chain, the per-step leakage scan, and the expected accepted-result diff computed
from the world and the declared precedence order. Every witness is shown able to
FIRE — a contract nothing can break is not a contract.
"""

from __future__ import annotations

import copy
import dataclasses
import hashlib
import json
from pathlib import Path

import pytest

from okg.bench.synth.corruption import Planner
from okg.bench.synth.goldseal import build_manifest, scan_unsanctioned_surface
from okg.bench.synth.history import (
    DELTA_DOMINATED_CLAIM,
    DELTA_PLANT_ERROR,
    DELTA_REAFFIRMATION,
    GOLD_PRECEDENCE_ORDER,
    LIVE_PROOF_SHAPES,
    OUTCOME_CANONICAL,
    OUTCOME_CONTESTED,
    REASON_DOMINATED,
    REASON_UNTOUCHED,
    SIG_SOURCE_AUTHORITY,
    SIG_SUPPORT,
    GoldClaim,
    History,
    HistoryConfig,
    HistoryError,
    build_history,
    gold_canonicalize_value,
    gold_resolve_bucket,
    history_from_json,
    history_to_json,
    predict_accepted_state,
    taxonomy_census,
    unsanctioned_history_surfaces,
    verify_chain,
)
from okg.bench.synth.prf import SynthPrf
from okg.bench.synth.render import pack_bytes, render_corpus

_UPDATES = 22


@pytest.fixture(scope="module")
def rendered_smoke(world, smoke_config):
    prf = SynthPrf(smoke_config.seed)
    planner = Planner(world, smoke_config, prf)
    plan = planner.plan()
    rendered = render_corpus(world, smoke_config, planner, plan, prf)
    manifest = build_manifest(
        config=smoke_config, world=world, plan=plan, rendered=rendered
    )
    return rendered, manifest


@pytest.fixture(scope="module")
def history(world, smoke_config, rendered_smoke) -> History:
    rendered, manifest = rendered_smoke
    return build_history(
        config=smoke_config,
        world=world,
        manifest=manifest,
        base_docs=rendered.clean_docs,
        history_config=HistoryConfig(updates=_UPDATES),
    )


# --------------------------------------------------------------------------- #
# req.hist.generator — determinism, gold side                                  #
# --------------------------------------------------------------------------- #
def test_two_generations_are_byte_identical(world, smoke_config, rendered_smoke):
    """ACCEPTANCE: the same (config, seed, world) yields byte-identical histories."""
    rendered, manifest = rendered_smoke
    first = build_history(
        config=smoke_config, world=world, manifest=manifest,
        base_docs=rendered.clean_docs, history_config=HistoryConfig(updates=_UPDATES),
    )
    second = build_history(
        config=smoke_config, world=world, manifest=manifest,
        base_docs=rendered.clean_docs, history_config=HistoryConfig(updates=_UPDATES),
    )
    assert first.json_bytes() == second.json_bytes()
    assert first.history_digest == second.history_digest

    # NON-VACUITY: a different seed must produce a DIFFERENT history, or
    # "byte-identical" is measuring a constant rather than a reproduction.
    other_seed = smoke_config.model_copy(update={"seed": smoke_config.seed + "-b"})
    other = build_history(
        config=other_seed, world=world, manifest=manifest,
        base_docs=rendered.clean_docs, history_config=HistoryConfig(updates=_UPDATES),
    )
    assert other.json_bytes() != first.json_bytes()


def test_round_trip_through_disk_form(history: History):
    """The replay reads gold from disk; the on-disk form must rehydrate exactly."""
    payload = json.loads(json.dumps(history_to_json(history), sort_keys=True))
    rebuilt = history_from_json(payload)
    assert rebuilt.json_bytes() == history.json_bytes()
    assert verify_chain(rebuilt) == []


def _touch_problems(history: History) -> list[str]:
    """Both directions of the touched-document contract, as a list of problems.

    Factored out so the same predicate can be pointed at a TAMPERED history and
    shown to fire — a contract asserted only against a correct artifact proves
    nothing about the contract.
    """
    problems: list[str] = []
    previous = dict(history.base_docs)
    for step in history.steps:
        current = dict(previous)
        current.update(step.doc_texts)
        actual = {
            doc for doc in set(previous) | set(current)
            if previous.get(doc) != current.get(doc)
        }
        declared = set(step.touched_docs)
        for doc in sorted(declared - actual):
            problems.append(f"update {step.index}: declared-but-identical touch {doc}")
        for doc in sorted(actual - declared):
            problems.append(f"update {step.index}: undeclared touch {doc}")
        previous = current
    return problems


def test_touched_document_set_equals_the_byte_diff(history: History):
    """ACCEPTANCE: no undeclared touch, no declared-but-identical touch."""
    assert _touch_problems(history) == []

    # FIRE (undeclared touch): a step's bytes move for a document it never
    # declared. The corpus and gold would be out of step and every downstream
    # per-step comparison would be against the wrong corpus.
    victim = sorted(history.base_docs)[0]
    smuggled = _with_step(
        history,
        1,
        doc_texts={
            **history.steps[1].doc_texts,
            victim: history.base_docs[victim] + "\nSmuggled: 1\n",
        },
    )
    fired = _touch_problems(smuggled)
    assert any("undeclared touch" in problem for problem in fired), fired

    # FIRE (declared-but-identical touch): a step claims a document moved when
    # its bytes did not, which would inflate the touched set a reuse mechanism
    # is graded against.
    inflated = _with_step(
        history, 1, touched_docs=tuple(list(history.steps[1].touched_docs) + [victim])
    )
    fired = _touch_problems(inflated)
    assert any("declared-but-identical" in problem for problem in fired), fired


def _with_step(history: History, index: int, **changes) -> History:
    """A tampered copy of a history: one step's sealed fields overwritten.

    The dataclasses are frozen on purpose (gold must not be edited in place), so
    a tamper witness has to mint a new artifact — which is exactly what an
    attacker or a buggy regeneration would do.
    """
    steps = list(history.steps)
    steps[index] = dataclasses.replace(steps[index], **changes)
    return dataclasses.replace(history, steps=tuple(steps))


def test_all_four_live_proof_shapes_occur(history: History):
    """ACCEPTANCE: the four live-proof shapes appear, labelled in sealed gold."""
    census = taxonomy_census(history)
    labels = {step.delta.label for step in history.steps}
    for kind, label in LIVE_PROOF_SHAPES.items():
        assert census[kind] >= 1, f"{kind} never occurred: {census}"
        assert label in labels
    assert census[DELTA_PLANT_ERROR] >= 5, census
    assert sum(census.values()) == _UPDATES


def test_digest_chain_breaks_loudly_on_any_altered_artifact(history: History):
    """ACCEPTANCE-adjacent: a history is tamper-evident end to end."""
    assert verify_chain(history) == []

    target = next(s for s in history.steps if s.expected_diff["changed"])
    diff = copy.deepcopy(dict(target.expected_diff))
    bucket = sorted(diff["changed"])[0]
    diff["changed"][bucket]["after"] = {
        "outcome": "canonical", "winner_value": "TAMPERED", "basis": "tie",
    }
    tampered = _with_step(history, target.index, expected_diff=diff)
    broken = verify_chain(tampered)
    assert broken, "an altered expected diff must break the chain"
    assert f"step {target.index}:" in broken[0]
    # and every LATER step inherits the break, so a tamper cannot be localized
    # away by re-sealing one artifact
    later = [s for s in history.steps if s.index > target.index]
    assert len(broken) >= 1 + min(1, len(later))

    # the same is true of a re-planted live corruption set, an altered touched
    # set, and a forged stamp: the seal covers all of them, not just the diff
    planted = next(s for s in history.steps if s.live_corruptions and s.touched_docs)
    for changes in (
        {"live_corruptions": ()},
        {"touched_docs": ()},
        {"stamps": {"claim:forged": {"source_authority": "99"}}},
    ):
        assert verify_chain(_with_step(history, planted.index, **changes)), changes


def test_per_step_leakage_scan_is_clean_and_can_fire(history, smoke_config, rendered_smoke):
    """ACCEPTANCE: no gold surface enters the engine at any step."""
    _rendered, manifest = rendered_smoke
    surfaces = {
        "history_config": json.dumps(HistoryConfig(updates=_UPDATES).as_json()),
        "synth_config": smoke_config.canonical_json_bytes().decode("utf-8"),
        "template_pack": pack_bytes().decode("utf-8"),
        **unsanctioned_history_surfaces(history),
    }
    assert scan_unsanctioned_surface(manifest, surfaces) == []

    # FIRE: a canary token is planted in the manifest and NEVER rendered, so its
    # appearance in any unsanctioned surface is a leak by construction.
    canary = manifest["canary_tokens"][0]
    dirty = dict(surfaces)
    dirty["history_step_00_stamps"] = json.dumps({"claim:x": {"note": canary}})
    violations = scan_unsanctioned_surface(manifest, dirty)
    assert violations and canary in violations[0]

    # FIRE: a patient record identifier is a forbidden token too, and fact ids
    # carry one — so leaking a fact id into a step's engine-visible surface
    # fires exactly as a canary does.
    fact_id = manifest["trace"][0]["fact_id"]
    dirty = dict(surfaces)
    dirty["history_step_01_delta_kind"] = json.dumps({"fact": fact_id})
    assert scan_unsanctioned_surface(manifest, dirty)


# --------------------------------------------------------------------------- #
# The gold-side precedence model                                               #
# --------------------------------------------------------------------------- #
def _claim(cid, value, doc, **kw) -> GoldClaim:
    return GoldClaim(claim_id=cid, bucket="p:e", source_id=doc, value=value, **kw)


def test_gold_precedence_model_states_the_order_it_claims():
    """The restatement's own unit tests: which signal settles, and which cannot."""
    # authority settles, and it OUTRANKS corroboration: two mirrored documents
    # asserting X lose to one authoritative Y.
    resolved = gold_resolve_bucket(
        [
            _claim("c1", "X", "a.md"),
            _claim("c2", "X", "b.md"),
            _claim("c3", "Y", "c.md", source_authority=9),
        ]
    )
    assert resolved["outcome"] == OUTCOME_CANONICAL
    assert resolved["winner_value"] == "Y"
    assert resolved["basis"] == SIG_SOURCE_AUTHORITY

    # with no authority anywhere, corroboration settles it the other way
    resolved = gold_resolve_bucket(
        [_claim("c1", "X", "a.md"), _claim("c2", "X", "b.md"), _claim("c3", "Y", "c.md")]
    )
    assert (resolved["outcome"], resolved["winner_value"], resolved["basis"]) == (
        OUTCOME_CANONICAL, "X", SIG_SUPPORT,
    )

    # a 1-vs-1 split with nothing to separate it is CONTESTED, never a coin flip
    resolved = gold_resolve_bucket([_claim("c1", "X", "a.md"), _claim("c2", "Y", "b.md")])
    assert resolved["outcome"] == OUTCOME_CONTESTED
    assert resolved["winner_value"] is None

    # a lead on a RANKING-ONLY signal is contested: recency ranks, never settles
    resolved = gold_resolve_bucket(
        [
            _claim("c1", "X", "a.md", document_recency="2030-01-01"),
            _claim("c2", "Y", "b.md", document_recency="2000-01-01"),
        ]
    )
    assert resolved["outcome"] == OUTCOME_CONTESTED
    assert resolved["basis"] == "document_recency"

    # a unanimous bucket is ABSENT from the accepted state (the engine writes no
    # resolution row for it), never present-and-unanimous
    assert predict_accepted_state([_claim("c1", "X", "a.md"), _claim("c2", "X", "b.md")]) == {}


def test_gold_canonicalization_matches_the_runner_over_real_surfaces(rendered_smoke):
    """The restatement is audited against the runner it must equal, on the REAL
    rendered surfaces — not on a hand-picked pair."""
    from okg.bench.synth.runner import canonicalize_value

    rendered, _manifest = rendered_smoke
    surfaces = {row.surface_value for row in rendered.trace}
    assert surfaces
    mismatches = [
        s for s in sorted(surfaces) if gold_canonicalize_value(s) != canonicalize_value(s)
    ]
    assert mismatches == []
    # and the fold is exercised, not vacuously identity on every input
    assert gold_canonicalize_value("82000 g") == "82.0 kg"
    assert gold_canonicalize_value("1750 mm") == "175.0 cm"


# --------------------------------------------------------------------------- #
# req.hist.expected-diff — reason classes and falsifiability                   #
# --------------------------------------------------------------------------- #
def test_expected_diff_reason_classes_are_total_and_meaningful(history: History):
    for step in history.steps:
        diff = step.expected_diff
        touched = set(diff["touched_buckets"])
        for bucket, cell in diff["unchanged"].items():
            expected = REASON_DOMINATED if bucket in touched else REASON_UNTOUCHED
            assert cell["reason"] == expected, (step.index, bucket)
        for bucket in diff["changed"]:
            assert bucket in touched, (step.index, bucket)
        assert set(diff["skippable"]) == {
            b for b, c in diff["unchanged"].items() if c["reason"] == REASON_DOMINATED
        }

    # the dominated-claim shape is the skippable one, by construction AND in gold
    dominated = [s for s in history.steps if s.delta.kind == DELTA_DOMINATED_CLAIM]
    assert dominated, "the history must realize the changed-inputs skip shape"
    for step in dominated:
        assert step.delta.bucket in step.expected_diff["skippable"], step.index
        assert step.delta.bucket not in step.expected_diff["changed"]

    # the no-claim-delta shape touches a document and NO bucket: pure carry-forward
    reaffirmations = [s for s in history.steps if s.delta.kind == DELTA_REAFFIRMATION]
    assert reaffirmations
    for step in reaffirmations:
        assert step.touched_docs, "a reaffirmation still revises a document"
        assert step.expected_diff["touched_buckets"] == []
        assert step.expected_diff["changed"] == {}


def test_expected_diff_names_the_post_update_winner(history: History):
    """Where precedence determines a winner, gold records WHICH value wins."""
    winners = [
        cell["after"]["winner_value"]
        for step in history.steps
        for cell in step.expected_diff["changed"].values()
        if cell["after"] and cell["after"]["outcome"] == OUTCOME_CANONICAL
    ]
    assert len(winners) >= 5, winners
    assert all(w is not None for w in winners)


def test_precedence_order_mutation_moves_the_dominated_labels(
    world, smoke_config, rendered_smoke
):
    """ACCEPTANCE: the skippable-step labelling is falsifiable.

    Transposing corroboration above declared authority in the GOLD-SIDE order
    turns the dominated shape decisive: the extra loser's arrival now carries a
    corroboration majority, so a step gold labelled skippable becomes a step gold
    says must change. If the labelling were insensitive to the order it would be
    describing nothing.
    """
    rendered, manifest = rendered_smoke
    declared = build_history(
        config=smoke_config, world=world, manifest=manifest,
        base_docs=rendered.clean_docs, history_config=HistoryConfig(updates=_UPDATES),
    )
    transposed = list(GOLD_PRECEDENCE_ORDER)
    i, j = transposed.index(SIG_SOURCE_AUTHORITY), transposed.index(SIG_SUPPORT)
    transposed[i], transposed[j] = transposed[j], transposed[i]
    mutated = build_history(
        config=smoke_config, world=world, manifest=manifest,
        base_docs=rendered.clean_docs,
        history_config=HistoryConfig(
            updates=_UPDATES, precedence_order=tuple(transposed)
        ),
    )
    declared_skippable = {
        s.index: tuple(s.expected_diff["skippable"]) for s in declared.steps
    }
    mutated_skippable = {
        s.index: tuple(s.expected_diff["skippable"]) for s in mutated.steps
    }
    assert declared_skippable != mutated_skippable
    moved = [i for i in declared_skippable if declared_skippable[i] != mutated_skippable[i]]
    assert moved, "no step changed its dominated label under a transposed order"
    # the DELTA PROGRAM is unchanged — the same updates, a different verdict on
    # which of them a sound reuse mechanism could have skipped
    assert [s.delta.kind for s in declared.steps] == [s.delta.kind for s in mutated.steps]


def test_funnel_live_set_grows_and_shrinks_with_the_history(history: History):
    """ACCEPTANCE: the live corruption set is derived from the history."""
    live_counts = [len(step.live_corruptions) for step in history.steps]
    assert live_counts[0] == 0, "a history starts from a clean corpus"
    assert max(live_counts) >= 3
    # planting grows the live set, correcting away shrinks it
    grew = shrank = False
    previous: set[str] = set()
    for step in history.steps:
        current = set(step.live_corruptions)
        if current > previous:
            grew = True
        if current < previous:
            shrank = True
        previous = current
    assert grew and shrank, "the history must both plant and unplant"


def test_history_refuses_a_relational_constraint_kind(world, smoke_config, rendered_smoke):
    """Fail-closed: the gold precedence model covers the functional Σ kind only."""
    _rendered, manifest = rendered_smoke
    poisoned = copy.deepcopy(dict(manifest))
    poisoned["gold"] = dict(poisoned["gold"])
    sites = [dict(s) for s in poisoned["gold"]["corruptions"]]
    sites[0]["params"] = {**dict(sites[0].get("params", {})),
                          "constraint_kind": "linked_pair"}
    poisoned["gold"]["corruptions"] = sites
    with pytest.raises(HistoryError, match="FUNCTIONAL"):
        build_history(
            config=smoke_config, world=world, manifest=poisoned,
            base_docs=_rendered.clean_docs,
            history_config=HistoryConfig(updates=4),
        )


def test_history_refuses_an_undersized_arm_pool(world, smoke_config, rendered_smoke):
    rendered, manifest = rendered_smoke
    with pytest.raises(HistoryError, match="arm buckets"):
        build_history(
            config=smoke_config, world=world, manifest=manifest,
            base_docs=rendered.clean_docs,
            history_config=HistoryConfig(updates=4, arm_buckets=10_000),
        )


def test_history_arm_buckets_never_collide_with_corruption_buckets(
    history: History, rendered_smoke
):
    """An update landing in a corruption's bucket would move that corruption's
    realized vote, and the funnel would read an operator update as an extraction
    effect. Asserted, not assumed."""
    from okg.bench.synth.resolution import corruption_bucket_map

    _rendered, manifest = rendered_smoke
    corruption_buckets = set(corruption_bucket_map(manifest).values())
    arm_buckets = {
        step.delta.bucket
        for step in history.steps
        if step.delta.kind not in (DELTA_PLANT_ERROR, "unplant_error") and step.delta.bucket
    }
    assert arm_buckets
    assert not (arm_buckets & corruption_buckets)


def test_corpus_at_reconstructs_every_step(history: History):
    docs = dict(history.base_docs)
    for step in history.steps:
        docs.update(step.doc_texts)
        rebuilt = history.corpus_at(step.index)
        assert rebuilt == docs
        assert {
            doc: hashlib.sha256(text.encode()).hexdigest()
            for doc, text in sorted(rebuilt.items())
        } == dict(step.corpus_digests)


def test_update_documents_join_the_scope_they_target(history: History):
    """An update document must sit under the SAME heading path as the claims it
    joins: the engine treats disjoint scopes as compatible, so an invented
    heading would silently scope-suppress the update out of its own bucket."""
    for step in history.steps:
        for doc, text in step.doc_texts.items():
            if not doc.startswith("update_"):
                continue
            section = next(line for line in text.splitlines() if line.startswith("## "))
            base_sections = {
                line
                for base in history.base_docs.values()
                for line in base.splitlines()
                if line.startswith("## ")
            }
            assert section in base_sections, (doc, section)
            assert text.splitlines()[0].startswith("# Patient ")


def test_written_history_is_stable_json(history: History, tmp_path: Path):
    path = tmp_path / "history.json"
    path.write_text(json.dumps(history_to_json(history), sort_keys=True, indent=1))
    again = history_from_json(json.loads(path.read_text()))
    assert again.json_bytes() == history.json_bytes()
