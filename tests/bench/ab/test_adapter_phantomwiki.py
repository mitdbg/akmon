"""PhantomWiki adapter — set-equality scoring and the gold boundary.

PACT okg-bench-graph-benchmarks / req.pw.adapter, task.pw.adapter.

The scoring tests are anchored to a GOLDEN TABLE produced by running the
real `phantom_eval.score` (upstream 1.0.3) over 340 cases and recording its
verdicts — `.scratch/compare_scorer.py` regenerates it. The table is what
makes these tests evidence rather than a restatement of our own code: a
hand-written expectation would agree with whatever we implemented.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from okg.bench.ab.adapters.phantomwiki import (
    ANSWER_SEP,
    FORBIDDEN_SNAPSHOT_NAMES,
    GOLD_BEARING_QUESTION_FIELDS,
    PhantomWikiAdapter,
    exact_match,
    f1,
    normalize_pred,
    precision,
    recall,
)
from okg.bench.ab.errors import DatasetError
from okg.bench.ab.model import TaskMetrics, TaskResult

FIXTURES = Path(__file__).parent / "fixtures"
GOLDEN = json.loads(
    (FIXTURES / "phantomwiki_scorer_golden.json").read_text(encoding="utf-8")
)

GENERATION = {
    "universe_seed": 1,
    "universe_family_trees": 2000,
    "universe_max_family_tree_size": 50,
    "universe_question_depth": 20,
    "universe_id": "deadbeefcafe0001",
}


def _question(**over):
    row = {
        "id": "q-1",
        "question": "Who is the great-granddaughter of Williams Smock?",
        "answer": ["Lenore Hackworth"],
        # every gold-bearing field the release carries, present so the
        # exclusion has something to exclude
        "solution_traces": '[{"Y_4": "Lenore Hackworth"}]',
        "prolog": {
            "query": ["great_granddaughter(\"Williams Smock\", Y_2)"],
            "answer": "Y_2",
        },
        "template": ["Who is", "the", "<relation>_3"],
        "type": 1,
        "difficulty": 5,
        "is_aggregation_question": False,
    }
    row.update(over)
    return row


def _fixture(tmp_path: Path, rows=None, generation=None) -> Path:
    payload = {
        "generation": generation if generation is not None else GENERATION,
        "questions": {"type0": rows if rows is not None else [_question()]},
    }
    tmp_path.mkdir(parents=True, exist_ok=True)
    path = tmp_path / "offline.json"
    path.write_text(json.dumps(payload), encoding="utf-8")
    return path


def _result(instance_id: str, answer: str) -> TaskResult:
    return TaskResult(
        instance_id=instance_id, arm="echo", status="completed", answer=answer,
        metrics=TaskMetrics(0, 0, 0, 0, 0, 0.0),
    )


# ----------------------------------------------------------------------
# scoring
# ----------------------------------------------------------------------


def test_scoring_is_set_equality():
    """Set EQUALITY, not intersection — the property the PACT names.

    A prediction that names one of two correct people shares a member with
    the gold and still scores zero exact match. This program has already
    found a benchmark whose scorer counted any shared token as correct.
    """
    assert exact_match("Dino Beltran,Shelli Beltran", "Shelli Beltran,Dino Beltran")
    assert not exact_match("Dino Beltran", "Dino Beltran,Shelli Beltran")
    assert not exact_match(
        "Dino Beltran,Shelli Beltran,Vicki Hackworth",
        "Dino Beltran,Shelli Beltran",
    )
    # ...while the partial credit is still visible, over the SAME normalisation
    assert precision("Dino Beltran", "Dino Beltran,Shelli Beltran") == 1.0
    assert recall("Dino Beltran", "Dino Beltran,Shelli Beltran") == 0.5


def test_scoring_matches_upstream_on_every_golden_case():
    """All four functions, over the recorded upstream verdicts."""
    assert len(GOLDEN) >= 300
    for case in GOLDEN:
        pred, true = case["pred"], case["true"]
        assert exact_match(pred, true) is case["exact_match"], case
        assert precision(pred, true) == pytest.approx(case["precision"]), case
        assert recall(pred, true) == pytest.approx(case["recall"]), case
        assert f1(pred, true) == pytest.approx(case["f1"]), case


def test_golden_table_exercises_both_verdicts():
    """A table on which every case agrees would prove nothing.

    Non-vacuity: the recorded upstream verdicts must contain both exact
    matches and non-matches, and both perfect and imperfect precision.
    """
    ems = {case["exact_match"] for case in GOLDEN}
    assert ems == {True, False}
    precisions = {case["precision"] for case in GOLDEN}
    assert 1.0 in precisions
    assert any(p < 1.0 for p in precisions)


def test_separator_and_normalisation_are_upstream():
    assert ANSWER_SEP == ","
    assert normalize_pred(" Dino Beltran ,SHELLI beltran") == {
        "dino beltran", "shelli beltran",
    }
    # duplicates collapse (it is a set), and a trailing separator adds an
    # EMPTY member rather than being trimmed away
    assert normalize_pred("a,a") == {"a"}
    assert normalize_pred("a,") == {"a", ""}


def test_empty_prediction_scores_zero_and_is_not_skipped(tmp_path):
    """The sharp edge the requirement says to preserve.

    `"".split(",")` is `[""]`, so the normalised prediction is the
    ONE-element set holding the empty string: precision is 0/1, not a
    ZeroDivisionError and not an unscored task. Special-casing it into a
    skip would drop every refusal and timeout out of the denominator.
    """
    assert precision("", "Dino Beltran") == 0.0
    assert recall("", "Dino Beltran") == 0.0
    assert f1("", "Dino Beltran") == 0.0
    assert not exact_match("", "Dino Beltran")

    adapter = PhantomWikiAdapter()
    inst = adapter.load_instances(
        data_root=tmp_path, offline_fixture=_fixture(tmp_path),
    )[0]
    for answer in ("", None):
        score = adapter.score_task(
            run_dir=tmp_path, data_root=tmp_path, instance=inst,
            result=_result(inst.instance_id, answer),
        )
        assert score.value == 0.0
        assert score.unscored_reason is None
        assert score.detail["precision"] == 0.0


def test_score_carries_the_generation_triple(tmp_path):
    adapter = PhantomWikiAdapter()
    inst = adapter.load_instances(
        data_root=tmp_path, offline_fixture=_fixture(tmp_path),
    )[0]
    score = adapter.score_task(
        run_dir=tmp_path, data_root=tmp_path, instance=inst,
        result=_result(inst.instance_id, "Lenore Hackworth"),
    )
    assert score.value == 1.0
    for key, value in GENERATION.items():
        assert score.detail[key] == value


# ----------------------------------------------------------------------
# the gold boundary
# ----------------------------------------------------------------------


def test_prolog_query_and_traces_never_reach_an_instance(tmp_path):
    """The solving query is worse to leak than the answer."""
    adapter = PhantomWikiAdapter()
    inst = adapter.load_instances(
        data_root=tmp_path, offline_fixture=_fixture(tmp_path),
    )[0]
    public = json.dumps(inst.public_dict())
    for field in GOLD_BEARING_QUESTION_FIELDS:
        assert field not in inst.extra
        assert field not in public
    assert "great_granddaughter(" not in public
    assert "Y_2" not in public
    # the answer itself is sealed in `gold`, which public_dict drops
    assert "Lenore Hackworth" not in public
    assert inst.gold["answer"] == ["Lenore Hackworth"]


def test_gold_leak_probe_can_fail(tmp_path):
    """Non-vacuity for the test above: a row whose question TEXT carries
    the query is caught, so the assertion is measuring the payload rather
    than the absence of the substring from any instance whatsoever."""
    adapter = PhantomWikiAdapter()
    leaky = _question(
        question="Solve great_granddaughter(\"Williams Smock\", Y_2)."
    )
    inst = adapter.load_instances(
        data_root=tmp_path, offline_fixture=_fixture(tmp_path, rows=[leaky]),
    )[0]
    assert "great_granddaughter(" in json.dumps(inst.public_dict())


def test_snapshot_holding_the_kb_is_refused(tmp_path):
    """The KB is not gold, and it is still corpus-destroying.

    `facts.pl` holds the same world the articles describe, so it leaks no
    answer — but a snapshot carrying it hands the PROSE arm the structured
    encoding that IS the treatment, and the contrast the A/B measures is
    gone either way.
    """
    adapter = PhantomWikiAdapter()
    snapshot = tmp_path / "snap"
    (snapshot / "articles").mkdir(parents=True)
    (snapshot / "articles" / "Dino Beltran.txt").write_text("# Dino Beltran\n")
    # clean snapshot passes
    adapter.bind_corpus([], {"snapshot_dir": str(snapshot)})

    for name, rel in (
        ("facts.pl", "facts.pl"),
        ("sidecar", "articles/Dino Beltran_facts.txt"),
        ("answers", "questions/type0.json"),
    ):
        path = snapshot / rel
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("x")
        with pytest.raises(DatasetError, match="gold-bearing"):
            adapter.bind_corpus([], {"snapshot_dir": str(snapshot)})
        path.unlink()
        if path.parent != snapshot / "articles" and path.parent != snapshot:
            # the `questions` DIRECTORY is itself a forbidden name, so an
            # emptied one still fails — deliberately, since an operator who
            # deleted the files but kept the directory has told us where the
            # answers were mounted from
            with pytest.raises(DatasetError, match="gold-bearing"):
                adapter.bind_corpus([], {"snapshot_dir": str(snapshot)})
            path.parent.rmdir()
    # ...and the snapshot is clean again once they are gone, so the guard
    # is keyed on those artifacts and not on the directory existing
    adapter.bind_corpus([], {"snapshot_dir": str(snapshot)})


def test_forbidden_names_cover_every_generated_artifact():
    assert set(FORBIDDEN_SNAPSHOT_NAMES) >= {
        "facts.pl", "*_facts.txt", "questions",
    }


# ----------------------------------------------------------------------
# generated, not downloaded
# ----------------------------------------------------------------------


def test_fetch_refuses_to_download_and_names_the_generator(tmp_path):
    adapter = PhantomWikiAdapter()
    with pytest.raises(DatasetError) as exc:
        adapter.fetch(data_root=tmp_path)
    message = str(exc.value)
    assert "pw-generate" in message
    # the refusal states the MEASUREMENT that rules the released splits out
    assert "2,614,872" in message
    assert "5,030" in message


def test_generation_triple_is_required(tmp_path):
    adapter = PhantomWikiAdapter()
    for missing in ("universe_seed", "universe_question_depth", "universe_id"):
        partial = {k: v for k, v in GENERATION.items() if k != missing}
        with pytest.raises(DatasetError, match="NESTED"):
            adapter.load_instances(
                data_root=tmp_path,
                offline_fixture=_fixture(tmp_path, generation=partial),
            )


def test_instances_carry_the_triple_so_nesting_is_detectable(tmp_path):
    """Two universes at the same seed and different sizes are NESTED.

    The adapter cannot stop anyone comparing them; it makes the comparison
    DETECTABLE by carrying the whole triple, so a run manifest can be
    checked after the fact instead of the nesting being invisible.
    """
    adapter = PhantomWikiAdapter()
    small = dict(GENERATION, universe_family_trees=1,
                 universe_id="aaaa000000000001")
    large = dict(GENERATION, universe_family_trees=2000,
                 universe_id="bbbb000000000002")
    a = adapter.load_instances(
        data_root=tmp_path,
        offline_fixture=_fixture(tmp_path / "a", generation=small),
    )[0]
    b = adapter.load_instances(
        data_root=tmp_path,
        offline_fixture=_fixture(tmp_path / "b", generation=large),
    )[0]
    assert a.extra["universe_seed"] == b.extra["universe_seed"]
    assert a.extra["universe_id"] != b.extra["universe_id"]
    assert a.extra["universe_family_trees"] != b.extra["universe_family_trees"]


@pytest.mark.parametrize("answer", [None, [], "Lenore Hackworth"])
def test_row_without_an_answer_list_is_refused(tmp_path, answer):
    """The answer unit is an entity SET. A row whose answer is a bare
    string would score against a comma-split of that string, which is a
    different question."""
    adapter = PhantomWikiAdapter()
    with pytest.raises(DatasetError, match="answer"):
        adapter.load_instances(
            data_root=tmp_path,
            offline_fixture=_fixture(tmp_path, rows=[_question(answer=answer)]),
        )


def test_pilot_spreads_across_question_types(tmp_path):
    adapter = PhantomWikiAdapter()
    payload = {
        "generation": GENERATION,
        "questions": {
            f"type{i}": [
                _question(id=f"q{i}-{j}", question=f"Q{i}.{j}?")
                for j in range(10)
            ]
            for i in range(5)
        },
    }
    path = tmp_path / "spread.json"
    path.write_text(json.dumps(payload), encoding="utf-8")
    instances = adapter.load_instances(data_root=tmp_path, offline_fixture=path)
    pilot = adapter.pilot_subset(instances)
    assert len(pilot) == adapter.pilot_size
    types = {i.extra["question_type"] for i in pilot}
    assert len(types) == 5, "a pilot drawn from one template measures one hop shape"
