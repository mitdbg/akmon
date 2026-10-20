"""req.dom.housing: Housing Statute QA adapter (reglab/housing_qa)."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from okg.bench.ab.adapters.housing_qa import (
    SNAPSHOT_TOKEN,
    HousingQaAdapter,
    _normalize_citation,
)
from okg.bench.ab.errors import DatasetError
from okg.bench.ab.model import TaskMetrics, TaskResult


@pytest.fixture
def adapter() -> HousingQaAdapter:
    return HousingQaAdapter()


def _row(idx: int, state: str, answer: str = "yes", **overrides) -> dict:
    row = {
        "idx": idx,
        "state": state,
        "question": f"May a landlord in {state} do thing {idx}?",
        "answer": answer,
        "question_group": idx % 2,
        "original_question": f"original {idx}",
        "caveats": [],
        "statutes": [
            {
                "citation": f"{state} Stat. § {idx}.10",
                "excerpt": "the landlord may not ...",
                "statute_idx": 1000 + idx,
            },
            {
                "citation": f"{state} Stat. § {idx}.20",
                "excerpt": "unless the tenant ...",
                "statute_idx": 2000 + idx,
            },
        ],
    }
    row.update(overrides)
    return row


def _fixture_rows() -> list[dict]:
    # Two states, 3+3 questions, all with statute annotations.
    return [_row(i, "CA") for i in range(3)] + [
        _row(10 + i, "TX", answer="no") for i in range(3)
    ]


@pytest.fixture
def fixture(tmp_path) -> Path:
    path = tmp_path / "questions.json"
    path.write_text(json.dumps(_fixture_rows()))
    return path


def _result(instance_id: str, answer: str) -> TaskResult:
    return TaskResult(
        instance_id=instance_id, arm="echo", status="completed", answer=answer,
        metrics=TaskMetrics(0, 0, 0, 0, 0, 0.0),
    )


# ---------- loading + validation ----------


def test_load_instances_shapes_and_open_book_prompt(adapter, tmp_path, fixture):
    instances = adapter.load_instances(data_root=tmp_path, offline_fixture=fixture)
    assert len(instances) == 6
    inst = next(i for i in instances if i.instance_id == "housing/CA/00001")
    assert inst.benchmark == "housing-qa"
    assert SNAPSHOT_TOKEN in inst.prompt
    assert "ANSWER: yes|no" in inst.prompt
    assert "CITATIONS:" in inst.prompt
    assert "May a landlord in CA do thing 1?" in inst.prompt
    assert inst.gold == {
        "answer": "yes",
        "statute_idxs": [1001, 2001],
        "citations": ["CA Stat. § 1.10", "CA Stat. § 1.20"],
    }
    assert inst.extra == {"state": "CA", "config": "questions"}
    assert adapter.requires_corpus_config is True


def test_aux_shaped_row_without_statutes_raises(adapter, tmp_path):
    for aux_shape in ({"statutes": []}, {"statutes": None}):
        rows = _fixture_rows() + [_row(99, "CA", **aux_shape)]
        path = tmp_path / "aux.json"
        path.write_text(json.dumps(rows))
        with pytest.raises(DatasetError, match="aux-config trap"):
            adapter.load_instances(data_root=tmp_path, offline_fixture=path)


def test_statute_annotation_without_statute_idx_raises(adapter, tmp_path):
    bad = _row(99, "CA")
    del bad["statutes"][0]["statute_idx"]
    path = tmp_path / "aux.json"
    path.write_text(json.dumps([bad]))
    with pytest.raises(DatasetError, match="aux-config trap"):
        adapter.load_instances(data_root=tmp_path, offline_fixture=path)


def test_malformed_rows_fail_loudly(adapter, tmp_path):
    path = tmp_path / "bad.json"
    path.write_text(json.dumps([_row(1, "CA", answer="maybe")]))
    with pytest.raises(DatasetError, match="non-yes/no answer"):
        adapter.load_instances(data_root=tmp_path, offline_fixture=path)
    path.write_text(json.dumps([{"idx": 1, "state": "CA"}]))
    with pytest.raises(DatasetError, match="missing"):
        adapter.load_instances(data_root=tmp_path, offline_fixture=path)
    path.write_text(json.dumps({"not": "a list"}))
    with pytest.raises(DatasetError, match="JSON array"):
        adapter.load_instances(data_root=tmp_path, offline_fixture=path)


def test_uncached_load_refuses_with_fetch_hint(adapter, tmp_path):
    with pytest.raises(DatasetError, match="okg-bench fetch"):
        adapter.load_instances(data_root=tmp_path)


# ---------- subsets ----------


def test_state_subset_and_instance_subset(adapter, tmp_path, fixture):
    instances = adapter.load_instances(data_root=tmp_path, offline_fixture=fixture)
    tx = adapter.subset(instances, "state:TX")
    assert len(tx) == 3
    assert all(i.extra["state"] == "TX" for i in tx)
    assert adapter.subset(instances, "state:tx") == tx  # case-insensitive
    with pytest.raises(DatasetError, match="no housing-qa questions"):
        adapter.subset(instances, "state:WY")
    one = adapter.subset(instances, "instance:housing/CA/00002")
    assert [i.instance_id for i in one] == ["housing/CA/00002"]
    with pytest.raises(DatasetError, match="no subset"):
        adapter.subset(instances, "nonsense")


def test_pilot_picks_the_most_question_rich_state(adapter, tmp_path):
    # TX has 4 questions to CA's 3: pilot must pick TX (count beats
    # alphabetical order).
    rows = [_row(i, "CA") for i in range(3)] + [
        _row(10 + i, "TX") for i in range(4)
    ]
    path = tmp_path / "questions.json"
    path.write_text(json.dumps(rows))
    instances = adapter.load_instances(data_root=tmp_path, offline_fixture=path)
    pilot = adapter.pilot_subset(instances)
    assert len(pilot) == 4
    assert all(i.extra["state"] == "TX" for i in pilot)
    assert [i.instance_id for i in pilot] == sorted(i.instance_id for i in pilot)


def test_pilot_tie_breaks_to_first_state_and_caps_at_pilot_size(
    adapter, tmp_path, fixture
):
    instances = adapter.load_instances(data_root=tmp_path, offline_fixture=fixture)
    pilot = adapter.pilot_subset(instances)  # 3 CA vs 3 TX -> CA
    assert all(i.extra["state"] == "CA" for i in pilot)
    rows = [_row(i, "CA") for i in range(20)]
    path = tmp_path / "many.json"
    path.write_text(json.dumps(rows))
    many = adapter.load_instances(data_root=tmp_path, offline_fixture=path)
    assert len(adapter.pilot_subset(many)) == adapter.pilot_size == 15


# ---------- open-book binding ----------


def test_bind_corpus_substitutes_and_mounts(adapter, tmp_path, fixture):
    snapshot = tmp_path / "snapshot"
    snapshot.mkdir()
    instances = adapter.load_instances(data_root=tmp_path, offline_fixture=fixture)
    bound = adapter.bind_corpus(instances, {"snapshot_dir": str(snapshot)})
    assert all(SNAPSHOT_TOKEN not in i.prompt for i in bound)
    assert all(str(snapshot) in i.prompt for i in bound)
    assert all(i.extra["add_dirs"] == [str(snapshot)] for i in bound)
    with pytest.raises(DatasetError, match="snapshot_dir"):
        adapter.bind_corpus(instances, {})


# ---------- scoring (PACT anchor: test_retrieval_match_scoring) ----------


def test_retrieval_match_scoring(adapter, tmp_path, fixture):
    instances = adapter.load_instances(data_root=tmp_path, offline_fixture=fixture)
    inst = next(i for i in instances if i.instance_id == "housing/CA/00001")
    # gold: answer=yes, citations "CA Stat. § 1.10" / "CA Stat. § 1.20"

    def score(answer):
        return adapter.score_task(
            run_dir=tmp_path, data_root=tmp_path, instance=inst,
            result=_result(inst.instance_id, answer),
        )

    # Exact yes/no from the last ANSWER: line.
    right = score(
        "Reasoning...\nANSWER: no\nrevised:\nANSWER: yes\n"
        "CITATIONS: CA Stat. § 1.10; CA Stat. § 1.20"
    )
    assert right.value == 1.0
    assert right.detail["retrieval"]["f1"] == 1.0
    assert right.detail["retrieval"]["recall"] == 1.0
    assert right.detail["retrieval"]["precision"] == 1.0

    wrong = score("ANSWER: no\nCITATIONS: CA Stat. § 1.10")
    assert wrong.value == 0.0
    assert wrong.detail["retrieval"]["recall"] == 0.5

    # A bare final yes/no is accepted.
    assert score("I believe the answer is:\nYes.").value == 1.0

    # Citation normalization: case + collapsed whitespace + trailing
    # punctuation must not break the overlap.
    fuzzy = score(
        "ANSWER: yes\nCITATIONS: ca  stat.  § 1.10.; Bogus Cite § 9"
    )
    retrieval = fuzzy.detail["retrieval"]
    assert retrieval["precision"] == 0.5  # 1 of 2 cited is gold
    assert retrieval["recall"] == 0.5  # 1 of 2 gold cited
    assert retrieval["f1"] == 0.5
    assert retrieval["matched"] == ["ca stat. § 1.10"]

    # Missing CITATIONS line: answer still scored, retrieval zeroed loudly.
    no_cites = score("ANSWER: yes")
    assert no_cites.value == 1.0
    assert no_cites.detail["retrieval"]["f1"] == 0.0
    assert no_cites.detail["retrieval"]["reason"] == "no CITATIONS line in answer"

    # Unparseable -> 0.0 with reason (never unscored-by-default).
    unparseable = score("I cannot decide either way.")
    assert unparseable.value == 0.0
    assert "no parseable ANSWER" in unparseable.detail["reason"]


def test_normalize_citation():
    assert _normalize_citation("  Wis.\tStat.   §  704.17. ") == "wis. stat. § 704.17"
    assert _normalize_citation("CA Stat. § 1.10;") == "ca stat. § 1.10"
