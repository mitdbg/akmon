"""req.dom.scifact: SciFact adapter — dev-only loading, deterministic
verdict + evidence-sentence-F1 scoring, open-book corpus binding."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from okg.bench.ab.adapters.scifact import (
    SNAPSHOT_TOKEN,
    SciFactAdapter,
)
from okg.bench.ab.errors import DatasetError
from okg.bench.ab.model import TaskMetrics, TaskResult


@pytest.fixture
def adapter() -> SciFactAdapter:
    return SciFactAdapter()


def _claim(cid: int, label: str, evidence: dict | None = None) -> dict:
    if evidence is None:
        if label == "NOINFO":
            evidence = {}
        else:
            evidence = {
                str(100 + cid): [{"label": label, "sentences": [0, 2]}]
            }
    return {
        "id": cid,
        "claim": f"Claim number {cid}.",
        "evidence": evidence,
        "cited_doc_ids": [100 + cid],
    }


def _write_claims(path: Path, rows: list[dict]) -> Path:
    path.write_text(
        "\n".join(json.dumps(r) for r in rows) + "\n", encoding="utf-8"
    )
    return path


@pytest.fixture
def small_fixture(tmp_path) -> Path:
    """2 SUPPORT / 2 CONTRADICT / 2 NOINFO — too small for a pilot."""
    rows = [
        _claim(1, "SUPPORT"),
        _claim(2, "SUPPORT"),
        _claim(3, "CONTRADICT"),
        _claim(4, "CONTRADICT"),
        _claim(5, "NOINFO"),
        _claim(6, "NOINFO"),
    ]
    return _write_claims(tmp_path / "claims_dev_small.jsonl", rows)


@pytest.fixture
def pilot_fixture(tmp_path) -> Path:
    """6 claims per label (interleaved ids) — enough for the 5/5/5 pilot."""
    rows = []
    labels = ("SUPPORT", "CONTRADICT", "NOINFO")
    for i in range(18):
        rows.append(_claim(i + 1, labels[i % 3]))
    return _write_claims(tmp_path / "claims_dev_pilot.jsonl", rows)


def _result(instance_id: str, answer: str) -> TaskResult:
    return TaskResult(
        instance_id=instance_id, arm="echo", status="completed", answer=answer,
        metrics=TaskMetrics(0, 0, 0, 0, 0, 0.0),
    )


def _score(adapter, tmp_path, inst, answer):
    return adapter.score_task(
        run_dir=tmp_path, data_root=tmp_path, instance=inst,
        result=_result(inst.instance_id, answer),
    )


# ---------- loading ----------


def test_loader_parses_dev_claims(adapter, tmp_path, small_fixture):
    instances = adapter.load_instances(
        data_root=tmp_path, offline_fixture=small_fixture
    )
    assert len(instances) == 6
    assert [i.instance_id for i in instances] == [
        f"scifact/dev/{n}" for n in range(1, 7)
    ]
    assert all(i.benchmark == "scifact" for i in instances)
    assert all(SNAPSHOT_TOKEN in i.prompt for i in instances)
    assert all(i.extra["split"] == "dev" for i in instances)
    first = instances[0]
    assert "Claim number 1." in first.prompt
    assert first.gold["label"] == "SUPPORT"
    assert first.gold["evidence"] == {"101": [0, 2]}


def test_prompt_contract_names_verdict_and_evidence(
    adapter, tmp_path, small_fixture
):
    instances = adapter.load_instances(
        data_root=tmp_path, offline_fixture=small_fixture
    )
    for inst in instances:
        assert "VERDICT: SUPPORT|CONTRADICT|NOINFO" in inst.prompt
        assert "EVIDENCE: <doc_id>:" in inst.prompt


def test_missing_cache_fails_loudly(adapter, tmp_path):
    with pytest.raises(DatasetError, match="okg-bench fetch"):
        adapter.load_instances(data_root=tmp_path)


def test_malformed_rows_fail_loudly(adapter, tmp_path):
    no_claim = _write_claims(
        tmp_path / "no_claim.jsonl",
        [{"id": 1, "evidence": {}, "cited_doc_ids": []}],
    )
    with pytest.raises(DatasetError, match="no claim text"):
        adapter.load_instances(data_root=tmp_path, offline_fixture=no_claim)

    bad_json = tmp_path / "bad.jsonl"
    bad_json.write_text('{"id": 1, "claim": "x"}\nnot json\n')
    with pytest.raises(DatasetError, match="line 2"):
        adapter.load_instances(data_root=tmp_path, offline_fixture=bad_json)

    dup = _write_claims(
        tmp_path / "dup.jsonl", [_claim(7, "NOINFO"), _claim(7, "NOINFO")]
    )
    with pytest.raises(DatasetError, match="duplicate claim id 7"):
        adapter.load_instances(data_root=tmp_path, offline_fixture=dup)


# ---------- test-split refusal ----------


def test_test_split_is_refused(adapter, tmp_path, small_fixture):
    instances = adapter.load_instances(
        data_root=tmp_path, offline_fixture=small_fixture
    )
    with pytest.raises(DatasetError, match="hidden.*AI2 leaderboard"):
        adapter.subset(instances, "test")
    # dev is the whole loadable set.
    assert adapter.subset(instances, "dev") == instances


# ---------- gold derivation ----------


def test_gold_label_derivation(adapter, tmp_path):
    rows = [
        # Empty evidence -> NOINFO.
        _claim(1, "NOINFO", evidence={}),
        # Majority across docs: 2 SUPPORT entries vs 1 CONTRADICT.
        _claim(
            2, "SUPPORT",
            evidence={
                "11": [
                    {"label": "SUPPORT", "sentences": [0]},
                    {"label": "SUPPORT", "sentences": [3, 4]},
                ],
                "12": [{"label": "CONTRADICT", "sentences": [1]}],
            },
        ),
    ]
    fixture = _write_claims(tmp_path / "gold.jsonl", rows)
    instances = adapter.load_instances(data_root=tmp_path, offline_fixture=fixture)
    by_id = {i.extra["claim_id"]: i for i in instances}
    assert by_id[1].gold == {"label": "NOINFO", "evidence": {}}
    # Union of sentence indices per doc; majority label wins.
    assert by_id[2].gold["label"] == "SUPPORT"
    assert by_id[2].gold["evidence"] == {"11": [0, 3, 4], "12": [1]}


def test_gold_label_tie_fails_loudly(adapter, tmp_path):
    tie = _write_claims(
        tmp_path / "tie.jsonl",
        [_claim(
            1, "SUPPORT",
            evidence={
                "11": [{"label": "SUPPORT", "sentences": [0]}],
                "12": [{"label": "CONTRADICT", "sentences": [1]}],
            },
        )],
    )
    with pytest.raises(DatasetError, match="tie"):
        adapter.load_instances(data_root=tmp_path, offline_fixture=tie)


# ---------- scoring (PACT anchor) ----------


def test_label_and_evidence_f1(adapter, tmp_path):
    """Verdict exact match is the score value; hand-computed sentence F1
    (including multi-doc) is recorded in detail."""
    rows = [_claim(
        1, "SUPPORT",
        evidence={
            "101": [{"label": "SUPPORT", "sentences": [0, 2]}],
            "102": [{"label": "SUPPORT", "sentences": [1]}],
        },
    )]
    fixture = _write_claims(tmp_path / "anchor.jsonl", rows)
    [inst] = adapter.load_instances(data_root=tmp_path, offline_fixture=fixture)
    assert inst.gold["evidence"] == {"101": [0, 2], "102": [1]}

    # Correct verdict, partially-correct multi-doc evidence:
    # gold pairs {101:0, 101:2, 102:1}; answered {101:0, 101:1, 102:1}
    # -> tp=2, precision=2/3, recall=2/3, F1=2/3.
    good = _score(
        adapter, tmp_path, inst,
        "Reasoning...\nEVIDENCE: 101: 0, 1\nEVIDENCE: 102: 1\nVERDICT: SUPPORT",
    )
    assert good.value == 1.0
    assert good.kind == "verdict"
    assert good.detail["evidence_precision"] == pytest.approx(2 / 3, abs=1e-3)
    assert good.detail["evidence_recall"] == pytest.approx(2 / 3, abs=1e-3)
    assert good.detail["evidence_f1"] == pytest.approx(2 / 3, abs=1e-3)
    assert good.detail["answered_evidence"] == {"101": [0, 1], "102": [1]}

    # Perfect evidence -> F1 = 1.0.
    perfect = _score(
        adapter, tmp_path, inst,
        "EVIDENCE: 101: 0, 2\nEVIDENCE: 102: 1\nVERDICT: SUPPORT",
    )
    assert perfect.value == 1.0
    assert perfect.detail["evidence_f1"] == 1.0

    # Wrong verdict -> 0.0; evidence F1 still recorded (both non-NOINFO).
    wrong = _score(
        adapter, tmp_path, inst,
        "EVIDENCE: 101: 0, 2\nVERDICT: CONTRADICT",
    )
    assert wrong.value == 0.0
    assert wrong.detail["evidence_f1"] == pytest.approx(2 * (2 / 3) / (5 / 3), abs=1e-3)

    # Non-NOINFO gold but NOINFO answer: wrong, and F1 not meaningful.
    noinfo_answer = _score(adapter, tmp_path, inst, "VERDICT: NOINFO")
    assert noinfo_answer.value == 0.0
    assert noinfo_answer.detail["evidence_f1"] is None

    # The LAST verdict line wins (deterministic).
    last_wins = _score(
        adapter, tmp_path, inst,
        "VERDICT: CONTRADICT\nOn reflection:\nVERDICT: SUPPORT",
    )
    assert last_wins.value == 1.0

    # Claimed evidence but none parseable: verdict scores, F1 = 0.0 with
    # the bad line recorded.
    unparseable_ev = _score(
        adapter, tmp_path, inst,
        "EVIDENCE: 101: first and third\nVERDICT: SUPPORT",
    )
    assert unparseable_ev.value == 1.0
    assert unparseable_ev.detail["evidence_f1"] == 0.0
    assert unparseable_ev.detail["evidence_parse_errors"] == [
        "EVIDENCE: 101: first and third"
    ]


def test_noinfo_gold_scores_verdict_only(adapter, tmp_path):
    fixture = _write_claims(tmp_path / "ni.jsonl", [_claim(9, "NOINFO")])
    [inst] = adapter.load_instances(data_root=tmp_path, offline_fixture=fixture)
    right = _score(adapter, tmp_path, inst, "VERDICT: NOINFO")
    assert right.value == 1.0
    assert right.detail["evidence_f1"] is None
    wrong = _score(adapter, tmp_path, inst, "EVIDENCE: 9: 0\nVERDICT: SUPPORT")
    assert wrong.value == 0.0
    assert wrong.detail["evidence_f1"] is None


def test_unparseable_answer_scores_zero_with_reason(adapter, tmp_path, small_fixture):
    instances = adapter.load_instances(
        data_root=tmp_path, offline_fixture=small_fixture
    )
    score = _score(adapter, tmp_path, instances[0], "I believe it is true.")
    assert score.value == 0.0
    assert score.detail["reason"] == "no parseable VERDICT line"


# ---------- corpus binding ----------


def test_bind_corpus_substitutes_and_mounts(adapter, tmp_path, small_fixture):
    snapshot = tmp_path / "snapshot"
    snapshot.mkdir()
    instances = adapter.load_instances(
        data_root=tmp_path, offline_fixture=small_fixture
    )
    bound = adapter.bind_corpus(instances, {"snapshot_dir": str(snapshot)})
    assert all(SNAPSHOT_TOKEN not in i.prompt for i in bound)
    assert all(str(snapshot) in i.prompt for i in bound)
    assert all(i.extra["add_dirs"] == [str(snapshot)] for i in bound)
    with pytest.raises(DatasetError, match="snapshot_dir"):
        adapter.bind_corpus(instances, {})
    assert adapter.requires_corpus_config is True


# ---------- pilot ----------


def test_pilot_short_bucket_fails_loudly(adapter, tmp_path, small_fixture):
    instances = adapter.load_instances(
        data_root=tmp_path, offline_fixture=small_fixture
    )
    with pytest.raises(DatasetError, match="needs 5 SUPPORT.*only 2"):
        adapter.pilot_subset(instances)


def test_pilot_takes_first_five_per_label(adapter, tmp_path, pilot_fixture):
    instances = adapter.load_instances(
        data_root=tmp_path, offline_fixture=pilot_fixture
    )
    pilot = adapter.pilot_subset(instances)
    assert len(pilot) == 15
    counts: dict[str, int] = {}
    for inst in pilot:
        counts[inst.gold["label"]] = counts.get(inst.gold["label"], 0) + 1
    assert counts == {"SUPPORT": 5, "CONTRADICT": 5, "NOINFO": 5}
    # First 5 by claim id per bucket: ids 1..15 kept, 16..18 dropped
    # (labels interleave by id in the fixture), returned in id order.
    assert [i.extra["claim_id"] for i in pilot] == list(range(1, 16))
