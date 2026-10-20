"""req.dom.finauditing: FinAuditing adapter (three subsets, re-split
XBRL doc sets, deterministic scorers)."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from okg.bench.ab.adapters import adapter_names, get_adapter
from okg.bench.ab.adapters.finauditing import (
    SNAPSHOT_TOKEN,
    FinAuditingAdapter,
    docset_slug,
    split_query,
)
from okg.bench.ab.errors import DatasetError
from okg.bench.ab.model import TaskMetrics, TaskResult


@pytest.fixture
def adapter() -> FinAuditingAdapter:
    return FinAuditingAdapter()


_SCHEMA_XML = (
    '<xsd:schema xmlns:xsd="http://www.w3.org/2001/XMLSchema" '
    'targetNamespace="http://example.com/ext">\n'
    '  <xsd:element name="Assets" id="ext_Assets"/>\n'
    "</xsd:schema>"
)
_INSTANCE_XML = (
    '<xbrli:xbrl xmlns:xbrli="http://www.xbrl.org/2003/instance" '
    'xmlns:ext="http://example.com/ext">\n'
    '  <ext:Assets contextRef="c1" unitRef="usd">100</ext:Assets>\n'
    "</xbrli:xbrl>"
)


#: Real rows end with a NON-document section plus the question itself
#: (verified against TheFinAI/* 2026-07-27): "##US GAAP Taxonomy", then
#: "Question: ...\nAnswer:". The fixture mirrors that so the trailer path
#: and the doc-body boundary are both exercised.
_TRAILER = (
    "##US GAAP Taxonomy\n"
    "[Concept Core]\nID: us-gaap:Assets\nLabel: Assets\n\n"
    "Question: Which concepts are misstated in this filing?\nAnswer:\n"
)


def _query(
    framing: str, *, schema_none: bool = False, trailer: str = _TRAILER
) -> str:
    schema = "None" if schema_none else _SCHEMA_XML
    return (
        f"{framing}\n"
        f"##Schema document\n{schema}\n"
        "##Presentation document\n<presentation/>\n"
        f"##Instance document\n{_INSTANCE_XML}\n"
        f"{trailer}"
    )


def _fixture_payload() -> dict:
    finsm = [
        {
            "query": _query(f"Find the erroneous concepts in filing {i}.",
                            schema_none=(i == 0)),
            "dqc_id": f"DQC_{i:04d}",
            "answer": json.dumps(["us-gaap:Assets", "us-gaap:LiabilitiesCurrent"]),
            "id": f"finsm-{i}",
        }
        for i in range(6)
    ]
    finre = [
        {
            "query": _query(f"Classify the error type in filing {i}."),
            "dqc_id": f"DQC_{i:04d}",
            "answer": "Inappropriateness",
            "id": f"finre-{i}",
            "choices": ["Reversal", "Inappropriateness", "CombinationErr"],
            "gold": 1,
        }
        for i in range(6)
    ]
    finmr = [
        {
            "query": _query(f"Extract and recompute the value in filing {i}."),
            "dqc_id": f"DQC_{i:04d}",
            "answer": json.dumps(
                {"extracted_value": 1234.5, "calculated_value": 1200.0}
            ),
            "id": f"finmr-{i}",
        }
        for i in range(6)
    ]
    return {"finsm": finsm, "finre": finre, "finmr": finmr}


@pytest.fixture
def fixture(tmp_path) -> Path:
    path = tmp_path / "finauditing.json"
    path.write_text(json.dumps(_fixture_payload()))
    return path


def _result(instance_id: str, answer: str) -> TaskResult:
    return TaskResult(
        instance_id=instance_id, arm="echo", status="completed", answer=answer,
        metrics=TaskMetrics(0, 0, 0, 0, 0, 0.0),
    )


def test_registered_as_adapter():
    assert "finauditing" in adapter_names()
    assert get_adapter("finauditing").requires_corpus_config is True


# ---------- loading + re-split ----------


def test_load_all_configs_and_open_book_token(adapter, tmp_path, fixture):
    instances = adapter.load_instances(data_root=tmp_path, offline_fixture=fixture)
    assert len(instances) == 18
    assert all(SNAPSHOT_TOKEN in i.prompt for i in instances)
    assert all(i.repo is None for i in instances)
    # The raw XML moved out of the prompt and into the docset.
    assert all("<xbrli:xbrl" not in i.prompt for i in instances)
    assert all("##Schema document" not in i.prompt for i in instances)

    finsm = next(i for i in instances if i.instance_id == "finauditing/finsm/0001")
    assert "Find the erroneous concepts in filing 1." in finsm.prompt
    assert finsm.gold == {
        "concepts": ["us-gaap:Assets", "us-gaap:LiabilitiesCurrent"]
    }
    assert finsm.extra["docset"]["instance"] == _INSTANCE_XML
    assert finsm.extra["missing_docs"] == []

    finre = next(i for i in instances if i.extra["config"] == "finre")
    assert finre.gold["gold"] == 1
    assert finre.gold["choices"][1] == "Inappropriateness"
    assert "B. Inappropriateness" in finre.prompt

    finmr = next(i for i in instances if i.extra["config"] == "finmr")
    assert finmr.gold["pair"] == {
        "extracted_value": 1234.5, "calculated_value": 1200.0,
    }


def test_none_document_kept_with_missing_docs_recorded(adapter, tmp_path, fixture):
    """The known FinRE/FinSM trap: a literal "None" sub-document keeps
    the instance; the absence is recorded, never silently dropped."""
    instances = adapter.load_instances(data_root=tmp_path, offline_fixture=fixture)
    trap = next(i for i in instances if i.instance_id == "finauditing/finsm/0000")
    assert trap.extra["missing_docs"] == ["schema"]
    assert trap.extra["docset"]["schema"] is None
    assert "schema" not in trap.extra["doc_kinds"]
    assert set(trap.extra["doc_kinds"]) == {"presentation", "instance"}


def test_split_query_round_trip():
    framing = "Audit this filing."
    query = _query(framing)
    got_framing, docs, trailer = split_query(query)
    assert got_framing == framing
    assert list(docs) == ["schema", "presentation", "instance"]
    assert docs["schema"] == _SCHEMA_XML
    # The LAST document must stop at the next '##' section, not swallow the
    # taxonomy block and the question (that produced unparseable exported
    # XML and a question-less prompt against real rows).
    assert docs["instance"] == _INSTANCE_XML
    assert "US GAAP Taxonomy" not in docs["instance"]
    assert "Question:" not in docs["instance"]
    assert trailer.startswith("##US GAAP Taxonomy")
    assert "Question: Which concepts are misstated" in trailer

    _, none_docs, _ = split_query(_query(framing, schema_none=True))
    assert none_docs["schema"] is None

    with pytest.raises(DatasetError, match="no '##<kind> document' markers"):
        split_query("just a prompt, no documents")
    with pytest.raises(DatasetError, match="twice"):
        split_query("p\n##Schema document\n<a/>\n##Schema document\n<b/>\n")


def test_prompt_carries_the_question(adapter, tmp_path, fixture):
    """The question lives AFTER the documents in released rows; a prompt
    without it gives the agent nothing to answer."""
    instances = adapter.load_instances(data_root=tmp_path, offline_fixture=fixture)
    for inst in instances:
        assert "Question: Which concepts are misstated" in inst.prompt
        # ...and the raw XML still never reaches the prompt.
        assert "<xbrl" not in inst.prompt


def test_row_without_a_trailer_refuses(adapter, tmp_path):
    bad = tmp_path / "bad.json"
    bad.write_text(json.dumps({"finsm": [{
        "query": _query("framing only", trailer=""),
        "dqc_id": "DQC_0", "answer": json.dumps(["us-gaap:Assets"]), "id": "x",
    }]}))
    with pytest.raises(DatasetError, match="no post-document trailer"):
        adapter.load_instances(data_root=tmp_path, offline_fixture=bad)


def test_malformed_rows_fail_loudly(adapter, tmp_path):
    bad = tmp_path / "bad.json"
    bad.write_text(json.dumps({"finsm": [{"answer": "[]", "id": "x"}]}))
    with pytest.raises(DatasetError, match="missing query"):
        adapter.load_instances(data_root=tmp_path, offline_fixture=bad)

    bad.write_text(json.dumps({"not-a-config": []}))
    with pytest.raises(DatasetError, match="unknown finauditing config"):
        adapter.load_instances(data_root=tmp_path, offline_fixture=bad)

    row = _fixture_payload()["finre"][0]
    row["gold"] = 7
    bad.write_text(json.dumps({"finre": [row]}))
    with pytest.raises(DatasetError, match="not an index"):
        adapter.load_instances(data_root=tmp_path, offline_fixture=bad)

    row = _fixture_payload()["finmr"][0]
    row["answer"] = json.dumps({"extracted_value": "n/a", "calculated_value": 1})
    bad.write_text(json.dumps({"finmr": [row]}))
    with pytest.raises(DatasetError, match="not numeric"):
        adapter.load_instances(data_root=tmp_path, offline_fixture=bad)

    row = _fixture_payload()["finsm"][0]
    row["answer"] = "not a list"
    bad.write_text(json.dumps({"finsm": [row]}))
    with pytest.raises(DatasetError, match="concept array"):
        adapter.load_instances(data_root=tmp_path, offline_fixture=bad)


# ---------- docset export ----------


def test_export_docsets_writes_files_and_refuses_existing(
    adapter, tmp_path, fixture,
):
    instances = adapter.load_instances(data_root=tmp_path, offline_fixture=fixture)
    dest = tmp_path / "snapshot"
    stats = adapter.export_docsets(instances, dest)
    assert stats["filings"] == 18
    # One "None" schema doc across the fixture is skipped, not written.
    assert stats["skipped_missing"] == 1
    assert stats["files"] == 18 * 3 - 1

    slug = docset_slug("finauditing/finsm/0001")
    filing_dir = dest / "docsets" / slug
    assert (filing_dir / "schema.xml").read_text() == _SCHEMA_XML
    assert (filing_dir / "instance.xml").read_text() == _INSTANCE_XML
    trap_dir = dest / "docsets" / docset_slug("finauditing/finsm/0000")
    assert not (trap_dir / "schema.xml").exists()
    assert (trap_dir / "instance.xml").exists()

    with pytest.raises(DatasetError, match="refusing to overwrite"):
        adapter.export_docsets(instances, dest)


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


# ---------- subsets ----------


def test_pilot_spreads_five_per_config(adapter, tmp_path, fixture):
    instances = adapter.load_instances(data_root=tmp_path, offline_fixture=fixture)
    pilot = adapter.pilot_subset(instances)
    counts: dict[str, int] = {}
    for inst in pilot:
        counts[inst.extra["config"]] = counts.get(inst.extra["config"], 0) + 1
    assert counts == {"finsm": 5, "finre": 5, "finmr": 5}
    # First-N in dataset order, deterministic.
    finsm_ids = [i.instance_id for i in pilot if i.extra["config"] == "finsm"]
    assert finsm_ids == [f"finauditing/finsm/{n:04d}" for n in range(5)]


def test_named_subsets(adapter, tmp_path, fixture):
    instances = adapter.load_instances(data_root=tmp_path, offline_fixture=fixture)
    assert len(adapter.subset(instances, "finre")) == 6
    one = adapter.subset(instances, "instance:finauditing/finmr/0002")
    assert [i.instance_id for i in one] == ["finauditing/finmr/0002"]
    with pytest.raises(DatasetError, match="no subset"):
        adapter.subset(instances, "nope")


# ---------- scoring ----------


def test_deterministic_scorers(adapter, tmp_path, fixture):
    instances = adapter.load_instances(data_root=tmp_path, offline_fixture=fixture)
    by_cfg = {i.extra["config"]: i for i in adapter.order(instances)}

    def score(inst, answer):
        return adapter.score_task(
            run_dir=tmp_path, data_root=tmp_path, instance=inst,
            result=_result(inst.instance_id, answer),
        )

    # finsm: exact SET match over the erroneous concept array.
    finsm = by_cfg["finsm"]
    exact = score(
        finsm,
        'Analysis...\n["us-gaap:Assets", "us-gaap:LiabilitiesCurrent"]',
    )
    assert exact.value == 1.0
    assert exact.detail["precision"] == 1.0 and exact.detail["recall"] == 1.0
    # The LAST array wins.
    assert score(
        finsm,
        'First guess: ["us-gaap:Revenues"]\n'
        'Final: ["us-gaap:LiabilitiesCurrent", "us-gaap:Assets"]',
    ).value == 1.0
    partial = score(finsm, '["us-gaap:Assets"]')
    assert partial.value == 0.0
    assert partial.detail["precision"] == 1.0
    assert partial.detail["recall"] == 0.5
    # Comma-list fallback without JSON.
    comma = score(
        finsm, "Erroneous: us-gaap:Assets, us-gaap:LiabilitiesCurrent."
    )
    assert comma.value == 1.0
    unparseable = score(finsm, "I could not decide.")
    assert unparseable.value == 0.0
    assert "no JSON array" in unparseable.detail["reason"]

    # finre: choice mapping by ANCHORED letter or verbatim text only.
    # Bare digits and free-floating letters are refused (review D3): prose
    # like "differ by 1" or a sentence-initial article "A" must not score.
    finre = by_cfg["finre"]
    prose_digit = score(finre, "The two values differ by 1")
    assert prose_digit.value == 0.0
    assert "names no choice" in prose_digit.detail["reason"]
    article = score(finre, "A mismatch exists between the schedules.")
    assert article.value == 0.0
    assert score(finre, "Answer: B").value == 1.0
    assert score(finre, "reasoning...\nANSWER: b").value == 1.0
    assert score(finre, "reasoning first\nB").value == 1.0  # bare final line
    by_text = score(finre, "This is an Inappropriateness error.")
    assert by_text.value == 1.0
    assert by_text.detail["matched_via"] == "text"
    # Last occurrence wins.
    assert score(
        finre, "Maybe Reversal... no — final answer: Inappropriateness."
    ).value == 1.0
    assert score(finre, "Reversal").value == 0.0
    no_map = score(finre, "cannot classify this filing")
    assert no_map.value == 0.0
    assert "names no choice" in no_map.detail["reason"]

    # finmr: numeric pair match at relative tolerance 1e-4.
    finmr = by_cfg["finmr"]
    assert score(
        finmr, '{"extracted_value": 1234.5, "calculated_value": 1200.0}'
    ).value == 1.0
    # Within tolerance (rel err ~4.9e-5) and key/format variants.
    assert score(
        finmr, '{"extracted": "1,234.56", "calculated": "1200"}'
    ).value == 1.0
    # Near miss: rel err ~3.2e-4 > 1e-4 on one field zeroes the score.
    near = score(
        finmr, '{"extracted_value": 1234.9, "calculated_value": 1200.0}'
    )
    assert near.value == 0.0
    assert near.detail["extracted_match"] is False
    assert near.detail["calculated_match"] is True
    # The LAST JSON object wins.
    assert score(
        finmr,
        'Draft: {"extracted_value": 1, "calculated_value": 1}\n'
        'Final: {"extracted_value": 1234.5, "calculated_value": 1200.0}',
    ).value == 1.0
    non_numeric = score(
        finmr, '{"extracted_value": "n/a", "calculated_value": 1200.0}'
    )
    assert non_numeric.value == 0.0
    assert "non-numeric" in non_numeric.detail["reason"]
    unparseable = score(finmr, "the value is 1234.5")
    assert unparseable.value == 0.0
    assert "no JSON object" in unparseable.detail["reason"]
