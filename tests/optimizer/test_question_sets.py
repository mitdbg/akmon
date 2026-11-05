"""Question-set format, digest, registry, coverage and held-out view (PACT akb-question-sets).

All fixtures are synthetic; no benchmark text is committed.
"""
from __future__ import annotations

import json
import zipfile
from pathlib import Path
from typing import Any

import pytest

from okg.optimizer.question_set_import import import_golden_export
from okg.optimizer.question_sets import (
    HeldOutQuestionError,
    LoopView,
    QuestionSetError,
    coverage_report,
    load_question_set_file,
    load_registry,
    loop_view,
    main,
    parse_question_set,
    question_set_document,
    summary,
)


def _q(qid: str, origin: str, tags: list[str], *, held_out: bool = False, **extra: Any) -> dict[str, Any]:
    q = {
        "id": qid,
        "question": f"synthetic question {qid}?",
        "origin": origin,
        "scope_tags": tags,
        "held_out": held_out,
        "required_facts": [
            {"id": "F1", "text": f"fact one of {qid}", "required": True, "source_refs": ["rec:1"]},
            {"id": "F2", "text": f"fact two of {qid}", "required": False},
        ],
    }
    q.update(extra)
    return q


def _doc() -> dict[str, Any]:
    return {
        "schema": "okg.question_set",
        "version": 1,
        "id": "synthetic-6",
        "questions": [
            _q("q1", "operator", ["alpha", "beta"]),
            _q("q2", "resolved_ticket", ["alpha"], held_out=True),
            _q("q3", "generated", ["alpha"]),
            _q("q4", "generated", ["gamma"]),
            _q("q5", "generated", ["gamma"], held_out=True),
            _q("q6", "operator", ["beta"], as_of={"snapshot": "snap-2026-10-01", "at": "2026-10-01T00:00:00Z"}),
        ],
    }


def _codes(raw: Any) -> tuple[str, ...]:
    with pytest.raises(QuestionSetError) as exc:
        parse_question_set(raw)
    return exc.value.codes


# --- validation -------------------------------------------------------------


def test_valid_set_loads_with_all_fields() -> None:
    qs = parse_question_set(_doc())
    assert qs.id == "synthetic-6"
    assert [q.id for q in qs.questions] == ["q1", "q2", "q3", "q4", "q5", "q6"]
    q6 = qs.question("q6")
    assert q6.as_of is not None and q6.as_of.snapshot == "snap-2026-10-01"
    assert q6.required_facts[0].source_refs == ("rec:1",)
    assert qs.digest.startswith("sha256:") and len(qs.digest) == len("sha256:") + 64


@pytest.mark.parametrize(
    ("mutate", "code"),
    [
        (lambda d: d.update(extra=1), "unknown_key"),
        (lambda d: d["questions"][0].update(colour="red"), "unknown_key"),
        (lambda d: d["questions"][0]["required_facts"][0].update(weight=2), "unknown_key"),
        (lambda d: d["questions"][1].update(id="q1"), "duplicate_question_id"),
        (lambda d: d["questions"][0]["required_facts"][1].update(id="F1"), "duplicate_fact_id"),
        (lambda d: d["questions"][0].pop("origin"), "origin_missing"),
        (lambda d: d["questions"][0].update(origin="benchmark"), "origin_invalid"),
        (lambda d: d["questions"][0].update(required_facts=[]), "required_facts_empty"),
        (lambda d: d["questions"][0].pop("required_facts"), "required_facts_missing"),
        (lambda d: d["questions"][0]["required_facts"][0].update(required=False), "no_required_fact"),
        (lambda d: d["questions"][0]["required_facts"][0].update(required="yes"), "fact_required_invalid"),
        (lambda d: d["questions"][0].pop("held_out"), "held_out_missing"),
        (lambda d: d["questions"][0].update(held_out="no"), "held_out_invalid"),
        (lambda d: d["questions"][0].update(scope_tags=[]), "scope_tags_empty"),
        (lambda d: d["questions"][0].pop("scope_tags"), "scope_tags_missing"),
        (lambda d: d["questions"][0].update(scope_tags=["a", "a"]), "scope_tag_duplicate"),
        (lambda d: d["questions"][0].update(question=" "), "question_text_invalid"),
        (lambda d: d["questions"][0].update(as_of={}), "as_of_invalid"),
        (lambda d: d["questions"][0].update(as_of={"when": "x"}), "unknown_key"),
        (lambda d: d["questions"][0].update(source_refs=["a", "a"]), "source_refs_duplicate"),
        (lambda d: d.update(schema="other"), "schema_invalid"),
        (lambda d: d.update(version=True), "version_invalid"),
        (lambda d: d.update(questions=[]), "questions_empty"),
        (lambda d: d.pop("id"), "missing_key"),
        (lambda d: d.update(version=1.0), "version_invalid"),
        (lambda d: d["questions"][0].update(scope_tags=[" alpha"]), "scope_tags_invalid"),
        (lambda d: d["questions"][0].update(source_refs=["rec:1 "]), "source_refs_invalid"),
        (lambda d: d["questions"][0].update(id="q1 "), "question_id_invalid"),
        (lambda d: d["questions"][0].update(held_out=1), "held_out_invalid"),
        (lambda d: d["questions"][0]["required_facts"][0].update(required=1), "fact_required_invalid"),
    ],
)
def test_refusals_are_named(mutate, code: str) -> None:
    raw = _doc()
    mutate(raw)
    assert code in _codes(raw)


def test_all_problems_reported_at_once() -> None:
    raw = _doc()
    raw["questions"][0].pop("origin")
    raw["questions"][3].update(required_facts=[])
    codes = _codes(raw)
    assert "origin_missing" in codes and "required_facts_empty" in codes


def test_repeated_yaml_key_is_refused(tmp_path: Path) -> None:
    path = tmp_path / "set.yaml"
    path.write_text("schema: okg.question_set\nversion: 1\nid: a\nid: b\nquestions: []\n", encoding="utf-8")
    with pytest.raises(QuestionSetError) as exc:
        load_question_set_file(path)
    assert exc.value.codes == ("question_set_parse",)


# --- digest -----------------------------------------------------------------


def test_digest_stable_across_order_and_layout(tmp_path: Path) -> None:
    base = parse_question_set(_doc())
    shuffled = _doc()
    shuffled["questions"].reverse()
    shuffled["questions"][0]["scope_tags"].reverse()
    as_yaml = tmp_path / "s.yaml"
    as_json = tmp_path / "s.json"
    import yaml

    as_yaml.write_text(yaml.safe_dump(shuffled, sort_keys=False), encoding="utf-8")
    as_json.write_text(json.dumps(_doc(), indent=4, sort_keys=True), encoding="utf-8")
    assert parse_question_set(shuffled).digest == base.digest
    assert load_question_set_file(as_yaml).digest == base.digest
    assert load_question_set_file(as_json).digest == base.digest
    # Round trip through the canonical document keeps the digest.
    assert parse_question_set(question_set_document(base)).digest == base.digest
    # Pinned value: a change to canonicalization breaks every registry pin.
    assert base.digest == "sha256:" + _PINNED_DIGEST


_PINNED_DIGEST = "847fd57fab2ba6620749e34b2898ef3a906b4827ee387071a47ae5d88fc0982d"


@pytest.mark.parametrize(
    "mutate",
    [
        lambda d: d["questions"][0].update(held_out=True),
        lambda d: d["questions"][0].update(origin="generated"),
        lambda d: d["questions"][0]["required_facts"][0].update(text="changed"),
        lambda d: d["questions"][0]["required_facts"][0].update(source_refs=["rec:2"]),
        lambda d: d.update(id="other"),
    ],
)
def test_digest_changes_with_content(mutate) -> None:
    raw = _doc()
    mutate(raw)
    assert parse_question_set(raw).digest != parse_question_set(_doc()).digest


# --- coverage ---------------------------------------------------------------


def test_coverage_rule_generated_counts_only_without_real() -> None:
    rows = {r.tag: r for r in coverage_report(parse_question_set(_doc()))}
    alpha, beta, gamma = rows["alpha"], rows["beta"], rows["gamma"]
    assert dict(alpha.by_origin) == {"operator": 1, "resolved_ticket": 1, "generated": 1}
    assert (alpha.counted, alpha.counted_from, alpha.generated_not_counted, alpha.held_out) == (2, "real", 1, 1)
    assert (beta.counted, beta.counted_from, beta.held_out) == (2, "real", 0)
    # A tag only generated questions cover: they count.
    assert dict(gamma.by_origin) == {"operator": 0, "resolved_ticket": 0, "generated": 2}
    assert (gamma.counted, gamma.counted_from, gamma.generated_not_counted, gamma.held_out) == (
        2, "generated_only", 0, 1,
    )


# --- held-out view ----------------------------------------------------------


def test_loop_view_hides_held_out_questions() -> None:
    qs = parse_question_set(_doc())
    view = loop_view(qs)
    assert [q.id for q in view] == ["q1", "q3", "q4", "q6"]
    assert len(view) == 4 and view.held_out_count == 2 and view.digest == qs.digest
    assert view.question("q1").id == "q1"
    with pytest.raises(HeldOutQuestionError) as exc:
        view.question("q2")
    assert "synthetic question q2" not in str(exc.value)
    with pytest.raises(KeyError):
        view.question("nope")
    assert all("q2" not in q.id and "q5" not in q.id for q in view.questions)


# --- registry ---------------------------------------------------------------


def _write_registry(tmp_path: Path, entry: dict[str, Any]) -> Path:
    path = tmp_path / "question_sets.yaml"
    path.write_text(json.dumps({"version": 1, "question_sets": {"synthetic-6": entry}}), encoding="utf-8")
    return path


def test_registry_loads_by_id_and_checks_digest(tmp_path: Path) -> None:
    (tmp_path / "sets").mkdir()
    (tmp_path / "sets" / "s.json").write_text(json.dumps(_doc()), encoding="utf-8")
    digest = parse_question_set(_doc()).digest
    reg = load_registry(_write_registry(tmp_path, {"format": "okg.question_set", "path": "sets/s.json", "digest": digest}))
    assert reg.load("synthetic-6").digest == digest

    bad = load_registry(_write_registry(
        tmp_path, {"format": "okg.question_set", "path": "sets/s.json", "digest": "sha256:" + "0" * 64},
    ))
    with pytest.raises(QuestionSetError) as exc:
        bad.load("synthetic-6")
    assert exc.value.codes == ("question_set_digest_mismatch",)
    with pytest.raises(QuestionSetError) as exc:
        reg.load("missing-set")
    assert exc.value.codes == ("question_set_unknown",)


def test_registry_env_path_and_refusals(tmp_path: Path) -> None:
    (tmp_path / "s.json").write_text(json.dumps(_doc()), encoding="utf-8")
    digest = parse_question_set(_doc()).digest
    reg = load_registry(_write_registry(
        tmp_path, {"format": "okg.question_set", "path": "${QS_PRIVATE_DIR}/s.json", "digest": digest},
    ))
    with pytest.raises(QuestionSetError) as exc:
        reg.load("synthetic-6", environ={})
    assert exc.value.codes == ("question_set_path_env_unset",)
    with pytest.raises(QuestionSetError) as exc:
        reg.load("synthetic-6", environ={"QS_PRIVATE_DIR": ""})
    assert exc.value.codes == ("question_set_path_env_unset",)
    assert reg.load("synthetic-6", environ={"QS_PRIVATE_DIR": str(tmp_path)}).digest == digest
    defaulted = load_registry(_write_registry(
        tmp_path, {"format": "okg.question_set", "path": "${QS_PRIVATE_DIR:-/tmp}/s.json",
                   "digest": digest},
    ))
    with pytest.raises(QuestionSetError) as exc:
        defaulted.load("synthetic-6", environ={"QS_PRIVATE_DIR": str(tmp_path)})
    assert exc.value.codes == ("question_set_path_env_unsupported",)
    missing = load_registry(_write_registry(
        tmp_path, {"format": "okg.question_set", "path": "nope.json", "digest": digest},
    ))
    with pytest.raises(QuestionSetError) as exc:
        missing.load("synthetic-6")
    assert exc.value.codes == ("question_set_file_missing",)


@pytest.mark.parametrize(
    ("entry", "code"),
    [
        ({"format": "csv", "path": "x", "digest": "sha256:" + "0" * 64}, "registry_format_invalid"),
        ({"format": "okg.question_set", "path": "x", "digest": "abc"}, "registry_digest_invalid"),
        ({"format": "okg.question_set", "path": "x", "digest": "sha256:" + "0" * 64, "origin": "operator"}, "unknown_key"),
        ({"format": "golden_export_dir", "path": "x", "digest": "sha256:" + "0" * 64, "held_out": []}, "missing_key"),
        (
            {"format": "golden_export_dir", "path": "x", "digest": "sha256:" + "0" * 64, "origin": "operator",
             "held_out": "none"},
            "registry_held_out_invalid",
        ),
    ],
)
def test_registry_entry_refusals(tmp_path: Path, entry: dict[str, Any], code: str) -> None:
    with pytest.raises(QuestionSetError) as exc:
        load_registry(_write_registry(tmp_path, entry))
    assert code in exc.value.codes


# --- golden export importer (synthetic export) ------------------------------


def _export(tmp_path: Path) -> Path:
    root = tmp_path / "export"
    root.mkdir()
    bench = [
        {
            "id": "e1", "question": "synthetic export question one?", "answer": "ref one", "category": "basic",
            "expected_atoms": [{"id": "F1", "text": "atom one", "required": True}],
            "metadata": {"source_types": ["github", "slack"], "question_type": "basic"},
        },
        {
            "id": "e2", "question": "synthetic export question two?", "answer": "ref two", "category": "semantic",
            "expected_atoms": str([{"id": "F1", "text": "atom a", "required": True},
                                   {"id": "F2", "text": "atom b", "required": False}]),
            "metadata": {"source_types": ["github"]},
        },
    ]
    labels = {
        "e1": {"tier": None, "time_sensitive": False, "atoms": [{"id": "F1"}]},
        "e2": {"tier": 2, "time_sensitive": True, "domain": "ops", "atoms": [{"id": "F1"}, {"id": "F2"}]},
    }
    (root / "benchmark.json").write_text(json.dumps(bench), encoding="utf-8")
    (root / "labels.json").write_text(json.dumps(labels), encoding="utf-8")
    (root / "expected_sources.json").write_text(json.dumps({"items": {"e1": ["doc-1", "doc-2"]}}), encoding="utf-8")
    return root


def test_import_golden_export_maps_fields(tmp_path: Path) -> None:
    qs = import_golden_export(_export(tmp_path), set_id="exp-2", origin="operator", held_out=["e2"])
    e1, e2 = qs.question("e1"), qs.question("e2")
    assert e1.scope_tags == ("category:basic", "source_type:github", "source_type:slack")
    assert e2.scope_tags == ("category:semantic", "source_type:github", "tier:2", "time_sensitive", "domain:ops")
    assert e1.source_refs == ("doc-1", "doc-2") and e2.source_refs == ()
    assert [f.required for f in e2.required_facts] == [True, False]
    assert all(not f.source_refs for f in e1.required_facts)
    assert (e1.held_out, e2.held_out, e1.origin) == (False, True, "operator")
    assert e1.reference_answer == "ref one"
    s = summary(qs)
    assert (s["questions"], s["held_out"], s["questions_with_source_refs"]) == (2, 1, 1)


def test_import_from_zip_matches_dir(tmp_path: Path) -> None:
    root = _export(tmp_path)
    zpath = tmp_path / "bundle.zip"
    with zipfile.ZipFile(zpath, "w") as z:
        for name in ("benchmark.json", "labels.json", "expected_sources.json"):
            z.write(root / name, f"bundle/data/{name}")
    from_dir = import_golden_export(root, set_id="exp-2", origin="operator", held_out=[])
    from_zip = import_golden_export(zpath, set_id="exp-2", origin="operator", held_out=[], member_root="bundle/data")
    assert from_zip.digest == from_dir.digest
    with pytest.raises(QuestionSetError) as exc:
        import_golden_export(zpath, set_id="exp-2", origin="operator", held_out=[])
    assert exc.value.codes == ("export_member_root_missing",)


@pytest.mark.parametrize(
    ("edit", "code"),
    [
        (lambda b, lab: lab.pop("e2"), "export_label_missing"),
        (lambda b, lab: lab["e1"].update(atoms=[]), "export_label_atom_missing"),
        (lambda b, lab: b[0]["expected_atoms"][0].pop("required"), "export_atom_required_invalid"),
        (lambda b, lab: b[0].pop("category"), "export_category_missing"),
        (lambda b, lab: (b[0].update(expected_atoms=[]), lab["e1"].update(atoms=[])), "required_facts_empty"),
        (lambda b, lab: lab.update(e9={"tier": None, "time_sensitive": False, "atoms": []}), "export_labels_unknown_id"),
        (lambda b, lab: lab["e1"]["atoms"].append({"id": "F9"}), "export_label_atom_unknown"),
        (lambda b, lab: lab["e1"].update(tier=True), "export_label_invalid"),
        (lambda b, lab: lab["e1"].update(tier={}), "export_label_invalid"),
        (lambda b, lab: lab["e1"].update(tier=""), "export_label_invalid"),
        (lambda b, lab: lab["e1"].update(domain=3), "export_label_invalid"),
        (lambda b, lab: lab["e1"].update(task_type=" "), "export_label_invalid"),
        (lambda b, lab: b[0]["metadata"].update(question_type="semantic"), "export_question_type_mismatch"),
        (lambda b, lab: b[0]["metadata"].update(source_types=["github", "github"]), "scope_tag_duplicate"),
    ],
)
def test_import_refusals(tmp_path: Path, edit, code: str) -> None:
    root = _export(tmp_path)
    bench = json.loads((root / "benchmark.json").read_text())
    labels = json.loads((root / "labels.json").read_text())
    edit(bench, labels)
    (root / "benchmark.json").write_text(json.dumps(bench), encoding="utf-8")
    (root / "labels.json").write_text(json.dumps(labels), encoding="utf-8")
    with pytest.raises(QuestionSetError) as exc:
        import_golden_export(root, set_id="exp-2", origin="operator", held_out=[])
    assert code in exc.value.codes


def test_import_refuses_unknown_held_out_and_source_ids(tmp_path: Path) -> None:
    root = _export(tmp_path)
    with pytest.raises(QuestionSetError) as exc:
        import_golden_export(root, set_id="exp-2", origin="operator", held_out=["zzz"])
    assert exc.value.codes == ("held_out_unknown_id",)
    (root / "expected_sources.json").write_text(json.dumps({"items": {"zzz": ["d"]}}), encoding="utf-8")
    with pytest.raises(QuestionSetError) as exc:
        import_golden_export(root, set_id="exp-2", origin="operator", held_out=[])
    assert exc.value.codes == ("export_sources_unknown_id",)


def test_registry_golden_export_entry_and_cli(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    root = _export(tmp_path)
    digest = import_golden_export(root, set_id="exp-2", origin="operator", held_out=["e2"]).digest
    reg_path = tmp_path / "question_sets.yaml"
    reg_path.write_text(json.dumps({"version": 1, "question_sets": {"exp-2": {
        "format": "golden_export_dir", "path": "export", "origin": "operator", "held_out": ["e2"], "digest": digest,
    }}}), encoding="utf-8")
    assert main(["coverage", "--registry", str(reg_path), "--set", "exp-2"]) == 0
    out = capsys.readouterr().out
    assert digest in out and "category:basic" in out and "2 questions" in out
    assert "synthetic export question" not in out and "atom one" not in out
    assert main(["coverage", "--registry", str(reg_path), "--set", "exp-2", "--json"]) == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["summary"]["questions"] == 2
    assert {r["tag"] for r in payload["coverage"]} >= {"category:basic", "time_sensitive"}
    assert main(["coverage", "--registry", str(reg_path), "--set", "nope", "--json"]) == 1
    assert json.loads(capsys.readouterr().out)["problems"][0]["code"] == "question_set_unknown"


# --- review round: leaks, load contract, remaining refusals ------------------


def test_parse_error_reports_position_not_text(tmp_path: Path) -> None:
    path = tmp_path / "set.yaml"
    secret = "HELD-OUT secret question about the reference count"
    path.write_text(
        f"schema: okg.question_set\nversion: 1\nid: s\nquestions:\n  - question: \"{secret}\n    held_out: [\n",
        encoding="utf-8",
    )
    with pytest.raises(QuestionSetError) as exc:
        load_question_set_file(path)
    problem = exc.value.problems[0]
    assert problem.code == "question_set_parse"
    assert "line" in problem.message and "column" in problem.message
    assert "secret" not in problem.message and "secret" not in str(exc.value)
    assert exc.value.__cause__ is None and exc.value.__suppress_context__


def _registered(tmp_path: Path, doc: dict[str, Any], set_id: str = "synthetic-6") -> Path:
    (tmp_path / "s.json").write_text(json.dumps(doc), encoding="utf-8")
    digest = parse_question_set(doc).digest
    path = tmp_path / "question_sets.yaml"
    path.write_text(json.dumps({"version": 1, "question_sets": {set_id: {
        "format": "okg.question_set", "path": "s.json", "digest": digest,
    }}}), encoding="utf-8")
    return path


def test_registry_load_defaults_to_loop_view(tmp_path: Path) -> None:
    reg = load_registry(_registered(tmp_path, _doc()))
    view = reg.load("synthetic-6")
    assert isinstance(view, LoopView)
    assert [q.id for q in view] == ["q1", "q3", "q4", "q6"]
    with pytest.raises(HeldOutQuestionError):
        view.question("q2")
    full = reg.load_with_held_out("synthetic-6")
    assert [q.id for q in full.questions] == ["q1", "q2", "q3", "q4", "q5", "q6"]
    assert full.digest == view.digest


def test_registry_refuses_id_mismatch(tmp_path: Path) -> None:
    reg = load_registry(_registered(tmp_path, _doc(), set_id="another-id"))
    with pytest.raises(QuestionSetError) as exc:
        reg.load("another-id")
    assert exc.value.codes == ("question_set_id_mismatch",)


@pytest.mark.parametrize(
    ("document", "code"),
    [
        (
            {"version": 1, "question_sets": {"s": {
                "format": "golden_export_dir", "path": "x", "digest": "sha256:" + "0" * 64,
                "origin": "operator", "held_out": ["e1", "e1"],
            }}},
            "registry_held_out_duplicate",
        ),
        (
            {"version": 1.0, "question_sets": {"s": {
                "format": "okg.question_set", "path": "x", "digest": "sha256:" + "0" * 64,
            }}},
            "registry_version_invalid",
        ),
    ],
)
def test_registry_document_refusals(tmp_path: Path, document: dict[str, Any], code: str) -> None:
    path = tmp_path / "question_sets.yaml"
    path.write_text(json.dumps(document), encoding="utf-8")
    with pytest.raises(QuestionSetError) as exc:
        load_registry(path)
    assert code in exc.value.codes


def test_import_refuses_member_root_on_folder_and_repeated_held_out(tmp_path: Path) -> None:
    root = _export(tmp_path)
    with pytest.raises(QuestionSetError) as exc:
        import_golden_export(root, set_id="exp-2", origin="operator", held_out=[], member_root="x")
    assert exc.value.codes == ("export_member_root_invalid",)
    with pytest.raises(QuestionSetError) as exc:
        import_golden_export(root, set_id="exp-2", origin="operator", held_out=["e1", "e1"])
    assert exc.value.codes == ("held_out_duplicate_id",)


def test_cli_refuses_file_with_set(tmp_path: Path) -> None:
    path = tmp_path / "s.json"
    path.write_text(json.dumps(_doc()), encoding="utf-8")
    with pytest.raises(SystemExit) as exc:
        main(["coverage", "--file", str(path), "--set", "synthetic-6"])
    assert exc.value.code == 2
    assert main(["coverage", "--file", str(path)]) == 0
