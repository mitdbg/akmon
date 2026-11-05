"""The proposer (issue #4282, PACT akb-propose): synthetic deployments, a fake
model and a faked ``okg`` binary; real git. No database, no model call."""
from __future__ import annotations

import asyncio
import json
import subprocess
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

from okg.optimizer.propose import apply as apply_mod
from okg.optimizer.propose import driver, loop
from okg.optimizer.propose.apply import Proposal, ProposalRefused, apply, leak_check
from okg.substrate import fastyaml

MANIFEST = """\
name: demo
description: test
schema_dir: schemas
agent_surface:
  served: [inspect, search]
enrichers:
  - class: some.Existing
"""

RULE = {"id": "ticket_mentions_site", "src": {"subtype": "jira_issue", "field": "site_name"},
        "dst": {"subtype": "site", "field": "name"}, "matcher": "exact", "edge": "references",
        "bidirectional": False}


def _deployment(tmp_path: Path, manifest: str = MANIFEST) -> Path:
    root = tmp_path / "demo"
    (root / "schemas" / "bridges").mkdir(parents=True)
    (root / "deployment.yaml").write_text(manifest, encoding="utf-8")
    (root / "schemas" / "demo.yaml").write_text("classes: {}\n", encoding="utf-8")
    return root


def _text(branch: str = "a4-r1-1", **layer: Any) -> Proposal:
    return Proposal(kind="text", branch=branch, change=layer or {"fix_hints": {"node_not_found": "copy the id; {default}"}},
                    stage="served_text", purpose="cost")


def _link(**change: Any) -> Proposal:
    return Proposal(kind="link", branch="a4-r1-2",
                    change=change or {"rule": RULE, "narrowing": {"src": "JiraIssue", "edge": "references", "dst": "Site"}},
                    stage="extraction_linking", purpose="accuracy")


# ── leaks ──────────────────────────────────────────────────────────────


def test_leak_check_finds_phrases_ids_and_values() -> None:
    key = ["the rule weight was 1000 for T1_US_FNAL", "ticket CMSTRANSF-1348 closed the transfer"]
    assert "1000" in leak_check("set weight to 1000", key)
    assert "cmstransf-1348" in leak_check("see CMSTRANSF-1348", key)
    assert "the rule weight was" in leak_check("say what the rule weight was before", key)
    assert leak_check("search the rule history first", key) == []


# ── text ───────────────────────────────────────────────────────────────


def test_a_text_change_becomes_a_branch_layer_and_the_key_is_added(tmp_path: Path) -> None:
    root = _deployment(tmp_path)
    files = apply(root, _text())
    assert files == ["surface_text.yaml", "deployment.yaml"]
    assert "  surface_text: surface_text.yaml\n" in (root / "deployment.yaml").read_text()
    doc = fastyaml.safe_load((root / "surface_text.yaml").read_text())
    assert doc == {"branches": {"a4-r1-1": {"fix_hints": {"node_not_found": "copy the id; {default}"}}}}
    # A second branch keeps the first; the key is not added twice.
    assert apply(root, _text("a4-r1-3", tools={"search": "Search records."})) == ["surface_text.yaml"]
    assert set(fastyaml.safe_load((root / "surface_text.yaml").read_text())["branches"]) == {"a4-r1-1", "a4-r1-3"}


@pytest.mark.parametrize("layer, why", [
    ({"fix_hints": {"node_not_found": "{code}"}}, "placeholder"),
    ({"instructions": "x.md"}, "tools, params and/or fix_hints"),
])
def test_a_text_change_the_server_would_refuse_is_refused(tmp_path: Path, layer: dict[str, Any], why: str) -> None:
    root = _deployment(tmp_path)
    with pytest.raises(Exception, match=why):
        apply(root, _text(**layer))
    assert not (root / "surface_text.yaml").exists()


def test_a_change_that_repeats_the_answer_key_is_refused(tmp_path: Path) -> None:
    root = _deployment(tmp_path)
    with pytest.raises(ProposalRefused, match="answer key"):
        apply(root, _text(tools={"search": "The weight is 1000."}), answer_key=["weight 1000 at FNAL"])


# ── link ───────────────────────────────────────────────────────────────


def test_a_link_change_writes_rule_narrowing_and_enricher(tmp_path: Path) -> None:
    root = _deployment(tmp_path)
    files = apply(root, _link())
    assert files == ["optimizer_rules/a4_r1_2/rule.yaml", "schemas/bridges/optimizer_a4_r1_2.yaml", "deployment.yaml"]
    from okg.substrate.library.linkers.rules import load_rules_file

    assert [r.id for r in load_rules_file(root / files[0])] == ["ticket_mentions_site"]
    bridge = fastyaml.safe_load((root / files[1]).read_text())
    attrs = bridge["classes"]["EdgeNarrowings"]["attributes"]
    (only,) = attrs.values()
    assert only["annotations"] == {"edge_archetype": "references", "src_subtypes": "JiraIssue", "dst_subtypes": "Site"}
    manifest = fastyaml.safe_load((root / "deployment.yaml").read_text())
    assert manifest["enrichers"][0] == {"class": "okg.substrate.library.linkers.declarative.DeclarativeLinker",
                                        "config": {"name": "optimizer_a4_r1_2", "rules_dir": "optimizer_rules/a4_r1_2"}}
    assert manifest["enrichers"][1] == {"class": "some.Existing"}


@pytest.mark.parametrize("change, why", [
    ({"rule": {**RULE, "matcher": "magic"}, "narrowing": {"src": "A", "edge": "references", "dst": "B"}}, "does not parse"),
    ({"rule": RULE, "narrowing": {"src": "A", "edge": "contains", "dst": "B"}}, "not the rule's edge"),
    ({"rule": RULE, "narrowing": {"src": "A; DROP", "edge": "references", "dst": "B"}}, "plain names"),
    ({"rule": RULE}, "`rule` and `narrowing`"),
])
def test_a_bad_link_change_is_refused(tmp_path: Path, change: dict[str, Any], why: str) -> None:
    with pytest.raises(ProposalRefused, match=why):
        apply(_deployment(tmp_path), _link(**change))


def test_a_bad_branch_name_is_refused(tmp_path: Path) -> None:
    with pytest.raises(ProposalRefused, match="branch"):
        apply(_deployment(tmp_path), Proposal(kind="link", branch="Bad/Name", change={"rule": RULE, "narrowing": {
            "src": "A", "edge": "references", "dst": "B"}}, stage="x", purpose="accuracy"))


# ── context and loop ───────────────────────────────────────────────────


def _jsonl(path: Path, rows: list[dict[str, Any]]) -> Path:
    path.write_text("".join(json.dumps(r) + "\n" for r in rows), encoding="utf-8")
    return path


def test_the_context_steers_on_high_confidence_c_f_g_only(tmp_path: Path) -> None:
    root = _deployment(tmp_path)
    labels = _jsonl(tmp_path / "labels.jsonl", [
        {"status": "labelled", "question_id": "q1", "try": 1, "fact_id": "f", "cls": "f", "stage": "served_text",
         "confidence": "high", "call": 3, "quote": "unsupported_operator_form", "note": "refusal not recovered"},
        {"status": "labelled", "question_id": "q2", "try": 1, "fact_id": "g", "cls": "a", "stage": "organization",
         "confidence": "high", "call": None, "quote": "", "note": "never reached"},
        {"status": "labelled", "question_id": "q3", "try": 1, "fact_id": "h", "cls": "c", "stage": "model",
         "confidence": "medium", "call": 1, "quote": "x", "note": "dropped"},
    ])
    waste = _jsonl(tmp_path / "waste.jsonl", [{"question_id": "q1", "try": 1, "calls": 5, "wasted_calls": 1,
        "by_kind": {"tool_error": 1}, "items": [{"kind": "tool_error", "call": 3, "evidence": "refused x"}]}])
    surface = {"instructions": "Use search first.", "tools": [
        {"name": "search", "description": "Search.", "input_schema": {"properties": {"query": {}}}}]}
    ctx = driver.build_context(root, labels=labels, waste=waste, surface=surface)
    assert "steering rows (high confidence, class in ['c', 'f', 'g']): 1; hints: 2" in ctx.summary
    steering, hints = ctx.summary.split("## Hint rows")
    assert "id q1/1/f class f" in steering and "id q2/1/g" not in steering and "id q2/1/g" in hints
    assert "tool_error call 3" in ctx.summary and "## search (arguments: query)" in ctx.summary
    assert set(ctx.readable) == {"deployment.yaml", "schemas/demo.yaml"}


class _Model:
    model, reasoning_effort = "gpt-6-luna", "high"

    def __init__(self, turns: list[list[tuple[str, dict[str, Any]]]]) -> None:
        self.turns = list(turns)
        self.sent: list[dict[str, Any]] = []

    async def respond(self, **kw: Any) -> Any:
        self.sent.append(kw)
        calls = [{"call_id": f"c{len(self.sent)}-{i}", "name": n, "arguments": json.dumps(a)}
                 for i, (n, a) in enumerate(self.turns.pop(0))]
        output = [{"type": "function_call", **c} for c in calls]
        return SimpleNamespace(output=output, calls=calls, text="", usd=0.001)


COMMON = {"stage": "served_text", "purpose": "cost", "targets": ["q1/1/f"], "expected_effect": "fewer retries",
          "rationale": "the refusal names no working call"}


def test_the_loop_reads_retries_a_refused_write_and_stops_at_the_first_accepted(tmp_path: Path) -> None:
    root = _deployment(tmp_path)
    ctx = loop.Context(summary="SUMMARY", readable={"deployment.yaml": root / "deployment.yaml"})
    model = _Model([
        [("read_context", {}), ("read_file", {"path": "deployment.yaml"}), ("read_file", {"path": "/etc/passwd"})],
        [("propose_text", {"layer": {"fix_hints": {"node_not_found": "{code}"}}, **COMMON})],
        [("propose_text", {"layer": {"fix_hints": {"node_not_found": "copy the id exactly; {default}"}}, **COMMON})],
    ])
    result = asyncio.run(loop.propose(model, ctx, root, "a4-r1-1", kinds=("text",)))
    assert result["status"] == "proposed" and result["turns"] == 3 and len(result["refusals"]) == 1
    assert result["proposal"].change == {"fix_hints": {"node_not_found": "copy the id exactly; {default}"}}
    outputs = [i["output"] for i in model.sent[1]["input_items"] if i.get("type") == "function_call_output"]
    assert outputs[0] == "SUMMARY" and "agent_surface" in outputs[1] and outputs[2].startswith("refused")
    assert [t["name"] for t in model.sent[0]["tools"]] == ["read_context", "read_file", "propose_text"]
    assert not (root / "surface_text.yaml").exists(), "the loop checks on a scratch copy only"


def test_the_loop_gives_up_at_the_turn_limit(tmp_path: Path) -> None:
    root = _deployment(tmp_path)
    model = _Model([[("read_context", {})]] * 3)
    result = asyncio.run(loop.propose(model, loop.Context("S"), root, "a4-r1-1",
                                      settings=loop.ProposeSettings(max_turns=3)))
    assert result["status"] == "no_proposal" and result["turns"] == 3


# ── publish (faked okg, real git) ──────────────────────────────────────


def _git_repo(root: Path) -> None:
    for args in (["init", "-q"], ["add", "-A"], ["-c", "user.name=t", "-c", "user.email=t@t", "commit", "-q", "-m", "base"]):
        subprocess.run(["git", "-C", str(root), *args], check=True, capture_output=True)


def _head(root: Path) -> tuple[str, str]:
    sha = subprocess.run(["git", "-C", str(root), "rev-parse", "HEAD"], capture_output=True, text=True).stdout.strip()
    ref = subprocess.run(["git", "-C", str(root), "symbolic-ref", "-q", "--short", "HEAD"], capture_output=True,
                         text=True).stdout.strip()
    return sha, ref


CAT = {"catalog_version_id": 7, "catalog_fingerprint": "sha256:base", "schema_fingerprint": "s"}
OWN = {"owner_fingerprint": "o", "schema_fingerprint": "s", "catalog_fingerprint": "sha256:base",
       "deployment_root_hash": "r", "manifest_fingerprint": "m", "status": "active"}


def _fake_db(monkeypatch: pytest.MonkeyPatch, *, cat_after: dict[str, Any] = CAT, own_after: dict[str, Any] = OWN,
             rule_edges: int = 12) -> None:
    cats, owns = iter([CAT, cat_after]), iter([OWN, own_after])
    monkeypatch.setattr(driver, "catalog_state", lambda *_a, **_k: next(cats))
    monkeypatch.setattr(driver, "ownership_state", lambda *_a, **_k: next(owns))
    # Base branch first (3 existing edges of that type), then the experiment branch.
    counts = iter([3, 3 + rule_edges])
    monkeypatch.setattr(driver, "rule_edges", lambda *_a, **_k: next(counts))


@pytest.mark.parametrize("kind, report, rule_edges, outcome", [
    ("text", {"status": "published", "generation_id": "g1", "edges_added": 0}, 0, "published"),
    ("link", {"status": "published", "generation_id": "g2", "edges_added": 500}, 0, "no_effect"),
    ("link", {"status": "published", "generation_id": "g3", "edges_added": 12}, 12, "published"),
    ("link", {"status": "published", "generation_id": "g4", "edges_added": 12, "enricher_failures": [{"x": 1}]},
     12, "enricher_failed"),
    ("text", {"status": "blocked"}, 0, "publish_blocked"),
])
def test_publish_trial_commits_publishes_checks_and_restores(tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
                                                            kind: str, report: dict[str, Any], rule_edges: int,
                                                            outcome: str) -> None:
    root = _deployment(tmp_path)
    _git_repo(root)
    base = _head(root)
    calls: list[list[str]] = []

    def fake_run(argv, *, cwd, env, timeout=0):
        calls.append(list(argv[1:]))
        assert Path(cwd) == root
        if "run" in argv:
            return (0 if report["status"] == "published" else 3), json.dumps(report) + "\n"
        return 0, "{}\n"

    monkeypatch.setattr(driver, "_run", fake_run)
    _fake_db(monkeypatch, rule_edges=rule_edges)
    proposal = _text() if kind == "text" else _link()
    out = driver.publish_trial(root, proposal, deployment="demo", dsn="postgresql://x", env={})
    assert out["outcome"] == outcome and out["diff_digest"].startswith("sha256:")
    assert _head(root) == base and out["restored_tree"] is True, "back on the base branch, clean"
    assert not (root / "surface_text.yaml").exists() and not (root / "optimizer_rules").exists()
    names = [c[1] if c[0] == "--json" else c[0] for c in calls]
    if kind == "link":
        assert names[:3] == ["branch", "catalog", "catalog"] and names[-2:] == ["catalog", "catalog"]
        assert out["restored_catalog"] is True and out["catalog_restored_verified"] is True
    else:
        assert names == ["branch", "run"] and out["serve_commit"] == out["commit"]
    record = driver.proposal_record({"status": "proposed", "proposal": proposal, "turns": 2, "usd": 0.01}, out)
    assert record["proposal"]["kind"] == kind and record["publish"]["outcome"] == outcome


@pytest.mark.parametrize("cat_after, own_after", [
    ({**CAT, "catalog_fingerprint": "sha256:trial"}, OWN),
    (CAT, {**OWN, "deployment_root_hash": "someone-else"}),
])
def test_a_catalog_or_ownership_not_back_at_base_is_not_verified(tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
                                                                 cat_after: dict[str, Any],
                                                                 own_after: dict[str, Any]) -> None:
    root = _deployment(tmp_path)
    _git_repo(root)
    monkeypatch.setattr(driver, "_run", lambda argv, **_k: (0, json.dumps({"status": "published", "edges_added": 3,
                                                                          "generation_id": "g"}) + "\n"))
    _fake_db(monkeypatch, cat_after=cat_after, own_after=own_after)
    out = driver.publish_trial(root, _link(), deployment="demo", dsn="d", env={})
    assert out["outcome"] == "published" and out["catalog_restored_verified"] is False


def test_a_failed_restore_command_is_not_verified(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    root = _deployment(tmp_path)
    _git_repo(root)
    seen: list[str] = []

    def fake_run(argv, **_k):
        seen.append(" ".join(argv[1:3]))
        if "run" in argv:
            return 0, json.dumps({"status": "published", "edges_added": 3, "generation_id": "g"}) + "\n"
        # The second ownership claim (the restore) fails.
        return (1 if seen.count("catalog ownership") == 2 and "ownership" in argv else 0), "{}\n"

    monkeypatch.setattr(driver, "_run", fake_run)
    _fake_db(monkeypatch)
    out = driver.publish_trial(root, _link(), deployment="demo", dsn="d", env={})
    assert out["restored_catalog"] is False and out["catalog_restored_verified"] is False


def test_a_trial_that_fails_midway_is_recorded_and_restored(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    root = _deployment(tmp_path)
    _git_repo(root)
    base = _head(root)
    monkeypatch.setattr(driver, "_run", lambda argv, **_k: (0, "{}\n"))
    _fake_db(monkeypatch)
    bad = Proposal(kind="text", branch="a4-r1-9", change={"tools": {"nope": "x"}}, stage="s", purpose="cost")
    out = driver.publish_trial(root, bad, deployment="demo", dsn="d", env={},
                               surface={"tools": [{"name": "search", "input_schema": {"properties": {}}}]})
    assert out["outcome"] == "trial_error" and "not published" in out["error"]
    assert _head(root) == base and out["restored_tree"] is True


def test_publish_trial_refuses_a_dirty_base_a_worker_deployment_and_a_held_lock(tmp_path: Path) -> None:
    import fcntl

    root = _deployment(tmp_path)
    _git_repo(root)
    (root / "stray.txt").write_text("x")
    with pytest.raises(RuntimeError, match="uncommitted"):
        driver.publish_trial(root, _text(), deployment="demo", dsn="d", env={})
    (root / "stray.txt").unlink()
    with (root / ".git" / "okg-optimizer.lock").open("w") as held:
        fcntl.flock(held, fcntl.LOCK_EX | fcntl.LOCK_NB)
        with pytest.raises(RuntimeError, match="lock"):
            driver.publish_trial(root, _text(), deployment="demo", dsn="d", env={})
    worker = _deployment(tmp_path / "w", MANIFEST + "runtime:\n  enabled: true\n")
    _git_repo(worker)
    with pytest.raises(RuntimeError, match="runtime.enabled"):
        driver.publish_trial(worker, _text(), deployment="demo", dsn="d", env={})


def test_constants_name_the_files_the_proposer_may_write() -> None:
    assert apply_mod.SURFACE_TEXT_FILE == "surface_text.yaml" and apply_mod.RULES_DIR == "optimizer_rules"
    assert driver.OPTIMIZER_PATHS == ("surface_text.yaml", "optimizer_rules", "schemas/bridges/optimizer_*")


# ── review fixes: indentation, surface, targets, hints ─────────────────

INDENT4 = """\
name: demo
agent_surface:
    served: [inspect, search]
enrichers:
    - class: some.Existing
"""


def test_four_space_manifests_keep_their_meaning(tmp_path: Path) -> None:
    root = _deployment(tmp_path, INDENT4)
    apply(root, _text())
    apply(root, _link())
    raw = fastyaml.safe_load((root / "deployment.yaml").read_text())
    assert raw["agent_surface"] == {"served": ["inspect", "search"], "surface_text": "surface_text.yaml"}
    assert [e["class"] for e in raw["enrichers"]] == [
        "okg.substrate.library.linkers.declarative.DeclarativeLinker", "some.Existing"]


SURFACE = {"tools": [{"name": "search", "input_schema": {"properties": {"query": {}}}}]}


@pytest.mark.parametrize("layer, why", [
    ({"tools": {"serch": "x"}}, "not published"),
    ({"params": {"search.limit": "x"}}, "not published"),
    ({"fix_hints": {"no_such_code_anywhere": "x"}}, "no_such_code_anywhere"),
])
def test_a_text_change_naming_what_the_server_lacks_is_refused(tmp_path: Path, layer: dict[str, Any], why: str) -> None:
    with pytest.raises(ProposalRefused, match=why):
        apply(_deployment(tmp_path), _text(**layer), surface=SURFACE)
    apply(_deployment(tmp_path / "ok"), _text(params={"search.query": "words"}), surface=SURFACE)


def test_the_leak_check_also_covers_rationale_and_ignores_plain_hyphenated_words(tmp_path: Path) -> None:
    leaky = Proposal(kind="text", branch="a4-r1-1", change={"tools": {"search": "Search."}}, stage="s",
                     purpose="cost", rationale="the answer was 1000 at FNAL")
    with pytest.raises(ProposalRefused, match="answer key"):
        apply(_deployment(tmp_path), leaky, answer_key=["weight 1000 at FNAL"])
    assert leak_check("a read-only follow-up", ["a read-only follow-up mode"], n=5) == []


def test_targets_must_be_diagnosis_ids(tmp_path: Path) -> None:
    root = _deployment(tmp_path)
    ctx = loop.Context(summary="S", target_ids=frozenset({"q1/1/f", "q2/1"}))
    model = _Model([
        [("propose_text", {"layer": {"tools": {"search": "Search."}}, **{**COMMON, "targets": ["search"]}})],
        [("propose_text", {"layer": {"tools": {"search": "Search."}}, **{**COMMON, "targets": ["q2/1"]}})],
    ])
    result = asyncio.run(loop.propose(model, ctx, root, "a4-r1-1", kinds=("text",)))
    assert result["status"] == "proposed" and "not ids listed" in result["refusals"][0]


def test_the_context_lists_hint_rows_and_collects_target_ids(tmp_path: Path) -> None:
    root = _deployment(tmp_path)
    labels = _jsonl(tmp_path / "labels.jsonl", [
        {"status": "labelled", "question_id": "q2", "try": 1, "fact_id": "g", "cls": "a", "stage": "organization",
         "confidence": "high", "call": None, "quote": "", "note": "never reached"}])
    waste = _jsonl(tmp_path / "waste.jsonl", [{"question_id": "q9", "try": 2, "calls": 3, "wasted_calls": 1,
        "by_kind": {"timeout": 1}, "items": [{"kind": "timeout", "call": 0, "evidence": "timed out"}]}])
    objectives = tmp_path / "objectives.yaml"
    objectives.write_text("objectives: [completeness]\n")
    ctx = driver.build_context(root, labels=labels, waste=waste, surface=SURFACE, objectives=objectives)
    assert "## Hint rows" in ctx.summary and "- id q2/1/g class a" in ctx.summary
    assert "objectives: [completeness]" in ctx.summary
    assert ctx.target_ids == frozenset({"q2/1/g", "q9/2"}) and ctx.surface == SURFACE


def test_a_link_rule_the_data_check_refuses_goes_back_to_the_model(tmp_path: Path) -> None:
    root = _deployment(tmp_path)
    good = {**RULE, "id": "second_try"}
    model = _Model([
        [("propose_link", {"rule": RULE, "narrowing": {"src": "JiraIssue", "edge": "references", "dst": "Site"},
                           **COMMON})],
        [("propose_link", {"rule": good, "narrowing": {"src": "JiraIssue", "edge": "references", "dst": "Site"},
                           **COMMON})],
    ])
    seen: list[str] = []

    def check(rule: dict[str, Any]) -> str | None:
        seen.append(rule["id"])
        return "the rule would link nothing: 0 jira_issue records carry 'site_name'" if rule["id"] == RULE["id"] else None

    result = asyncio.run(loop.propose(model, loop.Context("S"), root, "a4-r1-2", kinds=("link",), link_check=check))
    assert result["status"] == "proposed" and result["proposal"].change["rule"]["id"] == "second_try"
    assert seen == ["ticket_mentions_site", "second_try"] and "link nothing" in result["refusals"][0]


def test_a_rule_that_only_re_derives_existing_edges_has_no_effect(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    root = _deployment(tmp_path)
    _git_repo(root)
    monkeypatch.setattr(driver, "_run", lambda argv, **_k: (0, json.dumps({"status": "published", "edges_added": 40,
                                                                          "generation_id": "g"}) + "\n"))
    _fake_db(monkeypatch)
    counts = iter([40, 40])  # the same 40 edges on the base and on the branch
    monkeypatch.setattr(driver, "rule_edges", lambda *_a, **_k: next(counts))
    out = driver.publish_trial(root, _link(), deployment="demo", dsn="d", env={})
    assert out["outcome"] == "no_effect" and out["rule_edges_new"] == 0
