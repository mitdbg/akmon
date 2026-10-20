"""#2573 slice 10: a missing verdict is unknown, never a pass.

PACT ``unknown-s10-pact-cli-read-ok``. Thirteen sites read a check's ``ok``
with a default that turned "the check said nothing" into "the check passed":
nine ``.get("ok", True)`` calls in ``okg.substrate.pact.cli`` and four
``.get("ok") is not False`` tests elsewhere. Each now reads the verdict with
``read_ok``. Under decision D1 an unknown verdict fails closed: the envelope
is ``refused`` (exit 1) and ``blocked_by`` names ``<source>_unknown`` and the
``read_ok`` reason.

Each site has three cases: a payload with no ``ok`` (the promise; it passed
on the parent commit), and a well-formed pass and a well-formed failure,
which must behave exactly as before.
"""
from __future__ import annotations

import copy
import json
from collections.abc import Callable
from pathlib import Path
from typing import Any

import pytest

from okg.bench import orchestrator
from okg.substrate.cli.main import main
from okg.substrate.library.sources.base import NodeFact
from okg.substrate.library.sources.self_play import SelfPlayReportSource
from okg.substrate.pact import cli as pact_cli
from okg.substrate.pact import envelope
from okg.substrate.pact import review as pact_review
from okg.substrate.support import bug_report
from tests.pact.test_operator_brief_cli import _build_demo, _seed_root
from tests.substrate.test_service_handoff_dead_predecessor import (
    _no_process_is_alive,
    _verified_dead_authority,
)

# --------------------------------------------------------------------------
# The nine PACT CLI sites
# --------------------------------------------------------------------------

_MISSING = object()


def _with_ok(payload: dict[str, Any], ok: Any) -> dict[str, Any]:
    """The same payload with its ``ok`` replaced, or removed for _MISSING."""
    out = copy.deepcopy(payload)
    out.pop("ok", None)
    if ok is not _MISSING:
        out["ok"] = ok
    return out


@pytest.fixture
def demo_root(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    monkeypatch.delenv("OKG_DSN", raising=False)
    monkeypatch.delenv("OKG_AGENT", raising=False)
    monkeypatch.setenv("OKG_PACT_QUIET_DEPRECATION", "1")
    root = _seed_root(tmp_path)
    _build_demo(root)
    assert (
        main(["pact", "--root", str(root), "view", "demo", "--format=markdown", "--json"])
        == 0
    )
    return root


def _patch_status(monkeypatch: pytest.MonkeyPatch, ok: Any) -> None:
    real = pact_cli._status

    def fake(*args: Any, **kwargs: Any) -> dict[str, Any]:
        return _with_ok(real(*args, **kwargs), ok)

    monkeypatch.setattr(pact_cli, "_status", fake)


def _patch_queue(monkeypatch: pytest.MonkeyPatch, ok: Any) -> None:
    real = pact_review.build_queue

    def fake(*args: Any, **kwargs: Any) -> dict[str, Any]:
        return _with_ok(real(*args, **kwargs), ok)

    monkeypatch.setattr(pact_review, "build_queue", fake)


def _patch_list(monkeypatch: pytest.MonkeyPatch, ok: Any) -> None:
    real = pact_cli._list_changes

    def fake(*args: Any, **kwargs: Any) -> dict[str, Any]:
        return _with_ok(real(*args, **kwargs), ok)

    monkeypatch.setattr(pact_cli, "_list_changes", fake)


def _patch_pr_base_guard(monkeypatch: pytest.MonkeyPatch, ok: Any) -> None:
    guard = envelope.component(
        ok=True, status="base_ok", base="dev", expected_base="dev"
    )
    if ok is False:
        guard = envelope.component(
            ok=False,
            status="base_not_dev",
            base="main",
            expected_base="dev",
            recommended_commands=["gh pr edit --base dev"],
        )
    guard = _with_ok(guard, ok)
    monkeypatch.setattr(pact_cli, "_pr_base_guard", lambda root: dict(guard))


def _no_primary(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(pact_cli, "_select_workbench_primary", lambda root, rows: None)


# (site id, source name, patch the upstream payload, call the site)
_Site = tuple[
    str,
    str,
    Callable[[pytest.MonkeyPatch, Any], None],
    Callable[[Path, pytest.MonkeyPatch], dict[str, Any]],
]

PACT_SITES: list[_Site] = [
    (
        "brief:status",  # _brief, status.get("ok", True)
        "pact_status",
        _patch_status,
        lambda root, mp: pact_cli._brief(root, "demo"),
    ),
    (
        "chat_review:status",  # _chat_review, status.get("ok", True)
        "pact_status",
        _patch_status,
        lambda root, mp: pact_cli._chat_review(root, "demo"),
    ),
    (
        "chat_review_queue:queue",  # _chat_review_queue, raw.get("ok", True)
        "pact_review_queue",
        _patch_queue,
        lambda root, mp: pact_cli._chat_review_queue(root),
    ),
    (
        # _simple_gate: the three pr_base_guard.get("ok", True) reads (the
        # aggregate, the recommended commands, and the cause tokens).
        "gate_pre_merge:pr_base_guard",
        "pr_base_guard",
        _patch_pr_base_guard,
        lambda root, mp: pact_cli._simple_gate(root, "demo", "pre-merge"),
    ),
    (
        "workbench_primary:listing",  # listing.get("ok", True), with a primary
        "pact_list",
        _patch_list,
        lambda root, mp: pact_cli._workbench(
            root, "demo", related=[], include_graph=False
        ),
    ),
    (
        "workbench_primary:status",  # primary_status.get("ok", True)
        "pact_status",
        _patch_status,
        lambda root, mp: pact_cli._workbench(
            root, "demo", related=[], include_graph=False
        ),
    ),
    (
        "workbench_no_primary:listing",  # listing.get("ok", True), no primary
        "pact_list",
        _patch_list,
        lambda root, mp: (
            _no_primary(mp),
            pact_cli._workbench(root, None, related=[], include_graph=False),
        )[1],
    ),
]

PACT_SITE_IDS = [site[0] for site in PACT_SITES]


@pytest.mark.parametrize(("site", "source", "patch", "call"), PACT_SITES, ids=PACT_SITE_IDS)
def test_pact_site_missing_ok_is_unknown_and_fails_closed(
    demo_root: Path,
    monkeypatch: pytest.MonkeyPatch,
    site: str,
    source: str,
    patch: Callable[[pytest.MonkeyPatch, Any], None],
    call: Callable[[Path, pytest.MonkeyPatch], dict[str, Any]],
) -> None:
    patch(monkeypatch, _MISSING)

    payload = call(demo_root, monkeypatch)

    assert payload["verdict"] == envelope.REFUSED, site
    assert payload["ok"] is False
    assert envelope.EXIT_CODES[payload["verdict"]] == 1
    assert f"{source}_unknown" in payload["blocked_by"]
    assert f"{source}_reported_no_ok" in payload["blocked_by"]


@pytest.mark.parametrize(("site", "source", "patch", "call"), PACT_SITES, ids=PACT_SITE_IDS)
def test_pact_site_non_boolean_ok_is_unknown(
    demo_root: Path,
    monkeypatch: pytest.MonkeyPatch,
    site: str,
    source: str,
    patch: Callable[[pytest.MonkeyPatch, Any], None],
    call: Callable[[Path, pytest.MonkeyPatch], dict[str, Any]],
) -> None:
    patch(monkeypatch, None)

    payload = call(demo_root, monkeypatch)

    assert payload["verdict"] == envelope.REFUSED, site
    assert f"{source}_unknown" in payload["blocked_by"]
    assert f"{source}_ok_not_boolean" in payload["blocked_by"]


@pytest.mark.parametrize(("site", "source", "patch", "call"), PACT_SITES, ids=PACT_SITE_IDS)
def test_pact_site_well_formed_pass_is_unchanged(
    demo_root: Path,
    monkeypatch: pytest.MonkeyPatch,
    site: str,
    source: str,
    patch: Callable[[pytest.MonkeyPatch, Any], None],
    call: Callable[[Path, pytest.MonkeyPatch], dict[str, Any]],
) -> None:
    patch(monkeypatch, True)

    payload = call(demo_root, monkeypatch)

    assert payload["verdict"] == envelope.RECORDED, (site, payload["blocked_by"])
    assert payload["ok"] is True
    assert payload["blocked_by"] == []
    assert "message" not in payload


@pytest.mark.parametrize(("site", "source", "patch", "call"), PACT_SITES, ids=PACT_SITE_IDS)
def test_pact_site_well_formed_failure_is_unchanged(
    demo_root: Path,
    monkeypatch: pytest.MonkeyPatch,
    site: str,
    source: str,
    patch: Callable[[pytest.MonkeyPatch, Any], None],
    call: Callable[[Path, pytest.MonkeyPatch], dict[str, Any]],
) -> None:
    patch(monkeypatch, False)

    payload = call(demo_root, monkeypatch)

    assert payload["verdict"] == envelope.REFUSED, site
    assert not any(token.endswith("_unknown") for token in payload["blocked_by"])
    assert not any("reported_no_ok" in token for token in payload["blocked_by"])


def test_gate_well_formed_pr_base_failure_keeps_its_cause_and_commands(
    demo_root: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _patch_pr_base_guard(monkeypatch, False)

    payload = pact_cli._simple_gate(demo_root, "demo", "pre-merge")

    assert payload["verdict"] == envelope.REFUSED
    assert payload["blocked_by"] == ["base_not_dev"]
    assert payload["recommended_commands"] == ["gh pr edit --base dev"]
    assert payload["text"] == "gate pre-merge demo: refused"


def test_gate_unknown_pr_base_names_the_guard(
    demo_root: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    # A guard with commands but no verdict: the gate must pass its commands
    # on, the same as for a failed guard, so the operator knows what to run.
    guard = _with_ok(
        envelope.component(
            ok=False,
            status="base_unreadable",
            recommended_commands=["gh pr view --json baseRefName"],
        ),
        _MISSING,
    )
    monkeypatch.setattr(pact_cli, "_pr_base_guard", lambda root: dict(guard))

    payload = pact_cli._simple_gate(demo_root, "demo", "pre-merge")

    assert payload["blocked_by"] == [
        "pr_base_guard_unknown",
        "pr_base_guard_reported_no_ok",
    ]
    assert payload["text"] == "gate pre-merge demo: refused"
    assert "gh pr view --json baseRefName" in payload["recommended_commands"]
    assert "gh pr view --json baseRefName" in payload["next_commands"]


def test_brief_cli_exit_code_is_one_when_status_has_no_verdict(
    demo_root: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture
) -> None:
    capsys.readouterr()
    _patch_status(monkeypatch, _MISSING)

    rc = main(["pact", "--root", str(demo_root), "view", "demo", "--format=brief", "--json"])

    assert rc == 1
    lines = [line for line in capsys.readouterr().out.splitlines() if line.strip()]
    payload = json.loads(lines[-1])
    assert payload["verdict"] == "refused"
    assert "pact_status_unknown" in payload["blocked_by"]


def test_brief_cli_exit_code_is_zero_for_a_well_formed_pass(
    demo_root: Path, capsys: pytest.CaptureFixture
) -> None:
    capsys.readouterr()

    rc = main(["pact", "--root", str(demo_root), "view", "demo", "--format=brief", "--json"])

    assert rc == 0


# --------------------------------------------------------------------------
# The four `.get("ok") is not False` sites
# --------------------------------------------------------------------------


def _bench_states(monkeypatch: pytest.MonkeyPatch, count_gate: dict[str, Any]) -> None:
    def section(state: dict[str, Any]) -> Callable[..., tuple[str, dict[str, Any]]]:
        return lambda *args, **kwargs: ("", dict(state))

    monkeypatch.setattr(orchestrator, "_count_gate_report", section(count_gate))
    monkeypatch.setattr(
        orchestrator, "_incremental_truth_report", section({"ok": True, "missing": False})
    )
    monkeypatch.setattr(
        orchestrator,
        "_timing_profile_report",
        section({"ok": None, "missing": False, "available": False, "optional": True}),
    )
    monkeypatch.setattr(
        orchestrator, "_sweep_report", section({"ok": True, "missing": False})
    )
    monkeypatch.setattr(
        orchestrator, "_sensitivity_report", section({"ok": True, "missing": False})
    )


def test_bench_report_section_with_no_verdict_is_unknown_not_pass(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    # A count_gate.json that carries no `ok` used to leave the overall
    # report at PASS.
    _bench_states(monkeypatch, {"missing": False, "path": "count_gate.json"})

    html, summary = orchestrator.build_static_bench_report(tmp_path)

    assert summary["ok"] is False
    assert summary["unknown"] == {"count_gate": "bench_count_gate_reported_no_ok"}
    assert "Report sections with no verdict" in html


@pytest.mark.parametrize(
    ("count_ok", "expected"), [(True, True), (False, False)], ids=["pass", "fail"]
)
def test_bench_report_well_formed_verdicts_are_unchanged(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, count_ok: bool, expected: bool
) -> None:
    _bench_states(monkeypatch, {"ok": count_ok, "missing": False})

    html, summary = orchestrator.build_static_bench_report(tmp_path)

    assert summary["ok"] is expected
    assert summary["unknown"] == {}
    assert "Report sections with no verdict" not in html


def test_process_authority_with_no_verdict_is_not_proof_of_death() -> None:
    # This site already failed closed on the parent; the conversion names
    # the unknown instead of relying on `is not False`.
    authority = _verified_dead_authority()
    authority.pop("ok")

    assert _no_process_is_alive(authority) is False


@pytest.mark.parametrize(
    ("ok", "expected"), [(False, True), (True, False), ("false", False)]
)
def test_process_authority_well_formed_verdicts_are_unchanged(
    ok: Any, expected: bool
) -> None:
    authority = _verified_dead_authority()
    authority["ok"] = ok

    assert _no_process_is_alive(authority) is expected


def _self_play_findings(tmp_path: Path, tool_calls: list[Any]) -> list[NodeFact]:
    repo = tmp_path / "repo"
    report_dir = repo / "deployments" / "okg-workspace" / "self_play" / "reports"
    report_dir.mkdir(parents=True)
    (report_dir / "run.json").write_text(
        json.dumps(
            {
                "report_format": "okg-workspace-self-play/v1",
                "deployment": "okg-workspace",
                "generation_id": 9,
                "created_at": "2026-09-27T00:00:00Z",
                "scenario_results": [
                    {
                        "scenario_id": "tools",
                        "question_class": "code_lookup",
                        "substrate_stage_target": "read_side_surfacing",
                        "question": "q",
                        "answerability": {"score": 1.0},
                        "evidence_node_ids": [],
                        "tool_calls": tool_calls,
                    }
                ],
            }
        ),
        encoding="utf-8",
    )
    source = SelfPlayReportSource(
        repo="demo",
        repo_root=repo,
        report_globs=["deployments/okg-workspace/self_play/reports/*.json"],
    )
    facts = list(source.run("source-run-1", mode="scope_complete").facts)
    return [
        f for f in facts if isinstance(f, NodeFact) and f.subtype == "self_play_finding"
    ]


def test_self_play_tool_call_with_no_verdict_is_an_unknown_finding(
    tmp_path: Path,
) -> None:
    findings = _self_play_findings(tmp_path, [{"tool": "run_sql"}])

    kinds = [f.attrs["kind"] for f in findings]
    assert kinds == ["tool_outcome_unknown:0"]
    assert "self_play_tool_call_reported_no_ok" in findings[0].attrs["evidence_summary"]


@pytest.mark.parametrize(
    ("call", "kinds"),
    [
        ({"tool": "run_sql", "ok": True}, []),
        ({"tool": "run_sql", "ok": False, "error": "boom"}, ["tool_failure:0"]),
        ("not-a-call", []),
    ],
    ids=["pass", "fail", "not-a-dict"],
)
def test_self_play_well_formed_tool_calls_are_unchanged(
    tmp_path: Path, call: Any, kinds: list[str]
) -> None:
    findings = _self_play_findings(tmp_path, [call])

    assert [f.attrs["kind"] for f in findings] == kinds


def test_bug_report_doctor_check_with_no_verdict_is_unknown() -> None:
    summary = bug_report._doctor_issue_summary(
        {
            "returncode": 0,
            "json": {
                "ok": True,
                "extensions": {"vector": "0.7.0"},
                "roles": {"app_rw": True},
                "queue_depth": {"code": "queue_depth_ok", "message": "fine"},
            },
        }
    )

    assert "failed_checks" not in summary
    assert summary["unknown_checks"] == [
        {"check": "queue_depth", "reason": "doctor_queue_depth_reported_no_ok"}
    ]
    text = bug_report._doctor_summary_text(summary)
    assert text == "`passed`; no verdict from `queue_depth`"


def test_bug_report_doctor_well_formed_checks_are_unchanged() -> None:
    summary = bug_report._doctor_issue_summary(
        {
            "returncode": 1,
            "json": {
                "ok": False,
                "extensions": {"vector": "0.7.0"},
                "roles": {"app_rw": True},
                "queue_depth": {"ok": True, "code": "queue_depth_ok"},
                "lens_putback_backlog": {
                    "ok": False,
                    "code": "lens_backlog_runaway",
                    "message": "runaway",
                },
            },
        }
    )

    assert summary["failed_checks"] == [
        {
            "check": "lens_putback_backlog",
            "code": "lens_backlog_runaway",
            "message": "runaway",
        }
    ]
    assert "unknown_checks" not in summary
    assert bug_report._doctor_summary_text(summary) == "`failed` (`lens_backlog_runaway`)"
    assert bug_report._doctor_summary_text(
        bug_report._doctor_issue_summary({"json": {"ok": True, "roles": {}}})
    ) == "`passed`"
