"""`okg run --once` prints a short summary by default (issue #2282).

The inline publish used to print the publisher's whole cycle summary through
the generic ``key: value`` renderer: 147 lines for a cycle that changed
nothing and 201 for one that published two records on a one-folder
deployment. These tests pin the replacement:

* the default text is a few lines carrying the outcome, the graph version,
  the record and relationship deltas, the graph totals, and the per-source
  status and LLM-invariant counts when the cycle reports them;
* every warning in the cycle summary, and every refusal with its code and
  fix hint, still prints by default. The status vocabulary the summary
  classifies is read from the publisher source, so a new status value fails
  a test instead of printing "0 warning(s)";
* ``--verbose`` prints the full cycle report that used to be the default;
* ``--json`` carries the cycle summary unchanged, plus ``run_summary``.
"""
from __future__ import annotations

import ast
import copy
import json
import re
import shlex
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

import okg.substrate.cli.main as cli
from okg.substrate.cli.run_summary import (
    PROBLEM,
    STATUS_CLASSES,
    STATUS_SOURCE_MODULES,
    build_run_summary,
    classify_status,
    collect_run_warnings,
    render_run_summary,
)
from okg.substrate.errors import PublisherLeaseHeld

GEN = "okg:demo:branch:default:gen:20260923T035107860925Z:a8e05cbf058b"
DEFAULT_LINE_BUDGET = 8


def _cycle(**overrides: Any) -> dict[str, Any]:
    """A trimmed copy of a real successful cycle summary (2 records added)."""
    out: dict[str, Any] = {
        "generation_id": GEN,
        "predecessor_generation_id": GEN.replace("a8e05", "00000"),
        "compat_generation_id": 1790135467604458,
        "deployment_name": "demo",
        "status": "published",
        "catalog_version_id": 3,
        "phase_a_converged": True,
        "phase_b_blocked": False,
        "excluded_sources": [],
        "consistency_gates": [{
            "check_id": "transform.consistency.v4",
            "gate_name": "transform_consistency_gate",
            "result": "evaluated",
            "blocks_publish": False,
            "reason": "no_violations",
            "open_observation_count": 0,
            "newly_opened_count": 0,
        }],
        "nodes_added": 2, "nodes_retracted": 0, "nodes_updated": 0,
        "edges_added": 2, "edges_retracted": 0, "edges_updated": 0,
        "node_count": 6, "edge_count": 6,
        "views": [],
        "native_view_refresh": {
            "status": "disabled", "reason": "no_materializers",
            "failed_views": [], "failed_view_count": 0,
        },
        "attribute_effective_view_refresh": {"status": "materialized"},
        "lenses": [],
        "post_publish_consistency_checks": [],
        "per_source_status": {},
        "enrichers": [],
        "enricher_runs": [],
        "derived_rejections": {
            "rejected_producer_count": 0, "discarded_record_count": 0,
            "producers": [],
        },
        "envelope_violations": [],
        "noop": False,
        "blocked_reason": None,
        "timings_ms": {f"stage_{i}": float(i) for i in range(80)},
        "timing_details": {
            "schema": "okg.publisher.timing_details.v1",
            # A copy of a gate payload inside the timing receipt must not
            # produce a second warning.
            "consistency_gates": [{"check_id": "x", "result": "failed"}],
        },
        "first_publish_budget_warning": {
            "status": "not_applicable", "triggered": False,
            "triggered_by": [],
        },
        "total_duration_ms": 725,
        "admission_feedback": {"status": "published", "updated": 1},
    }
    out.update(overrides)
    return out


# ---------------------------------------------------------------------------
# Rendering
# ---------------------------------------------------------------------------


def test_successful_cycle_renders_short_summary_with_every_field() -> None:
    lines = render_run_summary(build_run_summary(_cycle(), deployment="demo"))
    text = "\n".join(lines)
    assert len(lines) <= DEFAULT_LINE_BUDGET
    assert lines[0] == f"Published graph version {GEN}"
    assert "Records: 2 added, 0 updated, 0 retracted." in text
    assert "Relationships: 2 added, 0 updated, 0 retracted." in text
    assert "Graph now holds 6 records and 6 relationships." in text
    # The inline publish runs no source, so there is no per-source status
    # to report and no Sources line.
    assert "Sources:" not in text
    assert "0 warning(s)" in text
    assert "timings_ms" not in text and "stage_79" not in text


def _reused_cycle(**overrides: Any) -> dict[str, Any]:
    """The publisher's reuse path: the current version stays, no predecessor."""
    out = _cycle(
        noop=True, reused_generation=True, nodes_added=0, edges_added=0,
        **overrides,
    )
    out.pop("predecessor_generation_id")
    return out


def test_noop_cycle_says_nothing_changed() -> None:
    out = _reused_cycle()
    lines = render_run_summary(build_run_summary(out, deployment="demo"))
    assert lines[0].startswith("No change: this cycle found nothing new")
    assert GEN in lines[0]


def test_new_version_with_zero_delta_says_published() -> None:
    """#2472: a cycle that minted a new version is a publish, even when its
    own delta counters are zero; the output names the new version."""
    out = _cycle(noop=True, nodes_added=0, edges_added=0)
    summary = build_run_summary(out, deployment="demo")
    assert summary["outcome"] == "published"
    assert summary["exit_code"] == 0
    lines = render_run_summary(summary)
    assert lines[0] == f"Published graph version {GEN}"


def test_first_publish_of_an_empty_deployment_says_published() -> None:
    """A deployment's first cycle with nothing to add mints its first version.

    The publisher reports ``noop`` and a ``None`` predecessor. It said "No
    change" and `okg init` refused the fresh academic-researcher install.
    """
    out = _cycle(
        noop=True, predecessor_generation_id=None, nodes_added=0,
        edges_added=0, node_count=0, edge_count=0,
    )
    summary = build_run_summary(out, deployment="demo")
    assert summary["outcome"] == "published"
    assert render_run_summary(summary)[0] == f"Published graph version {GEN}"


def test_same_version_id_as_predecessor_is_no_change() -> None:
    out = _cycle(noop=True, predecessor_generation_id=GEN)
    assert build_run_summary(out, deployment="demo")["outcome"] == "no_change"


def test_reported_sources_are_listed_with_status() -> None:
    out = _cycle(per_source_status={
        "notes": {"status": "ok"},
        "papers": SimpleNamespace(status="failed"),
    })
    summary = build_run_summary(out, deployment="demo")
    assert summary["sources"] == [
        {"source": "notes", "status": "ok"},
        {"source": "papers", "status": "failed"},
    ]
    text = "\n".join(render_run_summary(summary))
    assert "Sources: notes (ok), papers (failed)" in text


# ---------------------------------------------------------------------------
# Warnings are never dropped
# ---------------------------------------------------------------------------


def _kinds(out: dict[str, Any]) -> list[str]:
    return [w["kind"] for w in collect_run_warnings(out, deployment="demo")]


def test_clean_cycle_has_no_warnings() -> None:
    assert _kinds(_cycle()) == []


@pytest.mark.parametrize(
    ("overrides", "kind"),
    [
        (
            {
                "status": "blocked",
                "phase_b_blocked": True,
                "blocked_reason": {
                    "reason": "cross_source_error_invariant_failed",
                    "failed_invariants": [{"name": "no_orphan_papers"}],
                },
            },
            "publish_blocked",
        ),
        ({"status": "failed"}, "publish_failed"),
        # A blocked status with no recorded reason still warns.
        ({"status": "blocked", "blocked_reason": None}, "publish_blocked"),
        (
            {"consistency_gates": [{
                "check_id": "g0", "blocks_publish": True,
                "result": "evaluated",
            }]},
            "consistency_violation",
        ),
        ({"excluded_sources": ["papers"]}, "sources_excluded"),
        (
            {"consistency_gates": [{
                "check_id": "g1", "blocks_publish": True,
                "open_observation_count": 3, "newly_opened_count": 1,
            }]},
            "consistency_violation",
        ),
        (
            {"post_publish_consistency_checks": [{
                "check_id": "pp", "result": "failed",
            }]},
            "consistency_violation",
        ),
        (
            {"native_view_refresh": {
                "status": "partial", "failed_view_count": 1,
                "failed_views": ["paper_counts"],
            }},
            "view_refresh_failed",
        ),
        (
            {"derived_rejections": {
                "rejected_producer_count": 1, "discarded_record_count": 4,
            }},
            "derived_output_rejected",
        ),
        ({"envelope_violations": [{"producer": "e"}]}, "envelope_violation"),
        (
            {"first_publish_budget_warning": {
                "triggered": True, "triggered_by": ["node_count"],
            }},
            "first_publish_budget",
        ),
        ({"database_readiness": {"pending": 2}}, "migrations_pending"),
        # A stage this module does not know by name still surfaces.
        (
            {"brand_new_stage": {"status": "failed", "reason": "disk full"}},
            "stage_failure",
        ),
        (
            {"enricher_runs": [{"name": "e1", "status": "error"}]},
            "stage_failure",
        ),
    ],
)
def test_every_warning_shows_in_default_output(
    overrides: dict[str, Any], kind: str
) -> None:
    out = _cycle(**overrides)
    assert kind in _kinds(out)
    lines = render_run_summary(build_run_summary(out, deployment="demo"))
    text = "\n".join(lines)
    assert f"warning [{kind}]:" in text
    fix_lines = [line for line in lines if line.startswith("  fix: okg ")]
    assert fix_lines, text
    assert "0 warning(s)" not in text


def test_blocked_publish_names_reason_failure_and_fix() -> None:
    out = _cycle(
        status="blocked",
        blocked_reason={
            "reason": "cross_source_error_invariant_failed",
            "failed_invariants": [{"name": "no_orphan_papers"}],
        },
    )
    lines = render_run_summary(build_run_summary(out, deployment="demo"))
    text = "\n".join(lines)
    assert lines[0].startswith("NOT PUBLISHED (status blocked)")
    assert "cross_source_error_invariant_failed" in text
    assert "no_orphan_papers" in text
    assert f"fix: okg generation describe {GEN} --deployment demo" in text


# ---------------------------------------------------------------------------
# Through `_cmd_run`: default, --verbose, --json, and a refusal
# ---------------------------------------------------------------------------


@pytest.fixture
def inline_run(monkeypatch: pytest.MonkeyPatch):
    import okg.substrate.db.readiness as readiness_mod
    import okg.substrate.profiling as profiling_mod
    import okg.substrate.publisher.factory as publisher_factory_mod

    monkeypatch.setattr(
        cli, "_resolve_run_context",
        lambda args: ("postgres://test", "demo", None, False),
    )
    monkeypatch.setattr(
        readiness_mod, "require_database_ready",
        lambda dsn, **kwargs: readiness_mod.DatabaseReadiness(
            schema_present=True, schema_applied=True, ledger_present=True,
        ),
    )
    monkeypatch.setattr(cli, "_profiling_policy_for_args", lambda args: None)
    monkeypatch.setattr(cli, "_profile_force_deep", lambda args: False)

    class FakePublisher:
        branch_name = "default"

        def set_owner(self, *, owner_id, owner_meta):
            return None

    class FakeProfile:
        generation_id = None

        def __enter__(self):
            return self

        def __exit__(self, *exc):
            return False

    monkeypatch.setattr(
        publisher_factory_mod, "build_deployment_publisher",
        lambda **kwargs: FakePublisher(),
    )
    monkeypatch.setattr(
        profiling_mod, "profiling_context", lambda **kwargs: FakeProfile(),
    )

    def run(
        cycle_result: Any, *, as_json: bool = False, verbose: bool = False,
        include_llm_invariants: bool = False,
    ):
        def fake_cycle(**kwargs):
            if isinstance(cycle_result, Exception):
                raise cycle_result
            return copy.deepcopy(cycle_result)

        monkeypatch.setattr(cli, "_run_inline_publish_cycle", fake_cycle)
        args = SimpleNamespace(
            once=True, apply=True, inline=True, deployment="demo",
            owner_id=None, owner_meta=None, force_enrichers=False,
            reconcile_count_stamp=False,
            include_llm_invariants=include_llm_invariants,
            llm_invariant=None, profile=None, profile_collector=(),
            branch=None, verbose=verbose,
        )
        return cli._cmd_run(args, as_json=as_json)

    return run


def _old_default_text(out: dict[str, Any]) -> str:
    import io

    from okg.substrate.cli._output import emit_result

    stream = io.StringIO()
    emit_result(out, as_json=False, stream=stream)
    return stream.getvalue()


def test_default_output_is_the_short_summary(inline_run, capsys) -> None:
    assert inline_run(_cycle()) == 0
    captured = capsys.readouterr()
    lines = captured.out.splitlines()
    assert len(lines) <= DEFAULT_LINE_BUDGET, captured.out
    assert lines[0] == f"Published graph version {GEN}"
    assert "timings_ms" not in captured.out
    assert captured.err == ""
    # The old default for the same cycle was the full dump.
    assert len(_old_default_text(_cycle()).splitlines()) > 100


def test_verbose_adds_the_full_cycle_report(inline_run, capsys) -> None:
    assert inline_run(_cycle(), verbose=True) == 0
    out = capsys.readouterr().out
    head, _, full = out.partition("\nFull cycle report:\n")
    assert head.splitlines()[0] == f"Published graph version {GEN}"
    assert full == _old_default_text(_cycle())


def test_json_content_is_the_cycle_summary_plus_run_summary(
    inline_run, capsys
) -> None:
    assert inline_run(_cycle(), as_json=True) == 0
    payload = json.loads(capsys.readouterr().out)
    run_summary = payload.pop("run_summary")
    expected = json.loads(json.dumps(_cycle(), sort_keys=True, default=str))
    assert payload == expected
    assert run_summary["schema"] == "okg.run_summary.v1"
    assert run_summary["generation_id"] == GEN
    assert run_summary["records"] == {
        "added": 2, "updated": 0, "retracted": 0, "total": 6,
    }
    assert run_summary["relationships"]["added"] == 2
    assert run_summary["warnings"] == []


def test_warning_shows_by_default_through_the_command(
    inline_run, capsys
) -> None:
    out = _cycle(excluded_sources=["papers"])
    assert inline_run(out) == 0
    text = capsys.readouterr().out
    assert "warning [sources_excluded]:" in text
    assert "papers" in text
    assert "  fix: okg doctor --deployment demo" in text


def test_refusal_shows_code_and_fix_hint_by_default(
    inline_run, capsys
) -> None:
    err = PublisherLeaseHeld(
        "another publisher holds the publish lock",
        fix_hint="wait for the other publish to finish, then rerun",
    )
    assert inline_run(err) == 1
    captured = capsys.readouterr()
    assert "error [publisher_lease_held]:" in captured.err
    assert "fix_hint: wait for the other publish to finish" in captured.err


@pytest.mark.parametrize("verbose", [False, True])
def test_dbos_path_prints_full_report_only_with_verbose(
    monkeypatch: pytest.MonkeyPatch, capsys, verbose: bool
) -> None:
    import okg.substrate.db.readiness as readiness_mod
    import okg.substrate.runtime.admin as runtime_admin

    monkeypatch.setattr(
        cli, "_resolve_run_context",
        lambda args: ("postgres://test", "demo", None, True),
    )
    monkeypatch.setattr(
        readiness_mod, "require_database_ready",
        lambda dsn, **kwargs: readiness_mod.DatabaseReadiness(
            schema_present=True, schema_applied=True, ledger_present=True,
        ),
    )
    monkeypatch.setattr(
        runtime_admin, "client_enqueue_publisher_cycle",
        lambda **kwargs: SimpleNamespace(
            workflow_id="wf-2282", status="SUCCESS", summary=_cycle(),
            error=None, elapsed_seconds=1.0,
        ),
    )
    args = SimpleNamespace(
        once=True, apply=True, inline=False, deployment="demo", dsn=None,
        branch=None, force_enrichers=False, reconcile_count_stamp=False,
        include_llm_invariants=False, llm_invariant=None, verbose=verbose,
    )
    assert cli._cmd_run(args, as_json=False) == 0
    out = capsys.readouterr().out
    assert out.startswith("okg run --once (DBOS path): outcome=published")
    assert ("Full cycle report:" in out) is verbose
    assert ("timings_ms:" in out) is verbose


# ---------------------------------------------------------------------------
# The status vocabulary comes from the publisher source
# ---------------------------------------------------------------------------

_SUBSTRATE = Path(cli.__file__).resolve().parents[1]
_STATUS_KEYS = {"status", "result", "outcome"}
_STATUS_WORDS = ("status", "result", "outcome")


def _literal_strings(node: ast.AST) -> list[str]:
    """String constants a status expression can evaluate to."""
    if isinstance(node, ast.Constant) and isinstance(node.value, str):
        return [node.value]
    if isinstance(node, ast.IfExp):
        return _literal_strings(node.body) + _literal_strings(node.orelse)
    if isinstance(node, ast.JoinedStr) and node.values:
        # f"withheld:unknown_declared_source:{name}" -> "withheld:..."
        first = node.values[0]
        if isinstance(first, ast.Constant) and isinstance(first.value, str):
            return [first.value]
        return []
    if isinstance(node, (ast.Set, ast.Tuple, ast.List)):
        return [s for elt in node.elts for s in _literal_strings(elt)]
    if (
        isinstance(node, ast.Call) and isinstance(node.func, ast.Name)
        and node.func.id in {"frozenset", "set", "tuple"} and node.args
    ):
        return _literal_strings(node.args[0])
    if isinstance(node, ast.Subscript):
        value = node.value
        name = value.id if isinstance(value, ast.Name) else getattr(
            value, "attr", None,
        )
        if name == "Literal":
            return _literal_strings(node.slice)
    return []


def _names_a_status(node: ast.AST) -> bool:
    if isinstance(node, ast.Name):
        return any(word in node.id.lower() for word in _STATUS_WORDS)
    if isinstance(node, ast.Attribute):
        return any(word in node.attr.lower() for word in _STATUS_WORDS)
    if isinstance(node, ast.Subscript):
        return (
            isinstance(node.slice, ast.Constant)
            and node.slice.value in _STATUS_KEYS
        )
    if (
        isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute)
        and node.func.attr == "get" and node.args
    ):
        first = node.args[0]
        return isinstance(first, ast.Constant) and first.value in _STATUS_KEYS
    return False


def _emitted_status_values() -> dict[str, set[str]]:
    """Every status, result or outcome literal in ``STATUS_SOURCE_MODULES``.

    Read from the source with ``ast``: a dict entry keyed status/result/
    outcome, a ``status=``/``result=``/``outcome=`` keyword, an assignment to
    or a comparison with a name that says status/result/outcome, and a
    ``Literal[...]`` type. In ``cli/main.py`` only the ``_CYCLE_*`` status
    sets count; the rest of that file is command plumbing.
    """
    token = re.compile(r"[a-z][a-z0-9_]*(:.*)?\Z")
    found: dict[str, set[str]] = {}
    for pattern in STATUS_SOURCE_MODULES:
        paths = sorted(_SUBSTRATE.glob(pattern))
        assert paths, f"STATUS_SOURCE_MODULES entry {pattern} matches no file"
        for path in paths:
            rel = str(path.relative_to(_SUBSTRATE))
            tree = ast.parse(path.read_text(encoding="utf-8"))

            def add(values: list[str], rel: str = rel) -> None:
                for value in values:
                    if token.match(value):
                        found.setdefault(value, set()).add(rel)

            for node in ast.walk(tree):
                if rel == "cli/main.py":
                    if isinstance(node, ast.Assign) and any(
                        isinstance(t, ast.Name) and t.id.startswith("_CYCLE_")
                        for t in node.targets
                    ):
                        add(_literal_strings(node.value))
                    continue
                if isinstance(node, ast.Dict):
                    for key, value in zip(node.keys, node.values):
                        if (
                            isinstance(key, ast.Constant)
                            and key.value in _STATUS_KEYS
                        ):
                            add(_literal_strings(value))
                elif isinstance(node, ast.Call):
                    for keyword in node.keywords:
                        if keyword.arg in _STATUS_KEYS:
                            add(_literal_strings(keyword.value))
                elif (
                    isinstance(node, (ast.Assign, ast.AnnAssign))
                    and node.value is not None
                ):
                    targets = (
                        node.targets if isinstance(node, ast.Assign)
                        else [node.target]
                    )
                    for target in targets:
                        if isinstance(target, ast.Tuple) and isinstance(
                            node.value, ast.Tuple
                        ):
                            for name, value in zip(
                                target.elts, node.value.elts
                            ):
                                if _names_a_status(name):
                                    add(_literal_strings(value))
                        elif _names_a_status(target):
                            add(_literal_strings(node.value))
                elif isinstance(node, ast.Compare) and _names_a_status(
                    node.left
                ):
                    for comparator in node.comparators:
                        add(_literal_strings(comparator))
    return found


def test_every_status_the_publisher_emits_is_classified() -> None:
    emitted = _emitted_status_values()
    # The extractor must actually see the publisher's vocabulary; an
    # extractor that finds nothing would pass the check below vacuously.
    for known in (
        "blocked_error", "blocked_timeout", "blocked_incomplete",
        "unavailable", "partial", "withheld", "degraded",
        "materialized_with_degradation", "failed", "published",
    ):
        assert known in {v.split(":", 1)[0] for v in emitted}, known
    missing = {
        value: sorted(where) for value, where in emitted.items()
        if classify_status(value) == "unknown"
    }
    assert not missing, (
        "status values the publisher can emit that run_summary.STATUS_CLASSES "
        f"does not classify as ok or problem: {missing}"
    )


def test_status_table_classifies_the_known_failure_states_as_problems() -> None:
    for value in (
        "blocked_error", "blocked_timeout", "blocked_incomplete",
        "unavailable", "partial", "withheld", "degraded",
        "materialized_with_degradation", "materialized_with_failures",
        "failed", "rejected",
    ):
        assert STATUS_CLASSES[value] == PROBLEM, value
    assert classify_status("withheld:unknown_declared_source:notes") == PROBLEM
    assert classify_status("failed:incomplete_inputs") == PROBLEM
    assert classify_status("a_status_nobody_declared") == "unknown"


# ---------------------------------------------------------------------------
# Report-shaped failure states from the publisher (review of PR 2345)
# ---------------------------------------------------------------------------

_INTEGRITY_RECEIPT = {
    "advanced": False,
    "reason": "post_checkpoint_finalization_failed",
    "error_type": "OperationalError",
}


@pytest.mark.parametrize(
    ("overrides", "needle"),
    [
        pytest.param(
            # sql_gate.py: a crashed post-publish check does not block and
            # has zero counts; only its result says it failed.
            {"post_publish_consistency_checks": [{
                "check_id": "sql.no_orphans", "result": "blocked_error",
                "blocks_publish": False, "open_observation_count": 0,
                "newly_opened_count": 0,
                "reason": "post_publish_check_exception",
            }]},
            "blocked_error", id="post-publish-check-crashed",
        ),
        pytest.param(
            {"consistency_gates": [{
                "check_id": "g.timeout", "result": "blocked_timeout",
                "blocks_publish": False,
            }]},
            "blocked_timeout", id="gate-timeout",
        ),
        pytest.param(
            {"consistency_gates": [{
                "check_id": "g.incomplete", "result": "blocked_incomplete",
                "blocks_publish": False,
            }]},
            "blocked_incomplete", id="gate-incomplete",
        ),
        pytest.param(
            {"maintained_derivations": [{
                "name": "paper_rollup", "status": "unavailable",
                "fallback_reason": "maintain_failed",
                "error": "division by zero",
            }]},
            "maintain_failed", id="derivation-unavailable",
        ),
        pytest.param(
            {"enricher_runs": [{"name": "summarize", "status": "partial"}]},
            "partial", id="enricher-partial",
        ),
        pytest.param(
            {"enricher_runs": [{
                "name": "summarize",
                "status": "withheld:unknown_declared_source:papers",
            }]},
            "withheld", id="enricher-withheld",
        ),
        pytest.param(
            {"views": [{"view_id": "paper_counts", "status": "degraded"}]},
            "degraded", id="view-degraded",
        ),
        pytest.param(
            {"native_view_refresh": {
                "status": "materialized_with_degradation",
                "failed_view_count": 0, "failed_views": [],
                "degraded_view_count": 1, "degraded_views": ["paper_counts"],
            }},
            "paper_counts", id="view-refresh-degraded",
        ),
        pytest.param(
            {"attribute_effective_view_refresh": {
                "status": "materialized",
                "integrity_receipt": _INTEGRITY_RECEIPT,
            }},
            "post_checkpoint_finalization_failed",
            id="integrity-receipt-no-status",
        ),
        pytest.param(
            {"brand_new_stage": {"status": "reticulating"}},
            "reticulating", id="unknown-status-fails-closed",
        ),
        pytest.param(
            {"brand_new_stage": {"rows": 4, "error_count": 2}},
            "error_count 2", id="error-count-without-status",
        ),
        pytest.param(
            {"brand_new_stage": {"violation_count": 1}},
            "violation_count 1", id="violation-count-without-status",
        ),
        pytest.param(
            {"llm_judge": {"results": [{
                "name": "claims_cite_sources", "fires_count": 0,
                "call_failures_count": 2, "parse_errors_count": 1,
                "span_invalid_count": 0,
            }]}},
            "2 call failure(s)", id="llm-judge-call-failures",
        ),
        pytest.param(
            # Published, but the cycle still recorded a block reason.
            {"blocked_reason": {"reason": "late_gate_withheld_source"}},
            "late_gate_withheld_source", id="published-with-block-reason",
        ),
    ],
)
def test_publisher_failure_states_print_a_warning(
    overrides: dict[str, Any], needle: str
) -> None:
    out = _cycle(**overrides)
    lines = render_run_summary(build_run_summary(out, deployment="demo"))
    text = "\n".join(lines)
    assert "0 warning(s)" not in text, text
    warning_lines = [line for line in lines if line.startswith("warning [")]
    assert any(needle in line for line in warning_lines), text
    # A fix line follows every warning line and inspects this cycle's
    # graph version instead of publishing again.
    for index, line in enumerate(lines):
        if line.startswith("warning ["):
            assert lines[index + 1] == (
                f"  fix: okg generation describe {GEN} --deployment demo"
            ), text


def test_published_with_block_reason_is_a_publish_blocked_warning() -> None:
    out = _cycle(blocked_reason={"reason": "late_gate_withheld_source"})
    warnings = collect_run_warnings(out, deployment="demo")
    assert [w["kind"] for w in warnings] == ["publish_blocked"]
    assert "late_gate_withheld_source" in warnings[0]["message"]


def test_crashed_post_publish_check_is_reported_once() -> None:
    out = _cycle(post_publish_consistency_checks=[{
        "check_id": "sql.no_orphans", "result": "blocked_error",
        "blocks_publish": False, "reason": "post_publish_check_exception",
    }])
    assert _kinds(out) == ["consistency_violation"]


def test_walk_reports_post_publish_checks_the_gate_check_does_not_read() -> None:
    # A problem nested inside a post-publish check entry is found by the
    # generic walk; the key is not skipped.
    out = _cycle(post_publish_consistency_checks=[{
        "check_id": "sql.no_orphans", "result": "evaluated",
        "details": {"status": "failed", "reason": "worker crashed"},
    }])
    warnings = collect_run_warnings(out, deployment="demo")
    assert [w["kind"] for w in warnings] == ["stage_failure"]
    assert warnings[0]["message"].startswith(
        "post_publish_consistency_checks[0].details:"
    )


def test_blocked_publish_does_not_repeat_the_admission_feedback_status() -> None:
    out = _cycle(
        status="blocked",
        blocked_reason={"reason": "gate_failed"},
        admission_feedback={"status": "blocked", "updated": 1},
    )
    assert _kinds(out) == ["publish_blocked"]
    # An error in the same block is still reported.
    out["admission_feedback"]["feedback_error"] = "could not write feedback"
    kinds = _kinds(out)
    assert kinds == ["publish_blocked", "stage_failure"]


def test_llm_invariant_counts_print_by_default(
    inline_run, capsys, monkeypatch
) -> None:
    import okg.substrate.catalog.invariants_llm as invariants_llm

    monkeypatch.setattr(
        invariants_llm, "run_llm_judge_invariants",
        lambda *args, **kwargs: {
            "results": [
                {"name": "claims_cite_sources", "fires_count": 3,
                 "call_failures_count": 1, "parse_errors_count": 2,
                 "span_invalid_count": 0},
                {"name": "titles_are_titles", "fires_count": 0,
                 "call_failures_count": 0, "parse_errors_count": 0,
                 "span_invalid_count": 0},
            ],
            "llm_call_stats": {"calls": 9},
        },
    )
    assert inline_run(_cycle(), include_llm_invariants=True) == 0
    text = capsys.readouterr().out
    assert (
        "LLM invariants: 2 run; 3 fire(s), 1 call failure(s), "
        "2 parse error(s)." in text
    ), text
    assert (
        "warning [llm_invariant]: LLM invariant claims_cite_sources: "
        "3 fire(s), 1 call failure(s), 2 parse error(s)" in text
    ), text
    assert "titles_are_titles" not in text
    assert "1 warning(s)" in text


# ---------------------------------------------------------------------------
# Fix lines inspect; they parse with the real command line
# ---------------------------------------------------------------------------


def _every_warning_cycle() -> dict[str, Any]:
    return _cycle(
        status="blocked",
        blocked_reason={"reason": "gate_failed"},
        excluded_sources=["papers"],
        consistency_gates=[{"check_id": "g", "blocks_publish": True}],
        native_view_refresh={"failed_view_count": 1, "failed_views": ["v"]},
        envelope_violations=[{"producer": "e"}],
        brand_new_stage={"status": "failed"},
    )


@pytest.mark.parametrize("deployment", ["demo", "default"])
def test_fix_lines_parse_and_never_publish_again(deployment: str) -> None:
    parser = cli._build_parser()
    warnings = collect_run_warnings(
        _every_warning_cycle(), deployment=deployment,
    )
    assert len(warnings) >= 6
    for warning in warnings:
        hint = warning["fix_hint"]
        assert hint.startswith("okg "), hint
        assert "run --once" not in hint and "--apply" not in hint, hint
        argv = shlex.split(
            hint.replace("<the deployment DSN>", "postgres://x")
        )[1:]
        args = parser.parse_args(argv)
        if deployment == "default":
            # The ad-hoc --dsn run has no deployment name to point at.
            assert args.dsn == "postgres://x", hint
        else:
            assert args.deployment == "demo", hint


def test_fix_line_without_a_graph_version_is_doctor() -> None:
    out = _cycle(status="failed", generation_id=None)
    warnings = collect_run_warnings(out, deployment="demo")
    assert warnings[0]["fix_hint"] == "okg doctor --deployment demo"


def test_pending_migration_fix_is_the_blocks_own_command() -> None:
    out = _cycle(database_readiness={
        "pending_migration_count": 2,
        "migrate_command": "okg migrate --dsn <the deployment DSN> --apply",
    })
    warnings = collect_run_warnings(out, deployment="default")
    assert [w["kind"] for w in warnings] == ["migrations_pending"]
    assert warnings[0]["fix_hint"] == (
        "okg migrate --dsn <the deployment DSN> --apply"
    )


@pytest.mark.parametrize(("migrations", "objects", "text"), [
    (0, 4, "the database is missing search indexes `okg migrate --apply` builds"),
    (1, 4, "the database has pending migrations and is missing search indexes"),
    (2, 0, "the database has pending migrations"),
])
def test_missing_tool_indexes_are_not_called_migrations(migrations, objects, text) -> None:
    """Issue #3311: a block raised only by missing indexes must not say
    "pending migrations" while its own count of migrations is 0."""
    out = _cycle(database_readiness={
        "pending_migration_count": migrations,
        "pending_schema_object_count": objects,
        "migrate_command": "okg migrate --deployment demo --apply",
    })
    [warning] = collect_run_warnings(out, deployment="demo")
    assert warning["kind"] == "migrations_pending"
    assert text in warning["message"]


# ---------------------------------------------------------------------------
# Lock-hold headroom lives only in the timing receipt (re-review of PR 2345)
# ---------------------------------------------------------------------------


def _with_headroom(headroom: dict[str, Any]) -> dict[str, Any]:
    out = _cycle()
    out["timing_details"] = {
        **out["timing_details"],
        "lock": {"hold_ms": 870000.0, "headroom": headroom},
    }
    return out


@pytest.mark.parametrize("status", ["blocked", "unbounded", "unmeasured"])
def test_lock_headroom_failure_prints_its_own_fix(status: str) -> None:
    # Shaped from publish_lock_headroom.evaluate_lock_headroom.
    out = _with_headroom({
        "schema": "okg.publisher.lock_headroom.v1", "ok": False,
        "status": status,
        "reason": "publish held the lock 870.0s against a 900s hard "
                  "statement timeout",
        "fix_hint": "reduce publish lock hold until it fits the headroom",
    })
    lines = render_run_summary(build_run_summary(out, deployment="demo"))
    text = "\n".join(lines)
    assert f"warning [lock_headroom]: publish lock-hold headroom is {status}" in text
    assert "  fix: reduce publish lock hold until it fits the headroom" in text
    assert "1 warning(s)" in text, text


def test_healthy_lock_headroom_is_quiet() -> None:
    out = _with_headroom({"ok": True, "status": "pass", "reason": "within"})
    assert _kinds(out) == []
    out = _with_headroom({"ok": True, "status": "not_applicable"})
    assert _kinds(out) == []


# ---------------------------------------------------------------------------
# One producer verdict, one warning (re-review of PR 2345)
# ---------------------------------------------------------------------------


def test_rejected_producer_warns_once() -> None:
    # A producer that failed mid-emit shows up in enricher_runs, in the
    # derived-verdict events under enrichers (with its producer_policy),
    # and in the derived_rejections census.
    out = _cycle(
        enricher_runs=[{
            "name": "summarize", "producer_id": "enricher.summarize",
            "status": "failed", "reason": "enricher_failed",
            "exception_class": "RuntimeError",
        }],
        enrichers=[{
            "producer_id": "enricher.summarize", "status": "failed",
            "reason": "enricher_failed", "record_count": 4,
            "producer_policy": {"status": "failed"},
        }],
        derived_rejections={
            "rejected_producer_count": 1, "discarded_record_count": 4,
            "producers": [{
                "producer_id": "enricher.summarize", "status": "failed",
                "code": "enricher_failed", "discarded_record_count": 4,
            }],
        },
    )
    warnings = collect_run_warnings(out, deployment="demo")
    assert [w["kind"] for w in warnings] == ["derived_output_rejected"]
    message = warnings[0]["message"]
    assert "enricher.summarize failed enricher_failed" in message
    assert "exception_class RuntimeError" in message


def test_withheld_enricher_warns_once() -> None:
    out = _cycle(
        enricher_runs=[{
            "name": "summarize", "producer_id": "enricher.summarize",
            "status": "withheld", "reason": "unknown_declared_source",
        }],
        enrichers=[{
            "producer_id": "enricher.summarize", "status": "withheld",
            "reason": "unknown_declared_source",
            "producer_policy": {"status": "withheld"},
        }],
    )
    warnings = collect_run_warnings(out, deployment="demo")
    assert [w["kind"] for w in warnings] == ["stage_failure"]
    assert warnings[0]["message"].startswith("enricher_runs[0]:")


def test_verdict_event_alone_warns_once() -> None:
    # A rejected verdict event with no matching run still warns, once, not
    # a second time for its producer_policy block.
    out = _cycle(enrichers=[{
        "producer_id": "enricher.tag", "status": "rejected",
        "reason": "trust_lattice_violation",
        "producer_policy": {"status": "rejected"},
    }])
    warnings = collect_run_warnings(out, deployment="demo")
    assert [w["kind"] for w in warnings] == ["stage_failure"]
    assert warnings[0]["message"].startswith("enrichers[0]:")


# ---------------------------------------------------------------------------
# The exit code follows the publish verdict (issue #2346)
#
# A blocked in-process publish used to exit 0 while its first line read
# "NOT PUBLISHED", so a script or agent checking the exit code saw success.
# Contract: 0 published or no change (warnings included); 3 blocked; 1 a
# failed cycle or a status nothing classifies. The DBOS path gives the same
# code for the same cycle summary.
# ---------------------------------------------------------------------------


def _blocked_cycle(**overrides: Any) -> dict[str, Any]:
    out = _cycle(
        status="blocked",
        phase_b_blocked=True,
        blocked_reason={
            "reason": "consistency_gate_blocked",
            "failed_consistency_gates": [{"check_id": "no_orphan_papers"}],
        },
        admission_feedback={"status": "blocked", "updated": 1},
    )
    out.update(overrides)
    return out


_BLOCKED_CASES = [
    pytest.param(_blocked_cycle(), id="status-blocked"),
    pytest.param(
        _blocked_cycle(status="gate_blocked"), id="status-gate-blocked",
    ),
    pytest.param(
        _blocked_cycle(status="published", phase_b_blocked=False),
        id="block-reason-only",
    ),
    pytest.param(
        _blocked_cycle(status="published", blocked_reason=None),
        id="phase-b-blocked-only",
    ),
]


@pytest.mark.parametrize("out", _BLOCKED_CASES)
def test_blocked_publish_exits_3_in_text(inline_run, capsys, out) -> None:
    assert inline_run(out) == 3
    lines = capsys.readouterr().out.splitlines()
    assert lines[0].startswith("NOT PUBLISHED"), lines
    assert any("warning [publish_blocked]:" in line for line in lines), lines


@pytest.mark.parametrize("out", _BLOCKED_CASES)
def test_blocked_publish_exits_3_in_json_with_same_verdict(
    inline_run, capsys, out
) -> None:
    assert inline_run(out, as_json=True) == 3
    summary = json.loads(capsys.readouterr().out)["run_summary"]
    assert summary["outcome"] == "blocked"
    assert summary["exit_code"] == 3
    assert summary["published"] is False


@pytest.mark.parametrize("as_json", [False, True])
@pytest.mark.parametrize(
    "status", ["fail", "failed", "error", "errored"],
)
def test_failed_cycle_exits_1(inline_run, capsys, status, as_json) -> None:
    assert inline_run(_cycle(status=status), as_json=as_json) == 1
    out = capsys.readouterr().out
    if as_json:
        summary = json.loads(out)["run_summary"]
        assert (summary["outcome"], summary["exit_code"]) == ("failed", 1)
    else:
        assert out.startswith(f"NOT PUBLISHED (status {status})")


@pytest.mark.parametrize("status", [None, "", "mystery", "reticulating"])
def test_unclassified_status_fails_closed(inline_run, capsys, status) -> None:
    assert inline_run(_cycle(status=status), as_json=True) == 1
    summary = json.loads(capsys.readouterr().out)["run_summary"]
    assert (summary["outcome"], summary["published"]) == ("unknown", False)


@pytest.mark.parametrize("as_json", [False, True])
def test_no_change_cycle_exits_0(inline_run, capsys, as_json) -> None:
    out = _reused_cycle()
    assert inline_run(out, as_json=as_json) == 0
    text = capsys.readouterr().out
    if as_json:
        summary = json.loads(text)["run_summary"]
        assert (summary["outcome"], summary["exit_code"]) == ("no_change", 0)
        assert summary["published"] is True
    else:
        assert text.startswith("No change:")


_WARNING_CASES = [
    pytest.param(
        {"excluded_sources": ["papers"]}, "sources_excluded",
        id="excluded-source",
    ),
    pytest.param(
        {"derived_rejections": {
            "rejected_producer_count": 1, "discarded_record_count": 3,
            "producers": [{
                "producer_id": "enricher.tag", "status": "rejected",
            }],
        }},
        "derived_output_rejected", id="rejected-derived-output",
    ),
    pytest.param(
        {"native_view_refresh": {
            "status": "materialized_with_degradation",
            "degraded_view_count": 1, "degraded_views": [{"view_id": "v"}],
            "failed_view_count": 0, "failed_views": [],
        }},
        "view_refresh_degraded", id="degraded-view",
    ),
]


@pytest.mark.parametrize("as_json", [False, True])
@pytest.mark.parametrize(("overrides", "kind"), _WARNING_CASES)
def test_published_cycle_with_warnings_still_exits_0(
    inline_run, capsys, overrides, kind, as_json
) -> None:
    assert inline_run(_cycle(**overrides), as_json=as_json) == 0
    text = capsys.readouterr().out
    if as_json:
        summary = json.loads(text)["run_summary"]
        assert (summary["outcome"], summary["exit_code"]) == ("published", 0)
        assert kind in [w["kind"] for w in summary["warnings"]]
    else:
        assert text.startswith(f"Published graph version {GEN}")
        assert f"warning [{kind}]:" in text


def test_cycle_without_a_summary_fails_closed(inline_run, capsys) -> None:
    assert inline_run(None) == 1
    assert "no cycle summary" in capsys.readouterr().err


def test_skipped_cycle_exits_0_and_says_skipped(inline_run, capsys) -> None:
    # The runtime workflow's own skip summary: the cycle never ran. It is
    # not a publish and not a no-change cycle, and it is not a failure.
    out = {
        "deployment": "demo", "status": "skipped", "publish_skipped": True,
        "skipped": "source_sync_active", "generation_id": None,
    }
    assert inline_run(out, as_json=True) == 0
    summary = json.loads(capsys.readouterr().out)["run_summary"]
    assert (summary["outcome"], summary["exit_code"]) == ("skipped", 0)
    assert summary["published"] is False
    assert summary["skip_reason"] == "source_sync_active"
    assert [w["kind"] for w in summary["warnings"]] == ["publish_skipped"]
    assert inline_run(out) == 0
    assert capsys.readouterr().out.startswith(
        "Skipped: the runtime did not run this cycle "
        "(reason: source_sync_active)"
    )


@pytest.mark.parametrize("failed", ["fail", "failed", "error", "errored"])
@pytest.mark.parametrize(
    "block",
    [
        pytest.param({"blocked_reason": {"reason": "gate"}}, id="reason"),
        pytest.param({"phase_b_blocked": True}, id="phase-b"),
    ],
)
def test_blocked_wins_over_a_failure_status(failed, block) -> None:
    # Readers still see the previous version either way; the gate refusal is
    # the fact that decides that, so the verdict is blocked (exit 3).
    from okg.substrate.cli.run_summary import publish_outcome

    out = _cycle(status=failed, **block)
    assert publish_outcome(out) == "blocked"
    warnings = collect_run_warnings(out, deployment="demo")
    assert warnings[0]["kind"] == "publish_blocked"


_BOTH_PATH_CASES = [
    pytest.param(_cycle(), "published", id="published"),
    pytest.param(_reused_cycle(), "no_change", id="no-change"),
    pytest.param(
        _cycle(noop=True), "published", id="new-version-with-zero-delta",
    ),
    pytest.param(
        _cycle(excluded_sources=["papers"]), "published", id="warning",
    ),
    *[
        pytest.param(case.values[0], "blocked", id=case.id)
        for case in _BLOCKED_CASES
    ],
    pytest.param(_cycle(status="failed"), "failed", id="failed"),
    pytest.param(
        _cycle(status="failed", phase_b_blocked=True), "blocked",
        id="blocked-and-failed",
    ),
    pytest.param(
        {"status": "skipped", "skipped": "source_sync_active",
         "generation_id": None},
        "skipped", id="workflow-skip",
    ),
    pytest.param(
        {"status": "mystery", "generation_id": "g1"}, "unknown",
        id="unknown-status-with-version",
    ),
    pytest.param({"status": "mystery"}, "unknown", id="unknown-status"),
    pytest.param({"generation_id": "g1"}, "unknown", id="no-status"),
    pytest.param({}, "unknown", id="empty-summary"),
]


@pytest.mark.parametrize(("out", "outcome"), _BOTH_PATH_CASES)
def test_inline_and_dbos_paths_give_one_verdict(
    inline_run, capsys, monkeypatch, out, outcome
) -> None:
    from okg.substrate.cli.run_summary import EXIT_CODES

    inline_rc = inline_run(copy.deepcopy(out), as_json=True)
    inline = json.loads(capsys.readouterr().out)["run_summary"]

    import okg.substrate.runtime.admin as runtime_admin

    monkeypatch.setattr(
        cli, "_resolve_run_context",
        lambda args: ("postgres://test", "demo", None, True),
    )
    monkeypatch.setattr(
        runtime_admin, "client_enqueue_publisher_cycle",
        lambda **kwargs: SimpleNamespace(
            workflow_id="wf-2346", status="SUCCESS",
            summary=copy.deepcopy(out), error=None, elapsed_seconds=1.0,
        ),
    )
    args = SimpleNamespace(
        once=True, apply=True, inline=False, deployment="demo", dsn=None,
        branch=None, force_enrichers=False, reconcile_count_stamp=False,
        include_llm_invariants=False, llm_invariant=None, verbose=False,
    )
    dbos_rc = cli._cmd_run(args, as_json=True)
    dbos = json.loads(capsys.readouterr().out)

    assert inline["outcome"] == dbos["outcome"] == outcome
    assert inline_rc == dbos_rc == EXIT_CODES[outcome]
    assert inline["exit_code"] == dbos["exit_code"] == inline_rc
    assert inline["published"] is dbos["published"]


# ---------------------------------------------------------------------------
# post_publish_index_rebuild (issue #3929)
# ---------------------------------------------------------------------------

# The shape db/index_reclaim.py returns, trimmed to the keys that matter here.
_REBUILD_BASE: dict[str, Any] = {
    "deployment": "demo",
    "rebuilt": [],
    "analyzed": [],
    "reclaimed_bytes": 0,
    "max_publisher_lock_hold_seconds": 0.0,
    "leftover_invalid_indexes": [],
}


def _stage_failure_texts(rebuild: dict[str, Any]) -> list[str]:
    warnings = collect_run_warnings(
        _cycle(post_publish_index_rebuild=rebuild), deployment="demo",
    )
    return [w["message"] for w in warnings if w["kind"] == "stage_failure"]


@pytest.mark.parametrize("status", ["nothing_to_rebuild", "rebuilt"])
def test_successful_index_rebuild_prints_no_stage_failure(status: str) -> None:
    assert _stage_failure_texts({**_REBUILD_BASE, "status": status}) == []


@pytest.mark.parametrize("status", ["nothing_to_rebuild", "rebuilt"])
def test_successful_index_rebuild_warns_about_a_leftover_invalid_index(
    status: str,
) -> None:
    leftover = {
        "relation": "okg.graph_nodes",
        "index": "okg.graph_nodes_idx_ccnew",
        "valid": False,
        "drop_command": "DROP INDEX CONCURRENTLY okg.graph_nodes_idx_ccnew;",
    }
    rebuild = {
        **_REBUILD_BASE, "status": status, "leftover_invalid_indexes": [leftover],
    }
    warnings = collect_run_warnings(
        _cycle(post_publish_index_rebuild=rebuild), deployment="demo",
    )

    left = [w for w in warnings if w["kind"] == "invalid_index_left"]
    assert len(left) == 1
    assert "okg.graph_nodes_idx_ccnew" in left[0]["message"]
    assert "DROP INDEX CONCURRENTLY okg.graph_nodes_idx_ccnew;" in left[0]["fix_hint"]
    # It is not a stage failure: the publish and the rebuild succeeded.
    assert not [w for w in warnings if w["kind"] == "stage_failure"]


def test_index_rebuild_error_still_prints_a_stage_failure() -> None:
    texts = _stage_failure_texts({
        "status": "error", "error": "lock timeout on okg.graph_nodes",
    })
    assert len(texts) == 1
    assert "post_publish_index_rebuild" in texts[0]


def test_index_rebuild_stopped_still_prints_a_stage_failure() -> None:
    # ``stopped`` is not success: an unclassified status fails closed.
    texts = _stage_failure_texts({
        **_REBUILD_BASE, "status": "stopped", "reason": "reindex_failed",
    })
    assert len(texts) == 1
