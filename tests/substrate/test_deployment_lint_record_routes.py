"""Per-deployment record-route lint (#947 follow-up).

#947's guard composes EVERY module, so it cannot see the configuration the bug
was reported against: `study references project` exists only when `bookkeeping`
is active, so a deployment activating a subset still advertises `project_ref` on
`study` with nothing to route to. These tests pin the per-deployment answer.
"""

from __future__ import annotations

from pathlib import Path

import yaml

from okg.substrate.catalog.modules_compose import load_modules_root
from okg.substrate.deployment_lint import (
    _REFERENCE_FIELD_OWNER_HINT,
    _check_record_reference_routes,
    lint_deployment,
)
from okg.substrate.mcp.record import unroutable_record_reference_fields

#: The modules `agent_memory` needs to compose, WITHOUT `bookkeeping` — the
#: subset that reproduces #947's reported shape.
_AGENT_MEMORY_MODULES = (
    "git_graph",
    "openspec",
    "agent_sessions",
    "compute_env",
    "ci",
    "code_graph",
    "quality_evidence",
    "agent_memory",
)


def _deployment(
    tmp_path: Path,
    name: str,
    modules: list[str],
    *,
    record_prefixes: list[dict] | None = None,
) -> Path:
    dep = tmp_path / name
    dep.mkdir()
    manifest = {"name": name, "modules": modules}
    if record_prefixes is not None:
        manifest["record_prefixes"] = record_prefixes
    (dep / "deployment.yaml").write_text(
        yaml.safe_dump(manifest),
        encoding="utf-8",
    )
    (dep / "schemas").mkdir()
    return dep


def _run(deployment_dir: Path) -> list:
    raw_manifest = yaml.safe_load(
        (deployment_dir / "deployment.yaml").read_text(encoding="utf-8"),
    ) or {}
    findings: list = []
    _check_record_reference_routes(
        deployment_dir=deployment_dir,
        manifest_path=deployment_dir / "deployment.yaml",
        findings=findings,
        record_prefixes=tuple(raw_manifest.get("record_prefixes") or ()),
    )
    return [f.as_dict() if hasattr(f, "as_dict") else f for f in findings]


def _all_modules() -> list[str]:
    root = load_modules_root()
    return sorted(
        path.name for path in root.iterdir() if (path / "schema.yaml").is_file()
    )


def test_lint_deployment_surfaces_the_finding(tmp_path: Path) -> None:
    """The check is WIRED into lint_deployment, not merely callable.

    Every other test here calls `_check_record_reference_routes` directly, so
    they all pass with the call site deleted — the requirement says
    "`lint_deployment` SHALL evaluate", and nothing proved the integration.
    Adversarial review demonstrated exactly that by removing the call and
    watching 34 tests still pass.
    """
    dep = _deployment(
        tmp_path, "wired-subset", list(_AGENT_MEMORY_MODULES),
    )
    report = lint_deployment("wired-subset", deployments_root=tmp_path)

    codes = [f["code"] for f in report["findings"]]
    assert "deployment.record.unroutable_reference_field" in codes, codes

    routes = [
        f for f in report["findings"]
        if f["code"] == "deployment.record.unroutable_reference_field"
    ]
    assert any(
        f["details"]["record_kind"] == "study"
        and f["details"]["field"] == "project_ref"
        for f in routes
    ), routes
    # Severity is load-bearing: `ok` counts blockers only, so a warning must not
    # silently become a blocker (which would red every lint) nor drop to info.
    assert {f["severity"] for f in routes} == {"warning"}, routes
    # This check must contribute NO blockers: `ok` is `not any(blocker)`, so a
    # blocker here would red the lint for every deployment missing a module.
    # (The bare fixture trips unrelated manifest blockers, so assert on the
    # codes this check owns rather than on report["ok"].)
    assert not [
        f for f in report["findings"]
        if f["severity"] == "blocker" and f["code"].startswith("deployment.record.")
    ], report["findings"]
    assert dep.exists()


def test_deployment_with_no_record_templates_is_not_reported_clean(
    tmp_path: Path,
) -> None:
    """"Evaluated nothing" must be a distinct state, never silence.

    A deployment without the record subtypes makes every template unavailable,
    so the evaluator examines ZERO fields and returns an empty dict — identical
    to a healthy deployment. Without a distinct state, a deployment that later
    dropped `agent_memory` would see its findings fall to zero and read as
    FIXED. That is the "absence dressed as agreement" shape this whole change
    exists to prevent, so it must not live inside the change itself.
    """
    dep = _deployment(tmp_path, "no-record-modules", ["git_graph"])
    findings = _run(dep)

    not_applicable = [
        f for f in findings
        if f["code"] == "deployment.record.reference_routes_not_applicable"
    ]
    assert len(not_applicable) == 1, findings
    assert not_applicable[0]["severity"] == "skipped"
    assert not_applicable[0]["details"]["available_templates"] == 0
    assert not_applicable[0]["details"]["checked_fields"] == 0
    assert not_applicable[0]["details"]["total_templates"] > 0
    # And it must NOT also emit route warnings, which would be incoherent.
    assert not [
        f for f in findings
        if f["code"] == "deployment.record.unroutable_reference_field"
    ]

    # Negative control: a deployment that DOES activate the templates reports
    # real findings and no not-applicable state.
    active = _deployment(tmp_path, "has-records", list(_AGENT_MEMORY_MODULES))
    active_findings = _run(active)
    assert not [
        f for f in active_findings
        if f["code"] == "deployment.record.reference_routes_not_applicable"
    ]
    routes = [
        f for f in active_findings
        if f["code"] == "deployment.record.unroutable_reference_field"
    ]
    assert routes
    # Every finding carries the evaluated counts, so a verdict can never be
    # read without knowing how much surface produced it.
    assert routes[0]["details"]["checked_fields"] > 0
    assert routes[0]["details"]["available_templates"] > 0


def test_empty_modules_is_not_applicable(tmp_path: Path) -> None:
    """An explicit empty module list reports zero evaluated templates."""
    dep = _deployment(tmp_path, "empty-modules", [])
    findings = _run(dep)

    assert [f["code"] for f in findings] == [
        "deployment.record.reference_routes_not_applicable",
    ]
    assert findings[0]["details"] == {
        "total_templates": 11,
        "available_templates": 0,
        "checked_fields": 0,
    }


def test_missing_modules_key_is_not_applicable(tmp_path: Path) -> None:
    """A missing module key cannot look like a clean route evaluation."""
    dep = tmp_path / "missing-modules-key"
    dep.mkdir()
    (dep / "deployment.yaml").write_text(
        yaml.safe_dump({"name": "missing-modules-key"}),
        encoding="utf-8",
    )
    (dep / "schemas").mkdir()

    findings = _run(dep)
    assert [f["code"] for f in findings] == [
        "deployment.record.reference_routes_not_applicable",
    ]
    assert findings[0]["details"]["available_templates"] == 0
    assert findings[0]["details"]["checked_fields"] == 0


def test_evaluator_failure_is_a_finding(tmp_path: Path, monkeypatch) -> None:
    """An evaluator exception is contained and reported as a blocker."""
    from okg.substrate.mcp import record as record_module

    dep = _deployment(
        tmp_path, "evaluator-failure", list(_AGENT_MEMORY_MODULES),
    )

    def _raise_evaluator_failure(**_kwargs):
        raise RuntimeError("injected route evaluator failure")

    monkeypatch.setattr(
        record_module,
        "evaluate_record_reference_routes",
        _raise_evaluator_failure,
    )
    findings = _run(dep)

    assert [f["code"] for f in findings] == [
        "deployment.record.reference_routes_unevaluated",
    ]
    assert findings[0]["severity"] == "blocker"
    assert "injected route evaluator failure" in findings[0]["observed"]


def test_lint_evaluates_record_compatibility_once(tmp_path: Path, monkeypatch) -> None:
    """Warnings and coverage counts share one compatibility evaluation."""
    from okg.substrate.mcp import record as record_module

    dep = _deployment(
        tmp_path, "single-route-evaluation", list(_AGENT_MEMORY_MODULES),
    )
    real_compatibility = record_module._record_compatibility
    calls = 0

    def _counted_compatibility(*args, **kwargs):
        nonlocal calls
        calls += 1
        return real_compatibility(*args, **kwargs)

    monkeypatch.setattr(
        record_module,
        "_record_compatibility",
        _counted_compatibility,
    )
    findings = _run(dep)

    assert calls == 1
    routes = [
        finding for finding in findings
        if finding["code"] == "deployment.record.unroutable_reference_field"
    ]
    assert routes
    assert routes[0]["details"]["checked_fields"] == 78
    assert routes[0]["details"]["available_templates"] == 10


def test_subset_deployment_reports_unroutable_field(tmp_path: Path) -> None:
    """The exact configuration the all-modules CI gate cannot see."""
    dep = _deployment(
        tmp_path, "subset-no-bookkeeping", list(_AGENT_MEMORY_MODULES),
    )
    findings = _run(dep)

    routes = [
        f for f in findings
        if f["code"] == "deployment.record.unroutable_reference_field"
    ]
    by_pair = {
        (f["details"]["record_kind"], f["details"]["field"]): f for f in routes
    }

    assert ("study", "project_ref") in by_pair, sorted(by_pair)
    finding = by_pair[("study", "project_ref")]
    # The actionable part: name the module that would supply the target.
    assert finding["details"]["missing_module"] == "bookkeeping"
    assert "bookkeeping" in finding["next_action"]
    assert finding["details"]["edge_type"] == "references"
    assert finding["details"]["available_templates"] == 10
    assert finding["details"]["checked_fields"] == 78
    assert not [
        f for f in findings
        if f["code"] in {
            "deployment.record.reference_routes_not_applicable",
            "deployment.record.reference_routes_unevaluated",
        }
    ]
    # And it must say what actually happens, not just that something is absent.
    assert "DROP" in finding["why"]


def test_deployment_prefix_override_matches_record_lowering(tmp_path: Path) -> None:
    """Lint and record writes use the same deployment prefix overrides."""
    from okg.substrate.catalog.modules_compose import compose_catalog
    from okg.substrate.mcp.record import lower_record_payload

    prefixes = [{
        "prefix": "study:",
        "target_subtype": "deployment_missing_study",
        "owner": "deployment",
    }]
    dep = _deployment(
        tmp_path,
        "prefix-override",
        list(_AGENT_MEMORY_MODULES),
        record_prefixes=prefixes,
    )
    report = lint_deployment("prefix-override", deployments_root=tmp_path)
    route_findings = [
        finding for finding in report["findings"]
        if finding["code"] == "deployment.record.unroutable_reference_field"
    ]
    by_pair = {
        (finding["details"]["record_kind"], finding["details"]["field"]): finding
        for finding in route_findings
    }
    assert ("observation", "study_ref") in by_pair
    assert by_pair[("observation", "study_ref")]["details"]["reasons"] == [
        "target_subtype_missing",
    ]

    composed = compose_catalog(dep)
    lowered = lower_record_payload(
        kind="observation",
        payload={
            "summary": "Prefix parity check.",
            "study_ref": "study:not-in-catalog",
        },
        authority={"producer_id": "agent:test"},
        prefixes=prefixes,
        active_subtypes=composed.subtypes,
        narrowing_triples=composed.narrowing_triples(),
    )
    diagnostics = [
        row for row in lowered["reference_diagnostics"]
        if row["field"] == "study_ref"
    ]
    assert lowered["emitted_edge_count"] == 0
    assert len(diagnostics) == 1
    assert diagnostics[0]["reason"] == "target_subtype_missing"


def test_all_modules_deployment_matches_shared_evaluator(tmp_path: Path) -> None:
    """The lint and the CI gate agree, because they call the same evaluator.

    Compares the finding SET against the evaluator's own verdict rather than a
    second hand-written list — a hand-written expectation here is exactly how
    #947 stayed invisible.
    """
    dep = _deployment(tmp_path, "all-modules", _all_modules())
    findings = _run(dep)

    reported = {
        (f["details"]["record_kind"], f["details"]["field"])
        for f in findings
        if f["code"] == "deployment.record.unroutable_reference_field"
    }

    from okg.substrate.catalog.modules_compose import compose_catalog

    composed = compose_catalog(dep)
    expected = set(unroutable_record_reference_fields(
        active_subtypes=composed.subtypes,
        narrowing_triples=composed.narrowing_triples(),
    ))
    assert reported == expected

    # A subset deployment must report strictly MORE than the all-modules one,
    # which is the whole reason this check exists. If these were equal the check
    # would be redundant with the CI gate.
    subset = _deployment(
        tmp_path, "subset-cmp", list(_AGENT_MEMORY_MODULES),
    )
    subset_reported = {
        (f["details"]["record_kind"], f["details"]["field"])
        for f in _run(subset)
        if f["code"] == "deployment.record.unroutable_reference_field"
    }
    assert subset_reported > reported, (subset_reported, reported)


def test_uncomposable_catalog_is_a_finding(tmp_path: Path) -> None:
    """Fail-closed: an unevaluable catalog must not read as 'no problems'."""
    dep = _deployment(
        tmp_path, "broken-modules", ["agent_memory", "no_such_module_here"],
    )
    findings = _run(dep)

    unevaluated = [
        f for f in findings
        if f["code"] == "deployment.record.reference_routes_unevaluated"
    ]
    assert len(unevaluated) == 1, findings
    assert unevaluated[0]["severity"] == "blocker"
    # The underlying error must survive into the finding, or an operator cannot
    # act on it.
    assert unevaluated[0]["observed"]
    assert "no_such_module_here" in unevaluated[0]["observed"]

    # Negative control: the arm is not always-on. With the bad module removed
    # the unevaluated finding disappears.
    ok = _deployment(tmp_path, "fixed-modules", list(_AGENT_MEMORY_MODULES))
    assert not [
        f for f in _run(ok)
        if f["code"] == "deployment.record.reference_routes_unevaluated"
    ]


def test_field_owner_hints_match_catalog_origin(tmp_path: Path) -> None:
    """The owner-hint table cannot drift from the ontology.

    Each hint claims "this field's targets are owned by module X". Verify against
    the composed catalog's own `subtype_origin` rather than trusting the table,
    otherwise a module rename would leave the lint advising a nonexistent module.
    """
    from okg.substrate.catalog.modules_compose import compose_catalog
    from okg.substrate.mcp.record import _COMMON_REFERENCE_FIELD_SPECS

    dep = _deployment(tmp_path, "origin-check", _all_modules())
    composed = compose_catalog(dep)
    spec_by_field = {s.field: s for s in _COMMON_REFERENCE_FIELD_SPECS}

    assert _REFERENCE_FIELD_OWNER_HINT, "the hint table must not be empty"
    for field, claimed_owner in _REFERENCE_FIELD_OWNER_HINT.items():
        spec = spec_by_field[field]
        targets = spec.allowed_target_subtypes
        assert targets, field
        owners = {
            str(composed.subtype_origin.get(t)) for t in targets
        }
        assert owners == {claimed_owner}, (field, claimed_owner, owners)
