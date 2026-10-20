"""Behavioral guards for the formal aggregate runner's witness verification.

FORMAL_REPORT_SCHEMA_TEST = True -- the direct-counterexample test below feeds
canned TLC stdout through a monkeypatched ``subprocess.run``. It validates the
runner's DECISION logic (which invariant violations count as a demonstrated
witness), not any model property; it is not a proof.
"""
from __future__ import annotations

import importlib.util
import subprocess
import sys
from pathlib import Path
from types import SimpleNamespace


FORMAL_REPORT_SCHEMA_TEST = True

REPO_ROOT = Path(__file__).resolve().parents[2]


def _load_suite():
    spec = importlib.util.spec_from_file_location(
        "run_formal_suite", REPO_ROOT / "scripts" / "run_formal_suite.py"
    )
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def test_named_invariant_violated_requires_the_named_invariant():
    suite = _load_suite()
    stdout = "Error: Invariant BrokenBlockersNeverRaised is violated.\n"
    assert suite.named_invariant_violated("BrokenBlockersNeverRaised", stdout)
    # A different intentional Broken* witness firing does NOT demonstrate the
    # named one -- this was the any-Broken* fallback the audit flagged.
    assert not suite.named_invariant_violated("BrokenAuthorityNeverAccepted", stdout)
    assert not suite.named_invariant_violated("BrokenAuthorityNeverAccepted", "")


def _stub_jar(suite, monkeypatch, tmp_path):
    # CI lanes other than formal never bootstrap tla2tools.jar; these tests
    # exercise the DECISION logic on canned output, so the jar lookup must be
    # deterministic rather than depending on the machine's .tools/ state.
    fake_jar = tmp_path / "tla2tools.jar"
    fake_jar.write_text("fake jar", encoding="utf-8")
    monkeypatch.setattr(suite, "_find_jar", lambda: fake_jar)
    # Direct runs create their TLC metadir under SUITE_OUT; keep it out of
    # the checkout.
    monkeypatch.setattr(suite, "SUITE_OUT", tmp_path / "suite")


def test_direct_counterexample_rejects_wrong_invariant(monkeypatch, tmp_path):
    suite = _load_suite()
    _stub_jar(suite, monkeypatch, tmp_path)
    dc = suite.DIRECT_COUNTEREXAMPLES[0]

    def fake_run(cmd, **kwargs):
        return SimpleNamespace(
            returncode=12,
            stdout="Error: Invariant SomeOtherBrokenWitness is violated.\n",
            stderr="",
        )

    monkeypatch.setattr(subprocess, "run", fake_run)
    result = suite._run_direct_counterexample(dc)
    assert not result.ok, (
        "a witness run violating a different invariant than the named one "
        "must fail the suite"
    )
    assert not result.skipped_reason
    assert result.exit_code == 1, (
        "the wrong-invariant rejection must come from the named-invariant "
        "check, not from a missing jar or other setup failure"
    )


def test_direct_counterexample_accepts_named_invariant(monkeypatch, tmp_path):
    suite = _load_suite()
    _stub_jar(suite, monkeypatch, tmp_path)
    dc = suite.DIRECT_COUNTEREXAMPLES[0]

    def fake_run(cmd, **kwargs):
        return SimpleNamespace(
            returncode=12,
            stdout=f"Error: Invariant {dc.expected_invariant} is violated.\n",
            stderr="",
        )

    monkeypatch.setattr(subprocess, "run", fake_run)
    result = suite._run_direct_counterexample(dc)
    assert result.ok


def _cfg_witness_invariants(config_path: Path) -> list[str]:
    """The invariants a witness config asks TLC to refute (Broken* plus any
    named non-Broken witness target), excluding sanity invariants that must
    HOLD during the run."""
    sanity = {"TypeOK", "ConstantSetsOK"}
    names: list[str] = []
    in_block = False
    for raw_line in config_path.read_text(encoding="utf-8").splitlines():
        line = raw_line.split("\\*", 1)[0].strip()
        if not line:
            continue
        head = line.split()[0]
        if head in {"INVARIANT", "INVARIANTS"}:
            in_block = True
            names.extend(p.rstrip(",") for p in line.split()[1:])
            continue
        if head.isupper() and head not in names:
            in_block = False
        if in_block:
            names.append(head.rstrip(","))
    return [n for n in names if n not in sanity]


def test_every_direct_counterexample_config_isolates_one_witness():
    """TLC halts at the first violated invariant, so a config bundling several
    witness invariants can only ever demonstrate one of them. Each direct
    counterexample config must therefore declare exactly the one invariant the
    registry names for it."""
    suite = _load_suite()
    for dc in suite.DIRECT_COUNTEREXAMPLES:
        witnesses = _cfg_witness_invariants(REPO_ROOT / dc.config)
        assert witnesses == [dc.expected_invariant], (
            f"{dc.config}: expected exactly [{dc.expected_invariant}] as the "
            f"witness invariant set, found {witnesses}; split bundled configs "
            "one-invariant-per-config"
        )


def test_write_junit_emits_failure_and_skip_testcases(tmp_path):
    import xml.etree.ElementTree as ET

    suite = _load_suite()
    results = [
        suite.CheckerResult("check_formal_ok.py", "smoke", 0, 1.2),
        suite.CheckerResult("check_formal_bad.py", "smoke", 1, 2.5,
                            log=("Error: Invariant X is violated.",)),
        suite.CheckerResult("check_formal_slow.py", "smoke", 124, 60.0,
                            timed_out=True),
        suite.CheckerResult("check_formal_console.py", "smoke", 0, 0.0,
                            skipped_reason="archive-only"),
    ]
    path = tmp_path / "formal.xml"
    suite.write_junit(results, path)
    root = ET.parse(path).getroot()
    assert root.get("tests") == "4"
    assert root.get("failures") == "2"
    assert root.get("skipped") == "1"
    by_name = {case.get("name"): case for case in root.iter("testcase")}
    failure = by_name["check_formal_bad.py"].find("failure")
    assert failure is not None
    assert "Invariant X" in (failure.text or "")
    timeout = by_name["check_formal_slow.py"].find("failure")
    assert timeout is not None
    assert "timed out" in timeout.get("message", "")
    assert by_name["check_formal_console.py"].find("skipped") is not None
    assert by_name["check_formal_ok.py"].find("failure") is None


def test_no_any_broken_fallback_in_suite_source():
    text = (REPO_ROOT / "scripts" / "run_formal_suite.py").read_text(
        encoding="utf-8"
    )
    assert 'startswith("Broken")' not in text, (
        "the any-Broken* fallback must stay removed: a witness passes only "
        "when the invariant it NAMES is the one TLC violates"
    )


def test_deployment_kernel_registry_splits_per_pr_smoke_from_scheduled_full():
    suite = _load_suite()
    spec = next(
        row
        for row in suite.CHECKERS
        if row.name == "check_formal_deployment_control_plane_kernel.py"
    )

    assert spec.smoke_args == ("--profile", "smoke")
    assert spec.smoke_timeout == 120
    assert spec.full_timeout == 420
    assert spec.smoke_is_full_model is False


def test_formal_full_invokes_the_deployment_kernel_complete_profile(monkeypatch):
    suite = _load_suite()
    invoked = []

    def fake_invoke(spec, label, extra, out_sub, *, mode, timeout_override):
        invoked.append((spec.name, extra, mode, timeout_override))
        return suite.CheckerResult(label, mode, 0, 0.1)

    monkeypatch.setattr(suite, "_invoke", fake_invoke)
    results = suite.run_mode(
        "full",
        only="check_formal_deployment_control_plane_kernel.py",
        timeout_override=None,
        include_archive=False,
    )

    assert len(results) == 1 and results[0].ok
    assert invoked == [
        (
            "check_formal_deployment_control_plane_kernel.py",
            ("--profile", "full", "--timeout-seconds", "420"),
            "full",
            420,
        )
    ]


def test_inner_tlc_timeout_exit_is_classified_as_a_timeout(monkeypatch):
    """exit 124 from the checker itself is a timeout, not a bare failure.

    A TLC checker enforces its own --timeout-seconds and exits 124 before the
    suite's outer wall (timeout + 60) can trip, so reading `timed_out` off the
    wall alone reported every real timeout as `exit=124` -- indistinguishable
    in junit from an invariant violation.
    """
    suite = _load_suite()
    spec = suite.CheckerSpec("check_formal_x.py", "tlc")

    def fake_run(*_args, **_kwargs):
        return SimpleNamespace(returncode=124, stdout="", stderr="")

    monkeypatch.setattr(subprocess, "run", fake_run)
    res = suite._invoke(spec, spec.name, (), "full/x",
                        mode="full", timeout_override=900)
    assert res.exit_code == 124
    assert res.timed_out is True


def test_pymodel_exit_124_is_not_read_as_a_tlc_timeout(monkeypatch):
    suite = _load_suite()
    spec = suite.CheckerSpec("check_formal_y.py", "pymodel")

    def fake_run(*_args, **_kwargs):
        return SimpleNamespace(returncode=124, stdout="", stderr="")

    monkeypatch.setattr(subprocess, "run", fake_run)
    res = suite._invoke(spec, spec.name, (), "smoke/y",
                        mode="smoke", timeout_override=None)
    assert res.timed_out is False
    assert res.ok is False


def test_enrolled_full_profile_timeout_reports_unproven_not_pass(monkeypatch):
    suite = _load_suite()
    invoked = []

    def fake_invoke(spec, label, extra, out_sub, *, mode, timeout_override):
        invoked.append(spec.name)
        return suite.CheckerResult(label, mode, 124, 900.3, timed_out=True)

    monkeypatch.setattr(suite, "_invoke", fake_invoke)
    results = suite.run_mode("full", only="check_formal_admission.py",
                             timeout_override=None, include_archive=False)

    assert invoked == ["check_formal_admission.py"]
    res = results[0]
    assert res.unproven_reason is not None
    assert "NOT PROVEN" in res.unproven_reason
    assert "#1537" in res.unproven_reason
    # It does not red the lane...
    assert res.ok is True
    # ...and it is never recorded as a proof: junit gets a `skipped` case
    # whose message says so, not a passing testcase.
    assert res.skipped_reason is None


def test_unproven_bucket_is_bounded_to_full_profile_timeouts(monkeypatch):
    """A smoke timeout, and any non-124 exit, stay hard failures."""
    suite = _load_suite()

    def fake_invoke(spec, label, extra, out_sub, *, mode, timeout_override):
        # An invariant violation: exit 1, not a timeout.
        return suite.CheckerResult(label, mode, 1, 12.0, timed_out=False,
                                   log=("Error: Invariant X is violated.",))

    monkeypatch.setattr(suite, "_invoke", fake_invoke)
    results = suite.run_mode("full", only="check_formal_admission.py",
                             timeout_override=None, include_archive=False)
    assert results[0].unproven_reason is None
    assert results[0].ok is False

    def fake_timeout(spec, label, extra, out_sub, *, mode, timeout_override):
        return suite.CheckerResult(label, mode, 124, 240.2, timed_out=True)

    monkeypatch.setattr(suite, "_invoke", fake_timeout)
    smoke = suite.run_mode("smoke", only="check_formal_admission.py",
                           timeout_override=None, include_archive=False)
    timed = [r for r in smoke if r.exit_code == 124]
    assert timed, "expected the smoke run to be invoked"
    assert all(r.unproven_reason is None and not r.ok for r in timed), (
        "a SMOKE timeout must stay a hard failure: the bucket covers the "
        "full profile only"
    )


def test_only_checkers_with_a_tracking_issue_are_enrolled():
    suite = _load_suite()
    enrolled = {
        row.name: row.full_profile_unproven_issue
        for row in suite.CHECKERS
        if row.full_profile_unproven_issue is not None
    }
    assert enrolled == {
        "check_formal_admission.py": 1537,
        "check_formal_nomos.py": 1537,
    }, (
        "enrolling a checker in the bounded NOT-PROVEN bucket removes its "
        "full-profile proof from the gate; every entry needs a tracking issue "
        "and a deliberate edit here"
    )


def test_write_junit_records_unproven_as_a_named_skip(tmp_path):
    import xml.etree.ElementTree as ET

    suite = _load_suite()
    results = [
        suite.CheckerResult(
            "check_formal_admission.py", "full", 124, 900.3, timed_out=True,
            unproven_reason="NOT PROVEN: TLC did not finish ... issue #1537.",
        ),
    ]
    path = tmp_path / "formal.xml"
    suite.write_junit(results, path)
    root = ET.parse(path).getroot()
    assert root.get("failures") == "0"
    assert root.get("skipped") == "1"
    case = next(iter(root.iter("testcase")))
    assert case.find("failure") is None
    skipped = case.find("skipped")
    assert skipped is not None
    assert "NOT PROVEN" in skipped.get("message", "")


def _metadir_of(cmd: list[str]) -> Path:
    assert "-metadir" in cmd, f"direct TLC run without -metadir: {cmd}"
    metadir = Path(cmd[cmd.index("-metadir") + 1])
    # A JVM or TLC option after the module name is read as a TLC argument
    # error, so -metadir must precede the model file.
    assert cmd.index("-metadir") < len(cmd) - 2
    return metadir


def test_direct_runs_in_one_model_dir_get_distinct_empty_metadirs(
    monkeypatch, tmp_path
):
    """Two direct counterexamples in the same model directory must never share
    TLC's state directory.

    Without -metadir TLC uses <model dir>/states/<second-resolution time> and
    exits 1 before printing anything when that directory exists. The
    2026-09-29 nightly failed that way: deployment-authoring-no-local-runtime
    started within the same second as deployment-authoring-no-custom-exec.
    """
    suite = _load_suite()
    _stub_jar(suite, monkeypatch, tmp_path)
    by_dir: dict[str, list] = {}
    for dc in suite.DIRECT_COUNTEREXAMPLES:
        by_dir.setdefault(str(Path(dc.model).parent), []).append(dc)
    first, second = by_dir["formal/deployment_authoring"][:2]
    stale = suite.SUITE_OUT / "counterexamples" / "direct" / second.name / "tlc-meta"
    stale.mkdir(parents=True)
    (stale / "26-09-29-09-02-53").mkdir()
    seen: list[tuple[list[str], list[str]]] = []

    def fake_run(cmd, **kwargs):
        metadir = _metadir_of(list(cmd))
        seen.append((list(cmd), sorted(p.name for p in metadir.iterdir())))
        dc = first if len(seen) == 1 else second
        return SimpleNamespace(
            returncode=12,
            stdout=f"Error: Invariant {dc.expected_invariant} is violated.\n",
            stderr="",
        )

    monkeypatch.setattr(subprocess, "run", fake_run)
    assert suite._run_direct_counterexample(first).ok
    assert suite._run_direct_counterexample(second).ok

    (cmd_a, contents_a), (cmd_b, contents_b) = seen
    meta_a, meta_b = _metadir_of(cmd_a), _metadir_of(cmd_b)
    assert meta_a != meta_b
    assert contents_a == [] and contents_b == [], (
        "a direct run must start from an empty metadir; a leftover timestamp "
        "directory is exactly what makes TLC refuse to start"
    )
    model_dir = REPO_ROOT / "formal" / "deployment_authoring"
    for metadir in (meta_a, meta_b):
        assert model_dir not in metadir.parents


def test_direct_smoke_model_gets_its_own_metadir(monkeypatch, tmp_path):
    suite = _load_suite()
    _stub_jar(suite, monkeypatch, tmp_path)
    seen: list[list[str]] = []

    def fake_run(cmd, **kwargs):
        seen.append(list(cmd))
        return SimpleNamespace(
            returncode=0, stdout="No error has been found.\n", stderr=""
        )

    monkeypatch.setattr(subprocess, "run", fake_run)
    dm = suite.DIRECT_SMOKE_MODELS[0]
    assert suite._run_direct_model(dm).ok
    assert _metadir_of(seen[0]) == (
        suite.SUITE_OUT / "smoke" / "direct" / dm.name / "tlc-meta"
    )


def test_direct_run_names_are_unique_per_mode():
    """The metadir is keyed by mode and name; a duplicate name would let two
    runs share one directory again."""
    suite = _load_suite()
    for registry in (suite.DIRECT_COUNTEREXAMPLES, suite.DIRECT_SMOKE_MODELS):
        names = [entry.name for entry in registry]
        assert len(names) == len(set(names))


def test_direct_start_failure_reports_exit_code_and_stderr(monkeypatch, tmp_path):
    """A TLC that dies before printing anything must still say why.

    The 2026-09-29 nightly showed only "exit=1" for the collision above,
    because the failure tail read stdout alone and TLC had written its reason
    to stderr.
    """
    suite = _load_suite()
    _stub_jar(suite, monkeypatch, tmp_path)
    reason = (
        'Exception in thread "main" util.Assert$TLCRuntimeException: '
        "TLC writes its files to a directory whose name is generated from "
        "the current time."
    )

    def fake_run(cmd, **kwargs):
        return SimpleNamespace(returncode=1, stdout="", stderr=reason + "\n")

    monkeypatch.setattr(subprocess, "run", fake_run)
    cx = suite._run_direct_counterexample(suite.DIRECT_COUNTEREXAMPLES[0])
    smoke = suite._run_direct_model(suite.DIRECT_SMOKE_MODELS[0])
    for result in (cx, smoke):
        assert not result.ok
        assert result.exit_code == 1
        assert "TLC exit code: 1" in result.log
        assert f"stderr: {reason}" in result.log
