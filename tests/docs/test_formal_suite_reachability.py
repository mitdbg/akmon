"""Guards that keep the formal aggregate runner honest.

These tests enforce two invariants that the formal-hardening program depends on:

1. Every ``scripts/check_formal_*.py`` checker is reachable from the aggregate
   runner registry, so a new checker cannot be added without a gating run path.
2. The pytest modules that monkeypatch ``subprocess.run`` to emit canned TLC
   output declare themselves as report-SCHEMA tests (they validate report-row
   shape, not any model property), so a reader cannot mistake them for proofs.
"""
from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]


def _load_suite():
    spec = importlib.util.spec_from_file_location(
        "run_formal_suite", REPO_ROOT / "scripts" / "run_formal_suite.py"
    )
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    # Register before exec so the module's @dataclass can resolve its module.
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def test_every_checker_is_reachable_from_the_aggregate_runner():
    suite = _load_suite()
    discovered = suite.discovered_checker_names()
    registered = suite.registered_checker_names()
    orphans = sorted(discovered - registered)
    stale = sorted(registered - discovered)
    assert not orphans, (
        "these scripts/check_formal_*.py checkers have no aggregate run path; "
        f"add them to scripts/run_formal_suite.py CHECKERS: {orphans}"
    )
    assert not stale, (
        "these registered checkers no longer exist on disk; remove them from "
        f"scripts/run_formal_suite.py CHECKERS: {stale}"
    )


def test_smoke_full_model_gap_is_disclosed():
    """Every TLC checker either has a dedicated smoke profile or is flagged as
    running the full model in smoke, so the smoke/full coverage gap is honest."""
    suite = _load_suite()
    for spec in suite.CHECKERS:
        if spec.kind != "tlc" or spec.archive_only:
            continue
        has_profile = "--profile" in spec.smoke_args
        assert has_profile or spec.smoke_is_full_model, (
            f"{spec.name}: a TLC checker must either expose a smoke profile or be "
            "flagged smoke_is_full_model so the smoke/full gap is disclosed"
        )


# Modules that monkeypatch subprocess.run to feed TLC canned output. They prove
# report-row SHAPE, not model correctness, and must say so out loud.
_FAKED_TLC_MODULES = (
    "tests/pact/test_formal_report_producers.py",
    "tests/substrate/test_formal_admission.py",
    "tests/substrate/test_formal_nomos.py",
)

_REPORT_SCHEMA_MARKER = "FORMAL_REPORT_SCHEMA_TEST"


@pytest.mark.parametrize("rel_path", _FAKED_TLC_MODULES)
def test_faked_tlc_modules_declare_report_schema(rel_path: str):
    path = REPO_ROOT / rel_path
    text = path.read_text(encoding="utf-8")
    assert _REPORT_SCHEMA_MARKER in text, (
        f"{rel_path} monkeypatches TLC and must declare "
        f"`{_REPORT_SCHEMA_MARKER} = True` plus a docstring stating it validates "
        "report-row shape, not any model property (it is not a proof)."
    )


def test_a_registered_checker_missing_from_the_checkout_is_refused_by_name(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    """A registered checker whose script does not ship refuses the run up front.

    Otherwise the suite would start and fail that checker as an opaque
    subprocess error. A selection that does not name it still runs.
    """
    suite = _load_suite()
    registered = sorted(suite.registered_checker_names())
    absent, kept = registered[0], registered[1:]
    scripts = tmp_path / "scripts"
    scripts.mkdir()
    for name in kept:
        (scripts / name).write_text("", encoding="utf-8")
    monkeypatch.setattr(suite, "SCRIPTS_DIR", scripts)
    junit = tmp_path / "suite.xml"

    assert suite.main(["--mode", "smoke", "--junit-xml", str(junit)]) == 2
    error = capsys.readouterr().err
    assert f"registered checkers missing from this checkout: {absent}" in error
    assert "registry-missing" in junit.read_text(encoding="utf-8")

    assert suite.main(["--mode", "smoke", "--only", absent]) == 2
    assert absent in capsys.readouterr().err
    monkeypatch.setattr(suite, "run_mode", lambda *_a, **_k: [])
    assert suite.main(["--mode", "smoke", "--only", kept[0]]) == 0


def test_without_pact_records_is_forwarded_only_to_checkers_that_read_them(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A copy without PACT records can run the suite; the default still checks them."""
    suite = _load_suite()
    readers = sorted(spec.name for spec in suite.CHECKERS if spec.reads_pact_records)
    assert readers == [
        "check_formal_deployment_control_plane_kernel.py",
        "check_formal_external_action_review.py",
        "check_formal_worktree_branch_safety.py",
    ]
    calls: list[tuple[str, tuple[str, ...]]] = []

    def fake_invoke(spec, label, extra, out_sub, *, mode, timeout_override):
        calls.append((spec.name, tuple(extra)))
        return suite.CheckerResult(label, mode, 0, 0.0)

    monkeypatch.setattr(suite, "_invoke", fake_invoke)
    other = next(s.name for s in suite.CHECKERS if s.kind == "tlc" and not s.reads_pact_records
                 and not s.archive_only)
    for name in (*readers, other):
        with_flag: list[tuple[str, ...]] = []
        without_flag: list[tuple[str, ...]] = []
        for mode in ("smoke", "counterexamples"):
            calls.clear()
            suite.run_mode(mode, only=name, timeout_override=None,
                           include_archive=False, without_pact_records=True)
            with_flag += [extra for _name, extra in calls]
            calls.clear()
            suite.run_mode(mode, only=name, timeout_override=None, include_archive=False)
            without_flag += [extra for _name, extra in calls]
        # Smoke always invokes the checker, so neither list is empty.
        assert with_flag and without_flag, name
        assert all("--without-pact-records" not in e for e in without_flag), name
        if name in readers:
            assert all(e[-1] == "--without-pact-records" for e in with_flag), name
        else:
            assert all("--without-pact-records" not in e for e in with_flag), name
