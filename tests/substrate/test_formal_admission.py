"""REPORT-SCHEMA tests for the admission formal checker wrapper.

These tests install a fake ``java`` shell stub that echoes canned TLC output;
they verify the checker's report/exit-code plumbing, NOT any property of the
Admission model. Real model proof comes from the gating aggregate runner
(scripts/run_formal_suite.py) executing real TLC in CI.
"""
from __future__ import annotations

import stat

FORMAL_REPORT_SCHEMA_TEST = True


def test_formal_checker_writes_expected_counterexample(
    tmp_path,
    monkeypatch,
) -> None:
    from scripts import check_formal_admission

    fake_jar = tmp_path / "tla2tools.jar"
    fake_jar.write_text("fake jar", encoding="utf-8")
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    java = bin_dir / "java"
    java.write_text(
        "#!/bin/sh\n"
        "echo 'Error: Invariant BrokenNoPublish is violated.'\n"
        "exit 1\n",
        encoding="utf-8",
    )
    java.chmod(java.stat().st_mode | stat.S_IXUSR)
    monkeypatch.setenv("TLA2TOOLS_JAR", str(fake_jar))
    monkeypatch.setenv("PATH", str(bin_dir))

    out = tmp_path / "out"
    rc = check_formal_admission.main([
        "--model",
        "AdmissionCounterexample.tla",
        "--config",
        "AdmissionCounterexample.cfg",
        "--out",
        str(out),
        "--expect-counterexample",
    ])

    assert rc == 0
    artifact = out / "counterexample.txt"
    assert artifact.is_file()
    assert "BrokenNoPublish" in artifact.read_text(encoding="utf-8")


def test_formal_checker_expect_counterexample_fails_when_tlc_passes(
    tmp_path,
    monkeypatch,
) -> None:
    from scripts import check_formal_admission

    fake_jar = tmp_path / "tla2tools.jar"
    fake_jar.write_text("fake jar", encoding="utf-8")
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    java = bin_dir / "java"
    java.write_text(
        "#!/bin/sh\n"
        "echo 'Model checking completed. No error has been found.'\n"
        "exit 0\n",
        encoding="utf-8",
    )
    java.chmod(java.stat().st_mode | stat.S_IXUSR)
    monkeypatch.setenv("TLA2TOOLS_JAR", str(fake_jar))
    monkeypatch.setenv("PATH", str(bin_dir))

    rc = check_formal_admission.main([
        "--out",
        str(tmp_path / "out"),
        "--expect-counterexample",
    ])

    assert rc == 1
