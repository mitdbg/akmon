from __future__ import annotations

import stat

import yaml


def test_poros_formal_checker_writes_expected_counterexample(
    tmp_path,
    monkeypatch,
) -> None:
    from scripts import check_formal_poros

    fake_jar = tmp_path / "tla2tools.jar"
    fake_jar.write_text("fake jar", encoding="utf-8")
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    java = bin_dir / "java"
    java.write_text(
        "#!/bin/sh\n"
        "echo 'Error: Invariant BrokenReceiptImpliesPublication is violated.'\n"
        "exit 1\n",
        encoding="utf-8",
    )
    java.chmod(java.stat().st_mode | stat.S_IXUSR)
    monkeypatch.setenv("TLA2TOOLS_JAR", str(fake_jar))
    monkeypatch.setenv("PATH", str(bin_dir))

    out = tmp_path / "out"
    rc = check_formal_poros.main([
        "--model",
        "PorosExchangeCounterexample.tla",
        "--config",
        "PorosExchangeCounterexample.cfg",
        "--out",
        str(out),
        "--expect-counterexample",
    ])

    assert rc == 0
    artifact = out / "counterexample.txt"
    assert artifact.is_file()
    assert "BrokenReceiptImpliesPublication" in artifact.read_text(
        encoding="utf-8"
    )


def test_poros_formal_checker_expect_counterexample_fails_when_tlc_passes(
    tmp_path,
    monkeypatch,
) -> None:
    from scripts import check_formal_poros

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

    rc = check_formal_poros.main([
        "--model",
        "PorosExchangeCounterexample.tla",
        "--config",
        "PorosExchangeCounterexample.cfg",
        "--out",
        str(tmp_path / "out"),
        "--expect-counterexample",
    ])

    assert rc == 1


def test_poros_formal_checker_expect_counterexample_rejects_unexpected_invariant(
    tmp_path,
    monkeypatch,
) -> None:
    from scripts import check_formal_poros

    fake_jar = tmp_path / "tla2tools.jar"
    fake_jar.write_text("fake jar", encoding="utf-8")
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    java = bin_dir / "java"
    java.write_text(
        "#!/bin/sh\n"
        "echo 'Error: Invariant SomeOtherInvariant is violated.'\n"
        "exit 1\n",
        encoding="utf-8",
    )
    java.chmod(java.stat().st_mode | stat.S_IXUSR)
    monkeypatch.setenv("TLA2TOOLS_JAR", str(fake_jar))
    monkeypatch.setenv("PATH", str(bin_dir))

    out = tmp_path / "out"
    rc = check_formal_poros.main([
        "--model",
        "PorosExchangeCounterexample.tla",
        "--config",
        "PorosExchangeCounterexample.cfg",
        "--out",
        str(out),
        "--expect-counterexample",
    ])

    assert rc == 1
    artifact = out / "counterexample.txt"
    assert artifact.is_file()
    artifact_text = artifact.read_text(encoding="utf-8")
    assert "BrokenReceiptImpliesPublication" in artifact_text
    assert "SomeOtherInvariant" in artifact_text
    report = yaml.safe_load((out / "formal-report.yaml").read_text(encoding="utf-8"))
    rows = report["results"]
    assert rows[0]["property_id"] == "formal.poros.receipt-publication-nonvacuity"
    assert rows[0]["status"] == "fail"
