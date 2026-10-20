from __future__ import annotations

import subprocess
from pathlib import Path
from types import SimpleNamespace

import pytest
import yaml

from okg.substrate import formal_tlc
from okg.substrate.formal_tlc import (
    TLCProperty,
    TLCReportError,
    build_formal_report_rows,
    run_tlc_formal_report,
)


def _write_model(
    root: Path,
    *,
    invariants: tuple[str, ...],
    temporal_properties: tuple[str, ...] = (),
) -> tuple[Path, Path]:
    model_dir = root / "formal" / "demo"
    model_dir.mkdir(parents=True)
    model_path = model_dir / "Demo.tla"
    config_path = model_dir / "Demo.cfg"
    model_path.write_text(
        "---- MODULE Demo ----\n"
        "EXTENDS Naturals\n"
        "VARIABLE x\n"
        "Init == x = 0\n"
        "Next == x' = x\n"
        "Spec == Init /\\ [][Next]_x\n"
        "TypeOK == x \\in Nat\n"
        "DemoInvariant == x = 0\n"
        "DemoEventually == <> (x = 0)\n"
        "====\n",
        encoding="utf-8",
    )
    config_path.write_text(
        "SPECIFICATION Spec\n\n"
        "INVARIANTS\n"
        "  TypeOK\n"
        + "".join(f"  {invariant}\n" for invariant in invariants)
        + (
            "\nPROPERTIES\n"
            + "".join(f"  {temporal}\n" for temporal in temporal_properties)
            if temporal_properties
            else ""
        ),
        encoding="utf-8",
    )
    return model_dir, config_path


def _patch_tlc(monkeypatch, run) -> None:
    monkeypatch.setattr(
        formal_tlc,
        "find_tla2tools_jar",
        lambda _repo_root: Path("/tmp/tla2tools.jar"),
    )
    monkeypatch.setattr(formal_tlc.shutil, "which", lambda _name: "/usr/bin/java")
    monkeypatch.setattr(formal_tlc.subprocess, "run", run)


def _property() -> TLCProperty:
    return TLCProperty(
        "DemoProperty",
        "TLC verified the demo property.",
        refinement_tests=("tests/demo/test_demo.py::test_demo",),
        expected_invariants=("DemoInvariant",),
    )


def _temporal_property() -> TLCProperty:
    return TLCProperty(
        "DemoProgress",
        "TLC verified the demo temporal property.",
        refinement_tests=("tests/demo/test_demo.py::test_progress",),
        expected_temporal_properties=("DemoEventually",),
    )


def test_tlc_runner_writes_strict_success_report(
    tmp_path: Path,
    monkeypatch,
) -> None:
    model_dir, _config_path = _write_model(tmp_path, invariants=("DemoInvariant",))

    def run(cmd, **kwargs):
        assert "-metadir" in cmd
        assert kwargs["timeout"] == 7
        return SimpleNamespace(
            returncode=0,
            stdout="Model checking completed. No error has been found.\n"
            "200000000 states generated, 99000000 distinct states found, 0 states left on queue.\n",
            stderr="",
        )

    _patch_tlc(monkeypatch, run)

    result = run_tlc_formal_report(
        repo_root=tmp_path,
        model_dir=model_dir,
        model=Path("Demo.tla"),
        config=Path("Demo.cfg"),
        out_dir=tmp_path / "out",
        properties=[_property()],
        checker_command="check-demo",
        timeout_seconds=7,
    )

    assert result.exit_code == 0
    assert result.run_failed is False
    assert result.stdout_path.is_file()
    assert result.stderr_path.is_file()
    rows = yaml.safe_load(result.report_path.read_text(encoding="utf-8"))["results"]
    assert rows[0]["property_id"] == "DemoProperty"
    assert rows[0]["status"] == "pass"
    assert rows[0]["fresh"] is True
    assert rows[0]["proof_status"] == "freshly_proven"
    assert rows[0]["timed_out"] is False
    assert rows[0]["exit_code"] == 0
    assert rows[0]["checker_command"] == "check-demo"
    assert rows[0]["refinement_tests"] == ["tests/demo/test_demo.py::test_demo"]
    assert rows[0]["module_digest"].startswith("sha256:")
    assert rows[0]["config_digest"].startswith("sha256:")


def test_tlc_runner_validates_expected_properties_beyond_report_rows(
    tmp_path: Path,
    monkeypatch,
) -> None:
    model_dir, _config_path = _write_model(
        tmp_path,
        invariants=("DemoInvariant", "ExtraInvariant"),
    )
    extra_property = TLCProperty(
        "ExtraProperty",
        "TLC verified the extra property.",
        expected_invariants=("ExtraInvariant",),
    )
    _patch_tlc(
        monkeypatch,
        lambda *_args, **_kwargs: SimpleNamespace(
            returncode=0,
            stdout="Model checking completed. No error has been found.\n"
            "200000000 states generated, 99000000 distinct states found, 0 states left on queue.\n",
            stderr="",
        ),
    )

    result = run_tlc_formal_report(
        repo_root=tmp_path,
        model_dir=model_dir,
        model=Path("Demo.tla"),
        config=Path("Demo.cfg"),
        out_dir=tmp_path / "out",
        properties=[_property()],
        expected_properties=[_property(), extra_property],
        checker_command="check-demo",
    )

    assert result.run_failed is False
    assert {row["property_id"] for row in result.rows} == {"DemoProperty"}


def test_tlc_runner_accepts_expected_temporal_properties_in_config(
    tmp_path: Path,
    monkeypatch,
) -> None:
    model_dir, _config_path = _write_model(
        tmp_path,
        invariants=("DemoInvariant",),
        temporal_properties=("DemoEventually",),
    )
    _patch_tlc(
        monkeypatch,
        lambda *_args, **_kwargs: SimpleNamespace(
            returncode=0,
            stdout="Model checking completed. No error has been found.\n"
            "200000000 states generated, 99000000 distinct states found, 0 states left on queue.\n",
            stderr="",
        ),
    )

    result = run_tlc_formal_report(
        repo_root=tmp_path,
        model_dir=model_dir,
        model=Path("Demo.tla"),
        config=Path("Demo.cfg"),
        out_dir=tmp_path / "out",
        properties=[_temporal_property()],
        checker_command="check-demo",
    )

    assert result.run_failed is False
    assert result.rows[0]["property_id"] == "DemoProgress"


def test_tlc_runner_rejects_missing_temporal_property_config(
    tmp_path: Path,
    monkeypatch,
) -> None:
    model_dir, _config_path = _write_model(
        tmp_path,
        invariants=("DemoInvariant",),
    )

    def run(_cmd, **_kwargs):
        raise AssertionError("TLC must not run when expected properties are absent")

    _patch_tlc(monkeypatch, run)

    result = run_tlc_formal_report(
        repo_root=tmp_path,
        model_dir=model_dir,
        model=Path("Demo.tla"),
        config=Path("Demo.cfg"),
        out_dir=tmp_path / "out",
        properties=[_temporal_property()],
        checker_command="check-demo",
    )

    assert result.run_failed is True
    assert result.missing_invariants == ("PROPERTY DemoEventually",)
    assert result.rows[0]["status"] == "fail"
    assert result.rows[0]["fresh"] is False
    assert result.rows[0]["failure_class"] == "missing_obligation"
    assert result.rows[0]["proof_status"] == "not_freshly_proven"
    assert "omitted expected invariants/properties" in result.rows[0]["summary"]


def test_tlc_runner_times_out_as_failed_report(
    tmp_path: Path,
    monkeypatch,
) -> None:
    model_dir, _config_path = _write_model(tmp_path, invariants=("DemoInvariant",))

    def run(_cmd, **kwargs):
        raise subprocess.TimeoutExpired(
            cmd=kwargs.get("args", "tlc"),
            timeout=kwargs["timeout"],
            output="partial stdout",
            stderr="partial stderr",
        )

    _patch_tlc(monkeypatch, run)

    result = run_tlc_formal_report(
        repo_root=tmp_path,
        model_dir=model_dir,
        model=Path("Demo.tla"),
        config=Path("Demo.cfg"),
        out_dir=tmp_path / "out",
        properties=[_property()],
        checker_command="check-demo",
        timeout_seconds=1,
    )

    assert result.timed_out is True
    assert result.exit_code == formal_tlc.TLC_TIMEOUT_EXIT_CODE
    assert result.rows[0]["status"] == "fail"
    assert result.rows[0]["fresh"] is False
    assert result.rows[0]["failure_class"] == "timeout"
    assert result.rows[0]["proof_status"] == "not_freshly_proven"
    assert result.rows[0]["timed_out"] is True
    assert result.rows[0]["exit_code"] == formal_tlc.TLC_TIMEOUT_EXIT_CODE
    assert "TLC timed out" in result.rows[0]["summary"]
    assert "not freshly proven" in result.rows[0]["summary"]
    assert "TLC timed out after 1 seconds" in result.stderr_path.read_text(
        encoding="utf-8"
    )


def test_tlc_runner_failed_property_is_failed_report(
    tmp_path: Path,
    monkeypatch,
) -> None:
    model_dir, _config_path = _write_model(tmp_path, invariants=("DemoInvariant",))

    _patch_tlc(
        monkeypatch,
        lambda *_args, **_kwargs: SimpleNamespace(
            returncode=1,
            stdout="Error: Invariant DemoInvariant is violated.\n",
            stderr="",
        ),
    )

    result = run_tlc_formal_report(
        repo_root=tmp_path,
        model_dir=model_dir,
        model=Path("Demo.tla"),
        config=Path("Demo.cfg"),
        out_dir=tmp_path / "out",
        properties=[_property()],
        checker_command="check-demo",
    )

    assert result.invariant_violated is True
    assert result.run_failed is True
    assert result.rows[0]["status"] == "fail"
    assert result.rows[0]["fresh"] is False
    assert result.rows[0]["failure_class"] == "invariant_violation"
    assert result.rows[0]["proof_status"] == "semantic_failure"
    assert result.rows[0]["timed_out"] is False
    assert result.rows[0]["summary"] == "TLC failed."


def test_tlc_runner_counterexample_requires_expected_invariant(
    tmp_path: Path,
    monkeypatch,
) -> None:
    model_dir, _config_path = _write_model(
        tmp_path,
        invariants=("TypeOK", "DemoInvariant"),
    )

    _patch_tlc(
        monkeypatch,
        lambda *_args, **_kwargs: SimpleNamespace(
            returncode=1,
            stdout="Error: Invariant TypeOK is violated.\n",
            stderr="",
        ),
    )

    result = run_tlc_formal_report(
        repo_root=tmp_path,
        model_dir=model_dir,
        model=Path("Demo.tla"),
        config=Path("Demo.cfg"),
        out_dir=tmp_path / "out",
        properties=[_property()],
        checker_command="check-demo",
        expect_counterexample=True,
    )

    assert result.run_failed is True
    assert result.violated_invariants == ("TypeOK",)
    assert result.rows[0]["status"] == "fail"
    assert "Expected counter-example invariant" in result.rows[0]["summary"]
    assert "DemoInvariant" in result.rows[0]["summary"]


def test_tlc_runner_counterexample_accepts_expected_invariant(
    tmp_path: Path,
    monkeypatch,
) -> None:
    model_dir, _config_path = _write_model(tmp_path, invariants=("DemoInvariant",))

    _patch_tlc(
        monkeypatch,
        lambda *_args, **_kwargs: SimpleNamespace(
            returncode=1,
            stdout="Error: Invariant DemoInvariant is violated.\n",
            stderr="",
        ),
    )

    result = run_tlc_formal_report(
        repo_root=tmp_path,
        model_dir=model_dir,
        model=Path("Demo.tla"),
        config=Path("Demo.cfg"),
        out_dir=tmp_path / "out",
        properties=[_property()],
        checker_command="check-demo",
        expect_counterexample=True,
    )

    assert result.run_failed is False
    assert result.violated_invariants == ("DemoInvariant",)
    assert result.rows[0]["status"] == "pass"


def test_tlc_runner_counterexample_rejects_mixed_tlc_error_output(
    tmp_path: Path,
    monkeypatch,
) -> None:
    model_dir, _config_path = _write_model(tmp_path, invariants=("DemoInvariant",))

    _patch_tlc(
        monkeypatch,
        lambda *_args, **_kwargs: SimpleNamespace(
            returncode=1,
            stdout=(
                "Error: Invariant DemoInvariant is violated.\n"
                "Error: The behavior up to this point is:\n"
                "State 1: <Initial predicate>\n"
            ),
            stderr="Error: TLC threw an unexpected exception.\n",
        ),
    )

    result = run_tlc_formal_report(
        repo_root=tmp_path,
        model_dir=model_dir,
        model=Path("Demo.tla"),
        config=Path("Demo.cfg"),
        out_dir=tmp_path / "out",
        properties=[_property()],
        checker_command="check-demo",
        expect_counterexample=True,
    )

    assert result.run_failed is True
    assert result.violated_invariants == ("DemoInvariant",)
    assert result.unexpected_errors == (
        "stderr: Error: TLC threw an unexpected exception.",
    )
    assert result.rows[0]["status"] == "fail"
    assert "Unexpected TLC error" in result.rows[0]["summary"]


def test_tlc_runner_counterexample_rejects_parser_semantic_error_output(
    tmp_path: Path,
    monkeypatch,
) -> None:
    model_dir, _config_path = _write_model(tmp_path, invariants=("DemoInvariant",))

    _patch_tlc(
        monkeypatch,
        lambda *_args, **_kwargs: SimpleNamespace(
            returncode=1,
            stdout="Error: Invariant DemoInvariant is violated.\n",
            stderr=(
                "Error: Parsing or semantic analysis failed.\n"
                "Semantic errors:\n"
                "Line 1, column 1: unknown operator.\n"
            ),
        ),
    )

    result = run_tlc_formal_report(
        repo_root=tmp_path,
        model_dir=model_dir,
        model=Path("Demo.tla"),
        config=Path("Demo.cfg"),
        out_dir=tmp_path / "out",
        properties=[_property()],
        checker_command="check-demo",
        expect_counterexample=True,
    )

    assert result.run_failed is True
    assert result.violated_invariants == ("DemoInvariant",)
    assert result.unexpected_errors == (
        "stderr: Error: Parsing or semantic analysis failed.",
        "stderr: Semantic errors:",
    )
    assert result.rows[0]["status"] == "fail"
    assert "Unexpected TLC error" in result.rows[0]["summary"]


def test_tlc_runner_rejects_incomplete_expected_property_set(
    tmp_path: Path,
    monkeypatch,
) -> None:
    model_dir, _config_path = _write_model(tmp_path, invariants=())

    def run(_cmd, **_kwargs):
        raise AssertionError("TLC must not run when expected invariants are absent")

    _patch_tlc(monkeypatch, run)

    result = run_tlc_formal_report(
        repo_root=tmp_path,
        model_dir=model_dir,
        model=Path("Demo.tla"),
        config=Path("Demo.cfg"),
        out_dir=tmp_path / "out",
        properties=[_property()],
        checker_command="check-demo",
    )

    assert result.run_failed is True
    assert result.missing_invariants == ("DemoInvariant",)
    assert result.rows[0]["status"] == "fail"
    assert "omitted expected invariants" in result.rows[0]["summary"]


def test_report_rows_refuse_missing_artifact(tmp_path: Path) -> None:
    model_dir, config_path = _write_model(tmp_path, invariants=("DemoInvariant",))

    with pytest.raises(TLCReportError, match="artifact does not exist"):
        build_formal_report_rows(
            properties=[_property()],
            status="pass",
            checker_command="check-demo",
            command=("tlc2.TLC",),
            artifact=tmp_path / "missing.log",
            model_path=model_dir / "Demo.tla",
            config_path=config_path,
        )


def test_tlc_runner_prints_a_killed_run_to_the_job_log(
    tmp_path: Path,
    monkeypatch,
    capsys,
) -> None:
    """KILLS: a TLC kill recorded only in files an upload may drop (2026-09-25).

    The nightly's formal-full lane reported "TLC failed" with exit -9 twice,
    and the stdout/stderr files never left the runner because the artifact
    quota was full. The signal and the output tail must reach the job log.
    """
    model_dir, _config_path = _write_model(tmp_path, invariants=("DemoInvariant",))

    def run(_cmd, **_kwargs):
        return SimpleNamespace(
            returncode=-9,
            stdout="Starting... (2026-09-25)\nProgress(12): 22000000 states\n",
            stderr="",
        )

    _patch_tlc(monkeypatch, run)

    result = run_tlc_formal_report(
        repo_root=tmp_path,
        model_dir=model_dir,
        model=Path("Demo.tla"),
        config=Path("Demo.cfg"),
        out_dir=tmp_path / "out",
        properties=[_property()],
        checker_command="check-demo",
    )

    assert result.run_failed is True
    err = capsys.readouterr().err
    assert "Progress(12): 22000000 states" in err
    # The signal line comes last: run_formal_suite.py keeps only the final
    # few lines of a failed checker's output.
    assert err.rstrip().splitlines()[-1] == "TLC was killed by signal 9."
