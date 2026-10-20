"""The TLC resource profile: what one model-check run takes from the machine.

Left untuned, TLC runs one worker per core and the JVM sizes its max heap from
physical RAM, so a suite run pins every core and holds gigabytes per process.
These tests pin the interactive-vs-CI split and assert the caps actually reach
the spawned command line — a refactor that drops the flags would otherwise
regress silently into "my machine is unusable while the formal checks run".
"""

from __future__ import annotations

import subprocess
from pathlib import Path
from types import SimpleNamespace

import pytest

from okg.substrate import formal_tlc
from okg.substrate.formal_tlc import (
    DEFAULT_TLC_HEAP,
    TLCProperty,
    run_tlc_formal_report,
    tlc_jvm_args,
    tlc_workers,
)


@pytest.fixture(autouse=True)
def _clean_env(monkeypatch):
    for name in ("OKG_TLC_WORKERS", "OKG_TLC_HEAP", "CI"):
        monkeypatch.delenv(name, raising=False)


def test_explicit_worker_count_wins(monkeypatch) -> None:
    monkeypatch.setenv("OKG_TLC_WORKERS", "7")
    assert tlc_workers() == "7"


def test_explicit_auto_restores_full_parallelism(monkeypatch) -> None:
    monkeypatch.setenv("OKG_TLC_WORKERS", "auto")
    monkeypatch.setenv("CI", "")
    assert tlc_workers() == "auto"


def test_ci_keeps_every_core(monkeypatch) -> None:
    monkeypatch.setenv("CI", "true")
    assert tlc_workers() == "auto"


@pytest.mark.parametrize("ci_value", ["", "0", "false", "no"])
def test_falsey_ci_values_are_not_ci(monkeypatch, ci_value: str) -> None:
    monkeypatch.setenv("CI", ci_value)
    monkeypatch.setattr(formal_tlc.os, "cpu_count", lambda: 12)
    assert tlc_workers() == "4"


def test_interactive_default_leaves_cores_free(monkeypatch) -> None:
    monkeypatch.setattr(formal_tlc.os, "cpu_count", lambda: 12)
    workers = tlc_workers()
    assert workers == "4"
    assert int(workers) < 12


def test_interactive_default_never_reaches_zero(monkeypatch) -> None:
    """A 1- or 2-core host must still get a worker, not `-workers 0`."""
    monkeypatch.setattr(formal_tlc.os, "cpu_count", lambda: 1)
    assert tlc_workers() == "1"
    monkeypatch.setattr(formal_tlc.os, "cpu_count", lambda: None)
    assert tlc_workers() == "1"


def test_jvm_args_cap_heap_and_jvm_thread_pools() -> None:
    assert tlc_jvm_args("4") == (f"-Xmx{DEFAULT_TLC_HEAP}", "-XX:ActiveProcessorCount=4")


def test_jvm_args_omit_processor_count_when_workers_are_auto() -> None:
    """`auto` means "use the machine"; pinning ActiveProcessorCount would
    contradict it, and it is the CI default where throughput is what we want."""
    assert tlc_jvm_args("auto") == (f"-Xmx{DEFAULT_TLC_HEAP}",)


def test_heap_override_is_honored(monkeypatch) -> None:
    monkeypatch.setenv("OKG_TLC_HEAP", "16g")
    assert tlc_jvm_args("auto") == ("-Xmx16g",)


def test_ci_keeps_its_heap_headroom(monkeypatch) -> None:
    """The whole profile is interactive-only.

    The scheduled full-corpus lane runs profiles orders of magnitude larger
    than anything measured here -- admission's full profile floors at 22.3M
    distinct states against nomos's 380K -- so no cap is imposed on a gating
    lane without a measurement behind it.
    """
    monkeypatch.setenv("CI", "true")
    assert tlc_jvm_args("auto") == ()


def test_explicit_heap_still_applies_under_ci(monkeypatch) -> None:
    monkeypatch.setenv("CI", "true")
    monkeypatch.setenv("OKG_TLC_HEAP", "8g")
    assert tlc_jvm_args("auto") == ("-Xmx8g",)


def test_heap_cap_can_be_disabled(monkeypatch) -> None:
    """Escape hatch for a future model that outgrows the cap."""
    monkeypatch.setenv("OKG_TLC_HEAP", "off")
    assert tlc_jvm_args("4") == ("-XX:ActiveProcessorCount=4",)


def _demo_model(root: Path) -> Path:
    model_dir = root / "formal" / "demo"
    model_dir.mkdir(parents=True)
    (model_dir / "Demo.tla").write_text(
        "---- MODULE Demo ----\n"
        "EXTENDS Naturals\n"
        "VARIABLE x\n"
        "Init == x = 0\n"
        "Next == x' = x\n"
        "Spec == Init /\\ [][Next]_x\n"
        "DemoInvariant == x = 0\n"
        "====\n",
        encoding="utf-8",
    )
    (model_dir / "Demo.cfg").write_text(
        "SPECIFICATION Spec\n\nINVARIANTS\n  DemoInvariant\n",
        encoding="utf-8",
    )
    return model_dir


def test_runner_command_carries_the_caps(tmp_path: Path, monkeypatch) -> None:
    """The caps are only worth anything if they reach the real command line."""
    monkeypatch.setattr(formal_tlc.os, "cpu_count", lambda: 12)
    model_dir = _demo_model(tmp_path)
    seen: list[list[str]] = []

    def run(cmd, **_kwargs):
        seen.append(list(cmd))
        return SimpleNamespace(
            returncode=0,
            stdout="Model checking completed. No error has been found.\n"
            "900 states generated, 400 distinct states found, "
            "0 states left on queue.\n",
            stderr="",
        )

    monkeypatch.setattr(
        formal_tlc, "find_tla2tools_jar", lambda _root: Path("/tmp/tla2tools.jar")
    )
    monkeypatch.setattr(formal_tlc.shutil, "which", lambda _name: "/usr/bin/java")
    monkeypatch.setattr(formal_tlc.subprocess, "run", run)

    result = run_tlc_formal_report(
        repo_root=tmp_path,
        model_dir=model_dir,
        model=Path("Demo.tla"),
        config=Path("Demo.cfg"),
        out_dir=tmp_path / "out",
        properties=[TLCProperty("DemoProperty", "demo", expected_invariants=("DemoInvariant",))],
        checker_command="check-demo",
    )

    assert result.run_failed is False
    cmd = seen[0]
    assert f"-Xmx{DEFAULT_TLC_HEAP}" in cmd
    assert "-XX:ActiveProcessorCount=4" in cmd
    assert cmd[cmd.index("-workers") + 1] == "4"
    # JVM flags must precede the class name or the JVM treats them as program
    # arguments and TLC dies on an unrecognized option.
    assert cmd.index(f"-Xmx{DEFAULT_TLC_HEAP}") < cmd.index("tlc2.TLC")
    assert cmd.index("-XX:ActiveProcessorCount=4") < cmd.index("tlc2.TLC")


def test_ci_command_is_byte_identical_to_the_old_behavior(
    tmp_path: Path, monkeypatch
) -> None:
    """Under CI the profile must add nothing at all.

    This is what makes the change safe to land on a gating lane: not "the caps
    are probably fine in CI", but that CI emits the same argv it always did.
    """
    monkeypatch.setenv("CI", "true")
    model_dir = _demo_model(tmp_path)
    seen: list[list[str]] = []

    def run(cmd, **_kwargs):
        seen.append(list(cmd))
        return SimpleNamespace(
            returncode=0,
            stdout="Model checking completed. No error has been found.\n"
            "900 states generated, 400 distinct states found, "
            "0 states left on queue.\n",
            stderr="",
        )

    monkeypatch.setattr(
        formal_tlc, "find_tla2tools_jar", lambda _root: Path("/tmp/tla2tools.jar")
    )
    monkeypatch.setattr(formal_tlc.shutil, "which", lambda _name: "/usr/bin/java")
    monkeypatch.setattr(formal_tlc.subprocess, "run", run)

    run_tlc_formal_report(
        repo_root=tmp_path,
        model_dir=model_dir,
        model=Path("Demo.tla"),
        config=Path("Demo.cfg"),
        out_dir=tmp_path / "out",
        properties=[
            TLCProperty("DemoProperty", "demo", expected_invariants=("DemoInvariant",))
        ],
        checker_command="check-demo",
    )

    cmd = seen[0]
    assert not [arg for arg in cmd if arg.startswith("-Xmx")]
    assert not [arg for arg in cmd if arg.startswith("-XX:")]
    assert cmd[cmd.index("-workers") + 1] == "auto"
    assert cmd[1] == "-cp", f"a JVM flag leaked in ahead of -cp: {cmd[:4]}"


def test_suite_direct_models_use_the_same_profile(
    tmp_path: Path, monkeypatch
) -> None:
    """run_formal_suite spawns TLC directly for a few models; those launch
    sites must not drift back to an uncapped command."""
    import importlib.util
    import sys

    repo_root = Path(__file__).resolve().parents[2]
    spec = importlib.util.spec_from_file_location(
        "run_formal_suite", repo_root / "scripts" / "run_formal_suite.py"
    )
    suite = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    # Registered before exec: the module defines dataclasses, whose field
    # resolution looks the defining module up in sys.modules.
    sys.modules[spec.name] = suite
    spec.loader.exec_module(suite)

    monkeypatch.setattr(formal_tlc.os, "cpu_count", lambda: 12)
    monkeypatch.setattr(suite, "_find_jar", lambda: Path("/tmp/tla2tools.jar"))
    # The direct run creates its TLC metadir under SUITE_OUT; keep it out of
    # the checkout.
    monkeypatch.setattr(suite, "SUITE_OUT", tmp_path / "suite")
    seen: list[list[str]] = []

    def run(cmd, **_kwargs):
        seen.append(list(cmd))
        return SimpleNamespace(
            returncode=0,
            stdout="Model checking completed. No error has been found.\n",
            stderr="",
        )

    monkeypatch.setattr(subprocess, "run", run)
    suite._run_direct_model(suite.DIRECT_SMOKE_MODELS[0])

    cmd = seen[0]
    assert f"-Xmx{DEFAULT_TLC_HEAP}" in cmd
    assert "-XX:ActiveProcessorCount=4" in cmd
    assert cmd[cmd.index("-workers") + 1] == "4"
    assert cmd.index(f"-Xmx{DEFAULT_TLC_HEAP}") < cmd.index("tlc2.TLC")
