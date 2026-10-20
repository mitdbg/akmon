"""Sealed services never read bytecode from the sealed tree (issue #2861).

The release launcher starts every sealed service as ``python -I -B -X
pycache_prefix=<fresh private dir> <role arguments>``. ``-B`` writes no
bytecode; the private prefix means the interpreter never looks in a
``__pycache__`` directory inside the sealed tree. The launcher's receipt
records the prefix apart from the role arguments, and the two status checks
rebuild the exact live argv from them. Any other difference is still refused.
"""
from __future__ import annotations

import json
import os
import py_compile
import stat
import subprocess
import sys
import textwrap
import types
from pathlib import Path
from typing import Any, Mapping

import pytest

import okg.substrate.environment_services as environment_services
import okg.substrate.runtime.release_authority as release_authority
from okg.substrate.cli.child_argv import inherited_bytecode_flags, running_okg_argv
from okg.substrate.environment_services import environment_service_authority_status
from okg.substrate.errors import SealedReleaseInvalid
from okg.substrate.release_activation import (
    activate_environment_release,
    launcher_descriptor_path,
)
from okg.substrate.sealed_release import prepare_environment_release
from okg.substrate.deployments.environment import load_environment
from tests.substrate.test_environment_services import _environment_fixture, _load
from tests.substrate.test_sealed_releases import _environment_values, _fixture_repo

_LAUNCHER = Path(__file__).resolve().parents[2] / (
    "src/okg/substrate/runtime/release_launcher.py"
)
_SERVICE_ID = "org.okg.probe.fixture-production"


@pytest.fixture(autouse=True)
def _restore_tmp_permissions(tmp_path: Path):
    yield
    for path in [tmp_path, *tmp_path.rglob("*")]:
        if path.is_symlink():
            continue
        try:
            path.chmod(stat.S_IMODE(path.stat().st_mode) | 0o700)
        except OSError:
            # Best-effort fixture cleanup; the test failure remains primary.
            pass


def _real_interpreter_sync(
    repo_root: Path,
    venv_root: Path,
    environ: Mapping[str, str],
) -> None:
    """A sealed venv whose interpreter is this Python and holds one probe module."""
    del repo_root, environ
    bin_root = venv_root / "bin"
    bin_root.mkdir(parents=True)
    interpreter = bin_root / "python3"
    interpreter.write_text(
        f'#!/bin/sh\nexec "{sys.executable}" "$@"\n',
        encoding="utf-8",
    )
    interpreter.chmod(0o755)
    site = venv_root / "lib/python3.12/site-packages"
    (site / "okg").mkdir(parents=True)
    (site / "okg/__init__.py").write_text(
        "__version__ = 'fixture'\n", encoding="utf-8"
    )
    (site / "sealed_probe.py").write_text("VALUE = 'genuine'\n", encoding="utf-8")


def _planted_bytecode(scratch: Path, *, dfile: Path) -> Path:
    """Compile a .pyc that Python loads without checking its source file."""
    planted_source = scratch / "sealed_probe.py"
    planted_source.parent.mkdir(parents=True, exist_ok=True)
    planted_source.write_text("VALUE = 'planted'\n", encoding="utf-8")
    compiled = scratch / "planted.pyc"
    py_compile.compile(
        str(planted_source),
        cfile=str(compiled),
        dfile=str(dfile),
        doraise=True,
        invalidation_mode=py_compile.PycInvalidationMode.UNCHECKED_HASH,
    )
    return compiled


def test_launched_service_does_not_load_bytecode_planted_after_verification(
    tmp_path: Path,
) -> None:
    """Plant a .pyc between the launcher's tree check and the service start."""
    repo, commit = _fixture_repo(tmp_path)
    values = _environment_values(tmp_path / "state")
    environment = load_environment(
        repo / "ops/environments/fixture-production/environment.yaml",
        environ=values,
    )
    release = prepare_environment_release(
        environment=environment,
        source_repo=repo,
        revision=commit,
        dependency_sync=_real_interpreter_sync,
        environ=values,
    )
    activate_environment_release(
        environment=environment,
        release=release,
        source_repo=repo,
        activated_by="test",
        environ=values,
    )
    site = release.venv_root / "lib/python3.12/site-packages"
    probe = (
        "import json, sys\n"
        f"sys.path.insert(0, {str(site)!r})\n"
        "import sealed_probe\n"
        "print(json.dumps({'value': sealed_probe.VALUE,"
        " 'file': sealed_probe.__spec__.cached,"
        " 'dont_write_bytecode': sys.flags.dont_write_bytecode,"
        " 'pycache_prefix': sys.pycache_prefix,"
        " 'argv': sys.orig_argv[1:]}))\n"
    )
    planted = _planted_bytecode(tmp_path / "scratch", dfile=site / "sealed_probe.py")
    target = site / "__pycache__" / f"sealed_probe.{sys.implementation.cache_tag}.pyc"
    # The driver verifies the release, then plants the .pyc, then starts the
    # service: the window between the launcher's tree check and the exec.
    driver = textwrap.dedent(
        f"""
        import importlib.util, os, shutil, stat
        spec = importlib.util.spec_from_file_location("launcher", {str(_LAUNCHER)!r})
        launcher = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(launcher)
        plan = launcher.verify_launch({str(launcher_descriptor_path(environment))!r})
        site = {str(site)!r}
        mode = stat.S_IMODE(os.stat(site).st_mode)
        os.chmod(site, mode | stat.S_IWUSR)
        os.mkdir(os.path.join(site, "__pycache__"))
        shutil.copyfile({str(planted)!r}, {str(target)!r})
        os.chmod(site, mode)
        launcher._execute_plan(plan, command=["-c", {probe!r}], environ=None)
        """
    )
    proc = subprocess.run(
        [sys.executable, "-c", driver],
        env={**values, "OKG_SERVICE_ID": _SERVICE_ID},
        check=False,
        capture_output=True,
        text=True,
        timeout=120,
    )

    assert proc.returncode == 0, proc.stderr
    observed = json.loads(proc.stdout.strip().splitlines()[-1])
    assert target.is_file()  # the planted file is really there
    assert observed["value"] == "genuine"
    assert observed["dont_write_bytecode"] == 1
    prefix = observed["pycache_prefix"]
    cache_root = release_authority.service_pycache_root(
        environment.runtime.state_root
    )
    assert Path(prefix).parent == cache_root
    assert stat.S_IMODE(Path(prefix).stat().st_mode) == 0o700
    assert list(Path(prefix).iterdir()) == []  # -B: nothing written
    assert observed["argv"][:4] == ["-I", "-B", "-X", f"pycache_prefix={prefix}"]

    receipt = json.loads(
        (
            environment.runtime.state_root / "service-authority" / f"{_SERVICE_ID}.json"
        ).read_text(encoding="utf-8")
    )
    assert receipt["arguments"] == ["-I", "-c", probe]
    assert receipt["pycache_prefix"] == prefix
    assert release_authority.receipt_launched_arguments(
        receipt,
        receipt["arguments"],
        state_root=environment.runtime.state_root,
    ) == observed["argv"]


def _ui_receipt_fixture(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> tuple[Any, dict[str, str], Path, Path, list[str], Path]:
    root, values = _environment_fixture(tmp_path)
    environment = _load(root, "fixture-staging", values)
    artifact_root = tmp_path / "artifact"
    interpreter = artifact_root / "venv/bin/python3"
    repo_root = artifact_root / "repo"
    interpreter.parent.mkdir(parents=True)
    interpreter.write_text("fixture\n", encoding="utf-8")
    repo_root.mkdir()
    release_status = {
        "ok": True,
        "status": "active",
        "artifact_id": "sha256:" + "b" * 64,
        "artifact_root": str(artifact_root),
        "definition_digest": environment.definition_digest,
    }
    monkeypatch.setattr(
        "okg.substrate.environment_services.environment_release_status",
        lambda *_args, **_kwargs: release_status,
    )
    monkeypatch.setattr(
        environment_services,
        "_supervisor_process_status",
        lambda *_args, **_kwargs: {
            "ok": True,
            "status": "running",
            "pid": os.getpid(),
            "manager_outcome": {"adapter": "systemd"},
        },
    )
    expected = list(
        environment_services._expected_service_arguments(
            environment, environment.ui.service
        )
    )
    prefix = release_authority.service_pycache_root(
        environment.runtime.state_root
    ) / "launch-abc123"
    receipt = (
        environment.runtime.state_root
        / "service-authority"
        / f"{environment.ui.service}.json"
    )
    receipt.parent.mkdir(parents=True)
    _write_receipt(
        receipt,
        {
            "schema": "okg.service_release_authority/v1",
            "service_id": environment.ui.service,
            "environment_id": environment.id,
            "artifact_id": release_status["artifact_id"],
            "artifact_root": str(artifact_root),
            "definition_digest": environment.definition_digest,
            "active_pointer": str(environment.release.active_pointer),
            "interpreter": str(interpreter),
            "repo_root": str(repo_root),
            "isolated": True,
            "arguments": expected,
            "pycache_prefix": str(prefix),
            "pid": os.getpid(),
            "process_start_identity": release_authority.process_start_identity(
                os.getpid()
            ),
        },
    )
    return environment, values, receipt, interpreter, expected, prefix


def _write_receipt(receipt: Path, payload: Mapping[str, Any]) -> None:
    receipt.write_text(json.dumps(dict(payload)), encoding="utf-8")
    receipt.chmod(0o600)


def _live(
    monkeypatch: pytest.MonkeyPatch,
    module: types.ModuleType,
    interpreter: Path,
    argv: list[str],
) -> None:
    monkeypatch.setattr(
        module,
        "process_runtime_identity",
        lambda *_args: {
            "executable": str(interpreter.resolve()),
            "argv": [str(interpreter), *argv],
        },
    )


def _flagged(expected: list[str], prefix: Path) -> list[str]:
    return ["-I", "-B", "-X", f"pycache_prefix={prefix}", *expected[1:]]


def test_service_authority_accepts_exactly_the_launcher_bytecode_flags(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    environment, values, _receipt, interpreter, expected, prefix = (
        _ui_receipt_fixture(tmp_path, monkeypatch)
    )
    _live(monkeypatch, environment_services, interpreter, _flagged(expected, prefix))

    status = environment_service_authority_status(
        environment, service_id=environment.ui.service, environ=values
    )

    assert status["ok"] is True, status
    assert status["authority"]["arguments"] == expected


@pytest.mark.parametrize(
    "variant",
    [
        "no_flags",
        "other_prefix",
        "extra_x_option",
        "flags_reordered",
        "flags_after_module",
        "missing_b",
        "duplicate_prefix",
        "changed_role_argument",
    ],
)
def test_service_authority_refuses_any_other_live_argv(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    variant: str,
) -> None:
    environment, values, _receipt, interpreter, expected, prefix = (
        _ui_receipt_fixture(tmp_path, monkeypatch)
    )
    option = f"pycache_prefix={prefix}"
    rest = expected[1:]
    live = {
        "no_flags": expected,
        "other_prefix": _flagged(expected, prefix.parent / "launch-other"),
        "extra_x_option": ["-I", "-B", "-X", option, "-X", "dev", *rest],
        "flags_reordered": ["-I", "-X", option, "-B", *rest],
        "flags_after_module": ["-I", *rest[:2], "-B", "-X", option, *rest[2:]],
        "missing_b": ["-I", "-X", option, *rest],
        "duplicate_prefix": ["-I", "-B", "-X", option, "-X", option, *rest],
        "changed_role_argument": [*_flagged(expected, prefix)[:-1], "9999"],
    }[variant]
    _live(monkeypatch, environment_services, interpreter, live)

    status = environment_service_authority_status(
        environment, service_id=environment.ui.service, environ=values
    )

    assert status["ok"] is False
    assert "process arguments differ from sealed runtime" in status["message"]


@pytest.mark.parametrize(
    "variant",
    ["outside_root", "not_normalized", "relative", "null", "not_string"],
)
def test_service_authority_refuses_an_invalid_recorded_prefix(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    variant: str,
) -> None:
    environment, values, receipt, interpreter, expected, prefix = (
        _ui_receipt_fixture(tmp_path, monkeypatch)
    )
    recorded: object = {
        "outside_root": str(tmp_path / "elsewhere" / "launch-abc123"),
        "not_normalized": str(prefix.parent / ".." / prefix.parent.name / prefix.name),
        "relative": "service-pycache/launch-abc123",
        "null": None,
        "not_string": ["-B"],
    }[variant]
    raw = json.loads(receipt.read_text(encoding="utf-8"))
    raw["pycache_prefix"] = recorded
    _write_receipt(receipt, raw)
    # The live process matches whatever the receipt claims; only the recorded
    # prefix itself is wrong.
    live = (
        [*expected[:1], "-B", "-X", f"pycache_prefix={recorded}", *expected[1:]]
        if isinstance(recorded, str)
        else expected
    )
    _live(monkeypatch, environment_services, interpreter, live)

    status = environment_service_authority_status(
        environment, service_id=environment.ui.service, environ=values
    )

    assert status["ok"] is False
    assert "bytecode isolation is invalid" in status["message"]


def test_service_authority_refuses_flags_recorded_inside_the_role_arguments(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    environment, values, receipt, interpreter, expected, prefix = (
        _ui_receipt_fixture(tmp_path, monkeypatch)
    )
    raw = json.loads(receipt.read_text(encoding="utf-8"))
    raw["arguments"] = _flagged(expected, prefix)
    del raw["pycache_prefix"]
    _write_receipt(receipt, raw)
    _live(monkeypatch, environment_services, interpreter, _flagged(expected, prefix))

    status = environment_service_authority_status(
        environment, service_id=environment.ui.service, environ=values
    )

    assert status["ok"] is False
    assert "configured service role" in status["message"]


def test_service_receipt_without_prefix_is_refused_fail_closed(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    environment, values, receipt, interpreter, expected, prefix = (
        _ui_receipt_fixture(tmp_path, monkeypatch)
    )
    raw = json.loads(receipt.read_text(encoding="utf-8"))
    del raw["pycache_prefix"]
    _write_receipt(receipt, raw)

    results = []
    for live in (expected, _flagged(expected, prefix)):
        _live(monkeypatch, environment_services, interpreter, live)
        results.append(
            environment_service_authority_status(
                environment, service_id=environment.ui.service, environ=values
            )
        )

    for status in results:
        assert status["ok"] is False
        assert "records no bytecode cache prefix" in status["message"]


def test_dead_service_with_a_pre_2861_receipt_reports_not_alive(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A receipt that only proves a process is dead needs no cache prefix.

    The prefix is checked with the live argv, after the liveness check, so
    stopping an old service lets its handoff complete.
    """
    environment, values, receipt, interpreter, expected, _prefix = (
        _ui_receipt_fixture(tmp_path, monkeypatch)
    )
    raw = json.loads(receipt.read_text(encoding="utf-8"))
    del raw["pycache_prefix"]
    _write_receipt(receipt, raw)
    monkeypatch.setattr(environment_services, "pid_alive", lambda _pid: False)

    status = environment_service_authority_status(
        environment, service_id=environment.ui.service, environ=values
    )

    assert status["ok"] is False
    assert status["reason_code"] == "service_process_not_alive"


def _worker_fixture(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> tuple[Any, dict[str, str], Path, Path, list[str]]:
    root, values = _environment_fixture(tmp_path)
    environment = _load(root, "fixture-staging", values)
    artifact_root = tmp_path / "artifact"
    executable = artifact_root / "venv/bin/python3"
    package = artifact_root / "venv/lib/site-packages/okg/__init__.py"
    definition_root = artifact_root / "repo/definition"
    for path in (executable, package, definition_root / "definition.yaml"):
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("fixture\n", encoding="utf-8")
    release_status = {
        "ok": True,
        "status": "active",
        "artifact_id": "sha256:" + "a" * 64,
        "artifact_root": str(artifact_root),
        "definition_digest": environment.definition_digest,
    }
    monkeypatch.setattr(
        release_authority,
        "environment_release_status",
        lambda *_args, **_kwargs: release_status,
    )
    prefix = release_authority.service_pycache_root(
        environment.runtime.state_root
    ) / "launch-worker1"
    arguments = ["-I", "-m", "okg.substrate.runtime.worker"]
    release_authority.write_worker_release_authority(
        {
            "environment_id": environment.id,
            "tier": environment.tier,
            "deployment": environment.graph_deployment_name,
            "database": environment.database.name,
            "database_identity": environment.database.dbos_identity,
            "artifact_id": release_status["artifact_id"],
            "artifact_root": str(artifact_root),
            "definition_digest": environment.definition_digest,
            "active_pointer": str(environment.release.active_pointer),
            "definition_root": str(definition_root),
            "executable": str(executable),
            "package_file": str(package),
            "isolated": True,
            "arguments": arguments,
            "pycache_prefix": str(prefix),
            "pid": os.getpid(),
            "process_start_identity": release_authority.process_start_identity(
                os.getpid()
            ),
            "authority_path": str(
                environment.runtime.state_root / "worker-authority.json"
            ),
        }
    )
    return environment, values, executable, prefix, arguments


def test_worker_authority_accepts_flags_and_refuses_an_extra_option(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    environment, values, executable, prefix, arguments = _worker_fixture(
        tmp_path, monkeypatch
    )
    flagged = _flagged(arguments, prefix)

    _live(monkeypatch, release_authority, executable, flagged)
    ok = release_authority.environment_worker_authority_status(
        environment, environ=values, expected_arguments=arguments
    )
    _live(
        monkeypatch,
        release_authority,
        executable,
        [*flagged[:4], "-X", "importtime", *flagged[4:]],
    )
    extra = release_authority.environment_worker_authority_status(
        environment, environ=values, expected_arguments=arguments
    )
    _live(monkeypatch, release_authority, executable, arguments)
    unflagged = release_authority.environment_worker_authority_status(
        environment, environ=values, expected_arguments=arguments
    )

    assert ok["ok"] is True, ok
    assert extra["ok"] is False
    assert "process arguments differ from sealed runtime" in extra["message"]
    assert unflagged["ok"] is False
    assert "process arguments differ from sealed runtime" in unflagged["message"]


def test_worker_receipt_without_prefix_is_refused_fail_closed(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    environment, values, executable, _prefix, arguments = _worker_fixture(
        tmp_path, monkeypatch
    )
    path = environment.runtime.state_root / "worker-authority.json"
    raw = json.loads(path.read_text(encoding="utf-8"))
    del raw["pycache_prefix"]
    _write_receipt(path, raw)
    _live(monkeypatch, release_authority, executable, arguments)

    status = release_authority.environment_worker_authority_status(
        environment, environ=values, expected_arguments=arguments
    )

    assert status["ok"] is False
    assert "records no bytecode cache prefix" in status["message"]


def test_dead_worker_with_a_pre_2861_receipt_reports_not_alive(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    environment, values, _executable, _prefix, arguments = _worker_fixture(
        tmp_path, monkeypatch
    )
    path = environment.runtime.state_root / "worker-authority.json"
    raw = json.loads(path.read_text(encoding="utf-8"))
    del raw["pycache_prefix"]
    _write_receipt(path, raw)
    monkeypatch.setattr(release_authority, "pid_alive", lambda _pid: False)

    status = release_authority.environment_worker_authority_status(
        environment, environ=values, expected_arguments=arguments
    )

    assert status["ok"] is False
    assert status["reason_code"] == "worker_process_not_alive"


def test_split_takes_only_the_exact_leading_launcher_shape() -> None:
    split = release_authority.split_bytecode_isolation
    assert split(["-I", "-B", "-X", "pycache_prefix=/p/x", "-m", "m"]) == (
        ["-I", "-m", "m"],
        "/p/x",
    )
    for argv in (
        ["-I", "-m", "m"],
        ["-B", "-I", "-X", "pycache_prefix=/p/x", "-m", "m"],
        ["-I", "-B", "-X", "dev", "-m", "m"],
        ["-I", "-X", "pycache_prefix=/p/x", "-B", "-m", "m"],
    ):
        assert split(argv) == (argv, None)


def _flags_with(**overrides: int) -> Any:
    """``sys.flags`` as this interpreter has it, with named fields replaced."""
    real = sys.flags
    names = [name for name in dir(real) if not name.startswith("_")]
    values = {
        name: getattr(real, name)
        for name in names
        if not callable(getattr(real, name))
    }
    return types.SimpleNamespace(**{**values, **overrides})


def test_worker_self_record_refuses_flags_the_interpreter_does_not_have(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from tests.substrate.test_sealed_releases import _prepare_environment_fixture
    from okg.substrate.runtime.release_launcher import (
        build_exec_environment,
        verify_launch,
    )

    repo, commit = _fixture_repo(tmp_path)
    environment, release, values = _prepare_environment_fixture(
        repo, commit, tmp_path / "state"
    )
    activate_environment_release(
        environment=environment,
        release=release,
        source_repo=repo,
        activated_by="test",
        environ=values,
    )
    launched = build_exec_environment(
        verify_launch(launcher_descriptor_path(environment)), environ=values
    )
    package = release.venv_root / "lib/python3.12/site-packages/okg/__init__.py"
    prefix = str(tmp_path / "state" / "service-pycache" / "launch-x")
    monkeypatch.setattr(
        sys,
        "orig_argv",
        [str(release.interpreter), "-I", "-B", "-X", f"pycache_prefix={prefix}",
         "-m", "okg.substrate.runtime.worker"],
    )
    monkeypatch.setattr(sys, "pycache_prefix", None)

    for unflagged in (
        [str(release.interpreter), "-I", "-m", "okg.substrate.runtime.worker"],
        [str(release.interpreter), "-I", "-X", f"pycache_prefix={prefix}", "-B",
         "-m", "okg.substrate.runtime.worker"],
    ):
        monkeypatch.setattr(sys, "orig_argv", unflagged)
        with pytest.raises(SealedReleaseInvalid, match="did not start with the launcher"):
            release_authority.verify_worker_release_authority(
                deployment=environment.graph_deployment_name,
                dsn=values["TEST_PRODUCTION_DSN"],
                environ=launched,
                executable=release.interpreter,
                package_file=package,
                isolated=True,
            )
    monkeypatch.setattr(
        sys,
        "orig_argv",
        [str(release.interpreter), "-I", "-B", "-X", f"pycache_prefix={prefix}",
         "-m", "okg.substrate.runtime.worker"],
    )
    with pytest.raises(SealedReleaseInvalid, match="bytecode flags disagree"):
        release_authority.verify_worker_release_authority(
            deployment=environment.graph_deployment_name,
            dsn=values["TEST_PRODUCTION_DSN"],
            environ=launched,
            executable=release.interpreter,
            package_file=package,
            isolated=True,
        )

    monkeypatch.setattr(sys, "pycache_prefix", prefix)
    monkeypatch.setattr(sys, "flags", _flags_with(dont_write_bytecode=1))
    authority = release_authority.verify_worker_release_authority(
        deployment=environment.graph_deployment_name,
        dsn=values["TEST_PRODUCTION_DSN"],
        environ=launched,
        executable=release.interpreter,
        package_file=package,
        isolated=True,
    )
    assert authority is not None
    assert authority["arguments"] == ["-I", "-m", "okg.substrate.runtime.worker"]
    assert authority["pycache_prefix"] == prefix


def test_children_inherit_the_bytecode_flags_only_when_the_parent_has_them(
    tmp_path: Path,
) -> None:
    probe = (
        "import json\n"
        "from okg.substrate.cli.child_argv import running_okg_argv\n"
        "print(json.dumps(running_okg_argv()))\n"
    )
    prefix = tmp_path / "cache"
    prefix.mkdir()
    flagged = subprocess.run(
        [sys.executable, "-B", "-X", f"pycache_prefix={prefix}", "-c", probe],
        check=True, capture_output=True, text=True, timeout=120,
    )
    argv = json.loads(flagged.stdout)
    assert argv[1:4] == ["-B", "-X", f"pycache_prefix={prefix}"]
    assert argv[4:] == ["-P", "-m", "okg.substrate.cli.main"]

    if sys.flags.dont_write_bytecode and sys.pycache_prefix:
        pytest.skip("this test process itself runs bytecode-isolated")
    assert inherited_bytecode_flags() == []
    assert running_okg_argv() == [
        sys.executable, "-P", "-m", "okg.substrate.cli.main"
    ]


def test_extension_broker_child_inherits_the_bytecode_flags(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import okg.substrate.extension_broker as extension_broker
    from okg.substrate.extension_admission import execute_admitted_extension
    from okg.substrate.extension_broker import plan_extension_package
    from tests.substrate.extensions.test_external_extension_broker import (
        FIXTURE,
        _authorization,
    )

    prefix = tmp_path / "cache"
    prefix.mkdir(mode=0o700)
    launched: list[list[str]] = []
    real_run = extension_broker.subprocess.run

    def recording_run(argv: list[str], *args: Any, **kwargs: Any) -> Any:
        launched.append(list(argv))
        return real_run(argv, *args, **kwargs)

    plan = plan_extension_package(FIXTURE)
    admission, receipt, ledger, lease = _authorization(plan, "fixture.tool")
    monkeypatch.setattr(extension_broker.subprocess, "run", recording_run)
    monkeypatch.setattr(sys, "pycache_prefix", str(prefix))
    monkeypatch.setattr(sys, "flags", _flags_with(dont_write_bytecode=1))
    with lease:
        tool = execute_admitted_extension(
            plan,
            admission,
            receipt,
            ledger,
            extension_id="fixture.tool",
            binding_id="search-tool",
            capabilities=("provider.metadata.read",),
            payload={"query": "Alpha"},
        )

    assert tool.batch.observations == {"result": "Alpha"}
    assert launched == [
        [
            sys.executable,
            "-B",
            "-X",
            f"pycache_prefix={prefix}",
            "-m",
            "okg.substrate.extension_broker_runner",
        ]
    ]
    assert list(prefix.iterdir()) == []


def _launch_plan(tmp_path: Path) -> dict[str, str]:
    """The plan fields the prefix and role checks read."""
    authority_root = tmp_path / "state" / "service-authority"
    return {"service_authority_root": str(authority_root)}


def test_launcher_creates_the_launch_directory_under_a_private_root(
    tmp_path: Path,
) -> None:
    from okg.substrate.runtime.release_launcher import _private_pycache_prefix

    plan = _launch_plan(tmp_path)
    first = Path(_private_pycache_prefix(plan))
    second = Path(_private_pycache_prefix(plan))

    root = tmp_path / "state" / "service-pycache"
    assert first.parent == root and second.parent == root
    assert first != second
    assert not first.exists()  # the empty earlier directory was removed
    assert stat.S_IMODE(root.stat().st_mode) == 0o700
    assert stat.S_IMODE(second.stat().st_mode) == 0o700


@pytest.mark.parametrize("variant", ["symlink", "group_writable", "file"])
def test_launcher_refuses_an_unsafe_cache_root(
    tmp_path: Path,
    variant: str,
) -> None:
    from okg.substrate.runtime.release_launcher import (
        LauncherError,
        _private_pycache_prefix,
    )

    plan = _launch_plan(tmp_path)
    root = tmp_path / "state" / "service-pycache"
    root.parent.mkdir(parents=True)
    elsewhere = tmp_path / "attacker-owned"
    elsewhere.mkdir(mode=0o700)
    if variant == "symlink":
        root.symlink_to(elsewhere, target_is_directory=True)
    elif variant == "group_writable":
        root.mkdir()
        root.chmod(0o770)
    else:
        root.write_text("not a directory\n", encoding="utf-8")

    with pytest.raises(LauncherError, match="bytecode cache root"):
        _private_pycache_prefix(plan)
    assert list(elsewhere.iterdir()) == []


@pytest.mark.parametrize(
    "command",
    [
        ["-B", "-m", "okg.viz"],
        ["-I", "-sB", "-m", "okg.viz"],
        ["-X", "pycache_prefix=/tmp/x", "-m", "okg.viz"],
        ["-I", "-Xpycache_prefix=/tmp/x", "-m", "okg.viz"],
        ["-I", "-sXpycache_prefix=/x", "-m", "okg.viz"],
        ["-I", "-sX", "pycache_prefix=/x", "-m", "okg.viz"],
        ["-I", "--check-hash-based-pycs", "default", "-B", "-m", "okg.viz"],
        ["-I", "-W", "default", "-B", "-m", "okg.viz"],
        ["-I", "-X", "dev", "-B", "-m", "okg.viz"],
    ],
)
def test_launcher_refuses_role_arguments_that_set_bytecode_flags(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    command: list[str],
) -> None:
    from okg.substrate.runtime import release_launcher

    def unexpected(*_args: Any, **_kwargs: Any) -> None:
        raise AssertionError("the launcher must refuse before any other step")

    monkeypatch.setattr(release_launcher, "build_exec_environment", unexpected)
    monkeypatch.setattr(os, "execve", unexpected)

    with pytest.raises(release_launcher.LauncherError, match="must not set"):
        release_launcher._execute_plan(
            {"graph_deployment_name": "fixture", **_launch_plan(tmp_path)},
            command=command,
            environ={},
        )


@pytest.mark.parametrize(
    "command",
    [
        ["-I", "-m", "tool", "-B", "-X", "pycache_prefix=x"],
        ["-I", "script.py", "-B"],
        ["-I", "-mokg.viz", "-B"],
        ["-I", "-mokg.Bar"],
        ["-I", "-cimport B"],
        ["-I", "-c", "-B"],
        ["-I", "-sWerror::BytesWarning", "-m", "okg.viz"],
        ["-I", "-W", "error::BytesWarning", "-m", "okg.viz"],
        ["-I", "-Xdev", "-m", "okg.viz"],
    ],
)
def test_role_arguments_that_are_not_bytecode_flags_are_accepted(
    command: list[str],
) -> None:
    """An attached -m/-c value ends the interpreter options, and -W/-X take
    the rest of their token, so none of these sets -B or a cache prefix."""
    from okg.substrate.runtime.release_launcher import _refuse_role_bytecode_options

    _refuse_role_bytecode_options(command)


def test_sourceless_bytecode_is_out_of_scope_and_left_to_2859(
    tmp_path: Path,
) -> None:
    """Document the limit of #2861: a .pyc with no .py beside it still imports.

    The cache prefix only redirects ``__pycache__`` lookups for modules that
    have source. A sourceless ``.pyc`` placed directly in a sys.path directory
    is imported by name under ``-I -B -X pycache_prefix``. Refusing such files
    in the sealed tree is #2859's job; #2861's protection is complete only
    once #2859 lands.
    """
    site = tmp_path / "site"
    site.mkdir()
    source = tmp_path / "src" / "sourceless_probe.py"
    source.parent.mkdir()
    source.write_text("VALUE = 'sourceless'\n", encoding="utf-8")
    py_compile.compile(
        str(source), cfile=str(site / "sourceless_probe.pyc"), doraise=True
    )
    prefix = tmp_path / "cache"
    prefix.mkdir()

    proc = subprocess.run(
        [
            sys.executable, "-I", "-B", "-X", f"pycache_prefix={prefix}", "-c",
            f"import sys; sys.path.insert(0, {str(site)!r}); "
            "import sourceless_probe; print(sourceless_probe.VALUE)",
        ],
        check=True, capture_output=True, text=True, timeout=120,
    )

    assert proc.stdout.strip() == "sourceless"
