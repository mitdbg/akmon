"""okg entry points keep Python's bytecode cache out of deployment folders.

okg imports Python that lives in a deployment folder (the codebase-index
starter's ``doctor_checks.py``, a deployment's own sources and enrichers).
Python used to write ``__pycache__/*.pyc`` next to each such file, inside the
deployment's Git repository, and the publish's provenance check then refused
the untracked files: on a fresh codebase-index render, ``okg doctor``
followed by ``okg run --once --apply`` exited 1 with "repository provenance
inputs are not committed".

The process entry points now set ``sys.pycache_prefix`` to the per-user okg
cache (``okg.substrate.bytecode_cache``), so bytecode is read and written
only there. These tests start okg the way an operator does, as a separate
process through ``running_okg_argv()``, because an in-process
``cli.main(...)`` call deliberately leaves the interpreter alone.
"""

from __future__ import annotations

import ast
import json
import os
import subprocess
import sys
import tomllib
from pathlib import Path

import pytest

import okg.substrate.cli.main as cli
from okg.substrate.cli.child_argv import running_okg_argv
from tests.substrate._deployment_repo import commit_deployment_tree

REPO_ROOT = Path(__file__).resolve().parents[3]
TEMPLATE = "codebase-index"
DEPLOYMENT = "bytecode_provenance"


def _child_env(**extra: str) -> dict[str, str]:
    env = {
        key: value for key, value in os.environ.items()
        if key not in {"PYTHONPYCACHEPREFIX", "PYTHONDONTWRITEBYTECODE"}
    }
    env.update(extra)
    return env


def _python(code: str, **extra_env: str) -> str:
    # The environment is built here, not passed in: pytest prints a failing
    # frame's arguments, and the inherited environment can hold credentials.
    proc = subprocess.run(
        [sys.executable, "-P", "-c", code],
        env=_child_env(**extra_env), capture_output=True, text=True, check=False,
    )
    assert proc.returncode == 0, proc.stderr[-3000:]
    return proc.stdout.strip()


def _pycache_dirs(root: Path) -> list[str]:
    return sorted(str(p.relative_to(root)) for p in root.rglob("__pycache__"))


def _render(tmp_path: Path) -> Path:
    repo = tmp_path / "repo"
    repo.mkdir()
    (repo / "a.py").write_text("def f():\n    return 1\n", encoding="utf-8")
    commit_deployment_tree(repo, "starter code repository")
    claude_projects = tmp_path / "claude-projects"
    claude_projects.mkdir()
    target = tmp_path / "deployments"
    rc = cli.main([
        "init",
        "--template", TEMPLATE,
        "--target", str(target),
        "--repo-root", str(repo),
        "--claude-projects-root", str(claude_projects),
        "--deployment-name", DEPLOYMENT,
        "--no-publish",
        "--non-interactive",
        "--yes",
    ])
    assert rc == 0, f"okg init --template {TEMPLATE} failed"
    deployment_dir = target / DEPLOYMENT
    commit_deployment_tree(target, "okg init")
    return deployment_dir


def test_the_okg_console_script_is_the_process_entry_point() -> None:
    scripts = tomllib.loads((REPO_ROOT / "pyproject.toml").read_text())["project"]["scripts"]
    assert scripts["okg"] == "okg.substrate.cli.main:console_main"


def test_importing_okg_leaves_the_interpreter_bytecode_settings_alone(
    tmp_path: Path,
) -> None:
    code = (
        "import sys, os, okg.substrate.cli.main, okg.substrate.runtime.worker; "
        "print(repr(sys.pycache_prefix), repr(os.environ.get('PYTHONPYCACHEPREFIX')))"
    )
    out = _python(code, OKG_CACHE_HOME=str(tmp_path / "cache"))
    assert out == "None None"


def test_entry_point_moves_deployment_bytecode_to_the_user_cache(
    tmp_path: Path,
) -> None:
    """A deployment-local module imported after the entry point writes no
    ``__pycache__`` beside itself; its bytecode lands under the user cache."""
    deployment = tmp_path / "deployments" / "local"
    deployment.mkdir(parents=True)
    (deployment / "local_checks.py").write_text("VALUE = 1\n", encoding="utf-8")
    cache = tmp_path / "cache"
    code = (
        "import os, sys\n"
        "from pathlib import Path\n"
        "from okg.substrate.bytecode_cache import use_user_bytecode_cache\n"
        "from okg.substrate.deployments import inject_deployment_path\n"
        "prefix = use_user_bytecode_cache()\n"
        f"inject_deployment_path(Path({str(deployment)!r}))\n"
        "import local_checks\n"
        "print(prefix)\n"
        "print(os.environ['PYTHONPYCACHEPREFIX'])\n"
    )
    out = _python(code, OKG_CACHE_HOME=str(cache)).splitlines()
    assert out == [str(cache / "pycache")] * 2
    assert _pycache_dirs(deployment) == []
    cached = list((cache / "pycache").rglob("local_checks.*.pyc"))
    assert cached, sorted(str(p) for p in (cache / "pycache").rglob("*.pyc"))[:5]


def test_an_explicit_user_prefix_is_kept(tmp_path: Path) -> None:
    explicit = tmp_path / "explicit"
    code = (
        "import os, sys\n"
        "from okg.substrate.bytecode_cache import use_user_bytecode_cache\n"
        "print(use_user_bytecode_cache())\n"
        "print(sys.pycache_prefix)\n"
        "print(os.environ['PYTHONPYCACHEPREFIX'])\n"
    )
    out = _python(
        code, OKG_CACHE_HOME=str(tmp_path / "cache"), PYTHONPYCACHEPREFIX=str(explicit),
    ).splitlines()
    assert out == [str(explicit)] * 3


# Every process entry point that can load deployment code, with the
# function (or the module's `if __name__ == "__main__":` block) that must call
# enter_user_bytecode_cache. Each starts a process okg owns; none runs on
# import.
# Literal joins: the database lane's scope guard refuses a path built from a
# variable segment.
_ENTRY_POINTS = (
    (REPO_ROOT / "src/okg/substrate/cli/main.py", "console_main"),
    (REPO_ROOT / "src/okg/substrate/runtime/worker.py", "_cli_entry"),
    (REPO_ROOT / "src/okg/substrate/mcp/server.py", "__main__"),
    (REPO_ROOT / "src/okg/substrate/mcp/deployment_entry.py", "run_for_deployment"),
    (REPO_ROOT / "src/okg/viz/__main__.py", "__main__"),
)


def _entry_node(tree: ast.Module, name: str) -> ast.AST:
    for node in tree.body:
        if name == "__main__" and isinstance(node, ast.If):
            test = node.test
            if (
                isinstance(test, ast.Compare)
                and isinstance(test.left, ast.Name)
                and test.left.id == "__name__"
            ):
                return node
        if isinstance(node, ast.FunctionDef) and node.name == name:
            return node
    raise AssertionError(f"no {name} entry")


@pytest.mark.parametrize(
    ("path", "entry"),
    _ENTRY_POINTS,
    ids=[str(p.relative_to(REPO_ROOT)) for p, _ in _ENTRY_POINTS],
)
def test_each_process_entry_point_moves_the_bytecode_cache(path: Path, entry: str) -> None:
    tree = ast.parse(path.read_text(encoding="utf-8"))
    calls = [
        node for node in ast.walk(_entry_node(tree, entry))
        if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Name)
        and node.func.id == "enter_user_bytecode_cache"
    ]
    assert calls, f"{path}:{entry} does not call enter_user_bytecode_cache()"


def test_the_codebase_index_server_launches_through_run_for_deployment() -> None:
    server = REPO_ROOT / "src/okg/substrate/library/templates/codebase-index/server.py"
    tree = ast.parse(server.read_text(encoding="utf-8"))
    called = {
        node.func.id for node in ast.walk(tree)
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Name)
    }
    assert "run_for_deployment" in called


def test_a_relative_cache_home_gives_an_absolute_prefix(tmp_path: Path) -> None:
    code = (
        "from okg.substrate.bytecode_cache import use_user_bytecode_cache\n"
        "print(use_user_bytecode_cache())\n"
    )
    proc = subprocess.run(
        [sys.executable, "-P", "-c", code], cwd=tmp_path,
        env=_child_env(OKG_CACHE_HOME="rel/cache"),
        capture_output=True, text=True, check=False,
    )
    assert proc.returncode == 0, proc.stderr[-3000:]
    prefix = Path(proc.stdout.strip())
    assert prefix == (tmp_path / "rel" / "cache" / "pycache").resolve()
    assert oct(prefix.stat().st_mode & 0o777) == oct(0o700)


def test_a_cache_others_can_write_is_refused(tmp_path: Path) -> None:
    cache = tmp_path / "cache"
    (cache / "pycache").mkdir(parents=True)
    (cache / "pycache").chmod(0o770)
    proc = subprocess.run(
        [*running_okg_argv(), "--help"], cwd=tmp_path,
        env=_child_env(OKG_CACHE_HOME=str(cache)),
        capture_output=True, text=True, check=False,
    )
    assert proc.returncode == 2, proc.stdout[-2000:]
    assert "must be owned by this user with mode 0700" in proc.stderr
    assert "found uid" in proc.stderr and "mode 0770" in proc.stderr


def _okg_help(tmp_path: Path, *python_flags: str, **extra_env: str) -> subprocess.CompletedProcess[str]:
    argv = running_okg_argv()
    return subprocess.run(
        [argv[0], *python_flags, *argv[1:], "--help"], cwd=tmp_path,
        env=_child_env(**extra_env), capture_output=True, text=True, check=False,
    )


def test_a_symlinked_cache_directory_is_refused(tmp_path: Path) -> None:
    cache = tmp_path / "cache"
    cache.mkdir()
    real = tmp_path / "elsewhere"
    real.mkdir(mode=0o700)
    (cache / "pycache").symlink_to(real)
    proc = _okg_help(tmp_path, OKG_CACHE_HOME=str(cache))
    assert proc.returncode == 2, proc.stdout[-2000:]
    assert "must be a real directory" in proc.stderr


@pytest.mark.parametrize("mode", [0o757, 0o777], ids=["0757", "0777"])
def test_a_parent_others_can_write_without_the_sticky_bit_is_refused(
    tmp_path: Path, mode: int,
) -> None:
    cache = tmp_path / "cache"
    cache.mkdir()
    cache.chmod(mode)
    proc = _okg_help(tmp_path, OKG_CACHE_HOME=str(cache))
    assert proc.returncode == 2, proc.stdout[-2000:]
    assert f"writable by others (mode {mode:04o}) without the sticky bit" in proc.stderr
    assert not (cache / "pycache").exists()


@pytest.mark.parametrize(
    "mode", [0o755, 0o775, 0o1777], ids=["0755", "0775-umask-002", "1777-sticky"],
)
def test_an_ordinary_group_writable_or_sticky_parent_is_accepted(
    tmp_path: Path, mode: int,
) -> None:
    cache = tmp_path / "cache"
    cache.mkdir()
    cache.chmod(mode)
    proc = _okg_help(tmp_path, OKG_CACHE_HOME=str(cache))
    assert proc.returncode == 0, proc.stderr[-2000:]
    assert oct((cache / "pycache").stat().st_mode & 0o777) == oct(0o700)


def test_bytecode_writing_off_needs_no_cache(tmp_path: Path) -> None:
    """With -B Python writes no bytecode anywhere: the hook changes nothing
    and needs no directory, so an unusable cache does not stop okg."""
    cache = tmp_path / "cache"
    cache.mkdir()
    cache.chmod(0o777)
    proc = _okg_help(tmp_path, "-B", OKG_CACHE_HOME=str(cache))
    assert proc.returncode == 0, proc.stderr[-2000:]
    assert not (cache / "pycache").exists()
    code = (
        "import os, sys\n"
        "from okg.substrate.bytecode_cache import use_user_bytecode_cache\n"
        "print(use_user_bytecode_cache(), sys.pycache_prefix, os.environ.get('PYTHONPYCACHEPREFIX'))\n"
    )
    out = _python(code, OKG_CACHE_HOME=str(cache), PYTHONDONTWRITEBYTECODE="1")
    assert out == "None None None"
    assert not (cache / "pycache").exists()


def test_no_home_directory_is_a_clear_error(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str],
) -> None:
    import okg.substrate.deployments as deployments
    from okg.substrate import bytecode_cache

    def no_home() -> Path:
        raise RuntimeError("Could not determine home directory.")

    # Nothing is changed before the refusal, so the test process is safe.
    monkeypatch.setattr(sys, "pycache_prefix", None)
    monkeypatch.setattr(sys, "dont_write_bytecode", False)
    monkeypatch.setattr(deployments, "_cache_dir", no_home)
    with pytest.raises(SystemExit) as exited:
        bytecode_cache.enter_user_bytecode_cache()
    assert exited.value.code == 2
    err = capsys.readouterr().err
    assert "cannot locate the per-user okg cache" in err
    assert "OKG_CACHE_HOME" in err
    assert sys.pycache_prefix is None


@pytest.mark.postgres
def test_codebase_index_publishes_after_doctor_imports_its_checks(
    tmp_path: Path,
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.delenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", raising=False)
    # okg init inserts the deployment folder on sys.path in this process;
    # keep that and any imported deployment module from leaking out.
    monkeypatch.setattr(sys, "path", list(sys.path))
    monkeypatch.delitem(sys.modules, "doctor_checks", raising=False)
    deployment_dir = _render(tmp_path)
    cache = tmp_path / "cache"
    env = _child_env(
        OKG_DEPLOYMENTS_DIR=str(deployment_dir.parent),
        OKG_DSN=clean_runtime,
        OKG_CACHE_HOME=str(cache),
    )
    env.pop("OKG_NOMOS_RUNTIME_ENFORCEMENT", None)

    def okg(*args: str) -> subprocess.CompletedProcess[str]:
        return subprocess.run(
            [*running_okg_argv(), *args], env=env, cwd=tmp_path,
            capture_output=True, text=True, check=False,
        )

    for args in (
        ("--json", "migrate", "--deployment", DEPLOYMENT, "--apply"),
        ("--json", "catalog", "load", "--deployment", str(deployment_dir), "--apply"),
    ):
        proc = okg(*args)
        assert proc.returncode == 0, f"{args[1]}: {proc.stdout[-2000:]}{proc.stderr[-2000:]}"

    # Any doctor run loads the deployment's doctor_checks module. Its exit
    # code is not the subject here: --no-publish seeds no Nomos bundle, so
    # the nomos check reports an error.
    okg("--json", "doctor", "--deployment", DEPLOYMENT, "--check", "nomos")
    assert list((cache / "pycache").rglob("doctor_checks.*.pyc")), (
        "okg doctor wrote no bytecode for doctor_checks.py under the user "
        "cache; the publish below would prove nothing"
    )
    assert _pycache_dirs(deployment_dir) == []

    proc = okg("--json", "run", "--once", "--deployment", DEPLOYMENT, "--apply")
    assert proc.returncode == 0, (
        f"okg run --once after okg doctor: {proc.stdout[-3000:]}{proc.stderr[-3000:]}"
    )
    run = json.loads(proc.stdout.strip().splitlines()[-1])
    assert run["status"] == "published", run
    assert run["generation_id"].startswith(f"okg:{DEPLOYMENT}:"), run
    assert _pycache_dirs(deployment_dir) == []
    capsys.readouterr()
