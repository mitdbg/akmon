"""A wheel carrying earlier releases' contracts is still a clean build (PACT release-workflow).

The release cut and the release image build stage earlier contracts in
build/release-contracts, and setup.py bundles them under okg/_release_contracts.
Those files are generated from release assets, not tracked under src/okg, so
setup.py's "the build ships a file HEAD does not track" check must not count
them: otherwise every release wheel built in a checkout would record
source_dirty true and fail the prod channel.
"""

from __future__ import annotations

import runpy
import shutil
import subprocess
from pathlib import Path

import pytest
import setuptools

REPO_ROOT = Path(__file__).resolve().parents[2]


def _checkout(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> tuple[Path, dict]:
    if shutil.which("git") is None:
        pytest.fail("git is required to build the checkout this test inspects")
    checkout = tmp_path / "checkout"
    (checkout / "src" / "okg").mkdir(parents=True)
    (checkout / "src" / "okg" / "__init__.py").write_text("", encoding="utf-8")
    shutil.copy2(REPO_ROOT / "setup.py", checkout / "setup.py")
    for key, value in {
        "GIT_AUTHOR_NAME": "t", "GIT_AUTHOR_EMAIL": "t@example.invalid",
        "GIT_COMMITTER_NAME": "t", "GIT_COMMITTER_EMAIL": "t@example.invalid",
    }.items():
        monkeypatch.setenv(key, value)
    for name in ("OKG_BUILD_SOURCE_REVISION", "OKG_BUILD_SOURCE_TREE", "OKG_BUILD_SOURCE_DIRTY"):
        monkeypatch.delenv(name, raising=False)
    subprocess.run(["git", "init", "-q", str(checkout)], check=True)
    subprocess.run(["git", "-C", str(checkout), "add", "setup.py", "src"], check=True)
    subprocess.run(
        ["git", "-C", str(checkout), "-c", "commit.gpgsign=false", "commit", "-qm", "c"], check=True
    )
    monkeypatch.setattr(setuptools, "setup", lambda **_: None)
    return checkout, runpy.run_path(str(checkout / "setup.py"))


def _build_lib(tmp_path: Path, *extra: str) -> Path:
    build_lib = tmp_path / "build-lib"
    files = ["okg/__init__.py", "okg/_release_contracts/okg-contract-v0.2.0.json.gz", *extra]
    for relative in files:
        path = build_lib / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(b"x")
    return build_lib


def test_bundled_contracts_do_not_make_the_release_wheel_dirty(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    _checkout_dir, setup = _checkout(tmp_path, monkeypatch)
    build_lib = _build_lib(tmp_path)
    assert setup["_built_files_untracked"](str(build_lib)) == []
    assert setup["_build_info"](str(build_lib))["source_dirty"] is False


def test_an_untracked_module_still_makes_the_wheel_dirty(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    _checkout_dir, setup = _checkout(tmp_path, monkeypatch)
    build_lib = _build_lib(tmp_path, "okg/stray.py", "okg/_release_contracts_extra/x.json.gz")
    assert setup["_built_files_untracked"](str(build_lib)) == [
        "okg/_release_contracts_extra/x.json.gz",
        "okg/stray.py",
    ]
    assert setup["_build_info"](str(build_lib))["source_dirty"] is True


def _build_py(checkout: Path, out: Path) -> subprocess.CompletedProcess[str]:
    import sys

    return subprocess.run(
        [sys.executable, "setup.py", "-q", "build_py", "--build-lib", str(out)],
        cwd=checkout, capture_output=True, text=True, check=False,
    )


def test_bundled_contracts_are_recorded_by_name_and_hash(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    import hashlib
    import json

    checkout, _setup = _checkout(tmp_path, monkeypatch)
    (checkout / "pyproject.toml").write_text(
        '[project]\nname = "okg"\nversion = "0.0.0"\n\n'
        '[tool.setuptools.packages.find]\nwhere = ["src"]\n',
        encoding="utf-8",
    )
    (checkout / "schemas").mkdir()
    (checkout / "schemas" / "x.json").write_text("{}", encoding="utf-8")
    plain = _build_py(checkout, tmp_path / "plain")
    assert plain.returncode == 0, plain.stderr
    info = json.loads((tmp_path / "plain" / "okg" / "_build_info.json").read_text(encoding="utf-8"))
    assert "release_contracts" not in info

    staged = checkout / "build" / "release-contracts"
    staged.mkdir(parents=True)
    data = b"\x1f\x8bcontract"
    (staged / "okg-contract-v0.2.0.json.gz").write_bytes(data)
    bundled = _build_py(checkout, tmp_path / "bundled")
    assert bundled.returncode == 0, bundled.stderr
    # A stale directory is not silent: the build says so and records it.
    assert "bundling earlier release contracts from build/release-contracts" in bundled.stderr
    info = json.loads((tmp_path / "bundled" / "okg" / "_build_info.json").read_text(encoding="utf-8"))
    assert info["release_contracts"] == {"okg-contract-v0.2.0.json.gz": hashlib.sha256(data).hexdigest()}
