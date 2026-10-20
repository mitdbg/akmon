"""One version: pyproject.toml, `okg.__version__` and `okg --version` agree.

PACT release-tag-tooling (carrying repo-versioning-and-release-process's
task.repover.single-version-source). The declaration is read from the file on
disk, so an install whose metadata is stale against pyproject.toml fails here.
"""

from __future__ import annotations

import importlib.metadata
import tomllib
from pathlib import Path

import pytest

import okg
from okg.substrate.cli.main import main

PYPROJECT = Path(__file__).resolve().parents[2] / "pyproject.toml"


def declared() -> str:
    return tomllib.loads(PYPROJECT.read_text(encoding="utf-8"))["project"]["version"]


def test_okg_version_equals_the_pyproject_declaration() -> None:
    assert okg.__version__ == declared()


def test_okg_version_flag_equals_the_pyproject_declaration(capsys) -> None:
    with pytest.raises(SystemExit) as exc:
        main(["--version"])
    assert exc.value.code == 0
    assert capsys.readouterr().out == f"okg {declared()}\n"


def test_okg_version_refuses_without_package_metadata(monkeypatch) -> None:
    def missing(name: str) -> str:
        raise importlib.metadata.PackageNotFoundError(name)

    monkeypatch.setattr(importlib.metadata, "version", missing)
    with pytest.raises(AttributeError, match="no installed package metadata"):
        _ = okg.__version__


def test_other_missing_attributes_still_raise_attribute_error() -> None:
    assert not hasattr(okg, "definitely_not_an_attribute")
