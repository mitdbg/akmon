"""One version: pyproject.toml, `okg.__version__` and `okg --version` agree.

PACT release-tag-tooling (carrying repo-versioning-and-release-process's
task.repover.single-version-source). The declaration is read from the file on
disk, so an install whose metadata is stale against pyproject.toml fails here.
"""

from __future__ import annotations

import importlib.metadata
import re
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


def project_name() -> str:
    return tomllib.loads(PYPROJECT.read_text(encoding="utf-8"))["project"]["name"]


def test_the_distribution_is_akmon_and_the_import_package_is_okg() -> None:
    """PyPI project and wheel are akmon; `import okg` and `okg` stay okg."""
    assert project_name() == okg.DISTRIBUTION == "akmon"
    assert importlib.metadata.version(okg.DISTRIBUTION) == declared()
    # The import package okg comes from the akmon distribution, and no stale
    # okg distribution is installed beside it. A checkout that built a wheel
    # before the rename can hold src/okg.egg-info, which importlib reads as an
    # installed okg: remove it (and re-run `uv sync`) if this fails.
    assert okg.DISTRIBUTION in importlib.metadata.packages_distributions().get("okg", [])
    try:
        stale = importlib.metadata.distribution("okg")
    except importlib.metadata.PackageNotFoundError:
        stale = None
    assert stale is None, f"a stale okg distribution is installed at {getattr(stale, '_path', stale)}"


#: A package-metadata lookup of the framework by a literal name. The import
#: package is okg; the distribution is okg.DISTRIBUTION (akmon), so a literal
#: "okg" here would find nothing (or a stale install) at run time.
_LITERAL_DIST_LOOKUP = re.compile(
    r"""(?:metadata\.(?:version|distribution|metadata|requires|files|entry_points)"""
    r"""|\bversion|\bdistribution|\brequires)\(\s*(?:name\s*=\s*)?["'](?:okg|akmon)["']"""
)


def test_no_runtime_lookup_names_the_distribution_literally() -> None:
    root = PYPROJECT.parent / "src" / "okg"
    offenders = []
    for path in sorted(root.rglob("*.py")):
        for number, line in enumerate(path.read_text(encoding="utf-8").splitlines(), start=1):
            if _LITERAL_DIST_LOOKUP.search(line):
                offenders.append(f"{path.relative_to(PYPROJECT.parent)}:{number}: {line.strip()}")
    assert offenders == [], "\n".join(offenders)
