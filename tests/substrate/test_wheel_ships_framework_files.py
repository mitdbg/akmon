"""The wheel must carry every framework file the provenance proof demands.

The publisher proves the installed ``okg`` package is the pinned revision by
listing every file git tracks under ``src/okg`` and comparing each one with
the installed copy (``_installed_framework_matches_revision``). Any tracked
file the wheel leaves out therefore refuses every wheel-installed publish
with ``installed_file_missing`` -- which is what issue 1949 measured: 40
``bench/synth/world_prose/*.json`` fixtures were the first miss, and 303
tracked files in all were absent from the built wheel, including the whole
``substrate/library/templates/`` tree that ``library/resolver.py`` reads at
runtime.

So packaging and the proof have to agree, and neither file says so on its
own. This test builds the real wheel and holds them to it: every tracked
file under ``src/okg`` ships, except the trees ``_UNSHIPPED_FRAMEWORK_TREES``
declares the distribution does not carry, and each of those trees must
really be absent, so a stale entry cannot quietly widen the exemption.
"""

from __future__ import annotations

import os
import re
import shutil
import subprocess
import zipfile
from pathlib import Path

import pytest
import yaml

from okg.substrate.deployment_upgrade import (
    _UNSHIPPED_FRAMEWORK_TREES,
    _installed_framework_matches_revision,
)

# Deliberately NOT marked `slow`, though the wheel build costs a few seconds:
# `scripts/ci/lane_coverage.py` reports a `slow` file under tests/substrate as
# RUN BY NOTHING, and a packaging guard no CI job runs guards nothing.
REPO_ROOT = Path(__file__).resolve().parents[2]
# What the build backend reads: the project files, the packaged sources, the
# canonical schemas `setup.py` stages, and the ops/pg tree MANIFEST.in ships.
BUILD_INPUTS = (
    "pyproject.toml",
    "setup.py",
    "MANIFEST.in",
    "README.md",
    "LICENSE",
    "NOTICE.archi-physics-cooper",
    "CHANGELOG.md",
    "INSTALL.md",
    "schemas",
    "src",
    "ops",
)
UNSHIPPED_PREFIXES = tuple(os.fsdecode(tree) for tree in _UNSHIPPED_FRAMEWORK_TREES)
# The only wheel trees whose `.py` files are DATA, never imported as `okg.*`.
# Each is loaded by file path, and each sits under a hyphenated directory no
# dotted import can name. Any other `.py` outside a regular package is
# importable code riding in the wheel as package data (issue #1969), so the
# exemption names these exact trees, not their `templates/` or `profiles/`
# parents, whose identifier-named children (`templates/devops`,
# `profiles/_test`) would otherwise hide a stray module.
PATH_LOADED_PYTHON_TREES = (
    # The codebase-index deployment template copies `server.py` and
    # `doctor_checks.py` into the generated deployment, which runs them there.
    re.compile(r"^substrate/library/templates/codebase-index/"),
    # A profile's `cli/` plugin (for example `profiles/academic-researcher/cli/`) is
    # loaded with `spec_from_file_location` by `cli/profile_cli_loader.py`.
    re.compile(r"^substrate/library/profiles/[^/]+/cli/"),
)
# Shipped YAML that names framework code by dotted path: a profile's
# `source-defaults/*.yaml` and a template's `source_registry.yaml` name each
# adapter's `module:`, and an ontology's `module.yaml` names each source
# `class:` as `<module>.<Class>`.
SOURCE_DECLARATIONS = re.compile(
    r"^substrate/library/(?:"
    r"profiles/[^/]+/source-defaults/[^/]+\.yaml"
    r"|templates/[^/]+/source_registry\.yaml"
    r"|ontologies/[^/]+/module\.yaml"
    r")$"
)


def _tracked_paths(*pathspecs: str) -> list[str]:
    payload = subprocess.run(
        ["git", "-C", str(REPO_ROOT), "ls-files", "-z", "--", *pathspecs],
        capture_output=True,
        check=True,
    ).stdout
    return [os.fsdecode(entry) for entry in payload.split(b"\0") if entry]


@pytest.fixture(scope="module")
def built_wheel(tmp_path_factory: pytest.TempPathFactory) -> zipfile.ZipFile:
    """Build a wheel from a copy of the tracked working tree.

    A copy, not the checkout: the build backend writes ``build/`` beside
    ``pyproject.toml``, and a test has no business leaving residue in the
    repository or racing another worker doing the same. The copy carries the
    WORKING TREE's bytes for tracked paths, so a packaging edit is tested
    before it is committed rather than after.
    """
    if shutil.which("uv") is None:
        pytest.fail("uv is required to build the wheel this test inspects")
    source = tmp_path_factory.mktemp("wheel-source")
    for relative in _tracked_paths(*BUILD_INPUTS):
        origin = REPO_ROOT / relative
        if not origin.is_file():
            # A tracked path that is a submodule or a broken link is not
            # build input; the build would not read it either.
            continue
        target = source / relative
        target.parent.mkdir(parents=True, exist_ok=True)
        try:
            os.link(origin, target)
        except OSError:
            shutil.copy2(origin, target)
    output = tmp_path_factory.mktemp("wheel-output")
    build = subprocess.run(
        ["uv", "build", "--wheel", "--out-dir", str(output)],
        cwd=source,
        capture_output=True,
        text=True,
        timeout=900,
    )
    assert build.returncode == 0, (
        f"uv build --wheel failed:\n{build.stdout}\n{build.stderr}"
    )
    wheels = sorted(output.glob("*.whl"))
    assert len(wheels) == 1, f"expected one wheel, got {wheels}"
    return zipfile.ZipFile(wheels[0])


def _wheel_framework_members(wheel: zipfile.ZipFile) -> set[str]:
    prefix = "okg/"
    return {
        name[len(prefix) :]
        for name in wheel.namelist()
        if name.startswith(prefix) and not name.endswith("/")
    }


def test_the_wheel_carries_every_tracked_framework_file_the_proof_demands(
    built_wheel: zipfile.ZipFile,
) -> None:
    tracked = {path[len("src/okg/") :] for path in _tracked_paths("src/okg")}
    assert tracked, "no tracked files under src/okg; the comparison is empty"
    shipped = _wheel_framework_members(built_wheel)
    missing = sorted(
        path for path in tracked - shipped if not path.startswith(UNSHIPPED_PREFIXES)
    )
    assert missing == [], (
        "these tracked files are absent from the wheel, so every "
        "wheel-installed publish refuses with installed_file_missing; add a "
        "[tool.setuptools.package-data] glob that matches them "
        f"(first 20 of {len(missing)}): {missing[:20]}"
    )


def test_every_declared_unshipped_tree_is_really_absent_from_the_wheel(
    built_wheel: zipfile.ZipFile,
) -> None:
    """A stale exemption would silently stop proving files that DO ship."""
    shipped = _wheel_framework_members(built_wheel)
    for prefix in UNSHIPPED_PREFIXES:
        tracked_under = _tracked_paths(f"src/okg/{prefix}")
        assert tracked_under, (
            f"{prefix} is declared unshipped but tracks no files; drop the "
            "entry from _UNSHIPPED_FRAMEWORK_TREES"
        )
        shipped_under = sorted(path for path in shipped if path.startswith(prefix))
        assert shipped_under == [], (
            f"{prefix} is declared unshipped yet the wheel carries "
            f"{shipped_under[:20]}; either stop shipping it or drop the "
            "exemption so those files are proven again"
        )


def test_the_proof_passes_against_an_install_unpacked_from_the_wheel(
    built_wheel: zipfile.ZipFile, tmp_path: Path
) -> None:
    """The end the operator sees: unpack the wheel, prove it against HEAD.

    This is the publish-time failure issue 1949 reports, run for real: the
    wheel's own bytes stand in for the installed package, and this
    repository at HEAD is the framework authority.
    """
    install = tmp_path / "site-packages"
    built_wheel.extractall(install)
    running = install / "okg" / "substrate" / "deployment_upgrade.py"
    assert running.is_file()
    head = subprocess.run(
        ["git", "-C", str(REPO_ROOT), "rev-parse", "HEAD"],
        capture_output=True,
        text=True,
        check=True,
    ).stdout.strip()

    result = _installed_framework_matches_revision(REPO_ROOT, head, running)

    if result.reason == "installed_file_bytes_differ":
        # The wheel is built from the WORKING TREE, so uncommitted work under
        # src/okg makes its bytes differ from HEAD's by design. That is a
        # dirty checkout, not a packaging defect -- and CI checkouts are
        # clean, so this never skips there.
        changed = subprocess.run(
            ["git", "-C", str(REPO_ROOT), "status", "--porcelain", "--", "src/okg"],
            capture_output=True,
            text=True,
            check=True,
        ).stdout.strip()
        pytest.skip(f"src/okg differs from HEAD in this checkout:\n{changed}")
    assert result.status == "match", result.to_dict()


def test_every_python_module_in_the_wheel_ships_inside_a_regular_package(
    built_wheel: zipfile.ZipFile,
) -> None:
    """Importable code ships as a package, never as package data.

    Issue #1969: ``sources/academic_researcher/`` had no ``__init__.py``, so
    ``packages.find`` skipped it and its modules reached the wheel only
    through an ``academic_researcher/*.py`` package-data glob, importable as a
    namespace portion. Every directory in the wheel holding a ``.py`` file
    must carry an ``__init__.py``, except the declared data trees.
    """
    outside = _modules_outside_packages(_wheel_framework_members(built_wheel))
    assert outside == [], (
        "these modules ship in a directory without __init__.py, so they are "
        "package data importable only as a namespace portion; add the "
        f"__init__.py (first 20 of {len(outside)}): {outside[:20]}"
    )


@pytest.mark.parametrize(
    ("path", "exempt"),
    [
        ("substrate/library/templates/codebase-index/server.py", True),
        ("substrate/library/profiles/academic-researcher/cli/install.py", True),
        ("substrate/library/templates/devops/stray.py", False),
        ("substrate/library/templates/empty/stray.py", False),
        ("substrate/library/profiles/_test/adapter.py", False),
        ("substrate/library/profiles/academic-researcher/source-defaults/stray.py", False),
    ],
)
def test_only_the_path_loaded_trees_are_exempt_from_the_package_rule(
    path: str, exempt: bool
) -> None:
    """The exemption covers the two path-loaded trees and nothing beside them.

    A synthetic wheel listing: the regular packages above the file are
    present, only the file's own directory lacks ``__init__.py``.
    """
    shipped = {path}
    for ancestor in _ancestor_directories(path)[:3]:
        shipped.add(f"{ancestor}/__init__.py")
    assert _modules_outside_packages(shipped) == ([] if exempt else [path])


def test_every_declared_source_module_resolves_to_packaged_code(
    built_wheel: zipfile.ZipFile,
) -> None:
    """Each module shipped YAML names by dotted path is a module in a package."""
    shipped = _wheel_framework_members(built_wheel)
    modules: set[str] = set()
    for path in sorted(shipped):
        if SOURCE_DECLARATIONS.match(path):
            document = yaml.safe_load(built_wheel.read(f"okg/{path}"))
            modules.update(_declared_framework_modules(document))
    for expected, family in (
        ("okg.substrate.library.sources.academic_researcher.cv_ingest_llm", "profile"),
        ("okg.substrate.library.sources.jira", "template"),
        ("okg.substrate.library.sources.engram_runs", "ontology"),
    ):
        assert expected in modules, (
            f"the {family} declaration scan did not find {expected}; the scan "
            "is reading nothing from that family"
        )
    unresolved = []
    for module in sorted(modules):
        relative = module.removeprefix("okg.").replace(".", "/")
        as_file = f"{relative}.py"
        as_package = f"{relative}/__init__.py"
        target = as_file if as_file in shipped else as_package
        if target not in shipped or any(
            f"{ancestor}/__init__.py" not in shipped
            for ancestor in _ancestor_directories(target)
        ):
            unresolved.append(module)
    assert unresolved == [], (
        f"these profile source modules are not packaged code in the wheel: {unresolved}"
    )


def test_the_academic_researcher_classifier_prompt_ships_beside_its_adapter(
    built_wheel: zipfile.ZipFile,
) -> None:
    """``cv_ingest_llm`` reads ``prompts/`` relative to its own file."""
    shipped = _wheel_framework_members(built_wheel)
    package = "substrate/library/sources/academic_researcher"
    assert f"{package}/__init__.py" in shipped
    assert f"{package}/prompts/cv_section_classifier.yaml" in shipped


def _modules_outside_packages(shipped: set[str]) -> list[str]:
    """Shipped ``.py`` files with a directory lacking ``__init__.py``.

    Files under ``PATH_LOADED_PYTHON_TREES`` are exempt: they are data.
    """
    return sorted(
        path
        for path in shipped
        if path.endswith(".py")
        and not any(tree.match(path) for tree in PATH_LOADED_PYTHON_TREES)
        and any(
            f"{ancestor}/__init__.py" not in shipped
            for ancestor in _ancestor_directories(path)
        )
    )


def _declared_framework_modules(node: object) -> set[str]:
    """Every ``okg.*`` module a parsed declaration names.

    A ``module:`` value is the module itself; a dotted ``class:`` value is
    ``<module>.<Class>``, so its module is everything before the last dot.
    """
    found: set[str] = set()
    if isinstance(node, dict):
        for key, value in node.items():
            if isinstance(value, str) and value.startswith("okg."):
                if key == "module":
                    found.add(value)
                elif key == "class":
                    found.add(value.rsplit(".", 1)[0])
            else:
                found |= _declared_framework_modules(value)
    elif isinstance(node, list):
        for item in node:
            found |= _declared_framework_modules(item)
    return found


def _ancestor_directories(path: str) -> list[str]:
    """The package directories from ``okg/`` down to the file's parent.

    ``okg/__init__.py`` itself is the wheel root package and lies outside
    the framework-relative names, so it is not listed.
    """
    parts = path.split("/")[:-1]
    return ["/".join(parts[: depth + 1]) for depth in range(len(parts))]
