"""The provenance proof's one exemption: trees the distribution never ships.

``_installed_framework_matches_revision`` demands that every file git tracks
under ``src/okg`` is present in the installed package. Issue 1949: a wheel
carries the runtime, not the repository, so that demand refused every
wheel-installed publish. The packaging fix ships everything the runtime
reads; ``_UNSHIPPED_FRAMEWORK_TREES`` names what it deliberately does not --
today only ``operator_ui/web/``, the npm-built operator UI sources and their
screenshot baselines.

These tests hold the exemption to its narrow shape: absence is tolerated only
under a declared tree, a declared file that IS installed is still
byte-compared, a prefix match stops at the slash, and a comparison left with
nothing to compare says so instead of passing vacuously.
"""

from __future__ import annotations

import subprocess
from pathlib import Path

from okg.substrate.deployment_upgrade import (
    _UNSHIPPED_FRAMEWORK_TREES,
    _installed_framework_matches_revision,
)

UNSHIPPED = "operator_ui/web/src/operator-ui/app/HomeApp.tsx"


def _git(repo: Path, *args: str) -> str:
    return subprocess.run(
        ["git", "-C", str(repo), *args],
        capture_output=True,
        text=True,
        check=True,
    ).stdout


def _repo_with(
    tmp_path: Path, files: dict[str, bytes], installed: set[str] | None = None
) -> tuple[Path, str, Path]:
    """Build a git repo plus an 'installed' package carrying ``installed``."""
    repo = tmp_path / "repo"
    (repo / "src" / "okg" / "substrate").mkdir(parents=True)
    _git(repo.parent, "init", "-q", str(repo))
    _git(repo, "config", "user.email", "t@t")
    _git(repo, "config", "user.name", "t")

    install = tmp_path / "install" / "okg"
    for rel, body in files.items():
        tracked = repo / "src" / "okg" / rel
        tracked.parent.mkdir(parents=True, exist_ok=True)
        tracked.write_bytes(body)
        if installed is None or rel in installed:
            packaged = install / rel
            packaged.parent.mkdir(parents=True, exist_ok=True)
            packaged.write_bytes(body)

    _git(repo, "add", "-A")
    _git(repo, "commit", "-q", "-m", "seed")
    revision = _git(repo, "rev-parse", "HEAD").strip()
    running = install / "substrate" / "deployment_upgrade.py"
    running.parent.mkdir(parents=True, exist_ok=True)
    return repo, revision, running


def test_the_declared_tree_is_the_operator_ui_web_sources() -> None:
    assert _UNSHIPPED_FRAMEWORK_TREES == (b"operator_ui/web/",)


def test_a_file_the_wheel_does_not_ship_may_be_absent(tmp_path) -> None:
    repo, revision, running = _repo_with(
        tmp_path,
        {"substrate/deployment_upgrade.py": b"x = 1\n", UNSHIPPED: b"<div/>\n"},
        installed={"substrate/deployment_upgrade.py"},
    )

    result = _installed_framework_matches_revision(repo, revision, running)

    assert result.status == "match", result.to_dict()


def test_an_unshipped_file_that_is_installed_is_still_byte_compared(
    tmp_path,
) -> None:
    """The exemption covers absence only; installed bytes still have to match."""
    repo, revision, running = _repo_with(
        tmp_path,
        {"substrate/deployment_upgrade.py": b"x = 1\n", UNSHIPPED: b"<div/>\n"},
    )
    (running.parents[1] / UNSHIPPED).write_bytes(b"<div>tampered</div>\n")

    result = _installed_framework_matches_revision(repo, revision, running)

    assert result.status == "mismatch"
    assert result.reason == "installed_file_bytes_differ"
    assert result.path == f"src/okg/{UNSHIPPED}"


def test_a_shipped_file_that_is_absent_still_refuses(tmp_path) -> None:
    repo, revision, running = _repo_with(
        tmp_path,
        {
            "substrate/deployment_upgrade.py": b"x = 1\n",
            "bench/synth/world_prose/provenance.json": b"{}\n",
        },
        installed={"substrate/deployment_upgrade.py"},
    )

    result = _installed_framework_matches_revision(repo, revision, running)

    assert result.status == "mismatch"
    assert result.reason == "installed_file_missing"
    assert result.path == "src/okg/bench/synth/world_prose/provenance.json"


def test_the_exemption_stops_at_the_directory_boundary(tmp_path) -> None:
    """`operator_ui/web/` must not exempt a sibling named `web_client.py`."""
    sibling = "operator_ui/web_client.py"
    repo, revision, running = _repo_with(
        tmp_path,
        {"substrate/deployment_upgrade.py": b"x = 1\n", sibling: b"x = 2\n"},
        installed={"substrate/deployment_upgrade.py"},
    )

    result = _installed_framework_matches_revision(repo, revision, running)

    assert result.status == "mismatch"
    assert result.reason == "installed_file_missing"
    assert result.path == f"src/okg/{sibling}"


def test_a_tree_of_only_unshipped_absent_files_proves_nothing(tmp_path) -> None:
    """Skipping every file must not read as a passed proof."""
    repo, revision, running = _repo_with(
        tmp_path, {UNSHIPPED: b"<div/>\n"}, installed=set()
    )

    result = _installed_framework_matches_revision(repo, revision, running)

    assert result.status == "unevaluable"
    assert result.reason == "no_comparable_framework_files"
    assert result.matched is False
