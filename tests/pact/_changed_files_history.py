"""The git history and the product readers shared by the changed-files tests.

tests/pact/test_changed_files_agreement.py checks that the CI scripts under
scripts/ci/ agree with the product readers; it needs scripts/ci/ and does not
ship in the public copy. tests/pact/test_pact_changed_files_readers.py runs
the product readers alone over the same cases, and ships.
"""

from __future__ import annotations

import shutil
import subprocess
from pathlib import Path
from typing import Any, Callable

from okg.substrate.pact import proof_identity, scope_drift
from pact import git as pact_git

ODD_NAMES = ("docs/café.md", "src/new\nline.py")


def _git(root: Path, *args: str) -> str:
    return subprocess.run(
        ["git", *args], cwd=root, check=True, capture_output=True, text=True
    ).stdout.strip()


def _commit(root: Path, message: str) -> str:
    _git(root, "add", "-A")
    _git(root, "commit", "-qm", message)
    return _git(root, "rev-parse", "HEAD")


def _body(name: str) -> str:
    # Distinct content per file, so rename/copy detection pairs only the
    # files each case means to pair.
    return f"# {name}\n" + "".join(f"{name.upper()}_{i} = '{name}-{i}'\n" for i in range(30))


# The copy lands in experiments/, a tracked top-level directory no lane reads
# (lane_scope.TOP_LEVEL_DIRS_SHADOWING_NOTHING), so the copy alone selects no
# lane. A .py file under a top-level directory lane_scope does not know (the
# old tools/) runs every scoped lane: the repository root is first on sys.path
# under python -m pytest, so a new root directory can satisfy an import.
COPY_DESTINATION = "experiments/copy_of_source.py"


def build_history(root: Path) -> dict[str, Any]:
    """dev: B0 -> D1. feature: B0 -> rename -> delete -> copy -> merge D1 ->
    edit -> odd names. `origin` is a bare clone, for the pull-request path."""
    root.mkdir()
    _git(root, "init", "-q", "-b", "dev")
    _git(root, "config", "user.email", "ci-test@example.invalid")
    _git(root, "config", "user.name", "CI Test")
    _git(root, "config", "diff.renames", "true")
    _git(root, "config", "core.quotePath", "true")
    for relative in (
        "src/keep.py",
        "src/old_name.py",
        "src/gone.py",
        # Inside real lane closures, so lane selection has something to say
        # about the copy source.
        "src/chronos_core/source.py",
    ):
        path = root / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(_body(Path(relative).stem), encoding="utf-8")
    (root / "docs").mkdir()
    (root / "docs" / "a.md").write_text("# a\n", encoding="utf-8")
    b0 = _commit(root, "base")

    _git(root, "switch", "-q", "-c", "feature")
    _git(root, "mv", "src/old_name.py", "src/new_name.py")
    rename = _commit(root, "rename")
    (root / "src" / "gone.py").unlink()
    delete = _commit(root, "delete")
    (root / "experiments").mkdir()
    shutil.copy(root / "src" / "chronos_core" / "source.py", root / COPY_DESTINATION)
    copy = _commit(root, "copy")

    _git(root, "switch", "-q", "dev")
    (root / "docs" / "a.md").write_text("# a\n\nfrom dev\n", encoding="utf-8")
    d1 = _commit(root, "dev moves on")

    _git(root, "switch", "-q", "feature")
    _git(root, "merge", "-q", "--no-edit", "dev")
    merge = _git(root, "rev-parse", "HEAD")
    with (root / "src" / "keep.py").open("a", encoding="utf-8") as handle:
        handle.write("EXTRA = 1\n")
    head = _commit(root, "edit after merge")
    for name in ODD_NAMES:
        (root / name).write_text(f"odd {name!r}\n", encoding="utf-8")
    odd = _commit(root, "non-ASCII and newline names")

    origin = root.parent / "origin.git"
    _git(root.parent, "clone", "-q", "--bare", str(root), str(origin))
    _git(root, "remote", "add", "origin", str(origin))
    return {
        "root": root,
        "b0": b0,
        "rename": rename,
        "delete": delete,
        "copy": copy,
        "d1": d1,
        "merge": merge,
        "head": head,
        "odd": odd,
    }


PULL_REQUEST_PATHS = {
    "src/old_name.py",
    "src/new_name.py",
    "src/gone.py",
    COPY_DESTINATION,
    "src/keep.py",
}

# (case id, base commit key, head commit key, expected changed paths)
CASES: list[tuple[str, str, str, set[str]]] = [
    (
        "pull request: rename, delete, copy, merge from dev (base = dev tip)",
        "d1",
        "head",
        PULL_REQUEST_PATHS,
    ),
    ("rename only", "b0", "rename", {"src/old_name.py", "src/new_name.py"}),
    ("delete only", "rename", "delete", {"src/gone.py"}),
    # The copy's source is unchanged, so it is not a changed path.
    ("copy only", "delete", "copy", {COPY_DESTINATION}),
    # Push with no pull request: base is `before`, the previous branch tip.
    ("push with no pull request", "merge", "head", {"src/keep.py"}),
    # Push of the merge-from-dev commit itself: the push range carries dev's
    # file. This is the documented push-vs-pull-request difference: the
    # caller picks `before` or the target base; the reader is the same.
    ("push of the merge commit", "copy", "merge", {"docs/a.md"}),
    ("non-ASCII and newline names", "head", "odd", set(ODD_NAMES)),
]


def _merge_base(root: Path, base: str, head: str) -> str:
    value = pact_git.merge_base(root, base, head)
    assert value
    return value


def _at_head(root: Path, head: str, fn: Callable[[], Any]) -> Any:
    _git(root, "switch", "-q", "--detach", head)
    try:
        return fn()
    finally:
        _git(root, "switch", "-q", "feature")


def product_readers(root: Path) -> dict[str, Callable[[str, str], set[str]]]:
    """The readers that ship in the package, keyed by the name the table reports."""

    def scope_drift_paths(base: str, head: str) -> set[str]:
        _git(root, "branch", "-f", "agreement-base", base)

        def read() -> set[str]:
            result = scope_drift.changed_paths(root, base_branch="agreement-base")
            assert "diff_unreadable" not in result
            return set(result["paths"])

        return _at_head(root, head, read)

    return {
        # pact check, approval binding and verdict_policy's line counts.
        "pact.git.changed_paths (numstat -z)": lambda b, h: {
            path
            for path, _ in pact_git.changed_paths(root, _merge_base(root, b, h), h) or []
        },
        "pact.git.record_paths(diff_records)": lambda b, h: set(
            pact_git.record_paths(
                pact_git.diff_records(root, f"{_merge_base(root, b, h)}..{h}")
            )
        ),
        "scope_drift.changed_paths": scope_drift_paths,
        # Retired v4 proof identity keeps its own --no-renames -z read; it is
        # in the table to show it agrees, not because it was changed.
        "proof_identity._changed_paths": lambda b, h: set(
            proof_identity._changed_paths(root, _merge_base(root, b, h), h)
        ),
    }
