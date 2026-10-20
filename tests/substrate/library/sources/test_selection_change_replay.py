"""Issue #2483: a file-selection change must not keep newly excluded records.

Changing a repository source's include or exclude globs changes its registry
binding, and the runner answers that with an authoritative replay: the adapter
gets a cursor holding only the prior record-set authority and must return the
complete footprint under the NEW rules. These adapter-level tests pin that the
footprint drops a file the new exclude glob matches. The runner-level proof,
with retraction and search on PostgreSQL, is
``tests/substrate/test_source_runner_change_probe_gate_e2e.py::
test_exclude_globs_change_retracts_newly_excluded_files``.
"""

from __future__ import annotations

import subprocess
from pathlib import Path

import pytest

from okg.substrate.library.sources.base import RecordSet

_KEEP = (
    "import os\n\n"
    "def keep():\n"
    "    return helper()\n\n"
    "def helper():\n"
    "    return os.environ.get('HOME')\n"
)
_SCRATCH = (
    "import sys\n\n"
    "def scratch():\n"
    "    return other()\n\n"
    "def other():\n"
    "    return open('notes.txt').read()\n\n"
    "def configure(parser):\n"
    "    parser.add_parser('scratch-doctor')\n"
)
_EXCLUDE = "src/demo/scratch_*.py"


def _git(repo: Path, *args: str) -> None:
    subprocess.run(
        ["git", "-C", str(repo), *args],
        check=True,
        capture_output=True,
    )


@pytest.fixture
def repo(tmp_path: Path) -> Path:
    root = tmp_path / "repo"
    (root / "src" / "demo").mkdir(parents=True)
    _git(root, "init", "-q")
    _git(root, "config", "user.email", "test@example.com")
    _git(root, "config", "user.name", "OKG Test")
    (root / "README.md").write_text("# demo\n")
    (root / "src" / "demo" / "keep.py").write_text(_KEEP)
    (root / "src" / "demo" / "scratch_notes.py").write_text(_SCRATCH)
    _git(root, "add", "-A")
    _git(root, "commit", "-q", "-m", "initial")
    return root


def _replay_cursor(record_set: RecordSet) -> dict:
    """What `SourceRunner._cursor_for_authoritative_replay` hands the adapter."""
    return {"record_set": record_set.to_cursor()}


def _record_keys(run) -> set[str]:
    list(run.facts)
    assert run.completed_scope is True
    assert run.record_set is not None
    assert run.record_set_replacement_keys is None
    return set(run.record_set.records)


def test_git_files_full_inventory_drops_newly_excluded_prior_record(
    repo: Path,
) -> None:
    from okg.substrate.library.sources.git_graph_files import GitFilesSource

    first = GitFilesSource(repo="demo", repo_root=repo, exclude_globs=())
    first_run = first.run("first", mode="scope_complete")
    first_keys = _record_keys(first_run)
    assert "src/demo/scratch_notes.py" in first_keys

    replay = GitFilesSource(
        repo="demo",
        repo_root=repo,
        exclude_globs=(_EXCLUDE,),
    ).run(
        "replay",
        mode="scope_complete",
        cursor=_replay_cursor(first_run.record_set),
    )

    assert _record_keys(replay) == first_keys - {"src/demo/scratch_notes.py"}


def test_git_files_incremental_run_still_carries_prior_records(
    repo: Path,
) -> None:
    """Failure guard: a run WITH saved tree state still merges the prior
    footprint, so an unchanged tree does not read as every file deleted."""
    from okg.substrate.library.sources.git_graph_files import GitFilesSource

    source = GitFilesSource(repo="demo", repo_root=repo, exclude_globs=())
    first_run = source.run("first", mode="scope_complete")
    first_keys = _record_keys(first_run)
    (repo / "src" / "demo" / "keep.py").write_text(_KEEP + "\n# edit\n")
    _git(repo, "commit", "-q", "-am", "edit keep")

    second = source.run(
        "second",
        mode="scope_complete",
        cursor={
            **first_run.next_cursor,
            "record_set": first_run.record_set.to_cursor(),
        },
    )
    facts = list(second.facts)
    emitted_paths = {
        fact.attrs.get("path")
        for fact in facts
        if getattr(fact, "subtype", None) == "source_file"
    }
    assert emitted_paths == {"src/demo/keep.py"}
    assert set(second.record_set.records) == first_keys


@pytest.mark.parametrize(
    ("module", "class_name"),
    [
        ("code_graph_structure", "CodeStructureSource"),
        ("code_graph_dataflow", "CodeDataFlowSource"),
        ("code_graph_interface", "InterfaceSource"),
    ],
)
def test_code_source_replay_drops_newly_excluded_prior_record(
    repo: Path,
    module: str,
    class_name: str,
) -> None:
    import importlib

    cls = getattr(
        importlib.import_module(f"okg.substrate.library.sources.{module}"),
        class_name,
    )
    base = cls(repo="demo", repo_root=repo)
    first_run = base.run("first", mode="scope_complete")
    list(first_run.facts)
    prior = first_run.record_set
    assert prior is not None
    assert "src/demo/scratch_notes.py" in prior.records

    narrowed = cls(
        repo="demo",
        repo_root=repo,
        exclude_globs=(*base.exclude_globs, _EXCLUDE),
    )
    replay = narrowed.run(
        "replay",
        mode="scope_complete",
        cursor=_replay_cursor(prior),
        authoritative_replay=True,
    )

    assert _record_keys(replay) == (
        set(prior.records) - {"src/demo/scratch_notes.py"}
    )
