"""Pin how a code repository is limited to some paths.

The operator configuration guide documents these rules in its section
"Limit a code repository to some paths". A change here is a change to what
a deployment ingests, so it must change that section too. (The guide is
named, not given by path: a test that names a docs file makes every edit to
that file run the full test lanes.)

The git-files reader matches repo-relative paths with ``fnmatch``: ``*``
crosses ``/``, so ``DataFormats/*`` and ``DataFormats/**`` mean the same
thing, and ``**/x`` needs at least one directory before ``x``.
"""
from __future__ import annotations

import os
import subprocess
import textwrap
from pathlib import Path

import pytest

from okg.substrate.ingest.adapter_factory import construct_source_adapter
from okg.substrate.library.sources._git_helpers import (
    DEFAULT_EXCLUDE_GLOBS,
    path_included_by_globs,
)
from okg.substrate.library.sources.git_graph_files import GitFilesSource
from okg.substrate.sources.registry import load_source_registry


CMSSW_INCLUDES = ["DataFormats/**", "RecoTracker/**"]


@pytest.mark.parametrize(
    ("path", "included"),
    [
        ("DataFormats/TrackReco/interface/Track.h", True),
        ("DataFormats/BuildFile.xml", True),
        ("RecoTracker/src/Seed.cc", True),
        ("FWCore/Framework/src/Event.cc", False),
        ("DataFormatsExtra/x.h", False),
        ("README.md", False),
        # A leading slash or a Windows separator is normalised first.
        ("/DataFormats/x.h", True),
        ("DataFormats\\x.h", True),
    ],
)
def test_include_globs_keep_only_matching_paths(
    path: str, included: bool,
) -> None:
    assert path_included_by_globs(
        path, include_globs=CMSSW_INCLUDES,
    ) is included


def test_star_crosses_directory_separators() -> None:
    # fnmatch, not gitignore: `*` matches `/` too.
    assert path_included_by_globs("a/b/c.py", include_globs=["*.py"])
    assert path_included_by_globs(
        "DataFormats/a/b/c.h", include_globs=["DataFormats/*"],
    )
    assert path_included_by_globs(
        "DataFormats/a/b/c.h", include_globs=["DataFormats/*.h"],
    )


def test_double_star_slash_prefix_needs_a_directory() -> None:
    # `**/BuildFile.xml` does NOT match a root-level BuildFile.xml.
    assert not path_included_by_globs(
        "BuildFile.xml", include_globs=["**/BuildFile.xml"],
    )
    assert path_included_by_globs(
        "DataFormats/BuildFile.xml", include_globs=["**/BuildFile.xml"],
    )
    assert path_included_by_globs(
        "BuildFile.xml", include_globs=["BuildFile.xml", "**/BuildFile.xml"],
    )


def test_exclude_wins_over_include() -> None:
    assert not path_included_by_globs(
        "DataFormats/test/a_t.cpp",
        include_globs=CMSSW_INCLUDES,
        exclude_globs=["*/test/*"],
    )
    assert path_included_by_globs(
        "DataFormats/src/a.cc",
        include_globs=CMSSW_INCLUDES,
        exclude_globs=["*/test/*"],
    )


def test_no_include_globs_means_every_path() -> None:
    assert path_included_by_globs("anything/at/all.txt")
    assert path_included_by_globs("anything/at/all.txt", include_globs=[])


def _read_paths(adapter: GitFilesSource) -> list[str]:
    run = adapter.run("scope-test", mode="reconcile")
    return sorted(
        str(fact.attrs["path"])
        for fact in run.facts
        if getattr(fact, "subtype", None) == "source_file"
    )


def test_explicit_exclude_globs_replace_the_defaults(tmp_path: Path) -> None:
    """``exclude_globs`` is not merged with the reader's default list."""
    assert "*.png" in DEFAULT_EXCLUDE_GLOBS
    root = tmp_path / "repo"
    for rel in ("src/a.py", "doc/plot.png", "src/test/a_test.py"):
        path = root / rel
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("x\n", encoding="utf-8")
    _git(root, "init", "-q")
    _git(root, "add", "-A")
    _git(root, "commit", "-q", "-m", "seed")

    default = GitFilesSource(repo="r", repo_root=root)
    explicit = GitFilesSource(
        repo="r", repo_root=root, exclude_globs=("*/test/*",),
    )
    # Defaults drop the .png and keep the test file.
    assert _read_paths(default) == ["src/a.py", "src/test/a_test.py"]
    # The explicit list drops the test file and reads the .png again.
    assert _read_paths(explicit) == ["doc/plot.png", "src/a.py"]


def _git(root: Path, *args: str) -> None:
    env = {
        **os.environ,
        "GIT_AUTHOR_NAME": "t",
        "GIT_AUTHOR_EMAIL": "t@example.com",
        "GIT_COMMITTER_NAME": "t",
        "GIT_COMMITTER_EMAIL": "t@example.com",
    }
    subprocess.run(
        ["git", *args], cwd=root, env=env, check=True,
        stdout=subprocess.DEVNULL,
    )


def _cmssw_shaped_repo(base: Path) -> Path:
    root = base / "cmssw"
    for rel in (
        "BuildFile.xml",
        "DataFormats/TrackReco/interface/Track.h",
        "DataFormats/TrackReco/BuildFile.xml",
        "RecoTracker/src/Seed.cc",
        "FWCore/Framework/src/Event.cc",
        "Geometry/data/big.xml",
    ):
        path = root / rel
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(f"// {rel}\n", encoding="utf-8")
    _git(root, "init", "-q")
    _git(root, "add", "-A")
    _git(root, "commit", "-q", "-m", "seed")
    return root


def _registry(tmp_path: Path, *, strict: bool) -> Path:
    base = tmp_path / "repos"
    _cmssw_shaped_repo(base)
    marker = "admission_contract: strict_v1\n" if strict else ""
    body = marker + textwrap.dedent(
        f"""
        code_repos:
          base: {base}
          repos:
            - slug: cmssw
              params:
                git-files:
                  include_globs: ["DataFormats/**", "RecoTracker/**"]
        sources: {{}}
        """
    ).lstrip()
    path = tmp_path / "source_registry.yaml"
    path.write_text(body, encoding="utf-8")
    return path


@pytest.mark.parametrize("strict", [False, True], ids=["legacy", "strict_v1"])
def test_per_repo_include_globs_reach_the_git_files_reader(
    tmp_path: Path, strict: bool,
) -> None:
    entries = load_source_registry(_registry(tmp_path, strict=strict))
    entry = entries["git-files-cmssw"]
    assert entry.admission_contract == ("strict_v1" if strict else "legacy_v1")
    constructed = construct_source_adapter(entry, dsn="postgresql://unused")
    adapter = constructed.adapter
    assert isinstance(adapter, GitFilesSource)
    assert list(adapter.include_globs) == CMSSW_INCLUDES
    # The per-repo override replaced nothing else: the template's exclude
    # list is still the one in force.
    assert adapter.exclude_globs is not None
    assert "**/__pycache__/**" in adapter.exclude_globs
    # The template reads untracked files; the include list applies to them
    # as well.
    root = Path(adapter.repo_root)
    (root / "FWCore" / "untracked.cc").write_text("x\n", encoding="utf-8")
    (root / "DataFormats" / "untracked.h").write_text("x\n", encoding="utf-8")
    assert _read_paths(adapter) == [
        "DataFormats/TrackReco/BuildFile.xml",
        "DataFormats/TrackReco/interface/Track.h",
        "DataFormats/untracked.h",
        "RecoTracker/src/Seed.cc",
    ]


def test_file_name_include_pattern_skips_untracked_files_in_directories(
    tmp_path: Path,
) -> None:
    """Untracked files are found by walking directories, and a directory is
    entered only when `<dir>/.okg-prune-probe` passes the include list.
    `*.h` passes for tracked files anywhere but not for that probe."""
    root = _cmssw_shaped_repo(tmp_path / "repos")
    (root / "DataFormats" / "untracked.h").write_text("x\n", encoding="utf-8")
    (root / "untracked_root.h").write_text("x\n", encoding="utf-8")
    by_name = GitFilesSource(
        repo="r", repo_root=root, include_globs=["*.h"], include_untracked=True,
    )
    assert _read_paths(by_name) == [
        "DataFormats/TrackReco/interface/Track.h",
        "untracked_root.h",
    ]
    by_directory = GitFilesSource(
        repo="r", repo_root=root, include_globs=["DataFormats/**"],
        include_untracked=True,
    )
    assert _read_paths(by_directory) == [
        "DataFormats/TrackReco/BuildFile.xml",
        "DataFormats/TrackReco/interface/Track.h",
        "DataFormats/untracked.h",
    ]


def test_nested_include_pattern_needs_its_parent_probe_for_untracked_files(
    tmp_path: Path,
) -> None:
    """Every directory level is tested: `DataFormats/TrackReco/**` alone
    skips `DataFormats/` for the untracked walk; listing
    `DataFormats/.okg-prune-probe` lets the walk enter it. `.claude/` is
    always skipped."""
    root = _cmssw_shaped_repo(tmp_path / "repos")
    (root / "DataFormats" / "TrackReco" / "new.h").write_text(
        "x\n", encoding="utf-8",
    )
    (root / ".claude").mkdir()
    (root / ".claude" / "note.h").write_text("x\n", encoding="utf-8")
    nested = GitFilesSource(
        repo="r", repo_root=root, include_globs=["DataFormats/TrackReco/**"],
        include_untracked=True,
    )
    assert _read_paths(nested) == [
        "DataFormats/TrackReco/BuildFile.xml",
        "DataFormats/TrackReco/interface/Track.h",
    ]
    with_probe = GitFilesSource(
        repo="r", repo_root=root,
        include_globs=[
            "DataFormats/TrackReco/**", "DataFormats/.okg-prune-probe",
        ],
        include_untracked=True,
    )
    assert _read_paths(with_probe) == [
        "DataFormats/TrackReco/BuildFile.xml",
        "DataFormats/TrackReco/interface/Track.h",
        "DataFormats/TrackReco/new.h",
    ]
    everything = GitFilesSource(repo="r", repo_root=root, include_untracked=True)
    assert ".claude/note.h" not in _read_paths(everything)


def test_code_structure_has_no_include_globs_so_scope_it_with_excludes(
    tmp_path: Path,
) -> None:
    """code-structure walks the checkout itself; git-files include_globs do
    not narrow it. Its only path filter is ``exclude_globs``."""
    entries = load_source_registry(_registry(tmp_path, strict=False))
    params = entries["code-structure-cmssw"].raw["params"]
    assert "include_globs" not in params
    assert params["max_files"] == 5000


@pytest.mark.parametrize(
    ("path", "pattern", "matched"),
    [
        # `*` stays inside one directory, unlike git-files.
        ("a/b/c.md", "*.md", False),
        ("c.md", "*.md", True),
        ("DataFormats/a/b.md", "DataFormats/*", False),
        ("DataFormats/a/b.md", "DataFormats/**", True),
        # `**/` matches zero directories, so the root file matches.
        ("README.md", "**/README.md", True),
        ("a/b/README.md", "**/README.md", True),
        # `*`, `**` and `**/*` alone match everything.
        ("a/b/c.txt", "*", True),
    ],
)
def test_doc_corpus_patterns_are_gitignore_like(
    path: str, pattern: str, matched: bool,
) -> None:
    from okg.substrate.library.sources.doc_corpus import _match_any

    assert _match_any(Path(path), [pattern]) is matched


def test_doc_corpus_empty_include_list_reads_nothing() -> None:
    from okg.substrate.library.sources.doc_corpus import (
        DocCorpusSource,
        _match_any,
    )

    assert not _match_any(Path("README.md"), [])
    default = DocCorpusSource.__dataclass_fields__["include_globs"].default
    assert tuple(default) == ("**/*",)


def test_configure_doc_example_reads_only_the_two_directories(
    tmp_path: Path,
) -> None:
    """The worked example in the configuration guide (checkout already
    present, so manage_checkout is off and nothing is cloned)."""
    base = tmp_path / "repos"
    _cmssw_shaped_repo(base)
    path = tmp_path / "source_registry.yaml"
    path.write_text(
        textwrap.dedent(
            f"""
            code_repos:
              base: {base}
              expand: [git-files]
              repos:
                - slug: cmssw
                  url: https://github.com/cms-sw/cmssw.git
                  manage_checkout: false
                  params:
                    git-files:
                      include_globs: ["DataFormats/**", "RecoTracker/**"]
            """
        ).lstrip(),
        encoding="utf-8",
    )
    entries = load_source_registry(path)
    assert sorted(entries) == ["git-files-cmssw"]
    adapter = construct_source_adapter(
        entries["git-files-cmssw"], dsn="postgresql://unused",
    ).adapter
    assert _read_paths(adapter) == [
        "DataFormats/TrackReco/BuildFile.xml",
        "DataFormats/TrackReco/interface/Track.h",
        "RecoTracker/src/Seed.cc",
    ]


def test_code_structure_excluding_other_top_level_directories(
    tmp_path: Path,
) -> None:
    """The documented way to narrow code-structure: exclude every other
    top-level directory. Root-level files are still walked."""
    from okg.substrate.library.sources._code_graph_common import _walk_files

    root = _cmssw_shaped_repo(tmp_path / "repos")
    (root / "setup.py").write_text("x = 1\n", encoding="utf-8")
    walked = _walk_files(
        root,
        include_exts=[".h", ".cc", ".py"],
        exclude_globs=["FWCore/**", "Geometry/**"],
        max_files=5000,
        max_bytes_per_file=1_000_000,
    )
    assert sorted(path.relative_to(root).as_posix() for path in walked) == [
        "DataFormats/TrackReco/interface/Track.h",
        "RecoTracker/src/Seed.cc",
        "setup.py",
    ]
    with pytest.raises(ValueError, match="exceeded max_files=1"):
        _walk_files(
            root,
            include_exts=[".h", ".cc", ".py"],
            exclude_globs=["FWCore/**", "Geometry/**"],
            max_files=1,
            max_bytes_per_file=1_000_000,
        )
