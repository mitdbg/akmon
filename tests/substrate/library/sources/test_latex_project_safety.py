from __future__ import annotations

from pathlib import Path

import pytest

from okg.substrate.library.sources.scholarly.latex_project_manifest import (
    LatexProjectManifestBuilder,
)
from okg.substrate.library.sources._file_walk import FileContainmentError


def _write(root: Path, rel: str, text: str) -> Path:
    path = root / rel
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")
    return path


def _codes(manifest: object) -> list[str]:
    return [
        warning.code
        for project in manifest.projects  # type: ignore[attr-defined]
        for warning in project.warnings
    ]


def test_manifest_builder_refuses_outside_or_retargeted_content_reads(
    tmp_path: Path,
) -> None:
    source_root = tmp_path / "source"
    _write(source_root, "paper/main.tex", r"\documentclass{article}")
    outside = tmp_path / "outside.tex"
    outside.write_text("OUTSIDE_SENTINEL")
    (source_root / "paper/escape.tex").symlink_to(outside)
    with pytest.raises(FileContainmentError, match="escapes source root"):
        LatexProjectManifestBuilder(
            source_root=source_root,
            repository_authority="repo",
            projects=[{"project_dir": "paper", "entrypoints": ["main.tex"]}],
        ).build()


def test_manifest_lexical_aliases_count_separately_but_roles_deduplicate(
    tmp_path: Path,
) -> None:
    source_root = tmp_path / "source"
    target = _write(
        source_root, "paper/main.tex",
        r"\documentclass{article}\begin{document}Safe\end{document}",
    )
    (source_root / "paper/alias-a.tex").symlink_to(target)
    (source_root / "paper/alias-b.tex").symlink_to(target)
    manifest = LatexProjectManifestBuilder(
        source_root=source_root,
        repository_authority="repo",
        projects=[{"project_dir": "paper", "entrypoints": ["alias-a.tex"]}],
    ).build()
    assert {item.path for item in manifest.source_inventory} == {
        "paper/alias-a.tex", "paper/alias-b.tex", "paper/main.tex",
    }
    assert len(manifest.projects[0].files) == 1


@pytest.mark.parametrize("automatic", [False, True])
def test_manifest_root_selection_swap_refuses_outside_target(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, automatic: bool,
) -> None:
    source_root = tmp_path / "source"
    root = _write(
        source_root, "paper/main.tex",
        r"\documentclass{article}\begin{document}Safe\end{document}",
    )
    outside = tmp_path / "outside.tex"
    outside.write_text("OUTSIDE_SENTINEL")
    builder = LatexProjectManifestBuilder(
        source_root=source_root,
        repository_authority="repo",
        projects=[{
            "project_dir": "paper",
            **({} if automatic else {"entrypoints": ["main.tex"]}),
        }],
    )
    original = builder._secure_stat
    swapped = False

    def stat_then_swap(path: Path, *, directory: bool = False) -> object:
        nonlocal swapped
        result = original(path, directory=directory)
        if Path(path).name == "main.tex" and not swapped:
            swapped = True
            root.unlink()
            root.symlink_to(outside)
        return result

    monkeypatch.setattr(builder, "_secure_stat", stat_then_swap)
    with pytest.raises(FileContainmentError, match="escapes corpus root"):
        builder.build()


def test_comments_verbatim_and_dynamic_paths_are_never_followed(tmp_path: Path) -> None:
    _write(
        tmp_path,
        "paper/main.tex",
        r"""\documentclass{article}
% \input{commented}
\begin{verbatim}\input{verbatim}\end{verbatim}
\input{\dynamic/path}
Body
""",
    )
    _write(tmp_path, "paper/commented.tex", "secret")
    _write(tmp_path, "paper/verbatim.tex", "secret")
    manifest = LatexProjectManifestBuilder(
        source_root=tmp_path,
        repository_authority="repo",
        projects=[{"project_dir": "paper", "entrypoints": ["main.tex"]}],
    ).build()
    paths = {item.path for item in manifest.projects[0].files}
    assert paths == {"paper/main.tex"}
    assert "latex_dependency_dynamic" in _codes(manifest)


def test_escape_symlink_cycle_and_budget_fail_closed_without_losing_root(
    tmp_path: Path,
) -> None:
    _write(tmp_path, "outside.tex", "outside")
    _write(
        tmp_path,
        "paper/main.tex",
        r"""\documentclass{article}
\input{../outside}
\input{link}
\input{cycle}
\input{large}
""",
    )
    (tmp_path / "paper/link.tex").symlink_to(tmp_path / "outside.tex")
    _write(tmp_path, "paper/cycle.tex", r"\input{main}")
    _write(tmp_path, "paper/large.tex", "x" * 128)
    manifest = LatexProjectManifestBuilder(
        source_root=tmp_path,
        repository_authority="repo",
        projects=[{"project_dir": "paper", "entrypoints": ["main.tex"]}],
        max_file_bytes=100,
    ).build()
    project = manifest.projects[0]
    assert project.root_path == "paper/main.tex"
    assert "paper/cycle.tex" in {item.path for item in project.files}
    assert {"latex_dependency_escape", "latex_dependency_cycle"} <= set(_codes(manifest))
    assert "latex_dependency_over_file_bytes" in _codes(manifest)
    inventory_paths = {item.path for item in manifest.source_inventory}
    assert "outside.tex" not in inventory_paths
    assert "paper/link.tex" not in inventory_paths
    assert [warning.code for warning in manifest.warnings].count(
        "latex_inventory_escape"
    ) == 1


def test_invalid_configuration_and_missing_root_are_loud(tmp_path: Path) -> None:
    (tmp_path / "paper").mkdir()
    with pytest.raises(ValueError, match="positive integer"):
        LatexProjectManifestBuilder(
            source_root=tmp_path,
            repository_authority="repo",
            projects=[{"project_dir": "paper"}],
            max_depth=0,
        )
    manifest = LatexProjectManifestBuilder(
        source_root=tmp_path,
        repository_authority="repo",
        projects=[{"project_dir": "paper"}],
    ).build()
    assert [warning.code for warning in manifest.warnings] == ["latex_root_missing"]


def test_inventory_warns_generated_looking_escape_and_confined_dangling_symlink(
    tmp_path: Path,
) -> None:
    _write(tmp_path, "paper/main.tex", r"\documentclass{article}")
    _write(tmp_path, "outside/build/secret.tex", "outside")
    (tmp_path / "paper/leak-generated.tex").symlink_to(
        tmp_path / "outside/build/secret.tex"
    )
    (tmp_path / "paper/dangling.tex").symlink_to(
        tmp_path / "paper/missing-target.tex"
    )
    (tmp_path / "paper/loop.tex").symlink_to(tmp_path / "paper/loop.tex")
    manifest = LatexProjectManifestBuilder(
        source_root=tmp_path,
        repository_authority="repo",
        projects=[{"project_dir": "paper", "entrypoints": ["main.tex"]}],
    ).build()
    codes = [warning.code for warning in manifest.warnings]
    assert codes == [
        "latex_inventory_missing",
        "latex_inventory_escape",
        "latex_inventory_symlink_cycle",
    ]
    assert {item.path for item in manifest.source_inventory} == {"paper/main.tex"}


def test_automatic_roots_never_follow_realpath_escape(tmp_path: Path) -> None:
    _write(tmp_path, "outside.tex", r"\documentclass{article}Outside")
    (tmp_path / "paper").mkdir()
    (tmp_path / "paper/alias.tex").symlink_to(tmp_path / "outside.tex")

    manifest = LatexProjectManifestBuilder(
        source_root=tmp_path,
        repository_authority="repo",
        projects=[{"project_dir": "paper"}],
    ).build()

    assert manifest.projects == ()
    assert "latex_root_missing" in [warning.code for warning in manifest.warnings]
    assert "latex_inventory_escape" in [
        warning.code for warning in manifest.warnings
    ]


def test_dependency_symlink_loop_warns_instead_of_crashing(tmp_path: Path) -> None:
    _write(
        tmp_path,
        "paper/main.tex",
        r"\documentclass{article}\input{loop}",
    )
    (tmp_path / "paper/loop.tex").symlink_to(tmp_path / "paper/loop.tex")

    manifest = LatexProjectManifestBuilder(
        source_root=tmp_path,
        repository_authority="repo",
        projects=[{"project_dir": "paper", "entrypoints": ["main.tex"]}],
    ).build()

    assert [project.root_path for project in manifest.projects] == ["paper/main.tex"]
    assert "latex_dependency_missing" in _codes(manifest)
    assert "latex_inventory_symlink_cycle" in [
        warning.code for warning in manifest.warnings
    ]


def test_automatic_root_discovery_is_inventory_bounded_and_requires_literal_class(
    tmp_path: Path,
) -> None:
    _write(tmp_path, "paper/a.tex", r"\documentclass{article}A")
    _write(tmp_path, "paper/z.tex", r"\documentclass{article}Z")
    bounded = LatexProjectManifestBuilder(
        source_root=tmp_path,
        repository_authority="repo",
        projects=[{"project_dir": "paper"}],
        max_inventory_files=1,
    ).build()
    assert bounded.projects == ()
    assert "latex_root_discovery_incomplete" in [
        warning.code for warning in bounded.warnings
    ]

    (tmp_path / "paper/z.tex").unlink()
    _write(tmp_path, "paper/a.tex", r"\documentclass{\dynamic}Not a root")
    dynamic = LatexProjectManifestBuilder(
        source_root=tmp_path,
        repository_authority="repo",
        projects=[{"project_dir": "paper"}],
    ).build()
    assert dynamic.projects == ()
    assert "latex_root_missing" in [warning.code for warning in dynamic.warnings]


def test_escaped_verbatim_and_macro_definition_commands_are_inert(
    tmp_path: Path,
) -> None:
    _write(tmp_path, "paper/escaped.tex", r"\\documentclass{article}")
    _write(tmp_path, "paper/inline.tex", r"\verb|\documentclass{article}|")
    _write(
        tmp_path,
        "paper/macro.tex",
        r"\newcommand{\fake}{\documentclass{article}}",
    )
    automatic = LatexProjectManifestBuilder(
        source_root=tmp_path,
        repository_authority="repo",
        projects=[{"project_dir": "paper"}],
    ).build()
    assert automatic.projects == ()

    _write(
        tmp_path,
        "paper/main.tex",
        r"""\documentclass{article}
\\input{secret}
\verb|\input{secret}|
\newcommand{\fake}{\input{secret}}
""",
    )
    _write(tmp_path, "paper/secret.tex", "must remain unselected")
    explicit = LatexProjectManifestBuilder(
        source_root=tmp_path,
        repository_authority="repo",
        projects=[{"project_dir": "paper", "entrypoints": ["main.tex"]}],
    ).build()
    assert {item.path for item in explicit.projects[0].files} == {"paper/main.tex"}
