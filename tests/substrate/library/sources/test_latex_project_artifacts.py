from __future__ import annotations

from pathlib import Path

from okg.substrate.library.sources.scholarly.latex_project_manifest import (
    LatexProjectManifestBuilder,
)
from okg.substrate.library.sources.scholarly import latex_project_manifest


def _write(root: Path, rel: str, content: str | bytes = "") -> None:
    path = root / rel
    path.parent.mkdir(parents=True, exist_ok=True)
    if isinstance(content, bytes):
        path.write_bytes(content)
    else:
        path.write_text(content, encoding="utf-8")


def test_local_support_bibliography_and_selected_graphics_are_complete(
    tmp_path: Path,
) -> None:
    _write(
        tmp_path,
        "paper/main.tex",
        r"""\documentclass{local}
\usepackage{pkg}
\usepackage[style=custom,datamodel=model]{biblatex}
\bibliographystyle{localbst}
\addbibresource{refs/library.bib}
\graphicspath{{figures/}}
\includegraphics{chosen}
""",
    )
    _write(tmp_path, "paper/local.cls", r"\LoadClass{article}\RequirePackage{nested}")
    _write(tmp_path, "paper/pkg.sty", r"\input{defs.def}")
    _write(tmp_path, "paper/nested.sty")
    _write(tmp_path, "paper/defs.def")
    _write(tmp_path, "paper/custom.bbx", r"\RequireBibliographyStyle{base}")
    _write(tmp_path, "paper/custom.cbx", r"\RequireCitationStyle{basecite}")
    _write(tmp_path, "paper/base.bbx")
    _write(tmp_path, "paper/basecite.cbx")
    _write(tmp_path, "paper/model.dbx")
    _write(tmp_path, "paper/localbst.bst")
    _write(tmp_path, "paper/refs/library.bib", "@book{a,title={A}}")
    _write(tmp_path, "paper/figures/chosen.pdf", b"%PDF fixture")
    _write(tmp_path, "paper/figures/unrelated.pdf", b"%PDF unrelated")
    _write(tmp_path, "paper/orphan.tex", "not a project member")
    _write(tmp_path, "paper/unreferenced.sty", "not a project member")
    _write(tmp_path, "paper/refs/unreferenced.bib", "@book{z,title={Z}}")

    manifest = LatexProjectManifestBuilder(
        source_root=tmp_path,
        repository_authority="repo",
        projects=[{"project_dir": "paper", "entrypoints": ["main.tex"]}],
    ).build()
    paths = {item.path for item in manifest.projects[0].files}
    assert {
        "paper/main.tex",
        "paper/local.cls",
        "paper/pkg.sty",
        "paper/nested.sty",
        "paper/defs.def",
        "paper/custom.bbx",
        "paper/custom.cbx",
        "paper/base.bbx",
        "paper/basecite.cbx",
        "paper/model.dbx",
        "paper/localbst.bst",
        "paper/refs/library.bib",
        "paper/figures/chosen.pdf",
    } <= paths
    assert "paper/figures/unrelated.pdf" not in paths
    assert "paper/orphan.tex" not in paths
    inventory = {item.path for item in manifest.source_inventory}
    assert {
        "paper/orphan.tex",
        "paper/unreferenced.sty",
        "paper/refs/unreferenced.bib",
    } <= inventory
    assert "paper/figures/unrelated.pdf" not in inventory


def test_generated_artifacts_are_excluded_unless_exactly_overridden(
    tmp_path: Path,
) -> None:
    _write(
        tmp_path,
        "paper/main.tex",
        r"\documentclass{article}\input{main.aux}\input{build/kept.tex}",
    )
    _write(tmp_path, "paper/main.aux", "generated")
    _write(tmp_path, "paper/build/kept.tex", "generated")
    ordinary = LatexProjectManifestBuilder(
        source_root=tmp_path,
        repository_authority="repo",
        projects=[{"project_dir": "paper", "entrypoints": ["main.tex"]}],
    ).build()
    assert {item.path for item in ordinary.projects[0].files} == {"paper/main.tex"}
    assert [warning.code for warning in ordinary.projects[0].warnings].count(
        "latex_dependency_excluded"
    ) == 2

    overridden = LatexProjectManifestBuilder(
        source_root=tmp_path,
        repository_authority="repo",
        projects=[{"project_dir": "paper", "entrypoints": ["main.tex"]}],
        include_generated_paths=["main.aux", "build/kept.tex"],
    ).build()
    assert {item.path for item in overridden.projects[0].files} == {
        "paper/main.tex",
        "paper/main.aux",
        "paper/build/kept.tex",
    }


def test_nested_style_and_graphic_resolution_prefers_project_directory(
    tmp_path: Path,
) -> None:
    _write(tmp_path, "paper/main.tex", r"\documentclass{article}\input{sections/body}")
    _write(
        tmp_path,
        "paper/sections/body.tex",
        r"\usepackage{theme}\includegraphics{diagram}",
    )
    _write(tmp_path, "paper/theme.sty", "project style")
    _write(tmp_path, "paper/sections/theme.sty", "including style")
    _write(tmp_path, "paper/diagram.pdf", b"project graphic")
    _write(tmp_path, "paper/sections/diagram.pdf", b"including graphic")

    project = LatexProjectManifestBuilder(
        source_root=tmp_path,
        repository_authority="repo",
        projects=[{"project_dir": "paper", "entrypoints": ["main.tex"]}],
    ).build().projects[0]
    paths = {item.path for item in project.files}
    assert "paper/theme.sty" in paths
    assert "paper/sections/theme.sty" not in paths
    assert "paper/diagram.pdf" in paths
    assert "paper/sections/diagram.pdf" not in paths


def test_brace_grouped_dependency_arguments_resolve(tmp_path: Path) -> None:
    _write(
        tmp_path,
        "paper/main.tex",
        r"""\documentclass{article}
\usepackage{{theme}}
\addbibresource{{refs/library}.bib}
\input{{sections/body}.tex}
\includegraphics{figures/plain}
\includegraphics[width=\textwidth]{{figures/grouped}.pdf}
\includegraphics{{figures/elided.v2}}
""",
    )
    _write(tmp_path, "paper/theme.sty", "style")
    _write(tmp_path, "paper/refs/library.bib", "@book{a,title={A}}")
    _write(tmp_path, "paper/sections/body.tex", "Body text.")
    _write(tmp_path, "paper/figures/plain.pdf", b"%PDF plain")
    _write(tmp_path, "paper/figures/grouped.pdf", b"%PDF grouped")
    # The idiom's whole purpose: the group protects the dot in `.v2` from
    # being read as the extension, which graphicx then searches for.
    _write(tmp_path, "paper/figures/elided.v2.pdf", b"%PDF elided")

    project = LatexProjectManifestBuilder(
        source_root=tmp_path,
        repository_authority="repo",
        projects=[{"project_dir": "paper", "entrypoints": ["main.tex"]}],
    ).build().projects[0]

    figures = sorted(
        item.path for item in project.files if "figure" in item.roles
    )
    assert figures == [
        "paper/figures/elided.v2.pdf",
        "paper/figures/grouped.pdf",
        "paper/figures/plain.pdf",
    ]
    selected = {item.path for item in project.files}
    assert {
        "paper/sections/body.tex",
        "paper/refs/library.bib",
        "paper/theme.sty",
    } <= selected
    grouped = [
        occurrence for occurrence in project.dependencies
        if occurrence.literal == "{figures/grouped}.pdf"
    ]
    assert [occurrence.status for occurrence in grouped] == ["resolved"]
    assert grouped[0].resolved_path == "paper/figures/grouped.pdf"
    assert project.warnings == ()


def test_brace_grouping_never_invents_a_resolution(tmp_path: Path) -> None:
    _write(tmp_path, "outside.pdf", b"%PDF outside")
    _write(
        tmp_path,
        "paper/main.tex",
        r"""\documentclass{article}
\includegraphics{{figures/absent}.pdf}
\includegraphics{{../outside}.pdf}
\includegraphics{figures/{odd}.pdf}
""",
    )
    _write(tmp_path, "paper/figures/{odd}.pdf", b"%PDF literally braced")
    _write(tmp_path, "paper/figures/odd.pdf", b"%PDF debraced")

    project = LatexProjectManifestBuilder(
        source_root=tmp_path,
        repository_authority="repo",
        projects=[{"project_dir": "paper", "entrypoints": ["main.tex"]}],
    ).build().projects[0]

    statuses = {
        occurrence.literal: occurrence.status
        for occurrence in project.dependencies
        if occurrence.role == "figure"
    }
    assert statuses["{figures/absent}.pdf"] == "missing"
    assert statuses["{../outside}.pdf"] == "escape"
    # The verbatim literal is probed before the debraced one, so a file whose
    # name really contains braces still wins over its debraced neighbour.
    assert statuses["figures/{odd}.pdf"] == "resolved"
    figures = {item.path for item in project.files if "figure" in item.roles}
    assert figures == {"paper/figures/{odd}.pdf"}
    assert {warning.code for warning in project.warnings} == {
        "latex_dependency_missing",
        "latex_dependency_escape",
    }


def test_unbalanced_braces_are_not_unwrapped() -> None:
    # A guard against a future caller, not the thing that classifies \import
    # today: `_dynamic_request`'s `first}{second` literal already fails
    # `_is_literal`, so `_resolve_request` returns `dynamic` before it gets
    # here. It must still never be flattened into a path nobody wrote.
    variants = latex_project_manifest._literal_variants
    assert variants("dir}{body") == (("dir}{body", False),)
    assert variants("figures/fig.pdf") == (("figures/fig.pdf", False),)
    # Extension written outside the group -> use it as written.
    assert variants("{figures/fig}.pdf") == (
        ("{figures/fig}.pdf", False), ("figures/fig.pdf", False),
    )
    # Extension elided -> the dot inside the group is part of the basename.
    assert variants("{figures/fig.v2}") == (
        ("{figures/fig.v2}", False), ("figures/fig.v2", True),
    )


def test_import_requests_are_classified_dynamic_not_debraced(tmp_path: Path) -> None:
    _write(
        tmp_path,
        "paper/main.tex",
        "\\documentclass{article}\n\\import{\\subdir}{body}\n",
    )
    project = LatexProjectManifestBuilder(
        source_root=tmp_path,
        repository_authority="repo",
        projects=[{"project_dir": "paper", "entrypoints": ["main.tex"]}],
    ).build().projects[0]
    imports = [
        occurrence for occurrence in project.dependencies
        if occurrence.command == "import"
    ]
    assert [occurrence.status for occurrence in imports] == ["dynamic"]
    assert imports[0].resolved_path is None


def test_generated_explicit_root_requires_exact_override(tmp_path: Path) -> None:
    _write(tmp_path, "paper/build/main.tex", r"\documentclass{article}Body")
    ordinary = LatexProjectManifestBuilder(
        source_root=tmp_path,
        repository_authority="repo",
        projects=[{
            "project_dir": "paper",
            "entrypoints": ["build/main.tex"],
        }],
    ).build()
    assert ordinary.projects == ()
    assert "latex_root_excluded" in [warning.code for warning in ordinary.warnings]

    overridden = LatexProjectManifestBuilder(
        source_root=tmp_path,
        repository_authority="repo",
        projects=[{
            "project_dir": "paper",
            "entrypoints": ["build/main.tex"],
        }],
        include_generated_paths=["build/main.tex"],
    ).build()
    assert [project.root_path for project in overridden.projects] == [
        "paper/build/main.tex"
    ]
