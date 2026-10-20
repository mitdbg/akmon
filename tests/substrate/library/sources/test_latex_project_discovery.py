from __future__ import annotations

import hashlib
import json
from pathlib import Path

from okg.substrate.library.sources.scholarly.latex_project_manifest import (
    LatexProjectManifestBuilder,
)


def _write(root: Path, rel: str, text: str | bytes) -> Path:
    path = root / rel
    path.parent.mkdir(parents=True, exist_ok=True)
    if isinstance(text, bytes):
        path.write_bytes(text)
    else:
        path.write_text(text, encoding="utf-8")
    return path


def test_explicit_root_builds_depth_first_manifest_and_stable_paper_id(
    tmp_path: Path,
) -> None:
    _write(
        tmp_path,
        "paper/main.tex",
        r"""\documentclass{article}
\input{sections/one}
\include{sections/two}
\bibliography{refs/main,refs/extra}
""",
    )
    _write(tmp_path, "paper/sections/one.tex", r"\input{nested/three}")
    _write(tmp_path, "paper/sections/nested/three.tex", "Three")
    _write(tmp_path, "paper/sections/two.tex", "Two")
    _write(tmp_path, "paper/refs/main.bib", "@book{a,title={A}}")
    _write(tmp_path, "paper/refs/extra.bib", "@book{b,title={B}}")

    builder = LatexProjectManifestBuilder(
        source_root=tmp_path,
        repository_authority="example/repo",
        projects=[{"project_dir": "paper", "entrypoints": ["main.tex"]}],
    )
    first = builder.build()
    project = first.projects[0]
    assert [item.path for item in project.files] == [
        "paper/main.tex",
        "paper/sections/one.tex",
        "paper/sections/nested/three.tex",
        "paper/sections/two.tex",
        "paper/refs/main.bib",
        "paper/refs/extra.bib",
    ]
    assert project.root_path == "paper/main.tex"
    assert len({item.paper_id for item in first.projects}) == 1
    original_id = project.paper_id

    _write(tmp_path, "paper/sections/two.tex", "Edited without changing identity")
    second = builder.build()
    assert second.projects[0].paper_id == original_id
    assert second.digest != first.digest


def test_automatic_roots_suppress_reachable_documentclass_fragments_and_share_members(
    tmp_path: Path,
) -> None:
    _write(
        tmp_path,
        "paper/a.tex",
        r"\documentclass{article}\input{standalone-fragment}\input{shared}",
    )
    _write(tmp_path, "paper/b.tex", r"\documentclass{article}\input{shared}")
    _write(tmp_path, "paper/standalone-fragment.tex", r"\documentclass{subfiles}Body")
    _write(tmp_path, "paper/shared.tex", "Shared")

    manifest = LatexProjectManifestBuilder(
        source_root=tmp_path,
        repository_authority="example/repo",
        projects=[{"project_dir": "paper"}],
    ).build()
    assert [project.root_path for project in manifest.projects] == [
        "paper/a.tex",
        "paper/b.tex",
    ]
    assert all(
        "paper/shared.tex" in {item.path for item in project.files}
        for project in manifest.projects
    )
    assert all(
        project.root_path != "paper/standalone-fragment.tex"
        for project in manifest.projects
    )


def test_manifest_digest_is_deterministic_and_configuration_sensitive(
    tmp_path: Path,
) -> None:
    _write(tmp_path, "paper/main.tex", r"\documentclass{article}Body")
    kwargs = {
        "source_root": tmp_path,
        "repository_authority": "example/repo",
        "projects": [{"project_dir": "paper"}],
    }
    first = LatexProjectManifestBuilder(**kwargs).build()
    second = LatexProjectManifestBuilder(**kwargs).build()
    tighter = LatexProjectManifestBuilder(**kwargs, max_depth=12).build()
    assert first == second
    assert first.digest != tighter.digest


def test_base_precedence_and_import_exception_are_explicit(tmp_path: Path) -> None:
    _write(tmp_path, "paper/main.tex", r"\documentclass{article}\input{sections/a}")
    _write(
        tmp_path,
        "paper/sections/a.tex",
        r"""\input{common}
\input{../fallback}
\import{imports/}{piece}
\subimport{subimports/}{piece}
""",
    )
    _write(tmp_path, "paper/common.tex", "project wins")
    _write(tmp_path, "paper/sections/common.tex", "including loses")
    _write(tmp_path, "paper/fallback.tex", "safe including fallback")
    _write(tmp_path, "paper/imports/piece.tex", "project import loses")
    _write(tmp_path, "paper/sections/imports/piece.tex", "including import wins")
    _write(tmp_path, "paper/subimports/piece.tex", "project subimport loses")
    _write(tmp_path, "paper/sections/subimports/piece.tex", "including subimport wins")

    project = LatexProjectManifestBuilder(
        source_root=tmp_path,
        repository_authority="repo",
        projects=[{"project_dir": "paper", "entrypoints": ["main.tex"]}],
    ).build().projects[0]
    paths = {item.path for item in project.files}
    assert "paper/common.tex" in paths
    assert "paper/sections/common.tex" not in paths
    assert "paper/fallback.tex" in paths
    assert "paper/sections/imports/piece.tex" in paths
    assert "paper/imports/piece.tex" not in paths
    assert "paper/sections/subimports/piece.tex" in paths
    assert "paper/subimports/piece.tex" not in paths


def test_member_budgets_use_exact_boundaries_and_keep_dfs_order(
    tmp_path: Path,
) -> None:
    main = r"\documentclass{article}\input{z}\input{a}"
    _write(tmp_path, "paper/main.tex", main)
    _write(tmp_path, "paper/z.tex", r"\input{q}")
    _write(tmp_path, "paper/q.tex", "Q")
    _write(tmp_path, "paper/a.tex", "A")

    exact_total = len(main.encode()) + len(r"\input{q}".encode())
    project = LatexProjectManifestBuilder(
        source_root=tmp_path,
        repository_authority="repo",
        projects=[{"project_dir": "paper", "entrypoints": ["main.tex"]}],
        max_files=2,
    ).build().projects[0]

    assert [item.path for item in project.files] == [
        "paper/main.tex",
        "paper/z.tex",
    ]
    assert [
        (item.source_path, item.literal)
        for item in project.dependencies
        if item.role == "input"
    ] == [
        ("paper/main.tex", "z"),
        ("paper/z.tex", "q"),
        ("paper/main.tex", "a"),
    ]
    assert "latex_dependency_over_files" in [
        warning.code for warning in project.warnings
    ]

    total_limited = LatexProjectManifestBuilder(
        source_root=tmp_path,
        repository_authority="repo",
        projects=[{"project_dir": "paper", "entrypoints": ["main.tex"]}],
        max_total_bytes=exact_total,
    ).build().projects[0]
    assert [item.path for item in total_limited.files] == [
        "paper/main.tex",
        "paper/z.tex",
    ]
    assert "latex_dependency_over_total_bytes" in [
        warning.code for warning in total_limited.warnings
    ]


def test_depth_and_total_byte_overflow_are_loud_and_root_failure_emits_no_paper(
    tmp_path: Path,
) -> None:
    main = r"\documentclass{article}\input{a}"
    _write(tmp_path, "paper/main.tex", main)
    _write(tmp_path, "paper/a.tex", r"\input{b}")
    _write(tmp_path, "paper/b.tex", "B")

    depth_limited = LatexProjectManifestBuilder(
        source_root=tmp_path,
        repository_authority="repo",
        projects=[{"project_dir": "paper", "entrypoints": ["main.tex"]}],
        max_depth=1,
    ).build().projects[0]
    assert {item.path for item in depth_limited.files} == {
        "paper/main.tex",
        "paper/a.tex",
    }
    assert "latex_dependency_over_depth" in [
        warning.code for warning in depth_limited.warnings
    ]

    root_overflow = LatexProjectManifestBuilder(
        source_root=tmp_path,
        repository_authority="repo",
        projects=[{"project_dir": "paper", "entrypoints": ["main.tex"]}],
        max_total_bytes=len(main.encode()) - 1,
    ).build()
    assert root_overflow.projects == ()
    assert "latex_dependency_over_total_bytes" in [
        warning.code for warning in root_overflow.warnings
    ]


def test_digest_binds_canonical_configuration_and_rehashes_payload(
    tmp_path: Path,
) -> None:
    _write(tmp_path, "paper/main.tex", r"\documentclass{article}Body")
    common = {
        "source_root": tmp_path,
        "repository_authority": "repo",
    }
    automatic = LatexProjectManifestBuilder(
        **common,
        projects=[{"project_dir": "paper"}],
    ).build()
    explicit = LatexProjectManifestBuilder(
        **common,
        projects=[{"project_dir": "paper", "entrypoints": ["main.tex"]}],
    ).build()
    generated_config = LatexProjectManifestBuilder(
        **common,
        projects=[{"project_dir": "paper"}],
        generated_dirs=["scratch-output"],
        include_generated_paths=["scratch-output/keep.tex"],
    ).build()

    assert automatic.digest != explicit.digest
    assert automatic.digest != generated_config.digest
    encoded = json.dumps(
        automatic.canonical_payload(),
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    assert hashlib.sha256(encoded).hexdigest() == automatic.digest


def test_project_and_entrypoint_paths_normalize_to_posix_before_resolution(
    tmp_path: Path,
) -> None:
    _write(
        tmp_path,
        "foo/bar/sections/main.tex",
        r"\documentclass{article}Body",
    )
    manifest = LatexProjectManifestBuilder(
        source_root=tmp_path,
        repository_authority="repo",
        projects=[{
            "project_dir": "foo\\bar",
            "entrypoints": ["sections\\main.tex"],
        }],
    ).build()

    assert [project.project_dir for project in manifest.projects] == ["foo/bar"]
    assert [project.root_path for project in manifest.projects] == [
        "foo/bar/sections/main.tex"
    ]
