from __future__ import annotations

from pathlib import Path

import yaml


ROOT = Path(__file__).resolve().parents[4]
REGISTRY = ROOT / "deployments/okg-workspace/source_registry.yaml"
PROFILE_ROOT = ROOT / "src/okg/substrate/library/profiles"
PROJECT_SOURCE_NAMES = (
    "papers-git-files",
    "latex-project-doc-corpus",
    "bib-references",
    "latex-project",
)


def _load(path: Path) -> dict:
    return yaml.safe_load(path.read_text(encoding="utf-8"))


def test_okg_workspace_wires_one_shared_manifest_in_dependency_order() -> None:
    sources = _load(REGISTRY)["sources"]
    positions = [list(sources).index(name) for name in PROJECT_SOURCE_NAMES]
    assert positions == sorted(positions)

    git_files = sources["papers-git-files"]
    documents = sources["latex-project-doc-corpus"]
    bibliography = sources["bib-references"]
    project = sources["latex-project"]

    assert git_files["ownership_id"] == "okg-workspace.papers-git-files"
    assert documents["predecessor_source"] == git_files["ownership_id"]
    # The substrate injects `dsn`; an authored value is refused once the
    # registry turns strict, and lint now blocks on it (issue #2150).
    assert "dsn" not in documents["params"]
    assert bibliography["predecessor_source"] == git_files["ownership_id"]
    assert project["predecessor_source"] == [
        documents["ownership_id"],
        bibliography["ownership_id"],
    ]
    assert project["params"]["predecessor_sources"] == project["predecessor_source"]
    assert "dsn" not in project["params"]

    manifests = [
        documents["params"]["project_manifest"],
        bibliography["params"]["project_manifest"],
        project["params"]["project_manifest"],
    ]
    assert manifests[0] == manifests[1] == manifests[2]
    assert manifests[0] == {
        "repository_authority": "papers",
        "projects": [
            {"project_dir": "consistency", "entrypoints": ["consistency.tex"]},
            {"project_dir": "akmon", "entrypoints": ["vision.tex"]},
            {"project_dir": "althea", "entrypoints": ["althea.tex"]},
        ],
        "include_generated_paths": [],
        "generated_dirs": [],
    }
    assert documents["params"]["include_globs"] == ["**/*"]
    assert bibliography["ownership_id"] == "okg-workspace.bib_references"
    assert "migration" not in bibliography["params"]
    assert "migration" not in project["params"]


def test_researcher_profiles_preserve_file_only_defaults_and_offer_opt_in() -> None:
    for profile in ("academic-researcher",):
        defaults = PROFILE_ROOT / profile / "source-defaults"
        bibliography = _load(defaults / "bib_references.yaml")["bib_references"]
        git_files = _load(defaults / "latex_project_git_files.yaml")[
            "latex_project_git_files"
        ]
        documents = _load(defaults / "latex_project_doc_corpus.yaml")[
            "latex_project_doc_corpus"
        ]
        project = _load(defaults / "latex_project.yaml")["latex_project"]

        assert bibliography["ownership_id"] == "${deployment_name}.bib_references"
        assert bibliography["params"]["project_manifest"] is None
        assert bibliography["params"]["bib_paths"] == ["${papers_dir}/*.bib"]
        assert bibliography["params"]["migration"]["legacy_bib_paths"] == [
            "${papers_dir}/*.bib"
        ]
        assert bibliography["params"]["migration"]["legacy_ownership_id"] == (
            bibliography["ownership_id"]
        )
        # `source_name` and `dsn` are substrate-injected, so a shipped
        # default must NOT author them: the strict adapter contract refuses
        # `params` that name an injected parameter, and the refusal blocked
        # adding ANY source to a scaffolded deployment (issue #2126). These
        # assertions used to pin the authored values, which is why no test
        # caught the defect. The substrate supplies `source_name` from
        # `ownership_id` and `dsn` from deployment authority.
        assert "source_name" not in bibliography["params"]
        assert "dsn" not in bibliography["params"]

        assert documents["enabled_by_default"] is False
        assert git_files["enabled_by_default"] is False
        assert project["enabled_by_default"] is False
        assert git_files["params"]["repo"] == documents["params"]["repo"]
        assert git_files["params"]["repo_root"] == documents["params"]["repo_root"]
        assert documents["params"]["predecessor_source"] == git_files[
            "ownership_id"
        ]
        assert documents["params"]["include_globs"] == ["**/*"]
        assert "dsn" not in documents["params"]
        assert documents["params"]["project_manifest"] == project["params"][
            "project_manifest"
        ]
        assert project["params"]["predecessor_sources"] == [
            "${deployment_name}.latex_project_doc_corpus",
            "${deployment_name}.bib_references",
        ]
        assert "dsn" not in project["params"]
        assert documents["ownership_id"] == (
            "${deployment_name}.latex_project_doc_corpus"
        )
        assert project["ownership_id"] == "${deployment_name}.latex_project"
