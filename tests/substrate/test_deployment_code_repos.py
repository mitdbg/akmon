"""code_repos: expansion in the source-registry loader.

A `code_repos:` block in source_registry.yaml expands into per-repo
git-files-<slug> + code-structure-<slug> sources, rendered from the
canonical codebase-index template. See PACT code-repos-multi-repo.
"""
from __future__ import annotations

import textwrap
from pathlib import Path

import pytest

from okg.substrate.sources.registry import load_source_registry


def _write_registry(tmp_path: Path, body: str) -> Path:
    p = tmp_path / "source_registry.yaml"
    p.write_text(textwrap.dedent(body).lstrip(), encoding="utf-8")
    return p


def test_code_repos_expands_per_repo_sources(tmp_path: Path) -> None:
    base = tmp_path / "repos"
    reg = _write_registry(
        tmp_path,
        f"""
        code_repos:
          base: {base}
          repos:
            - {{slug: alpha, url: https://example.com/alpha.git}}
            - {{slug: beta,  url: https://example.com/beta.git}}
        sources: {{}}
        """,
    )
    entries = load_source_registry(reg)

    for slug in ("alpha", "beta"):
        gf = entries.get(f"git-files-{slug}")
        cs = entries.get(f"code-structure-{slug}")
        assert gf is not None, f"missing git-files-{slug}"
        assert cs is not None, f"missing code-structure-{slug}"
        # repo_root resolves to <base>/<slug>
        assert gf.raw["params"]["repo_root"] == str(base / slug)
        assert cs.raw["params"]["repo_root"] == str(base / slug)
        # the per-repo identity is unique (ownership_id carries the slug)
        assert slug in gf.ownership_id
        # code-structure depends on its git-files sibling for this repo
        assert cs.raw["predecessor_source"] == gf.ownership_id


def test_code_repos_expanded_sync_watch_roots_resolved(tmp_path: Path) -> None:
    """Expanded sources must not leak the __OKG_REPO_ROOT__ template sentinel
    into sync.watch.roots — else live fs-event watching points at a nonexistent
    dir. The root must resolve to <base>/<dir>."""
    base = tmp_path / "repos"
    reg = _write_registry(
        tmp_path,
        f"""
        code_repos:
          base: {base}
          repos:
            - {{slug: alpha, url: https://example.com/alpha.git}}
        sources: {{}}
        """,
    )
    gf = load_source_registry(reg)["git-files-alpha"]
    roots = ((gf.raw.get("sync") or {}).get("watch") or {}).get("roots") or []
    assert "__OKG_REPO_ROOT__" not in roots
    assert str(base / "alpha") in roots


def test_code_repos_emits_no_doc_sources(tmp_path: Path) -> None:
    """code_repos expands ONLY git-files/code-structure (no doc_corpus), so it
    introduces no document->source_file cross-module edge and needs no bridge.
    The document->source_file narrowing stays a per-deployment opt-in by design
    (extraction/schema.yaml)."""
    base = tmp_path / "repos"
    reg = _write_registry(
        tmp_path,
        f"""
        code_repos:
          base: {base}
          repos:
            - {{slug: alpha, url: https://example.com/alpha.git}}
        sources: {{}}
        """,
    )
    expanded = {
        name for name in load_source_registry(reg) if name not in {}
    }
    # only the git-files/code-structure pair is synthesized
    assert expanded == {"git-files-alpha", "code-structure-alpha"}
    assert not any("doc" in name for name in expanded)


def test_code_repos_additive_explicit_source_wins(tmp_path: Path) -> None:
    base = tmp_path / "repos"
    reg = _write_registry(
        tmp_path,
        f"""
        code_repos:
          base: {base}
          repos:
            - {{slug: alpha, url: https://example.com/alpha.git}}
        sources:
          git-files-alpha:
            module: example_sources.placeholder
            class: Placeholder
            ownership_id: explicit.git-files-alpha
            source_class: discovery_crawl
            record_identity_kind: scoped_locator
            record_identity_fields: [repo, path]
            source_revision_kind: content_hash
            deletion_semantics: missing_from_completed_scope
            publication_mode: published_generation
            params: {{repo: alpha, repo_root: /explicit/path}}
        """,
    )
    entries = load_source_registry(reg)
    # explicit entry of the same name wins; expansion does not overwrite it
    assert entries["git-files-alpha"].ownership_id == "explicit.git-files-alpha"
    assert entries["git-files-alpha"].raw["params"]["repo_root"] == "/explicit/path"
    # the code-structure half (not explicitly declared) is still expanded
    assert "code-structure-alpha" in entries
    assert (
        entries["code-structure-alpha"].raw["predecessor_source"]
        == "explicit.git-files-alpha"
    )

    from okg.substrate.ingest.planning import resolve_predecessor_deps

    assert resolve_predecessor_deps(entries)["code-structure-alpha"] == {
        "git-files-alpha"
    }


def test_code_repos_repo_root_dir_from_url(tmp_path: Path) -> None:
    """The checkout dir comes from the URL basename (slug != dir), with an
    optional `dir` override. slug `wmcore` + url .../WMCore.git -> <base>/WMCore."""
    base = tmp_path / "repos"
    reg = _write_registry(
        tmp_path,
        f"""
        code_repos:
          base: {base}
          repos:
            - {{slug: wmcore,    url: https://github.com/dmwm/WMCore.git}}
            - {{slug: rucioflux, url: https://github.com/dmwm/rucio-flux.git}}
            - {{slug: custom,    url: https://x/y.git, dir: ExplicitDir}}
        sources: {{}}
        """,
    )
    e = load_source_registry(reg)
    assert e["git-files-wmcore"].raw["params"]["repo_root"] == str(base / "WMCore")
    assert e["git-files-rucioflux"].raw["params"]["repo_root"] == str(base / "rucio-flux")
    assert e["git-files-custom"].raw["params"]["repo_root"] == str(base / "ExplicitDir")
    # the slug (not the dir) remains the node/source identity
    assert "wmcore" in e["git-files-wmcore"].ownership_id


def test_code_repos_base_env_var_expansion(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    base = tmp_path / "envrepos"
    monkeypatch.setenv("OKG_CODE_REPOS_ROOT", str(base))
    reg = _write_registry(
        tmp_path,
        """
        code_repos:
          base: ${OKG_CODE_REPOS_ROOT}
          repos:
            - {slug: gamma, url: https://example.com/gamma.git}
        sources: {}
        """,
    )
    entries = load_source_registry(reg)
    assert entries["git-files-gamma"].raw["params"]["repo_root"] == str(base / "gamma")


@pytest.mark.parametrize(
    "block, match",
    [
        # missing base
        ("code_repos:\n  repos:\n    - {slug: a, url: x/a.git}\nsources: {}\n", "base"),
        # relative base
        ("code_repos:\n  base: ./rel\n  repos:\n    - {slug: a, url: x/a.git}\nsources: {}\n", "absolute"),
        # duplicate slug
        ("code_repos:\n  base: /abs\n  repos:\n    - {slug: a, url: x/a.git}\n    - {slug: a, url: x/b.git}\nsources: {}\n", "duplicate"),
        # empty slug
        ("code_repos:\n  base: /abs\n  repos:\n    - {slug: '', url: x/a.git}\nsources: {}\n", "slug"),
        # non-dict entry
        ("code_repos:\n  base: /abs\n  repos:\n    - just-a-string\nsources: {}\n", "mapping"),
        # absolute checkout dir
        ("code_repos:\n  base: /abs\n  repos:\n    - {slug: a, url: x/a.git, dir: /tmp/outside}\nsources: {}\n", "dir"),
        # checkout dir escaping base
        ("code_repos:\n  base: /abs\n  repos:\n    - {slug: a, url: x/a.git, dir: ../outside}\nsources: {}\n", "dir"),
    ],
)
def test_code_repos_error_paths_are_loud(
    tmp_path: Path, block: str, match: str
) -> None:
    reg = _write_registry(tmp_path, block)
    with pytest.raises(ValueError, match=match):
        load_source_registry(reg)


def test_code_repos_relative_base_rejected(tmp_path: Path) -> None:
    reg = _write_registry(
        tmp_path,
        """
        code_repos:
          base: ./relative
          repos:
            - {slug: delta, url: https://example.com/delta.git}
        sources: {}
        """,
    )
    with pytest.raises(ValueError, match="absolute"):
        load_source_registry(reg)
