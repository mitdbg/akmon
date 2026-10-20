"""code_repos `expand:` — render a configurable set of template blocks per repo.

By default code_repos expands only git-files + code-structure. `expand: [...]`
lets a deployment generate additional codebase-index blocks (interfaces,
doc-corpus, ...) for EVERY declared repo instead of hand-grafting them onto one.
"""
from __future__ import annotations

import textwrap
from pathlib import Path

import pytest

from okg.substrate.sources.registry import load_source_registry


def _write(tmp_path: Path, body: str) -> Path:
    p = tmp_path / "source_registry.yaml"
    p.write_text(textwrap.dedent(body).lstrip(), encoding="utf-8")
    return p


def test_expand_list_renders_extra_blocks_per_repo(tmp_path: Path) -> None:
    base = tmp_path / "repos"
    reg = _write(
        tmp_path,
        f"""
        code_repos:
          base: {base}
          expand: [git-files, code-structure, interfaces, doc-corpus]
          repos:
            - {{slug: alpha, url: https://x/alpha.git}}
            - {{slug: beta,  url: https://x/beta.git}}
        sources: {{}}
        """,
    )
    e = load_source_registry(reg)
    for slug in ("alpha", "beta"):
        for block in ("git-files", "code-structure", "interfaces", "doc-corpus"):
            assert f"{block}-{slug}" in e, f"missing {block}-{slug}"
    # interfaces depends on its repo's git-files + code-structure (auto-wired)
    assert e["interfaces-alpha"].raw["predecessor_source"] == [
        "alpha.git-files",
        "alpha.code-structure",
    ]


def test_expand_defaults_to_git_files_and_code_structure(tmp_path: Path) -> None:
    base = tmp_path / "repos"
    reg = _write(
        tmp_path,
        f"""
        code_repos:
          base: {base}
          repos:
            - {{slug: alpha, url: https://x/alpha.git}}
        sources: {{}}
        """,
    )
    expanded = {n for n in load_source_registry(reg) if n.endswith("-alpha")}
    assert expanded == {"git-files-alpha", "code-structure-alpha"}


def test_expand_interfaces_predecessor_resolves(tmp_path: Path) -> None:
    """The generated interfaces/doc-corpus predecessors don't just *declare* the
    right names — they RESOLVE to this repo's expanded git-files/code-structure."""
    from okg.substrate.ingest.planning import resolve_predecessor_deps

    base = tmp_path / "repos"
    reg = _write(
        tmp_path,
        f"""
        code_repos:
          base: {base}
          expand: [git-files, code-structure, interfaces, doc-corpus]
          repos:
            - {{slug: alpha, url: https://x/alpha.git}}
        sources: {{}}
        """,
    )
    deps = resolve_predecessor_deps(load_source_registry(reg))
    assert deps["interfaces-alpha"] == {"git-files-alpha", "code-structure-alpha"}
    assert deps["doc-corpus-alpha"] == {"git-files-alpha"}


def test_expand_conditional_only_block_rejected(tmp_path: Path) -> None:
    """openspec sits behind a {{ if openspec_enabled }} conditional (forced off
    during expansion), so it isn't available to expand — must fail loudly."""
    base = tmp_path / "repos"
    reg = _write(
        tmp_path,
        f"""
        code_repos:
          base: {base}
          expand: [git-files, openspec]
          repos:
            - {{slug: alpha, url: https://x/alpha.git}}
        sources: {{}}
        """,
    )
    with pytest.raises(ValueError, match="openspec"):
        load_source_registry(reg)


def test_expand_unknown_block_errors(tmp_path: Path) -> None:
    base = tmp_path / "repos"
    reg = _write(
        tmp_path,
        f"""
        code_repos:
          base: {base}
          expand: [git-files, not-a-real-block]
          repos:
            - {{slug: alpha, url: https://x/alpha.git}}
        sources: {{}}
        """,
    )
    with pytest.raises(ValueError, match="not-a-real-block"):
        load_source_registry(reg)


# ---- checkout acquisition rendering (code-repos-auto-acquire) ----

def _params(entry) -> dict:
    return dict(entry.raw.get("params") or {})


def test_acquire_owner_git_files_gets_url_and_manage_checkout(tmp_path: Path) -> None:
    base = tmp_path / "repos"
    reg = _write(
        tmp_path,
        f"""
        code_repos:
          base: {base}
          expand: [git-files, code-structure]
          repos:
            - {{slug: alpha, url: https://x/alpha.git}}
        sources: {{}}
        """,
    )
    e = load_source_registry(reg)
    owner = _params(e["git-files-alpha"])
    assert owner["url"] == "https://x/alpha.git"
    assert owner["manage_checkout"] is True


def test_acquire_dependents_are_not_managed(tmp_path: Path) -> None:
    base = tmp_path / "repos"
    reg = _write(
        tmp_path,
        f"""
        code_repos:
          base: {base}
          expand: [git-files, code-structure, interfaces, doc-corpus]
          repos:
            - {{slug: alpha, url: https://x/alpha.git}}
        sources: {{}}
        """,
    )
    e = load_source_registry(reg)
    # Only git-files owns the checkout; dependents inherit it (no manage_checkout).
    for block in ("code-structure", "interfaces", "doc-corpus"):
        params = _params(e[f"{block}-alpha"])
        assert "manage_checkout" not in params, block


def test_acquire_repo_opt_out_leaves_owner_unmanaged(tmp_path: Path) -> None:
    base = tmp_path / "repos"
    reg = _write(
        tmp_path,
        f"""
        code_repos:
          base: {base}
          repos:
            - {{slug: alpha, url: https://x/alpha.git, manage_checkout: false}}
        sources: {{}}
        """,
    )
    e = load_source_registry(reg)
    owner = _params(e["git-files-alpha"])
    assert "manage_checkout" not in owner
    # Per citation-source-urls the url still flows (it feeds stored
    # citation urls on emitted nodes); opting out only stops cloning.
    assert owner["url"] == "https://x/alpha.git"


def test_acquire_tracked_branch_propagates_to_owner(tmp_path: Path) -> None:
    base = tmp_path / "repos"
    reg = _write(
        tmp_path,
        f"""
        code_repos:
          base: {base}
          repos:
            - {{slug: alpha, url: https://x/alpha.git, tracked_branch: main}}
        sources: {{}}
        """,
    )
    e = load_source_registry(reg)
    owner = _params(e["git-files-alpha"])
    assert owner["tracked_branch"] == "main"
    assert owner["manage_checkout"] is True


def test_explicit_git_files_override_rewires_all_dependents(tmp_path: Path) -> None:
    """An explicit git-files-<slug> override (with its own ownership_id) must be
    inherited by code-structure AND interfaces AND doc-corpus — not just
    code-structure (the earlier code only rewired code-structure, so interfaces
    lost the owner and doc-corpus lost all deps)."""
    from okg.substrate.ingest.planning import resolve_predecessor_deps

    base = tmp_path / "repos"
    reg = _write(
        tmp_path,
        f"""
        code_repos:
          base: {base}
          expand: [git-files, code-structure, interfaces, doc-corpus]
          repos:
            - {{slug: alpha, url: https://x/alpha.git}}
        sources:
          git-files-alpha:
            module: okg.substrate.library.sources.git_graph_files
            class: GitFilesSource
            ownership_id: explicit.git-files-alpha
            params:
              repo: alpha
              repo_root: {base}/alpha
        """,
    )
    e = load_source_registry(reg)
    assert e["git-files-alpha"].ownership_id == "explicit.git-files-alpha"
    deps = resolve_predecessor_deps(e)
    assert "git-files-alpha" in deps["code-structure-alpha"]
    assert "git-files-alpha" in deps["interfaces-alpha"]
    assert "git-files-alpha" in deps["doc-corpus-alpha"]


def test_explicit_empty_expand_renders_no_blocks(tmp_path: Path) -> None:
    """`expand: []` is honoured as 'no per-repo blocks', not silently replaced
    by the git-files+code-structure defaults."""
    base = tmp_path / "repos"
    reg = _write(
        tmp_path,
        f"""
        code_repos:
          base: {base}
          expand: []
          repos:
            - {{slug: alpha, url: https://x/alpha.git}}
        sources: {{}}
        """,
    )
    expanded = {n for n in load_source_registry(reg) if n.endswith("-alpha")}
    assert expanded == set()


def test_nomos_baseline_enumerators_cover_code_repos(tmp_path: Path) -> None:
    """Regression: the Nomos baseline seed enumerates its sources by expanding
    `code_repos:` into `sources:` (via `expand_code_repos_sources`) before
    reading it. Without that, code_repos-generated sources are invisible to
    Nomos, get no allow rule, and hit `default_deny` at source admission — so no
    code graph can publish. Guards that the loader and the seed agree on the
    source set.
    """
    import yaml

    from okg.substrate.cli.nomos_rollout_audit import (
        _nomos_source_registry_entries,
    )
    from okg.substrate.sources.registry import expand_code_repos_sources

    base = tmp_path / "repos"
    reg = _write(
        tmp_path,
        f"""
        code_repos:
          base: {base}
          expand: [git-files, code-structure, interfaces, doc-corpus]
          repos:
            - {{slug: alpha, url: https://x/alpha.git}}
        sources: {{}}
        """,
    )
    doc = yaml.safe_load(reg.read_text())

    # The seam the seed relies on: raw `sources` is empty, but expansion adds
    # every code_repos-generated source.
    expanded_sources = expand_code_repos_sources(doc, reg)
    for block in ("git-files", "code-structure", "interfaces", "doc-corpus"):
        assert f"{block}-alpha" in expanded_sources, f"missing {block}-alpha"

    # And the Nomos enumerator, fed the expanded doc as the seed now does, sees
    # them (it previously read only the literal — empty — `sources:` block).
    entries = _nomos_source_registry_entries({**doc, "sources": expanded_sources})
    assert "git-files-alpha" in entries
    assert "code-structure-alpha" in entries


def test_nomos_runtime_subjects_include_producer_id(tmp_path: Path) -> None:
    """Regression for the second root cause: a code source is NAMED
    `git-files-<slug>` but its runtime producer_id is `<slug>.git-files`. The
    baseline emit rule authorizes by producer subject, so `runtime_source_names`
    must carry the producer_id, and `_source_subject_variants` must turn it into
    `source:<slug>.git-files` — otherwise `source.emit` hits default_deny.
    """
    import yaml

    from okg.substrate.nomos.store import _source_subject_variants
    from okg.substrate.sources.registry import (
        expand_code_repos_sources,
        runtime_source_subject_ids,
    )

    base = tmp_path / "repos"
    reg = _write(
        tmp_path,
        f"""
        code_repos:
          base: {base}
          expand: [git-files, code-structure]
          repos:
            - {{slug: alpha, url: https://x/alpha.git}}
        sources: {{}}
        """,
    )
    doc = yaml.safe_load(reg.read_text())
    sources = expand_code_repos_sources(doc, reg)

    ids = runtime_source_subject_ids(sources)
    # Both the registry NAME and the runtime PRODUCER_ID must be present.
    assert "git-files-alpha" in ids
    assert "alpha.git-files" in ids  # producer_id form — the bug

    # And the subject actually presented at admission must be produced.
    subjects: set[str] = set()
    for name in ids:
        subjects.update(_source_subject_variants("cms", name))
    assert "source:alpha.git-files" in subjects


def test_nomos_code_repos_expansion_substitutes_placeholders(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Nomos callers pass raw YAML into ``expand_code_repos_sources``. It must
    still apply the same registry placeholder substitution as the loader, or the
    baseline authorizes literal ``${...}`` source ids that runtime never emits.
    """
    import yaml

    from okg.substrate.sources.registry import (
        expand_code_repos_sources,
        runtime_source_subject_ids,
    )

    monkeypatch.setenv("OKG_REPO_SLUG", "alpha")
    base = tmp_path / "repos"
    reg = _write(
        tmp_path,
        f"""
        code_repos:
          base: {base}
          expand: [git-files]
          repos:
            - slug: "${{OKG_REPO_SLUG}}"
              url: https://x/alpha.git
        sources: {{}}
        """,
    )
    doc = yaml.safe_load(reg.read_text())
    sources = expand_code_repos_sources(
        doc,
        reg,
        allow_unresolved_base=True,
    )

    assert "git-files-alpha" in sources
    assert "git-files-${OKG_REPO_SLUG}" not in sources
    assert "alpha.git-files" in runtime_source_subject_ids(sources)


def test_expand_code_repos_governance_tolerates_unresolved_base(
    tmp_path: Path,
) -> None:
    """Governance callers (audit/readiness) may run without OKG_CODE_REPOS_ROOT.
    `allow_unresolved_base` enumerates the source set anyway (same names /
    producer_ids → same bundle fingerprint) instead of raising, while the loader
    default still fails loud on a bad base.
    """
    from okg.substrate.sources.registry import expand_code_repos_sources

    reg = _write(
        tmp_path,
        """
        code_repos:
          base: ${OKG_CODE_REPOS_ROOT_DEFINITELY_UNSET}
          expand: [git-files]
          repos:
            - {slug: alpha, url: https://x/alpha.git}
        sources: {}
        """,
    )
    import yaml

    doc = yaml.safe_load(reg.read_text())

    # Default (loader) path fails loud on an unresolvable base.
    with pytest.raises(ValueError):
        expand_code_repos_sources(doc, reg)

    # Governance path still enumerates the source set.
    sources = expand_code_repos_sources(doc, reg, allow_unresolved_base=True)
    assert "git-files-alpha" in sources

    relative_reg = _write(
        tmp_path,
        """
        code_repos:
          base: ./relative
          expand: [git-files]
          repos:
            - {slug: alpha, url: https://x/alpha.git}
        sources: {}
        """,
    )
    relative_doc = yaml.safe_load(relative_reg.read_text())

    # Plain relative paths are not "unresolved env" placeholders; keep the
    # loader's config error loud so governance cannot bless an invalid registry.
    with pytest.raises(ValueError, match="absolute"):
        expand_code_repos_sources(
            relative_doc,
            relative_reg,
            allow_unresolved_base=True,
        )


# ---- per-block parameter overrides (code_repos.params) ----

def test_params_override_applies_to_every_repo(tmp_path: Path) -> None:
    base = tmp_path / "repos"
    reg = _write(
        tmp_path,
        f"""
        code_repos:
          base: {base}
          expand: [git-files, doc-corpus]
          params:
            doc-corpus: {{include_globs: ["**/*"], text_fallback: true}}
          repos:
            - {{slug: alpha, url: https://x/alpha.git}}
            - {{slug: beta,  url: https://x/beta.git}}
        sources: {{}}
        """,
    )
    e = load_source_registry(reg)
    for slug in ("alpha", "beta"):
        p = _params(e[f"doc-corpus-{slug}"])
        assert p["include_globs"] == ["**/*"]  # replaced, not merged
        assert p["text_fallback"] is True
        assert p["url"] == f"https://x/{slug}.git"  # template-derived keys kept
        assert "text_fallback" not in _params(e[f"git-files-{slug}"])


def test_expanded_doc_corpus_turns_on_text_fallback_by_default(tmp_path: Path) -> None:
    """With no `params`, every expanded repository reads unparseable files as text.

    A Python 2 script or a templated YAML file fails its structured parse; the
    template's doc-corpus block sets `text_fallback: true` so it is stored as
    text rather than as a document with no chunks (#2671).
    """
    base = tmp_path / "repos"
    reg = _write(
        tmp_path,
        f"""
        code_repos:
          base: {base}
          expand: [git-files, doc-corpus]
          repos:
            - {{slug: alpha, url: https://x/alpha.git}}
            - {{slug: beta,  url: https://x/beta.git}}
        sources: {{}}
        """,
    )
    e = load_source_registry(reg)
    for slug in ("alpha", "beta"):
        assert _params(e[f"doc-corpus-{slug}"])["text_fallback"] is True


def test_params_for_block_not_expanded_errors(tmp_path: Path) -> None:
    base = tmp_path / "repos"
    reg = _write(
        tmp_path,
        f"""
        code_repos:
          base: {base}
          expand: [git-files]
          params:
            doc-corpus: {{text_fallback: true}}
          repos:
            - {{slug: alpha, url: https://x/alpha.git}}
        sources: {{}}
        """,
    )
    with pytest.raises(ValueError, match="not in code_repos.expand"):
        load_source_registry(reg)


@pytest.mark.parametrize(
    "params",
    ["{doc-corpus: [not, a, mapping]}", "{doc-corpus: ''}", "''", "false", "[doc-corpus]"],
)
def test_params_must_be_mapping_of_mappings(tmp_path: Path, params: str) -> None:
    base = tmp_path / "repos"
    reg = _write(
        tmp_path,
        f"""
        code_repos:
          base: {base}
          expand: [git-files, doc-corpus]
          params: {params}
          repos:
            - {{slug: alpha, url: https://x/alpha.git}}
        sources: {{}}
        """,
    )
    with pytest.raises(ValueError, match="code_repos.params must map"):
        load_source_registry(reg)


@pytest.mark.parametrize("key", ["repo", "repo_root", "url", "manage_checkout", "tracked_branch"])
def test_params_cannot_set_per_repo_keys(tmp_path: Path, key: str) -> None:
    base = tmp_path / "repos"
    reg = _write(
        tmp_path,
        f"""
        code_repos:
          base: {base}
          expand: [git-files, doc-corpus]
          params:
            doc-corpus: {{{key}: x}}
          repos:
            - {{slug: alpha, url: https://x/alpha.git}}
        sources: {{}}
        """,
    )
    with pytest.raises(ValueError, match="cannot set per-repo keys"):
        load_source_registry(reg)


def test_params_values_are_not_shared_between_repos(tmp_path: Path) -> None:
    base = tmp_path / "repos"
    reg = _write(
        tmp_path,
        f"""
        code_repos:
          base: {base}
          expand: [git-files, doc-corpus]
          params:
            doc-corpus: {{exclude_globs: ["**/vendor/**"]}}
          repos:
            - {{slug: alpha, url: https://x/alpha.git}}
            - {{slug: beta,  url: https://x/beta.git}}
        sources: {{}}
        """,
    )
    e = load_source_registry(reg)
    alpha = _params(e["doc-corpus-alpha"])["exclude_globs"]
    beta = _params(e["doc-corpus-beta"])["exclude_globs"]
    assert alpha == beta == ["**/vendor/**"]
    assert alpha is not beta  # one repo's params cannot mutate another's


def test_repo_entry_params_apply_to_that_repo_only(tmp_path: Path) -> None:
    """A repos entry's own params reach only its blocks and win over the
    block-wide params (#2671 B: commit diffs for one repository)."""
    base = tmp_path / "repos"
    reg = _write(
        tmp_path,
        f"""
        code_repos:
          base: {base}
          expand: [git-files, git-history]
          params:
            git-history: {{diff_max_file_bytes: 4096}}
          repos:
            - slug: config
              url: https://x/config.git
              params:
                git-history: {{store_diffs: true, diff_max_file_bytes: 8192}}
            - {{slug: code, url: https://x/code.git}}
        sources: {{}}
        """,
    )
    e = load_source_registry(reg)
    config = _params(e["git-history-config"])
    code = _params(e["git-history-code"])
    assert config["store_diffs"] is True
    assert config["diff_max_file_bytes"] == 8192  # the repo's own value wins
    assert config["repo"] == "config"  # template-derived keys kept
    assert "store_diffs" not in code
    assert code["diff_max_file_bytes"] == 4096  # block-wide value only
    assert "store_diffs" not in _params(e["git-files-config"])


@pytest.mark.parametrize(
    ("params", "message"),
    [
        ("{doc-corpus: {text_fallback: true}}", "not in code_repos.expand"),
        ("{git-history: {repo: other}}", "params cannot set"),
        ("{git-history: [store_diffs]}", "params must map"),
        ("store_diffs", "params must map"),
    ],
)
def test_repo_entry_params_are_validated(
    tmp_path: Path, params: str, message: str
) -> None:
    base = tmp_path / "repos"
    reg = _write(
        tmp_path,
        f"""
        code_repos:
          base: {base}
          expand: [git-files, git-history]
          repos:
            - slug: config
              url: https://x/config.git
              params: {params}
        sources: {{}}
        """,
    )
    with pytest.raises(ValueError, match=message):
        load_source_registry(reg)
