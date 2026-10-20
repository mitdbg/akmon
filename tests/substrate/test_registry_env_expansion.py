"""Registry-wide ${ENV_VAR} expansion in source_registry.yaml values.

Any value in source_registry.yaml may reference ${ENV_VAR}; it is expanded from
the environment at load time (after the fixed ${repo_root}/${deployment_dir}
anchors), so the same registry stays portable across machines — e.g. repo paths
via ${OKG_CODE_REPOS_ROOT}. Braced ${VAR} only; undefined vars stay literal.
"""
from __future__ import annotations

from pathlib import Path

import pytest

from okg.substrate.sources.registry import load_source_registry

_HEAD = (
    "sources:\n"
    "  demo:\n"
    "    module: example.mod\n"
    "    class: Example\n"
    "    ownership_id: cms.demo\n"
    "    source_class: discovery_crawl\n"
    "    record_identity_kind: scoped_locator\n"
    "    record_identity_fields: [repo, path]\n"
    "    source_revision_kind: content_hash\n"
    "    deletion_semantics: missing_from_completed_scope\n"
    "    publication_mode: published_generation\n"
    "    params:\n"
    "      repo: demo\n"
)


def _write(tmp_path: Path, param_line: str) -> Path:
    p = tmp_path / "source_registry.yaml"
    p.write_text(_HEAD + f"      {param_line}\n", encoding="utf-8")
    return p


def test_env_var_expands_in_source_params(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("MY_REPOS_ROOT", "/data/repos")
    reg = _write(tmp_path, 'repo_root: "${MY_REPOS_ROOT}/WMCore"')
    e = load_source_registry(reg)
    assert e["demo"].raw["params"]["repo_root"] == "/data/repos/WMCore"


def test_undefined_env_var_left_literal(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.delenv("NOPE_UNSET", raising=False)
    reg = _write(tmp_path, 'repo_root: "${NOPE_UNSET}/x"')
    e = load_source_registry(reg)
    # left literal -> fails loudly downstream, not silently empty
    assert e["demo"].raw["params"]["repo_root"] == "${NOPE_UNSET}/x"


def test_bare_dollar_not_expanded(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    # only the braced ${VAR} form is expanded; bare $x in a glob/regex is safe
    monkeypatch.setenv("x", "BAD")
    reg = _write(tmp_path, "pattern: 'foo$ bar $x'")
    e = load_source_registry(reg)
    assert e["demo"].raw["params"]["pattern"] == "foo$ bar $x"


def test_env_expands_when_nested_and_is_single_pass(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Expansion recurses into nested params, and is single-pass: an env value
    that itself contains ${...} is NOT re-expanded."""
    monkeypatch.setenv("ROOT", "/data")
    monkeypatch.setenv("REC", "${ROOT}/inner")  # value contains a ${...} ref
    p = tmp_path / "source_registry.yaml"
    p.write_text(
        _HEAD
        + "      nested:\n"
        + '        - inner: "${ROOT}/a"\n'
        + '        - deep:\n'
        + '            k: "${REC}"\n',
        encoding="utf-8",
    )
    params = load_source_registry(p)["demo"].raw["params"]
    assert params["nested"][0]["inner"] == "/data/a"          # recurses
    assert params["nested"][1]["deep"]["k"] == "${ROOT}/inner"  # single-pass
