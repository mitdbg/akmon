"""Phase 4 tests for A4 — AuthorAliasesSource.

Operator-curated alias YAML → same_as EdgeFacts between Person nodes.
"""
from __future__ import annotations

from pathlib import Path

import pytest

from okg.substrate.errors import SubstrateError
from okg.substrate.library.sources.scholarly.author_aliases import (
    AuthorAliasesSource,
)


FIXTURE = Path(__file__).resolve().parent / "fixtures" / "author_aliases.yaml"


def _collect(source: AuthorAliasesSource) -> tuple[list, dict]:
    run = source.run("test-run", mode="scope_complete")
    edges = [f for f in run.facts if hasattr(f, "edge_type")]
    return edges, source.last_run_stats


def test_author_aliases_emits_name_variant_edges() -> None:
    src = AuthorAliasesSource(aliases_yaml_path=str(FIXTURE))
    edges, stats = _collect(src)

    # Vale: 3 aliases; Codd: 2 aliases → 5 name_variant edges
    name_variants = [e for e in edges if e.attrs["alias_kind"] == "name_variant"]
    assert len(name_variants) == 5

    # All edges point to canonical Person ids
    canonicals = {e.dst for e in name_variants}
    assert "person:morgan-vale" in canonicals
    assert "person:e-f-codd" in canonicals

    # Verify edge type is same_as
    assert all(e.edge_type == "same_as" for e in name_variants)


def test_author_aliases_emits_external_id_edges() -> None:
    src = AuthorAliasesSource(aliases_yaml_path=str(FIXTURE))
    edges, stats = _collect(src)

    ext = [e for e in edges if e.attrs["alias_kind"] == "external_id"]
    # Vale: dblp + orcid = 2; Codd: dblp = 1 → 3 external_id edges
    assert len(ext) == 3
    providers = {e.attrs["alias_provider"] for e in ext}
    assert providers == {"dblp", "orcid"}


def test_author_aliases_stats() -> None:
    src = AuthorAliasesSource(aliases_yaml_path=str(FIXTURE))
    _, stats = _collect(src)
    assert stats["groups"] == 2
    assert stats["alias_edges"] == 5
    assert stats["external_id_edges"] == 3


def test_author_aliases_missing_path_raises() -> None:
    with pytest.raises(SubstrateError) as exc:
        AuthorAliasesSource()
    assert "aliases_yaml_path is required" in str(exc.value)


def test_author_aliases_nonexistent_yaml_raises(tmp_path: Path) -> None:
    src = AuthorAliasesSource(aliases_yaml_path=str(tmp_path / "missing.yaml"))
    with pytest.raises(SubstrateError) as exc:
        list(src.run("run", mode="scope_complete").facts)
    assert "does not resolve to a file" in str(exc.value)


def test_author_aliases_missing_canonical_raises(tmp_path: Path) -> None:
    bad = tmp_path / "bad.yaml"
    bad.write_text("- aliases: [a, b]\n", encoding="utf-8")
    src = AuthorAliasesSource(aliases_yaml_path=str(bad))
    with pytest.raises(SubstrateError) as exc:
        list(src.run("run", mode="scope_complete").facts)
    assert "missing `canonical`" in str(exc.value)


def test_author_aliases_malformed_yaml_raises(tmp_path: Path) -> None:
    bad = tmp_path / "bad.yaml"
    bad.write_text("not a list\n", encoding="utf-8")
    src = AuthorAliasesSource(aliases_yaml_path=str(bad))
    with pytest.raises(SubstrateError) as exc:
        list(src.run("run", mode="scope_complete").facts)
    assert "must be a list" in str(exc.value)
