"""fix-projection-alias-identity #710: the cardinality annotation flows from a
LinkML EdgeNarrowings attribute through compose into okg.narrowings.attr_rules.

Covers the compose-side extraction, its closed-vocabulary validation, the
cross-triple conflict check, and (with Postgres) the modules_apply stamp so the
annotation is not lost the moment a catalog is applied.
"""
from __future__ import annotations

import textwrap
from pathlib import Path

import pytest

from okg.substrate.catalog.modules_compose import compose_catalog
from okg.substrate.errors import CatalogModuleMissing


def _fake_modules_root(tmp_path: Path, schema: str) -> Path:
    root = tmp_path / "ontologies"
    (root / "bib").mkdir(parents=True)
    (root / "bib" / "schema.yaml").write_text(schema)
    return root


def _deployment(tmp_path: Path, modules: list[str]) -> Path:
    import yaml as _yaml

    d = tmp_path / "dep"
    d.mkdir()
    (d / "deployment.yaml").write_text(
        _yaml.safe_dump({"name": "dep", "schema_dir": "schemas", "modules": modules})
    )
    (d / "schemas").mkdir()
    return d


_ANNOTATED = textwrap.dedent("""\
    id: https://example.com/okg/modules/bib
    name: bib
    classes:
      BibReference:
        annotations:
          archetype: artifact
        attributes:
          key: {required: true}
      Paper:
        annotations:
          archetype: artifact
        attributes:
          title: {required: true}
      EdgeNarrowings:
        annotations:
          okg_edge_narrowings: "true"
        attributes:
          bib_resolves_to_paper:
            annotations:
              edge_archetype: resolves_to
              src_subtypes: BibReference
              dst_subtypes: Paper
              cardinality: at_most_one_target
""")


@pytest.fixture
def empty_cross_links_root(tmp_path):
    root = tmp_path / "cross_links"
    root.mkdir()
    return root


def test_compose_extracts_cardinality_annotation(
    tmp_path, empty_cross_links_root
) -> None:
    root = _fake_modules_root(tmp_path, _ANNOTATED)
    composed = compose_catalog(
        _deployment(tmp_path, ["bib"]),
        modules_root=root,
        cross_links_root=empty_cross_links_root,
    )
    triple = ("bib_reference", "resolves_to", "paper")
    assert triple in composed.narrowing_triples()
    row = next(r for r in composed.narrowings if r.narrowing_name == "bib_resolves_to_paper")
    assert row.cardinality == "at_most_one_target"
    assert composed.narrowing_attr_rules()[triple] == {
        "cardinality": "at_most_one_target"
    }


def test_compose_rejects_unknown_cardinality(
    tmp_path, empty_cross_links_root
) -> None:
    bad = _ANNOTATED.replace("at_most_one_target", "at_most_seven")
    root = _fake_modules_root(tmp_path, bad)
    with pytest.raises(CatalogModuleMissing) as exc:
        compose_catalog(
            _deployment(tmp_path, ["bib"]),
            modules_root=root,
            cross_links_root=empty_cross_links_root,
        )
    assert "cardinality" in str(exc.value)


_CONFLICTING = textwrap.dedent("""\
    id: https://example.com/okg/modules/bib
    name: bib
    classes:
      BibReference:
        annotations:
          archetype: artifact
        attributes:
          key: {required: true}
      Paper:
        annotations:
          archetype: artifact
        attributes:
          title: {required: true}
      EdgeNarrowings:
        annotations:
          okg_edge_narrowings: "true"
        attributes:
          bib_resolves_to_paper:
            annotations:
              edge_archetype: resolves_to
              src_subtypes: BibReference
              dst_subtypes: Paper
              cardinality: at_most_one_target
          bib_resolves_to_paper_dup:
            annotations:
              edge_archetype: resolves_to
              src_subtypes: BibReference
              dst_subtypes: Paper
""")


def test_compose_rejects_conflicting_cardinality_for_same_pair(
    tmp_path, empty_cross_links_root
) -> None:
    # Two EdgeNarrowings attributes declare (BibReference, resolves_to) with
    # DIFFERENT cardinality (annotated vs absent) -> compose must fail.
    root = _fake_modules_root(tmp_path, _CONFLICTING)
    composed = compose_catalog(
        _deployment(tmp_path, ["bib"]),
        modules_root=root,
        cross_links_root=empty_cross_links_root,
    )
    with pytest.raises(CatalogModuleMissing) as exc:
        composed.narrowing_attr_rules()
    assert "conflicting cardinality" in str(exc.value)


@pytest.mark.postgres
def test_apply_stamps_cardinality_into_narrowings(
    clean_runtime, tmp_path, empty_cross_links_root
) -> None:
    import psycopg

    from okg.substrate.catalog.modules_apply import apply_composed_catalog

    root = _fake_modules_root(tmp_path, _ANNOTATED)
    composed = compose_catalog(
        _deployment(tmp_path, ["bib"]),
        modules_root=root,
        cross_links_root=empty_cross_links_root,
    )
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        cv = apply_composed_catalog(conn, composed)
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT attr_rules->>'cardinality'
                  FROM okg.narrowings
                 WHERE catalog_version_id = %s
                   AND parent_subtype = 'bib_reference'
                   AND edge_type = 'resolves_to'
                """,
                (cv,),
            )
            assert cur.fetchone()[0] == "at_most_one_target"
