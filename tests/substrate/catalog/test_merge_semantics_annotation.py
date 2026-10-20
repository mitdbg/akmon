"""A schema can declare how each attribute merges when two nodes are collapsed.

`okg remediate entity-merge` reads these declarations from
`okg.subtypes.metadata` to decide what to do with an attribute that differs
between the two nodes. Undeclared stays undeclared and the merge refuses, so
this is opt-in per attribute.
"""
from __future__ import annotations

from pathlib import Path

import pytest

from okg.substrate.catalog.modules_compose import (
    MERGE_SEMANTICS,
    compose_catalog,
    load_modules_root,
)
from okg.substrate.entity_merge import MERGE_DISPOSITIONS


def _deployment(tmp_path: Path, nodes_yaml: str) -> Path:
    deployment = tmp_path / "merge_semantics_deployment"
    schemas = deployment / "schemas"
    schemas.mkdir(parents=True)
    (deployment / "deployment.yaml").write_text(
        "name: default\nschema_dir: schemas\nmodules: []\n"
    )
    (schemas / "nodes.yaml").write_text(nodes_yaml)
    return deployment


def _compose(tmp_path: Path, nodes_yaml: str):
    cross_links = tmp_path / "cross_links"
    cross_links.mkdir()
    return compose_catalog(
        _deployment(tmp_path, nodes_yaml),
        modules_root=load_modules_root(),
        cross_links_root=cross_links,
    )


def test_declared_merge_semantics_reaches_subtype_metadata(tmp_path: Path) -> None:
    composed = _compose(
        tmp_path,
        """
classes:
  BackgroundProcess:
    annotations:
      archetype: artifact
    attributes:
      grounded_in:
        annotations:
          merge_semantics: union
      name:
        annotations:
          merge_semantics: keep_retiree
      confidence: {}
""",
    )
    assert composed.subtype_metadata["background_process"]["merge_semantics"] == {
        "grounded_in": "union",
        "name": "keep_retiree",
    }


def test_a_subtype_that_declares_nothing_carries_no_declaration(
    tmp_path: Path,
) -> None:
    """Absence must stay absence — the merge refuses rather than guessing."""
    composed = _compose(
        tmp_path,
        """
classes:
  BackgroundProcess:
    annotations:
      archetype: artifact
    attributes:
      grounded_in: {}
""",
    )
    metadata = composed.subtype_metadata.get("background_process", {})
    assert "merge_semantics" not in metadata


def test_an_unknown_disposition_is_rejected_not_ignored(tmp_path: Path) -> None:
    """A typo that read as "no declaration" would silently drop the other
    node's entries — the exact loss this annotation exists to prevent."""
    with pytest.raises(ValueError, match="invalid merge_semantics"):
        _compose(
            tmp_path,
            """
classes:
  BackgroundProcess:
    annotations:
      archetype: artifact
    attributes:
      grounded_in:
        annotations:
          merge_semantics: unionn
""",
        )


def test_composer_and_merge_agree_on_the_vocabulary() -> None:
    """The composer keeps its own literal so composing a catalog does not pull
    in the remediation module. This is the check that keeps them in step."""
    assert MERGE_SEMANTICS == frozenset(MERGE_DISPOSITIONS)
