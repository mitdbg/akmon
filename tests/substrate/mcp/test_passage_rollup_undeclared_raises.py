"""The P0: an undeclared passage relation RAISES; it never self-ranks.

PACT ``recover-passage-rollup-reads`` states the refusal in its own right. The
reason is the defect the rollup exists to fix, restated as a fallback: on a
subtype whose indexed text is a LOCATOR, degrading `rank_by="passages"` to
self-ranking hands the caller a filename search that is indistinguishable, from
the page alone, from a content search. Under this repo's standing rule a
default-value-as-fallback is a P0, not an acceptable degradation.

The negative test is paired with a control. Without the control it would pass
vacuously on a graph where the query matched nothing at all: here the same
query on the same subtype DOES return rows when self-ranked, and the parent has
real chunks underneath it, so the refusal is refusing something that would
otherwise have worked.
"""

from __future__ import annotations

from pathlib import Path

import psycopg
import pytest

from okg.substrate.catalog.modules_compose import (
    CatalogModuleMissing,
    PassageRollupDeclarationInvalid,
    compose_catalog,
)
from okg.substrate.mcp import open_session, search
from okg.substrate.mcp.passage_rollup import (
    PassageRollupSubtypeRequired,
    PassageRollupUndeclared,
    RankByNotApplicable,
    passage_rollup_declaration,
    validate_rank_by,
)
from tests.substrate.mcp.erag_rollup_fixture import (
    NARROWINGS,
    SUBTYPES,
    TERM,
    build_facts,
)
from tests.substrate.mcp.search_fixture import (
    make_search_deployment,
    publish_nodes,
)


pytestmark = pytest.mark.postgres


def _publish(dsn: str, tmp_path) -> str:
    deployment = make_search_deployment(tmp_path, dsn=dsn)
    facts = build_facts()
    publish_nodes(
        dsn,
        deployment,
        subtypes=SUBTYPES,
        narrowings=NARROWINGS,
        nodes=[f for f in facts if hasattr(f, "node_id")],
        edges=[f for f in facts if not hasattr(f, "node_id")],
    )
    return deployment


def test_undeclared_parent_raises_and_does_not_self_rank(
    clean_runtime: str,
    tmp_path,
) -> None:
    """The refusal, with the control that makes it non-vacuous."""
    deployment = _publish(clean_runtime, tmp_path)
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        session = open_session(conn, deployment_name=deployment)

        # CONTROL: self-ranking this subtype works and returns rows. So the
        # refusal below is not "there was nothing to return".
        control = search(
            conn,
            session,
            TERM,
            method="lexical",
            filter={"subtype": "undeclared_doc"},
            rank_by="self",
            limit=10,
        )
        assert control["results"], control
        assert control["rank_by"] == "self"

        with pytest.raises(PassageRollupUndeclared) as exc:
            search(
                conn,
                session,
                TERM,
                method="lexical",
                filter={"subtype": "undeclared_doc"},
                rank_by="passages",
                limit=10,
            )

    message = str(exc.value)
    # The subtype is NAMED, per the requirement.
    assert "undeclared_doc" in message, message
    assert "okg_passage_rollup" in message, message
    # And the refusal points at the subtype that DOES declare one, so the
    # caller learns the affordance exists rather than concluding it does not.
    assert "doc" in (exc.value.data or {}).get("subtypes_with_passage_rollup", []), (
        exc.value.data
    )
    assert exc.value.code == "passage_rollup_undeclared"


def test_declaration_lookup_raises_rather_than_returning_none(
    clean_runtime: str,
    tmp_path,
) -> None:
    """The reader itself refuses; the refusal is not the caller's to forget.

    A lookup returning `None` would let any future call site treat "no
    declaration" as "use the default", which is the fallback shape this
    requirement forbids. So absence raises at the read.
    """
    deployment = _publish(clean_runtime, tmp_path)
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        session = open_session(conn, deployment_name=deployment)
        declared = passage_rollup_declaration(
            conn,
            catalog_version_id=session.catalog_version_id,
            subtype="doc",
        )
        assert (declared.edge_type, declared.child_subtype) == (
            "contains",
            "chunk",
        )
        with pytest.raises(PassageRollupUndeclared):
            passage_rollup_declaration(
                conn,
                catalog_version_id=session.catalog_version_id,
                subtype="chunk",
            )


def test_rollup_without_a_named_parent_subtype_raises(
    clean_runtime: str,
    tmp_path,
) -> None:
    """No subtype named ⇒ refuse, rather than pick one from the catalog.

    Choosing "the subtype that happens to declare a rollup" would make the
    operator's meaning a function of the catalog's current contents: adding a
    second declaring subtype later would silently change what an unchanged
    call returns.
    """
    deployment = _publish(clean_runtime, tmp_path)
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        session = open_session(conn, deployment_name=deployment)
        with pytest.raises(PassageRollupSubtypeRequired) as exc:
            search(conn, session, TERM, method="lexical", rank_by="passages", limit=10)
    assert "subtype" in str(exc.value)
    assert exc.value.code == "passage_rollup_subtype_required"


def test_rank_by_is_validated_without_a_database() -> None:
    """Pure argument validation, so it is provable without a graph.

    A rollup named on a method that cannot rank child text must RAISE rather
    than be accepted and discarded: a discarded mode hands back a self-ranked
    page to a caller who believes it was rolled up.
    """
    # Lexical defaults to `records`, except when the caller passed a
    # `max_per_parent` (it asked for sibling rows); other methods to `self`.
    assert validate_rank_by(None, method="lexical") == "records"
    assert validate_rank_by(
        None, method="lexical", max_per_parent_argument=True,
    ) == "self"
    assert validate_rank_by(None, method="semantic") == "self"
    assert validate_rank_by("self", method="alias") == "self"
    assert validate_rank_by("passages", method="lexical") == "passages"
    assert validate_rank_by("records", method="lexical") == "records"
    with pytest.raises(ValueError, match="rank_by must be one of"):
        validate_rank_by("passage", method="lexical")
    for method in ("semantic", "alias", "attrs", "identifier_mentions"):
        for mode in ("passages", "records"):
            with pytest.raises(RankByNotApplicable, match="only to method='lexical'"):
                validate_rank_by(mode, method=method)


def _write_module(root: Path, name: str, body: str) -> None:
    (root / name).mkdir(parents=True, exist_ok=True)
    (root / name / "schema.yaml").write_text(body, encoding="utf-8")


def _empty_cross_links(root: Path) -> Path:
    """A present-but-EMPTY cross-links root: the composer's hermetic mode."""
    path = root / "empty_cross_links"
    path.mkdir(parents=True, exist_ok=True)
    return path


def _deployment(root: Path, modules: list[str]) -> Path:
    deployment = root / "dep"
    (deployment / "schemas").mkdir(parents=True, exist_ok=True)
    (deployment / "deployment.yaml").write_text(
        "name: dep\nmodules:\n"
        + "".join(f"- {m}\n" for m in modules)
        + "schema_dir: schemas\n",
        encoding="utf-8",
    )
    return deployment


_CLASSES = """
id: https://example.org/m
name: m
default_range: string
classes:
  Doc:
    attributes:
      path:
        range: string
    annotations:
      archetype: artifact
      okg_searchable_text_fields: path
      okg_searchable_text_path_fields: path
      okg_passage_rollup:
        edge_type: {edge_type}
        child_subtype: {child_subtype}
  Chunk:
    attributes:
      text:
        range: string
    annotations:
      archetype: artifact
      okg_searchable_text_fields: text
  EdgeNarrowings:
    annotations:
      okg_edge_narrowings: 'true'
    attributes:
      doc_contains_chunk:
        annotations:
          edge_archetype: contains
          src_subtypes: Doc
          dst_subtypes: Chunk
"""


def test_compose_accepts_a_declaration_backed_by_a_composed_narrowing(
    tmp_path: Path,
) -> None:
    """The composer harvests the annotation into catalog metadata."""
    modules = tmp_path / "modules"
    _write_module(
        modules,
        "m",
        _CLASSES.format(edge_type="contains", child_subtype="Chunk"),
    )
    composed = compose_catalog(
        _deployment(tmp_path, ["m"]),
        modules_root=modules,
        cross_links_root=_empty_cross_links(tmp_path),
    )
    assert composed.subtype_metadata["doc"]["passage_rollup"] == {
        "edge_type": "contains",
        "child_subtype": "chunk",
    }


def test_compose_refuses_a_declaration_with_no_such_narrowing(
    tmp_path: Path,
) -> None:
    """A declaration the catalog does not admit fails COMPOSITION.

    Deferring this to read time would ship a deployment whose rollup can only
    ever return nothing — an affordance that is silently empty, which is the
    state the requirement forbids.
    """
    modules = tmp_path / "modules"
    _write_module(
        modules,
        "m",
        _CLASSES.format(edge_type="references", child_subtype="Chunk"),
    )
    with pytest.raises(CatalogModuleMissing) as exc:
        compose_catalog(
            _deployment(tmp_path, ["m"]),
            modules_root=modules,
            cross_links_root=_empty_cross_links(tmp_path),
        )
    assert "references" in str(exc.value)
    assert "no such narrowing" in str(exc.value)


def test_compose_refuses_a_declaration_naming_an_absent_child(
    tmp_path: Path,
) -> None:
    """A child subtype outside the composed catalog fails composition."""
    modules = tmp_path / "modules"
    _write_module(
        modules,
        "m",
        _CLASSES.format(edge_type="contains", child_subtype="Passage"),
    )
    with pytest.raises(CatalogModuleMissing) as exc:
        compose_catalog(
            _deployment(tmp_path, ["m"]),
            modules_root=modules,
            cross_links_root=_empty_cross_links(tmp_path),
        )
    assert "passage" in str(exc.value)


@pytest.mark.parametrize(
    "invalid_block, expected_field",
    [
        ("okg_passage_rollup: self", None),
        (
            "okg_passage_rollup:\n"
            "        edge_type: contains\n"
            "        child_subtype: Chunk\n"
            "        fallback: self",
            "fallback",
        ),
        (
            "okg_passage_rollup:\n"
            "        edge_type: ''\n"
            "        child_subtype: Chunk",
            "edge_type",
        ),
    ],
    ids=["non-mapping", "unknown-key", "blank-field"],
)
def test_compose_refuses_malformed_rollup_declarations_with_a_named_error(
    tmp_path: Path,
    invalid_block: str,
    expected_field: str | None,
) -> None:
    """Every invalid declaration shape fails under one stable wire code."""
    modules = tmp_path / "modules"
    valid = _CLASSES.format(edge_type="contains", child_subtype="Chunk")
    valid_block = (
        "okg_passage_rollup:\n"
        "        edge_type: contains\n"
        "        child_subtype: Chunk"
    )
    _write_module(modules, "m", valid.replace(valid_block, invalid_block))

    with pytest.raises(PassageRollupDeclarationInvalid) as exc:
        compose_catalog(
            _deployment(tmp_path, ["m"]),
            modules_root=modules,
            cross_links_root=_empty_cross_links(tmp_path),
        )

    assert exc.value.code == "passage_rollup_declaration_invalid"
    assert exc.value.data["class_name"] == "Doc"
    if expected_field == "fallback":
        assert exc.value.data["unknown_keys"] == ["fallback"]
    elif expected_field is not None:
        assert exc.value.data["field"] == expected_field
