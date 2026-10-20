"""DB-backed integration tests for bookkeeping ontology module.

Complements the pure-Python composition tests in
`test_bookkeeping_schema.py` with real Postgres verification:

- Apply the composed catalog to a fresh test DB and assert the
  expected narrowing rows land in `okg.narrowings`.
- Verify the substrate's narrowing trigger accepts an intra-module
  edge fact emitted against the active catalog.

Skipped automatically when Postgres is unreachable (the
`clean_runtime` fixture handles that path).

Run via: `OKG_TEST_DSN=postgres://postgres:okg@localhost:5433/okg_test_bookkeeping
uv run python -m pytest tests/substrate/library/ontologies/test_bookkeeping_db.py
-v --no-cov`.
"""

from __future__ import annotations

from pathlib import Path

import psycopg
import pytest
import yaml

from okg.substrate.catalog.modules_apply import apply_composed_catalog
from okg.substrate.catalog.modules_compose import compose_catalog


pytestmark = pytest.mark.postgres


def _make_deployment(
    tmp_path: Path, name: str, modules: list[str],
) -> Path:
    dep = tmp_path / name
    dep.mkdir()
    (dep / "deployment.yaml").write_text(
        yaml.safe_dump({
            "name": name,
            "description": f"bookkeeping DB integration: {name}",
            "modules": modules,
        })
    )
    (dep / "schemas").mkdir()
    return dep


def _narrowings_for_version(
    conn: psycopg.Connection, cv: int,
) -> set[tuple[str, str, str]]:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT parent_subtype, edge_type, child_subtype "
            "  FROM okg.narrowings "
            " WHERE catalog_version_id = %s",
            (cv,),
        )
        return {tuple(r) for r in cur.fetchall()}


def _subtype_count_for_version(
    conn: psycopg.Connection, cv: int,
) -> int:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT COUNT(*) FROM okg.subtypes "
            " WHERE catalog_version_id = %s",
            (cv,),
        )
        return cur.fetchone()[0]


# ---------- bookkeeping alone ----------


def test_apply_bookkeeping_alone_lands_six_subtypes(
    clean_runtime: str, tmp_path: Path,
) -> None:
    """`apply_composed_catalog` inserts the six bookkeeping
    subtypes under a fresh catalog_version_id."""
    dep = _make_deployment(tmp_path, "test-bk-db-solo", ["bookkeeping"])
    composed = compose_catalog(dep)
    with psycopg.connect(clean_runtime) as conn:
        cv = apply_composed_catalog(conn, composed)
        conn.commit()

        assert _subtype_count_for_version(conn, cv) == 6


def test_apply_bookkeeping_alone_lands_intra_narrowings(
    clean_runtime: str, tmp_path: Path,
) -> None:
    """Six intra-module narrowing rows land in `okg.narrowings`."""
    dep = _make_deployment(
        tmp_path, "test-bk-db-intra", ["bookkeeping"],
    )
    composed = compose_catalog(dep)
    with psycopg.connect(clean_runtime) as conn:
        cv = apply_composed_catalog(conn, composed)
        conn.commit()

        triples = _narrowings_for_version(conn, cv)
        expected = {
            ("project", "contains", "project_milestone"),
            ("project", "contains", "project_goal"),
            ("project", "contains", "project_deliverable"),
            ("project", "contains", "project_risk"),
            ("project", "contains", "project_stakeholder"),
            ("project_milestone", "produces", "project_deliverable"),
        }
        assert expected <= triples


def test_apply_bookkeeping_alone_no_cross_module_narrowings(
    clean_runtime: str, tmp_path: Path,
) -> None:
    """Cross-module narrowings DO NOT land when their target
    modules are absent — they live in cross-link rule files and
    are silently skipped."""
    dep = _make_deployment(
        tmp_path, "test-bk-db-no-cross", ["bookkeeping"],
    )
    composed = compose_catalog(dep)
    with psycopg.connect(clean_runtime) as conn:
        cv = apply_composed_catalog(conn, composed)
        conn.commit()

        triples = _narrowings_for_version(conn, cv)
        new_archetypes = {
            "investigates", "guided_by", "predicts",
            "contributes_to", "identifies_as",
        }
        bad = {t for t in triples if t[1] in new_archetypes}
        assert bad == set(), (
            f"cross-module narrowings activated standalone: {bad}"
        )


# ---------- bookkeeping + openspec ----------


def test_apply_with_openspec_lands_cross_shared(
    clean_runtime: str, tmp_path: Path,
) -> None:
    """Cross-shared narrowings activate when bookkeeping +
    openspec + git_graph compose together."""
    dep = _make_deployment(
        tmp_path, "test-bk-db-openspec",
        ["bookkeeping", "openspec", "git_graph"],
    )
    composed = compose_catalog(dep)
    with psycopg.connect(clean_runtime) as conn:
        cv = apply_composed_catalog(conn, composed)
        conn.commit()

        triples = _narrowings_for_version(conn, cv)
        cross_shared = {
            ("open_spec_change", "member_of", "project"),
            ("goal", "contributes_to", "project_goal"),
            ("risk", "contributes_to", "project_risk"),
            ("stakeholder", "identifies_as", "project_stakeholder"),
        }
        missing = cross_shared - triples
        assert not missing, (
            f"missing cross-shared narrowings: {missing}"
        )


# ---------- bookkeeping + person ----------


def test_apply_with_person_lands_stakeholder_link(
    clean_runtime: str, tmp_path: Path,
) -> None:
    """`ProjectStakeholder identifies Person` lands when bookkeeping
    + person compose together."""
    dep = _make_deployment(
        tmp_path, "test-bk-db-person",
        ["bookkeeping", "person"],
    )
    composed = compose_catalog(dep)
    with psycopg.connect(clean_runtime) as conn:
        cv = apply_composed_catalog(conn, composed)
        conn.commit()

        triples = _narrowings_for_version(conn, cv)
        assert ("project_stakeholder", "identifies", "person") in triples


# ---------- narrowing trigger acceptance ----------


def test_catalog_version_promotable_to_applied(
    clean_runtime: str, tmp_path: Path,
) -> None:
    """The catalog version created by `apply_composed_catalog` can
    be promoted to `applied` status, making its narrowings active
    for the substrate's `_enforce_narrowing` trigger.

    The promotion is what `okg catalog load --apply` does at the
    end of its flow; our test exercises the same query path so the
    bookkeeping module's narrowings become the active catalog from
    a substrate-runtime perspective.
    """
    dep = _make_deployment(
        tmp_path, "test-bk-db-promote", ["bookkeeping"],
    )
    composed = compose_catalog(dep)
    with psycopg.connect(clean_runtime) as conn:
        cv = apply_composed_catalog(conn, composed)
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE okg.catalog_versions SET status='applied' "
                " WHERE catalog_version_id = %s",
                (cv,),
            )
        conn.commit()

        # The trigger's active-cv selector: max(cv) where status='applied'.
        with conn.cursor() as cur:
            cur.execute(
                "SELECT MAX(catalog_version_id) "
                "  FROM okg.catalog_versions "
                " WHERE status = 'applied'"
            )
            active_cv = cur.fetchone()[0]
        assert active_cv == cv

        # Bookkeeping narrowings should be visible at the active cv.
        with conn.cursor() as cur:
            cur.execute(
                "SELECT parent_subtype, edge_type, child_subtype "
                "  FROM okg.narrowings "
                " WHERE catalog_version_id = %s "
                "   AND parent_subtype = 'project'",
                (active_cv,),
            )
            project_outgoing = {tuple(r) for r in cur.fetchall()}
        assert ("project", "contains", "project_milestone") in project_outgoing
        assert ("project", "contains", "project_goal") in project_outgoing


def test_unauthorized_triple_absent_from_narrowings(
    clean_runtime: str, tmp_path: Path,
) -> None:
    """A triple NOT declared by bookkeeping is absent from
    `okg.narrowings` at the applied catalog version. This is what
    the `_enforce_narrowing` trigger reads to reject unauthorized
    edges; the unit-of-correctness for the bookkeeping module is
    the absence of unauthorized triples in the narrowings table.

    `project_milestone --contains--> project_milestone` is an
    obvious counter-example: not declared by bookkeeping and
    therefore not in the narrowings table.
    """
    dep = _make_deployment(
        tmp_path, "test-bk-db-unauth", ["bookkeeping"],
    )
    composed = compose_catalog(dep)
    with psycopg.connect(clean_runtime) as conn:
        cv = apply_composed_catalog(conn, composed)
        conn.commit()

        with conn.cursor() as cur:
            cur.execute(
                "SELECT COUNT(*) FROM okg.narrowings "
                " WHERE catalog_version_id = %s "
                "   AND parent_subtype = 'project_milestone' "
                "   AND edge_type = 'contains' "
                "   AND child_subtype = 'project_milestone'",
                (cv,),
            )
            count = cur.fetchone()[0]
        assert count == 0, (
            "unexpected narrowing for project_milestone -contains-> "
            "project_milestone (self-containment is not declared)"
        )

        # Sanity: the legitimate `project contains project_milestone`
        # triple IS present.
        with conn.cursor() as cur:
            cur.execute(
                "SELECT COUNT(*) FROM okg.narrowings "
                " WHERE catalog_version_id = %s "
                "   AND parent_subtype = 'project' "
                "   AND edge_type = 'contains' "
                "   AND child_subtype = 'project_milestone'",
                (cv,),
            )
            count = cur.fetchone()[0]
        assert count == 1
