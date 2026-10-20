"""Verify the `person` ontology is active in okg-workspace.

Backs `task.anchor-today.activate-person-ontology` and
`req.anchor-today.person-ontology-active`. The substrate
library declares `Person` / `Identity` / `Team` under
`src/okg/substrate/library/ontologies/person/`; this test
asserts the okg-workspace deployment activates them by
including `person` in its module list and running
`okg catalog load --apply`.

Auto-skips when `OKG_DSN` is unset.
"""
from __future__ import annotations

import os
from pathlib import Path

import pytest


REPO = Path(__file__).resolve().parents[2]
DEPLOYMENT_YAML = REPO / "deployments" / "okg-workspace" / "deployment.yaml"
EXPECTED_SUBTYPES = ("person", "identity", "team")


def test_deployment_yaml_lists_person_module() -> None:
    """`deployment.yaml`'s modules list includes `person`.

    Backs req.anchor-today.person-ontology-active /
    scenario scn.anchor-today.person-ontology-active (given).
    """
    text = DEPLOYMENT_YAML.read_text()
    # Match a list-item line `- person` under the modules: key.
    # Looking for the literal `- person` token; comments after
    # the name (e.g. `- person  # ...`) are OK.
    found = False
    in_modules = False
    for line in text.splitlines():
        stripped = line.strip()
        if stripped.startswith("modules:"):
            in_modules = True
            continue
        if in_modules:
            if stripped and not stripped.startswith("-") and not stripped.startswith("#"):
                # next top-level key; modules block ended
                break
            if stripped.startswith("- person") and (
                len(stripped) == len("- person")
                or stripped[len("- person")] in (" ", "#")
            ):
                found = True
                break
    assert found, (
        f"`deployment.yaml` at {DEPLOYMENT_YAML} does not list "
        f"`person` in its `modules:` block. Add it to activate "
        f"the Person / Identity / Team subtypes that the "
        f"anchor-today-on-operator-identity PACT depends on."
    )


@pytest.fixture(scope="module")
def live_dsn() -> str:
    dsn = os.environ.get("OKG_DSN")
    if not dsn:
        pytest.skip("OKG_DSN not set; live person-ontology check skipped")
    return dsn


@pytest.fixture(scope="module")
def active_subtype_names(live_dsn: str) -> frozenset[str]:
    import psycopg

    with psycopg.connect(live_dsn) as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT name
              FROM okg.subtypes
             WHERE catalog_version_id = (
                 SELECT max(catalog_version_id)
                   FROM okg.catalog_versions
                  WHERE status = 'applied'
             )
            """,
        )
        names = {str(row[0]) for row in cur.fetchall()}
    if not names:
        pytest.skip("no applied catalog version; nothing to check")
    return frozenset(names)


def test_person_identity_team_subtypes_active(
    active_subtype_names: frozenset[str],
) -> None:
    """All three person-ontology subtypes are active at the
    latest applied catalog version.

    Backs req.anchor-today.person-ontology-active /
    scenario scn.anchor-today.person-ontology-active (then).
    """
    missing = sorted(
        s for s in EXPECTED_SUBTYPES if s not in active_subtype_names
    )
    assert not missing, (
        f"expected person-ontology subtypes {list(EXPECTED_SUBTYPES)} "
        f"to be active in the catalog after `okg catalog load --apply`, "
        f"but these are missing: {missing}. "
        f"Run: `okg catalog load --deployment deployments/okg-workspace --apply`."
    )


def test_person_has_identity_narrowing_exists(live_dsn: str) -> None:
    """The `person_has_identity` narrowing is active.

    Backs req.anchor-today.person-ontology-active. Without
    this narrowing the self-person source cannot link Identity
    rows to a Person row.
    """
    import psycopg

    with psycopg.connect(live_dsn) as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT count(*)
              FROM okg.narrowings n
              JOIN okg.subtype_ancestors sa_src
                ON sa_src.catalog_version_id = n.catalog_version_id
               AND sa_src.ancestor_name = n.parent_subtype
              JOIN okg.subtype_ancestors sa_dst
                ON sa_dst.catalog_version_id = n.catalog_version_id
               AND sa_dst.ancestor_name = n.child_subtype
             WHERE n.catalog_version_id = (
                 SELECT max(catalog_version_id)
                   FROM okg.catalog_versions
                  WHERE status = 'applied'
             )
               AND sa_src.subtype_name = 'person'
               AND n.edge_type = 'has_identity'
               AND sa_dst.subtype_name = 'identity'
            """,
        )
        count = cur.fetchone()[0]
    assert count >= 1, (
        "expected the (person, has_identity, identity) narrowing "
        "to be active at the latest catalog version; got 0 rows. "
        "The person ontology may have been activated incorrectly."
    )
