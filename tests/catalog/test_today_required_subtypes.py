"""Substrate-subtype audit for the
`anchor-today-on-operator-identity` PACT.

This test backs `task.anchor-today.substrate-subtype-audit` and
`req.anchor-today.substrate-subtype-audit`. It re-confirms at
implementation time the inventory the PACT's rationale recorded
at author time — that the work-product subtypes the
person-anchored Today view depends on are all active in the
okg-workspace catalog at the latest applied catalog version.

PACT changes carry `subtype = 'open_spec_change'` with a
`node_id` prefix of `pact-change:`; the original draft of this
PACT named a non-existent `pact_change` subtype, and the stale
`today_brief_pi_scientist.py` SQL still filters on it. The
defensive lock-in below asserts `pact_change` is NOT in the
catalog so the renaming sticks.

Auto-skips when `OKG_DSN` is unset; CI runs it in the live-DB
lane.
"""
from __future__ import annotations

import os

import pytest


REQUIRED_SUBTYPES: tuple[str, ...] = (
    "git_commit",
    "agent_session",
    "document",
    "open_spec_change",
)


@pytest.fixture(scope="module")
def live_dsn() -> str:
    dsn = os.environ.get("OKG_DSN")
    if not dsn:
        pytest.skip("OKG_DSN not set; substrate-subtype audit skipped")
    return dsn


@pytest.fixture(scope="module")
def active_catalog_version_id(live_dsn: str) -> int:
    """The latest applied catalog version id."""
    import psycopg

    with psycopg.connect(live_dsn) as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT max(catalog_version_id)
              FROM okg.catalog_versions
             WHERE status = 'applied'
            """,
        )
        row = cur.fetchone()
    if row is None or row[0] is None:
        pytest.skip("no applied catalog version; nothing to audit")
    return int(row[0])


@pytest.fixture(scope="module")
def active_subtype_names(
    live_dsn: str, active_catalog_version_id: int,
) -> frozenset[str]:
    import psycopg

    with psycopg.connect(live_dsn) as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT name
              FROM okg.subtypes
             WHERE catalog_version_id = %s
            """,
            (active_catalog_version_id,),
        )
        names = {str(row[0]) for row in cur.fetchall()}
    return frozenset(names)


def test_required_work_product_subtypes_present(
    active_subtype_names: frozenset[str],
    active_catalog_version_id: int,
) -> None:
    """Every subtype the person-anchored view depends on is
    active in the catalog.

    Backs req.anchor-today.substrate-subtype-audit /
    scenario scn.anchor-today.required-subtypes-active.
    """
    missing = sorted(
        s for s in REQUIRED_SUBTYPES if s not in active_subtype_names
    )
    assert not missing, (
        f"required subtypes missing from active catalog version "
        f"{active_catalog_version_id}: {missing}. "
        f"`anchor-today-on-operator-identity` PACT requires "
        f"{list(REQUIRED_SUBTYPES)} for its person-bearing "
        f"edges; file a follow-up PACT to register the missing "
        f"subtype(s) before proceeding."
    )


def test_pact_change_is_not_a_subtype(
    active_subtype_names: frozenset[str],
) -> None:
    """`pact_change` is NOT a substrate subtype. PACT changes
    carry `subtype = 'open_spec_change'` with `node_id` prefix
    `pact-change:`.

    Defensive lock-in: the original Today view's
    `WHERE subtype = 'pact_change'` clauses returned 0 rows
    silently. This test fails loudly if a regression
    reintroduces `pact_change` as a substrate subtype name
    (which would mask the real bug again).

    Backs req.anchor-today.substrate-subtype-audit /
    scenario scn.anchor-today.required-subtypes-active.
    """
    assert "pact_change" not in active_subtype_names, (
        "`pact_change` reappeared as an active substrate subtype. "
        "PACT changes are `open_spec_change` rows with "
        "`node_id LIKE 'pact-change:%'`; the stale "
        "`WHERE subtype = 'pact_change'` filters in "
        "today_brief views must NOT come back. "
        "Revisit the view-rewrite task in "
        "anchor-today-on-operator-identity."
    )


def test_intent_review_is_not_a_subtype(
    active_subtype_names: frozenset[str],
) -> None:
    """`intent_review` is NOT a substrate subtype in this
    PACT's scope. Intent reviews live as `pact_change.edits[]`
    entries plus lifecycle status.

    Backs the substrate-audit findings in the PACT
    rationale.
    """
    assert "intent_review" not in active_subtype_names, (
        "`intent_review` is now an active subtype. The "
        "`anchor-today-on-operator-identity` PACT scoped its "
        "'needs my decision' section to filter "
        "`open_spec_change` rows by lifecycle status (proposal "
        "/ changes-requested) because intent_review was not a "
        "substrate subtype. Revisit req.anchor-today.view-"
        "filtered-by-person and consider whether the section "
        "should now join through an `(intent_review, "
        "assigned_to, person)` narrowing instead."
    )
