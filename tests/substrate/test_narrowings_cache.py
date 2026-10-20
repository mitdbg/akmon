"""The connection-scoped NarrowingChecker cache is invalidated on cv change."""

from __future__ import annotations

import pytest

psycopg = pytest.importorskip("psycopg")

from okg.substrate.catalog.narrowings import (  # noqa: E402
    assert_narrowing_permitted,
)
from okg.substrate.errors import SubstrateError  # noqa: E402
from tests.pytest.catalog_fixture_builders import (  # noqa: E402
    SubtypeSpec,
    seed_catalog,
)


pytestmark = pytest.mark.postgres


def _subtypes() -> list[SubtypeSpec]:
    return [
        SubtypeSpec("entity"),
        SubtypeSpec("work_item", parent="entity"),
        SubtypeSpec("person", parent="entity"),
        SubtypeSpec("engineer", parent="person"),
        SubtypeSpec("team", parent="entity"),
    ]


def test_narrowing_cache_refreshes_when_catalog_version_changes(
    clean_runtime: str,
) -> None:
    """A catalog apply within a connection's lifetime invalidates the cached
    NarrowingChecker, so a post-apply check sees the new whitelist -- matching the
    module docstring's "Invalidated on cv change". This fails against the pre-fix
    cache, which returned the first checker for the connection's lifetime and so
    kept enforcing the stale whitelist across the apply.
    """
    subtypes = _subtypes()
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        seed_catalog(
            conn,
            subtypes=subtypes,
            narrowings=[("work_item", "assigned_to", "person")],
        )
        # Prime the connection-scoped cache against the first catalog version.
        assert_narrowing_permitted(conn, "work_item", "assigned_to", "person")
        with pytest.raises(SubstrateError):
            assert_narrowing_permitted(conn, "team", "assigned_to", "engineer")

        # Apply a NEW catalog version on the SAME connection with a different
        # narrowing set: drop the work_item rule, add a team rule.
        seed_catalog(
            conn,
            subtypes=subtypes,
            narrowings=[("team", "assigned_to", "engineer")],
        )

        # The cache must now reflect the new cv, not the stale first checker:
        # the team rule is permitted...
        assert_narrowing_permitted(conn, "team", "assigned_to", "engineer")
        # ...and the dropped work_item rule is now refused.
        with pytest.raises(SubstrateError):
            assert_narrowing_permitted(conn, "work_item", "assigned_to", "person")
