"""Per-parent diversity: deterministic, post-ranking, and disclosed.

Issue #1444 (part 2), PACT `mcp-authority-diversity-budgets`.

Reaches the substrate through psycopg + `open_session`, not `build_server`.

The corpus is the same adversarial one the authority tests use. Its shape
matters here for a different reason: `doc:code` owns four of the seven ranked
chunks and owns the top two, so without a cap it fills the head of every page,
and one chunk has NO declared parent so the "never suppress a parentless row"
property is testable rather than assumed.
"""

from __future__ import annotations

import psycopg
import pytest

from okg.substrate.mcp import open_session, search
from okg.substrate.mcp.result_diversity import DiversityNotApplicable
from okg.substrate.mcp.search_profiles import (
    DiversityBoundInvalid,
    parse_search_config,
    resolve_scope,
    validate_max_per_parent,
)
from tests.substrate.mcp.authority_diversity_fixture import (
    ALL_CHUNK_IDS,
    CHUNKS_BY_PARENT,
    NARROWINGS,
    ORPHAN_IDS,
    SEARCH_BLOCK,
    SUBTYPES,
    TERM,
    node_groups,
)
from tests.substrate.mcp.search_fixture import (
    make_search_deployment,
    publish_nodes,
)


pytestmark = pytest.mark.postgres


CONFIG = parse_search_config(SEARCH_BLOCK)


def _publish(dsn: str, tmp_path) -> str:
    deployment = make_search_deployment(tmp_path, dsn=dsn)
    publish_nodes(
        dsn,
        deployment,
        subtypes=SUBTYPES,
        narrowings=NARROWINGS,
        nodes=[],
        node_groups=node_groups(),
    )
    return deployment


def _scope(conn, session, *, profile=None, bound=None):
    return resolve_scope(
        conn,
        catalog_version_id=session.catalog_version_id,
        config=CONFIG,
        requested_profile=profile,
        requested_max_per_parent=bound,
        deployment_name=session.deployment_name,
    )


def _ids(page) -> list[str]:
    return [row["node_id"] for row in page["results"]]


def _parent_of(node_id: str) -> str | None:
    for parent, children in CHUNKS_BY_PARENT.items():
        if node_id in children:
            return parent
    return None


# ---------- req.diversity.per-parent ----------

def test_one_parent_owns_the_page_before_the_cap_is_applied(
    clean_runtime: str, tmp_path,
) -> None:
    """The control. Without it, every cap assertion below could be vacuous."""
    deployment = _publish(clean_runtime, tmp_path)
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        session = open_session(conn, deployment_name=deployment)
        page = search(conn, session, TERM, method="lexical", rank_by="self", limit=50)
    ids = _ids(page)
    assert sorted(ids) == sorted(ALL_CHUNK_IDS)
    owners = [_parent_of(node_id) for node_id in ids]
    assert owners.count("doc:code") == 4, owners
    assert page["diversity_applied"] is False
    assert page["max_per_parent"] is None
    assert page["suppressed_sibling_count"] == 0
    assert page["total_count_before_diversity"] is None


def test_the_cap_keeps_the_top_row_per_parent_and_discloses_the_rest(
    clean_runtime: str, tmp_path,
) -> None:
    deployment = _publish(clean_runtime, tmp_path)
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        session = open_session(conn, deployment_name=deployment)
        control = search(conn, session, TERM, method="lexical", completeness="exact", rank_by="self", limit=50)
        page = search(
            conn, session, TERM, method="lexical", completeness="exact", rank_by="self", limit=50,
            scope=_scope(conn, session, bound=1),
        )
    ranked = _ids(control)
    kept = _ids(page)
    # Exactly the best-ranked chunk of each parent, plus the parentless row.
    expected = []
    seen: set[str] = set()
    for node_id in ranked:
        parent = _parent_of(node_id)
        if parent is None:
            expected.append(node_id)
        elif parent not in seen:
            seen.add(parent)
            expected.append(node_id)
    assert kept == expected, kept
    assert page["diversity_applied"] is True
    assert page["max_per_parent"] == 1
    assert page["total_count_before_diversity"] == len(ranked)
    assert page["suppressed_sibling_count"] == len(ranked) - len(kept)
    assert page["diversity_parents_resolved"] == len(CHUNKS_BY_PARENT)
    assert page["diversity_rows_without_parent"] == len(ORPHAN_IDS)
    # `total_count` counts what a caller can page through, which under
    # diversity is the survivors; the pre-suppression figure is stated too so
    # the two can never be confused.
    assert page["total_count"] == len(kept)


def test_a_cap_of_two_keeps_two_and_the_result_is_repeatable(
    clean_runtime: str, tmp_path,
) -> None:
    deployment = _publish(clean_runtime, tmp_path)
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        session = open_session(conn, deployment_name=deployment)
        first = search(
            conn, session, TERM, method="lexical", rank_by="self", limit=50,
            scope=_scope(conn, session, bound=2),
        )
        second = search(
            conn, session, TERM, method="lexical", rank_by="self", limit=50,
            scope=_scope(conn, session, bound=2),
        )
    kept = _ids(first)
    assert _ids(second) == kept, "the cap must return the same page twice"
    for parent, children in CHUNKS_BY_PARENT.items():
        assert len([i for i in kept if i in children]) <= 2, parent
    assert len([i for i in kept if _parent_of(i) == "doc:code"]) == 2


def test_a_parentless_row_is_never_suppressed(
    clean_runtime: str, tmp_path,
) -> None:
    """Absence of a declared relation is not evidence of a shared parent."""
    deployment = _publish(clean_runtime, tmp_path)
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        session = open_session(conn, deployment_name=deployment)
        page = search(
            conn, session, TERM, method="lexical", rank_by="self", limit=50,
            scope=_scope(conn, session, bound=1),
        )
    assert set(ORPHAN_IDS) <= set(_ids(page))


def test_surviving_rows_keep_their_raw_scores_and_their_order(
    clean_runtime: str, tmp_path,
) -> None:
    """Diversity is selection. It removes rows; it does not rescore or reorder.

    Scores alone are not enough: a reranker that preserved every score while
    permuting the rows would pass a score-only assertion and still be exactly
    the thing `read_affordances.py:20-24` prohibits. So the survivors must also
    be a SUBSEQUENCE of the unfiltered ranking, in order — the same invariant
    the family-filter test asserts.
    """
    deployment = _publish(clean_runtime, tmp_path)
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        session = open_session(conn, deployment_name=deployment)
        control = search(conn, session, TERM, method="lexical", rank_by="self", limit=50)
        page = search(
            conn, session, TERM, method="lexical", rank_by="self", limit=50,
            scope=_scope(conn, session, bound=1),
        )
    scores = {row["node_id"]: row["score"] for row in control["results"]}
    for row in page["results"]:
        assert row["score"] == scores[row["node_id"]], row["node_id"]

    ranked = _ids(control)
    kept = _ids(page)
    assert kept == [node_id for node_id in ranked if node_id in set(kept)]
    # Non-vacuous: the cap really did remove rows, so the subsequence check is
    # over a proper subset rather than the whole ranking.
    assert 0 < len(kept) < len(ranked)


def test_a_profile_may_declare_the_default_bound(
    clean_runtime: str, tmp_path,
) -> None:
    deployment = _publish(clean_runtime, tmp_path)
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        session = open_session(conn, deployment_name=deployment)
        scope = _scope(conn, session, profile="diverse")
        page = search(
            conn, session, TERM, method="lexical", rank_by="self", limit=50, scope=scope,
        )
        # An explicit bound overrides the profile's default.
        override = search(
            conn, session, TERM, method="lexical", rank_by="self", limit=50,
            scope=_scope(conn, session, profile="diverse", bound=2),
        )
    assert scope.max_per_parent == 1
    assert page["max_per_parent"] == 1
    assert override["max_per_parent"] == 2
    assert len(_ids(override)) > len(_ids(page))


def test_diversity_runs_before_pagination_not_after(
    clean_runtime: str, tmp_path,
) -> None:
    """A capped page must be FULL, not a full page with rows removed from it."""
    deployment = _publish(clean_runtime, tmp_path)
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        session = open_session(conn, deployment_name=deployment)
        whole = search(
            conn, session, TERM, method="lexical", rank_by="self", limit=50,
            scope=_scope(conn, session, bound=1),
        )
        first_page = search(
            conn, session, TERM, method="lexical", rank_by="self", limit=2,
            scope=_scope(conn, session, bound=1),
        )
        second_page = search(
            conn, session, TERM, method="lexical", rank_by="self", limit=2, offset=2,
            scope=_scope(conn, session, bound=1),
        )
    assert len(_ids(first_page)) == 2
    assert _ids(first_page) + _ids(second_page) == _ids(whole)
    assert first_page["next_offset"] == 2


# ---------- refusals ----------

def test_an_invalid_bound_is_refused_rather_than_coerced() -> None:
    """Pure argument validation; no database."""
    for bad in (0, -1, "2", 1.5, True):
        with pytest.raises(DiversityBoundInvalid):
            validate_max_per_parent(bad, origin="search: max_per_parent")
    assert validate_max_per_parent(None, origin="x") is None
    assert validate_max_per_parent(3, origin="x") == 3


def test_the_passages_combination_is_refused_as_a_guaranteed_no_op(
    clean_runtime: str, tmp_path,
) -> None:
    """`rank_by='passages'` already returns one row per parent.

    Accepting a per-parent cap there could never remove a row, and a control
    that silently does nothing is indistinguishable from one that worked.
    """
    deployment = _publish(clean_runtime, tmp_path)
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        session = open_session(conn, deployment_name=deployment)
        with pytest.raises(DiversityNotApplicable) as exc:
            search(
                conn, session, TERM, method="lexical", rank_by="passages",
                filter={"subtype": "doc"}, limit=50,
                scope=_scope(conn, session, bound=1),
            )
    assert exc.value.code == "diversity_not_applicable"
    assert "passages" in str(exc.value)
