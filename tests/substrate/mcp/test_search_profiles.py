"""Deployment search profiles scope ranking to content (issue #797).

These reach the substrate through psycopg + `open_session`, NOT `build_server`:
they exercise the lower-level search path directly and do not depend on the
MCP server's role-aware pool. The MCP tool's own profile behaviour that needs
no database (refusing a profile on a method that cannot honour one) is proven
in `tests/substrate/mcp/test_search_profile_arguments.py`, which runs in
`fast`.

Every scoped assertion here is paired with an unscoped control, so a test
cannot pass merely because the row it wanted was the only row.
"""
from __future__ import annotations

import sys

import psycopg
import pytest

from okg.substrate.mcp import open_session, search
from okg.substrate.mcp.search_profiles import (
    ProfileSubtypeAbsent,
    ResolvedScope,
    SubtypeClosureIncomplete,
    SubtypeOutsideProfile,
    UnknownSearchProfile,
    expand_subtypes,
    resolve_scope,
    parse_search_config,
)
from tests.pytest.catalog_fixture_builders import SubtypeSpec
from tests.substrate.mcp.search_fixture import (
    make_search_deployment,
    node,
    publish_nodes,
)
from tests.pytest.catalog_fixture_builders import bind_test_catalog_writer  # noqa: E402


pytestmark = pytest.mark.postgres


ANALYST_BLOCK = {
    "default_profile": "analyst",
    "profiles": {
        "analyst": {"subtypes": ["chunk"]},
        "ops": {"subtypes": ["pact_task"]},
    },
}


def _catalog_version(dsn: str) -> int:
    with psycopg.connect(dsn, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT max(catalog_version_id) FROM okg.catalog_versions "
            " WHERE status = 'applied'"
        )
        return int(cur.fetchone()[0])


# ---------- req.search-profiles.declared-resolution ----------

def test_profile_expands_to_refinements_and_reports_scope(
    clean_runtime: str, tmp_path,
) -> None:
    """A profile naming a parent subtype covers subtypes refined from it.

    Without closure expansion, a profile written when only `chunk` existed
    would silently stop covering content the day someone refined
    `analysis_chunk` under it — and would report a confident scope while doing
    so.
    """
    deployment = make_search_deployment(tmp_path, dsn=clean_runtime)
    publish_nodes(
        clean_runtime,
        deployment,
        subtypes=[
            SubtypeSpec("chunk"),
            SubtypeSpec("analysis_chunk", parent="chunk"),
            SubtypeSpec("pact_task"),
        ],
        nodes=[
            node("chunk:sr", "analysis_chunk",
                 "the signal region is a W boson recoiling against a cluster"),
            node("task:sr", "pact_task",
                 "signal region definition task tracking"),
        ],
    )
    cv = _catalog_version(clean_runtime)
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        session = open_session(conn, deployment_name=deployment)
        scope = resolve_scope(
            conn,
            catalog_version_id=cv,
            config=parse_search_config(ANALYST_BLOCK),
            requested_profile="analyst",
            subtype_filter=None,
        )
        scoped = search(
            conn, session, "signal region", method="lexical", scope=scope,
        )
        unscoped = search(conn, session, "signal region", method="lexical")

    # The declared name was `chunk`; the refinement came along.
    assert set(scope.subtypes or ()) == {"chunk", "analysis_chunk"}
    assert scoped["scoped_subtypes"] == sorted(["analysis_chunk", "chunk"])
    assert scoped["search_profile"] == "analyst"

    ids = [row["node_id"] for row in scoped["results"]]
    assert "chunk:sr" in ids
    # The control: the bookkeeping row really was a competing match, so the
    # scoped result is narrower because of the scope and not because the
    # corpus was trivial.
    assert "task:sr" not in ids
    assert "task:sr" in [row["node_id"] for row in unscoped["results"]]


def test_undeclared_profile_raises_and_lists_declared_names(
    clean_runtime: str, tmp_path,
) -> None:
    make_search_deployment(tmp_path, dsn=clean_runtime)
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        with pytest.raises(UnknownSearchProfile) as exc:
            resolve_scope(
                conn,
                catalog_version_id=1,
                config=parse_search_config(ANALYST_BLOCK),
                requested_profile="anaylst",
                subtype_filter=None,
            )
    # Not an unscoped page: a typo must not silently return the bookkeeping
    # noise the profile exists to remove.
    assert "anaylst" in str(exc.value)
    assert exc.value.data["declared"] == ["analyst", "ops"]


def test_profile_naming_absent_subtype_raises_naming_it(
    clean_runtime: str, tmp_path,
) -> None:
    deployment = make_search_deployment(tmp_path, dsn=clean_runtime)
    publish_nodes(
        clean_runtime, deployment,
        subtypes=["chunk"],
        nodes=[node("chunk:a", "chunk", "some content")],
    )
    cv = _catalog_version(clean_runtime)
    block = {"profiles": {"analyst": {"subtypes": ["chunk", "figrue"]}}}
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        with pytest.raises(ProfileSubtypeAbsent) as exc:
            resolve_scope(
                conn,
                catalog_version_id=cv,
                config=parse_search_config(block),
                requested_profile="analyst",
                subtype_filter=None,
            )
    # Zero results would have been the silent alternative.
    assert exc.value.data["absent"] == ["figrue"]


# ---------- console-search-profile-subtypes (issue #2545) ----------

def test_allowed_absent_subtypes_are_left_out_and_named(
    clean_runtime: str, tmp_path,
) -> None:
    """A catalog behind its manifest still answers, and says what it lacks.

    okg-workspace's profile names `paper` and 24 more types its published
    catalog does not define; every console search failed. With the allowance
    the scope keeps the types that exist and records the rest.
    """
    deployment = make_search_deployment(tmp_path, dsn=clean_runtime)
    publish_nodes(
        clean_runtime, deployment,
        subtypes=[
            SubtypeSpec("chunk"),
            SubtypeSpec("analysis_chunk", parent="chunk"),
            SubtypeSpec("pact_task"),
        ],
        nodes=[
            node("chunk:sr", "analysis_chunk", "signal region definition"),
            node("task:sr", "pact_task", "signal region task tracking"),
        ],
    )
    cv = _catalog_version(clean_runtime)
    block = {
        "default_profile": "analyst",
        "profiles": {"analyst": {"subtypes": ["chunk", "paper", "venue"]}},
    }
    offered: list[tuple[str, ...]] = []

    def allowed(absent: tuple[str, ...]) -> tuple[str, ...]:
        offered.append(absent)
        return absent

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        session = open_session(conn, deployment_name=deployment)
        scope = resolve_scope(
            conn,
            catalog_version_id=cv,
            config=parse_search_config(block),
            requested_profile=None,
            subtype_filter=None,
            absent_subtypes_allowed=allowed,
        )
        scoped = search(
            conn, session, "signal region", method="lexical", scope=scope,
        )

    assert offered == [("paper", "venue")]
    assert scope.profile == "analyst"
    assert scope.absent_subtypes == ("paper", "venue")
    # The present parent still expands to its refinement.
    assert set(scope.subtypes or ()) == {"chunk", "analysis_chunk"}
    ids = [row["node_id"] for row in scoped["results"]]
    assert ids == ["chunk:sr"]


def test_absent_subtype_the_allowance_rejects_still_raises(
    clean_runtime: str, tmp_path,
) -> None:
    """A name the allowance does not accept is a broken profile: refused."""
    deployment = make_search_deployment(tmp_path, dsn=clean_runtime)
    publish_nodes(
        clean_runtime, deployment,
        subtypes=["chunk"],
        nodes=[node("chunk:a", "chunk", "some content")],
    )
    cv = _catalog_version(clean_runtime)
    block = {"profiles": {"analyst": {"subtypes": ["chunk", "paper", "figrue"]}}}
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        with pytest.raises(ProfileSubtypeAbsent) as exc:
            resolve_scope(
                conn,
                catalog_version_id=cv,
                config=parse_search_config(block),
                requested_profile="analyst",
                subtype_filter=None,
                absent_subtypes_allowed=lambda absent: [
                    name for name in absent if name == "paper"
                ],
            )
    assert exc.value.data["absent"] == ["figrue"]
    assert exc.value.data["undefined_by_deployment"] is True


def test_allowance_never_yields_an_empty_scope(
    clean_runtime: str, tmp_path,
) -> None:
    """A profile with no present type raises rather than scoping to nothing."""
    deployment = make_search_deployment(tmp_path, dsn=clean_runtime)
    publish_nodes(
        clean_runtime, deployment,
        subtypes=["chunk"],
        nodes=[node("chunk:a", "chunk", "some content")],
    )
    cv = _catalog_version(clean_runtime)
    block = {"profiles": {"analyst": {"subtypes": ["paper", "venue"]}}}
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        with pytest.raises(ProfileSubtypeAbsent) as exc:
            resolve_scope(
                conn,
                catalog_version_id=cv,
                config=parse_search_config(block),
                requested_profile="analyst",
                subtype_filter=None,
                absent_subtypes_allowed=lambda absent: absent,
            )
    assert exc.value.data["absent"] == ["paper", "venue"]
    assert "empty page" in str(exc.value)


def test_incomplete_ancestor_closure_raises_instead_of_under_scoping(
    clean_runtime: str, tmp_path,
) -> None:
    """A short closure is refused, not quietly treated as "no refinements".

    `okg.subtype_ancestors` is backfilled with depth-0 reflexive rows only for
    catalog versions compiled before that table existed, so this state is real,
    not hypothetical. Reproduce it by deleting the depth>=1 row.
    """
    deployment = make_search_deployment(tmp_path, dsn=clean_runtime)
    publish_nodes(
        clean_runtime, deployment,
        subtypes=[
            SubtypeSpec("chunk"),
            SubtypeSpec("analysis_chunk", parent="chunk"),
        ],
        nodes=[node("chunk:a", "analysis_chunk", "refined content")],
    )
    cv = _catalog_version(clean_runtime)
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        # Control: complete closure resolves and DOES cover the refinement.
        assert set(expand_subtypes(
            conn, catalog_version_id=cv, subtypes=["chunk"],
        )) == {"chunk", "analysis_chunk"}

        with conn.cursor() as cur:
            cur.execute(
                "DELETE FROM okg.subtype_ancestors "
                " WHERE catalog_version_id = %s AND subtype_name = %s "
                "   AND ancestor_name = %s AND depth >= 1",
                (cv, "analysis_chunk", "chunk"),
            )
        with pytest.raises(SubtypeClosureIncomplete) as exc:
            expand_subtypes(
                conn, catalog_version_id=cv, subtypes=["chunk"],
            )
    assert "analysis_chunk -> chunk" in str(exc.value)
    assert exc.value.data["catalog_version_id"] == cv


# ---------- req.search-profiles.scope-precedes-rank-cap ----------

def test_profile_returns_a_row_the_unscoped_rank_cap_evicts(
    clean_runtime: str, tmp_path, monkeypatch,
) -> None:
    """The scope narrows the RANKED set, not the page.

    "Absent from the unscoped result" is asserted across every page the
    unscoped result can REACH — `total_count` saturates at the rank cap, so
    that page space is the whole of what a caller can ever see. That
    unreachability IS the defect: the answering row is evicted by rows the
    caller never wanted before any page is cut.

    The cap is monkeypatched down so the corpus stays small. That is sound
    here because "the predicate sits before the SQL LIMIT" is a structural
    property independent of the cap's value — and the patch drives the module
    constant the SQL actually reads. `test_scope_precedes_the_shipped_rank_cap`
    below runs the same shape un-patched at the real 200, so this can never
    drift from the constant.
    """
    # The MODULE, not the re-exported `search` function: the package namespace
    # binds the name `search` to the function, so an `import ... as` here would
    # patch an attribute onto the function and prove nothing. The patch has to
    # land on the module global the SQL builder actually reads.
    search_mod = sys.modules["okg.substrate.mcp.search"]

    monkeypatch.setattr(search_mod, "_LEXICAL_RANK_CAP", 10)

    deployment = make_search_deployment(tmp_path, dsn=clean_runtime)
    # 40 bookkeeping rows whose text matches the query strongly, and one
    # content row that matches it too. With a cap of 10 the content row is
    # evicted from the unscoped ranking outright.
    nodes = [
        node(f"task:{i:03d}", "pact_task",
             f"background estimation background estimation task {i}")
        for i in range(40)
    ]
    nodes.append(
        node("chunk:abcd", "chunk",
             "the background is estimated with an extended ABCD method")
    )
    publish_nodes(
        clean_runtime, deployment,
        subtypes=["chunk", "pact_task"],
        nodes=nodes,
    )
    cv = _catalog_version(clean_runtime)
    query = "background estimation"

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        session = open_session(conn, deployment_name=deployment)
        scope = resolve_scope(
            conn, catalog_version_id=cv,
            config=parse_search_config(ANALYST_BLOCK),
            requested_profile="analyst", subtype_filter=None,
        )
        # Exhaust every reachable unscoped page.
        unscoped_ids: list[str] = []
        offset, first = 0, None
        while True:
            page = search(
                conn, session, query, method="lexical", completeness="exact",
                limit=5, offset=offset,
            )
            first = first or page
            unscoped_ids.extend(row["node_id"] for row in page["results"])
            if page["next_offset"] is None:
                break
            offset = page["next_offset"]
        scoped = search(
            conn, session, query, method="lexical", completeness="exact", scope=scope,
        )

    assert first is not None
    # The unscoped ranking really did saturate: this is eviction, not a page
    # boundary.
    assert first["rank_capped"] is True
    assert first["total_count"] == 10
    assert "chunk:abcd" not in unscoped_ids
    # ...and the scoped search returns it.
    assert "chunk:abcd" in [row["node_id"] for row in scoped["results"]]
    # rank_capped is measured over the SCOPED set: one in-scope row, so the
    # scoped ranking did not saturate.
    assert scoped["rank_capped"] is False


def test_scope_precedes_the_shipped_rank_cap(
    clean_runtime: str, tmp_path,
) -> None:
    """The same property at the real `_LEXICAL_RANK_CAP`, un-patched.

    Guards the monkeypatched test above from drifting away from the constant
    the code ships with.
    """
    from okg.substrate.mcp.search import _LEXICAL_RANK_CAP

    deployment = make_search_deployment(tmp_path, dsn=clean_runtime)
    nodes = [
        node(f"task:{i:03d}", "pact_task",
             "background estimation background estimation task")
        for i in range(_LEXICAL_RANK_CAP + 20)
    ]
    nodes.append(
        node("chunk:abcd", "chunk",
             "the background is estimated with an extended ABCD method")
    )
    publish_nodes(
        clean_runtime, deployment,
        subtypes=["chunk", "pact_task"],
        nodes=nodes,
    )
    cv = _catalog_version(clean_runtime)
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        session = open_session(conn, deployment_name=deployment)
        scope = resolve_scope(
            conn, catalog_version_id=cv,
            config=parse_search_config(ANALYST_BLOCK),
            requested_profile="analyst", subtype_filter=None,
        )
        unscoped = search(
            conn, session, "background estimation", method="lexical",
            limit=_LEXICAL_RANK_CAP,
        )
        scoped = search(
            conn, session, "background estimation", method="lexical",
            scope=scope,
        )

    assert unscoped["rank_capped"] is True
    assert "chunk:abcd" not in [r["node_id"] for r in unscoped["results"]]
    assert "chunk:abcd" in [r["node_id"] for r in scoped["results"]]


def test_effective_attrs_candidate_query_is_scoped(
    clean_runtime: str, tmp_path,
) -> None:
    """The second ranked query carries the scope too.

    `_effective_view_lexical_candidates` has its own LIMIT and its rows are
    unioned into the candidate set. Without the scope here, an `attr_filters`
    query under a profile would return out-of-scope rows while the envelope
    reported a scope — the disclosure would be a lie.
    """
    deployment = make_search_deployment(tmp_path, dsn=clean_runtime)
    publish_nodes(
        clean_runtime, deployment,
        subtypes=["chunk", "pact_task"],
        nodes=[
            node("chunk:a", "chunk", "puppimet requirement selection",
                 tier="a"),
            node("task:a", "pact_task", "puppimet requirement selection",
                 tier="a"),
        ],
    )
    cv = _catalog_version(clean_runtime)
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        session = open_session(conn, deployment_name=deployment)
        scope = resolve_scope(
            conn, catalog_version_id=cv,
            config=parse_search_config(ANALYST_BLOCK),
            requested_profile="analyst", subtype_filter=None,
        )
        unscoped = search(
            conn, session, "puppimet requirement", method="lexical",
            filter={"attr_filters": {"tier": "a"}},
        )
        scoped = search(
            conn, session, "puppimet requirement", method="lexical",
            filter={"attr_filters": {"tier": "a"}}, scope=scope,
        )

    # Control: unscoped, the filter matches both.
    assert {r["node_id"] for r in unscoped["results"]} == {"chunk:a", "task:a"}
    assert {r["node_id"] for r in scoped["results"]} == {"chunk:a"}
    assert scoped["scoped_subtypes"] == ["chunk"]


def test_subtype_filter_intersects_the_expanded_profile_set(
    clean_runtime: str, tmp_path,
) -> None:
    """Naming a refinement of a profiled parent is legal.

    `filter["subtype"]` stays exact-match and is intersected against the
    EXPANDED set, so `--subtype analysis_chunk` under a profile declaring
    `chunk` succeeds rather than tripping the outside-profile refusal.
    """
    deployment = make_search_deployment(tmp_path, dsn=clean_runtime)
    publish_nodes(
        clean_runtime, deployment,
        subtypes=[
            SubtypeSpec("chunk"),
            SubtypeSpec("analysis_chunk", parent="chunk"),
            SubtypeSpec("pact_task"),
        ],
        nodes=[
            node("chunk:plain", "chunk", "selection criteria overview"),
            node("chunk:refined", "analysis_chunk",
                 "selection criteria in detail"),
        ],
    )
    cv = _catalog_version(clean_runtime)
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        session = open_session(conn, deployment_name=deployment)
        scope = resolve_scope(
            conn, catalog_version_id=cv,
            config=parse_search_config(ANALYST_BLOCK),
            requested_profile="analyst", subtype_filter="analysis_chunk",
        )
        result = search(
            conn, session, "selection criteria", method="lexical",
            filter={"subtype": "analysis_chunk"}, scope=scope,
        )
    ids = [r["node_id"] for r in result["results"]]
    assert ids == ["chunk:refined"]


def test_subtype_outside_profile_raises_instead_of_empty_page(
    clean_runtime: str, tmp_path,
) -> None:
    deployment = make_search_deployment(tmp_path, dsn=clean_runtime)
    publish_nodes(
        clean_runtime, deployment,
        subtypes=["chunk", "pact_task"],
        nodes=[node("task:a", "pact_task", "some tracking text")],
    )
    cv = _catalog_version(clean_runtime)
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        session = open_session(conn, deployment_name=deployment)
        scope = resolve_scope(
            conn, catalog_version_id=cv,
            config=parse_search_config(ANALYST_BLOCK),
            requested_profile="analyst", subtype_filter=None,
        )
        with pytest.raises(SubtypeOutsideProfile) as exc:
            search(
                conn, session, "tracking", method="lexical",
                filter={"subtype": "pact_task"}, scope=scope,
            )
    assert exc.value.data["subtype"] == "pact_task"
    assert exc.value.data["profile"] == "analyst"


def test_alias_promotion_honours_the_resolved_scope(
    clean_runtime: str, tmp_path,
) -> None:
    """Promotion is not a side door into a scoped page, in either direction.

    Out-of-scope canonicals stay out. In-scope SCOPED aliases are admitted —
    a deliberate widening relative to `analyst-search-result-context`, under
    which an unscoped query sees NULL-scope aliases only. The last assertion
    pins that difference so it is a decision rather than an accident.
    """
    deployment = make_search_deployment(tmp_path, dsn=clean_runtime)
    publish_nodes(
        clean_runtime, deployment,
        subtypes=["chunk", "pact_task"],
        aliases=[
            # names a pact_task canonical, globally scoped
            ("ABCD", "task:abcd", None),
            # chunk-scoped alias naming a chunk canonical
            ("JES", "chunk:jes", "chunk"),
        ],
        nodes=[
            node("task:abcd", "pact_task", "ABCD tracking task"),
            node("chunk:jes", "chunk", "JES jet energy scale systematic"),
            node("chunk:decoy", "chunk", "ABCD appears in this chunk too"),
        ],
    )
    cv = _catalog_version(clean_runtime)
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        session = open_session(conn, deployment_name=deployment)
        scope = resolve_scope(
            conn, catalog_version_id=cv,
            config=parse_search_config(ANALYST_BLOCK),
            requested_profile="analyst", subtype_filter=None,
        )
        # Control: unscoped, the global alias DOES promote its pact_task.
        unscoped_abcd = search(conn, session, "ABCD", method="lexical")
        scoped_abcd = search(
            conn, session, "ABCD", method="lexical", scope=scope,
        )
        unscoped_jes = search(conn, session, "JES", method="lexical")
        scoped_jes = search(
            conn, session, "JES", method="lexical", scope=scope,
        )

    assert unscoped_abcd["results"][0]["node_id"] == "task:abcd"
    scoped_ids = [r["node_id"] for r in scoped_abcd["results"]]
    assert "task:abcd" not in scoped_ids
    assert "chunk:decoy" in scoped_ids

    # The chunk-scoped alias promotes under the profile...
    assert scoped_jes["results"][0]["node_id"] == "chunk:jes"
    assert scoped_jes["results"][0]["match_kind"] == "catalog_alias"
    # ...and still does NOT promote in an unscoped query.
    assert all(
        r.get("match_kind") != "catalog_alias"
        for r in unscoped_jes["results"]
    )


def test_scoped_ranking_pushes_the_predicate_into_the_ranked_scan(
    clean_runtime: str, tmp_path,
) -> None:
    """The scope narrows the SCAN, so the cap is not spent on out-of-scope rows.

    Asserted as query SHAPE rather than wall-clock: this repo's CI runners vary
    by ~15% run to run, and a millisecond budget on them reds PRs that changed
    nothing. What actually matters is structural and deterministic — the
    subtype predicate is IN the ranked query, and the rows the scan returns are
    bounded by the cap rather than by the whole match list.
    """
    deployment = make_search_deployment(tmp_path, dsn=clean_runtime)
    nodes = [
        node(f"task:{i:03d}", "pact_task", f"estimation of the background {i}")
        for i in range(120)
    ]
    nodes += [
        node(f"chunk:{i:03d}", "chunk", f"estimation of the background {i}")
        for i in range(5)
    ]
    publish_nodes(
        clean_runtime, deployment,
        subtypes=["chunk", "pact_task"], nodes=nodes,
    )
    cv = _catalog_version(clean_runtime)
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        session = open_session(conn, deployment_name=deployment)
        scope = resolve_scope(
            conn, catalog_version_id=cv,
            config=parse_search_config(ANALYST_BLOCK),
            requested_profile="analyst", subtype_filter=None,
        )
        scoped = search(
            conn, session, "estimation background", method="lexical", completeness="exact",
            scope=scope, limit=200,
        )
        unscoped = search(
            conn, session, "estimation background", method="lexical", completeness="exact",
            limit=200,
        )

    # Only in-scope rows were ever candidates: the count is the in-scope
    # population, not the cap. If the predicate had been applied to the PAGE
    # instead of the ranked query, total_count would reflect the 125-row
    # match list.
    assert scoped["total_count"] == 5
    assert unscoped["total_count"] == 125
    assert all(r["subtype"] == "chunk" for r in scoped["results"])


# ---------- req.search-profiles.no-method-silently-ignores-scope ----------

def test_semantic_and_alias_methods_honour_the_profile(
    clean_runtime: str, tmp_path,
) -> None:
    """Neither method may accept a scope and quietly return unscoped rows."""
    from okg.substrate.embeddings import HashEmbedder, embedder_model_id
    from okg.substrate.graph_store import ChronosGraphStore
    from okg.substrate.publisher import Publisher

    deployment = make_search_deployment(tmp_path, dsn=clean_runtime)
    publish_nodes(
        clean_runtime, deployment,
        subtypes=["chunk", "pact_task"],
        aliases=[("ABCD", "task:abcd", None)],
        nodes=[
            node("task:abcd", "pact_task", "ABCD background estimation task"),
            node("chunk:abcd", "chunk", "ABCD background estimation method"),
        ],
    )
    cv = _catalog_version(clean_runtime)
    embedder = HashEmbedder()
    # Semantic ranking only sees rows with a non-null embedding, so both the
    # in-scope and the out-of-scope row must carry one — otherwise the scoped
    # result would look right for the wrong reason.
    query = "background estimation"
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        graph = ChronosGraphStore.from_connection(conn).checkout()
        for node_id, text in (
            ("task:abcd", query),
            ("chunk:abcd", query + " method"),
        ):
            vec = embedder.embed(text)
            graph.execute(
                "UPDATE okg.graph_nodes "
                "   SET embedding = :embedding::vector, "
                "       embedding_model = :model_id "
                " WHERE node_id = :node_id",
                {
                    "embedding": "[" + ",".join(f"{v:.6f}" for v in vec) + "]",
                    "node_id": node_id,
                    # The REAL identity of the embedder querying below. A
                    # literal here (this said 'test-hash') is model skew, which
                    # `reachable-semantic-search` now refuses.
                    "model_id": embedder_model_id(embedder),
                },
            )
        # A publish cycle with no new facts is a no-op, so the embedding edit
        # would never be captured into a generation the session can pin.
        ChronosGraphStore.from_connection(conn).apply_facts(
            [node("chunk:delta", "chunk", "publish delta")],
            source="system:analyst_search_fixture",
            strict_endpoints=True,
        )
    Publisher(clean_runtime, deployment, catalog_identity=bind_test_catalog_writer(clean_runtime, deployment)).cycle()

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        session = open_session(conn, deployment_name=deployment)
        scope = resolve_scope(
            conn, catalog_version_id=cv,
            config=parse_search_config(ANALYST_BLOCK),
            requested_profile="analyst", subtype_filter=None,
        )
        alias_unscoped = search(conn, session, "ABCD", method="alias")
        alias_scoped = search(
            conn, session, "ABCD", method="alias", scope=scope,
        )
        semantic_unscoped = search(
            conn, session, query, method="semantic", embedder=embedder,
        )
        semantic_scoped = search(
            conn, session, query, method="semantic",
            embedder=embedder, scope=scope,
        )

    # Control: unscoped, the alias resolves to the pact_task canonical.
    assert [r["canonical"] for r in alias_unscoped["results"]] == ["task:abcd"]
    # Scoped, that canonical is outside the profile, so no row survives.
    assert alias_scoped["results"] == []
    assert alias_scoped["search_profile"] == "analyst"
    # And the empty page is legible: the scope holds nodes, so this is
    # "nothing matched in scope", not "the scope is empty".
    assert alias_scoped["scope_holds_no_nodes"] is False

    # Control: unscoped, semantic ranks the pact_task row (it embeds the query
    # text exactly, so it is the nearest neighbour).
    assert "task:abcd" in [r["node_id"] for r in semantic_unscoped["results"]]
    assert "task:abcd" not in [
        r["node_id"] for r in semantic_scoped["results"]
    ]
    assert all(
        r["subtype"] == "chunk" for r in semantic_scoped["results"]
    )
    assert semantic_scoped["scoped_subtypes"] == ["chunk"]


def test_default_profile_does_not_rescope_internal_callers(
    clean_runtime: str, tmp_path,
) -> None:
    """A deployment default is applied by the operator surfaces, not discovered.

    Paired in ONE test with a scoped call on the SAME generation, so it cannot
    pass against code where profiles do not exist at all — the unscoped call
    must return a row that the scoped call excludes.
    """
    deployment = make_search_deployment(tmp_path, dsn=clean_runtime)
    publish_nodes(
        clean_runtime, deployment,
        subtypes=["chunk", "pact_task"],
        nodes=[
            node("task:a", "pact_task", "shared wording here"),
            node("chunk:a", "chunk", "shared wording here"),
        ],
    )
    cv = _catalog_version(clean_runtime)
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        session = open_session(conn, deployment_name=deployment)
        scope = resolve_scope(
            conn, catalog_version_id=cv,
            config=parse_search_config(ANALYST_BLOCK),
            requested_profile=None, subtype_filter=None,
        )
        # An in-process caller that never asked for a profile.
        internal = search(conn, session, "shared wording", method="lexical")
        # The operator surface, having resolved the deployment default.
        surfaced = search(
            conn, session, "shared wording", method="lexical", scope=scope,
        )
        with pytest.raises(ValueError, match="ResolvedScope"):
            search(
                conn, session, "shared wording", method="lexical",
                scope="analyst",
            )

    assert scope.profile == "analyst"
    assert "task:a" in [r["node_id"] for r in internal["results"]]
    assert "task:a" not in [r["node_id"] for r in surfaced["results"]]
    # Present AND null, not merely absent: an assertion on `.get(...)` would
    # hold against code that never emits the key.
    assert "search_profile" in internal
    assert internal["search_profile"] is None
    assert "scoped_subtypes" in internal
    assert internal["scoped_subtypes"] is None


# ---------- req.search-profiles.disclosed-and-escapable ----------

def test_envelope_reports_applied_profile_and_resolved_scope(
    clean_runtime: str, tmp_path,
) -> None:
    deployment = make_search_deployment(tmp_path, dsn=clean_runtime)
    publish_nodes(
        clean_runtime, deployment,
        subtypes=["chunk", "pact_task"],
        nodes=[node("chunk:a", "chunk", "content about selection")],
    )
    cv = _catalog_version(clean_runtime)
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        session = open_session(conn, deployment_name=deployment)
        # The default applies when the caller names neither profile nor
        # subtype, and the envelope says which one ran.
        scope = resolve_scope(
            conn, catalog_version_id=cv,
            config=parse_search_config(ANALYST_BLOCK),
            requested_profile=None, subtype_filter=None,
        )
        defaulted = search(
            conn, session, "selection", method="lexical", scope=scope,
        )
        # A deployment with no `search:` block resolves to unscoped.
        no_block = resolve_scope(
            conn, catalog_version_id=cv, config=parse_search_config(None),
            requested_profile=None, subtype_filter=None,
        )
        plain = search(
            conn, session, "selection", method="lexical", scope=no_block,
        )

    assert defaulted["search_profile"] == "analyst"
    assert defaulted["scoped_subtypes"] == ["chunk"]
    assert "search_profile" in plain and plain["search_profile"] is None
    assert "scoped_subtypes" in plain and plain["scoped_subtypes"] is None


def test_empty_scope_reports_a_measured_zero_node_count(
    clean_runtime: str, tmp_path,
) -> None:
    """A scope that holds nothing is a reported state, not an inference.

    "Scoped to subtypes with no data" and "scoped correctly, matched nothing"
    produce identical empty pages. The per-subtype count is MEASURED so the
    two can be told apart.
    """
    deployment = make_search_deployment(tmp_path, dsn=clean_runtime)
    publish_nodes(
        clean_runtime, deployment,
        subtypes=["chunk", "figure", "pact_task"],
        nodes=[node("task:a", "pact_task", "only bookkeeping exists here")],
    )
    cv = _catalog_version(clean_runtime)
    block = {"profiles": {"analyst": {"subtypes": ["chunk", "figure"]}}}
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        session = open_session(conn, deployment_name=deployment)
        scope = resolve_scope(
            conn, catalog_version_id=cv, config=parse_search_config(block),
            requested_profile="analyst", subtype_filter=None,
        )
        result = search(
            conn, session, "bookkeeping", method="lexical", scope=scope,
        )

    assert result["results"] == []
    assert result["scope_node_counts"] == {"chunk": 0, "figure": 0}
    assert result["scope_holds_no_nodes"] is True


def test_reserved_all_profile_overrides_the_deployment_default(
    clean_runtime: str, tmp_path,
) -> None:
    deployment = make_search_deployment(tmp_path, dsn=clean_runtime)
    publish_nodes(
        clean_runtime, deployment,
        subtypes=["chunk", "pact_task"],
        nodes=[
            node("task:a", "pact_task", "shared wording here"),
            node("chunk:a", "chunk", "shared wording here"),
        ],
    )
    cv = _catalog_version(clean_runtime)
    config = parse_search_config(ANALYST_BLOCK)
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        session = open_session(conn, deployment_name=deployment)
        escaped = resolve_scope(
            conn, catalog_version_id=cv, config=config,
            requested_profile="all", subtype_filter=None,
        )
        defaulted = resolve_scope(
            conn, catalog_version_id=cv, config=config,
            requested_profile=None, subtype_filter=None,
        )
        result = search(
            conn, session, "shared wording", method="lexical", scope=escaped,
        )
        default_result = search(
            conn, session, "shared wording", method="lexical", scope=defaulted,
        )

    # The default really did exclude bookkeeping...
    assert "task:a" not in [r["node_id"] for r in default_result["results"]]
    # ...and `all` gets it back.
    assert "task:a" in [r["node_id"] for r in result["results"]]
    assert result["search_profile"] == "all"
    assert "scoped_subtypes" in result and result["scoped_subtypes"] is None


def test_a_resolved_scope_is_deterministic_at_a_pin(
    clean_runtime: str, tmp_path,
) -> None:
    deployment = make_search_deployment(tmp_path, dsn=clean_runtime)
    publish_nodes(
        clean_runtime, deployment,
        subtypes=["chunk", "pact_task"],
        nodes=[node("chunk:a", "chunk", "repeatable content")],
    )
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        session = open_session(conn, deployment_name=deployment)
        scope = ResolvedScope(profile="analyst", subtypes=("chunk",))
        first = search(
            conn, session, "repeatable", method="lexical", scope=scope,
        )
        second = search(
            conn, session, "repeatable", method="lexical", scope=scope,
        )
    assert first["results"] == second["results"]
