"""Source-family authority controls select and disclose; they never rerank.

Issue #1444 (part 2), PACT `mcp-authority-diversity-budgets`.

These reach the substrate through psycopg + `open_session`, NOT `build_server`,
matching `test_search_profiles.py`. The argument-only refusals that need no
database live in `test_search_profile_arguments.py` (fast lane) and the
manifest parsing in `tests/substrate/test_search_profile_manifest.py`.

The stance every assertion here encodes: authority is EXPLICIT SELECTION.
`read_affordances.py` records a standing prohibition on an unqualified
source-formality prior, with a counterexample where the informal source held
the answer, so nothing here may reorder rows or touch a score. The tests check
that too — the surviving rows carry the same raw scores an unfiltered search
gives them.

Every scoped assertion is paired with an unscoped control, so a test cannot
pass merely because the row it wanted was the only row.
"""

from __future__ import annotations

import psycopg
import pytest

from okg.substrate.mcp import open_session, search
from okg.substrate.mcp.search_profiles import (
    APPLICATION_RANKED_SQL,
    APPLICATION_RESIDUAL,
    FAMILY_DISCLOSURE_KEYS,
    RESERVED_ALL,
    SourceFamilyEmptyIntersection,
    SourceFamilyNotApplicable,
    SourceFamilySourceAbsent,
    UnknownSourceFamily,
    assert_method_can_filter_family,
    parse_search_config,
    resolve_scope,
    restrict_scope_to_method,
)
from tests.substrate.mcp.authority_diversity_fixture import (
    ALL_CHUNK_IDS,
    CODE_IDS,
    CODE_PRODUCER,
    DOCS_IDS,
    DOCS_PRODUCER,
    NARROWINGS,
    OPS_IDS,
    OPS_PRODUCER,
    SEARCH_BLOCK,
    SUBTYPES,
    TERM,
    TEST_PATH_IDS,
    node_groups,
    register_producer_profiles,
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


def _catalog_version(dsn: str) -> int:
    with psycopg.connect(dsn, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT max(catalog_version_id) FROM okg.catalog_versions "
            " WHERE status = 'applied'"
        )
        return int(cur.fetchone()[0])


def _scope(conn, session, *, profile=None, families=None, bound=None):
    return resolve_scope(
        conn,
        catalog_version_id=session.catalog_version_id,
        config=CONFIG,
        requested_profile=profile,
        requested_families=families,
        requested_max_per_parent=bound,
        deployment_name=session.deployment_name,
    )


def _ids(page) -> list[str]:
    return [row["node_id"] for row in page["results"]]


# ---------- req.authority.family-selection ----------

def test_the_corpus_is_adversarial_before_anything_is_selected(
    clean_runtime: str, tmp_path,
) -> None:
    """The control the rest of this file depends on.

    If the documentation answer already outranked the test file, every
    selection assertion below would pass on a corpus where no selection was
    needed. It does not: the TEST file is first.
    """
    deployment = _publish(clean_runtime, tmp_path)
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        session = open_session(conn, deployment_name=deployment)
        page = search(conn, session, TERM, method="lexical", rank_by="self", limit=50)
    ids = _ids(page)
    assert set(ids) == set(ALL_CHUNK_IDS), ids
    assert ids[0] == "chunk:test:core", ids
    assert ids.index("chunk:test:core") < ids.index("chunk:doc:guide"), ids
    # No family was applied, and the envelope says so rather than omitting it.
    assert page["source_families"] is None
    assert page["scoped_source_ids"] is None
    assert page["source_families_declared"] is False


def test_a_family_filter_returns_only_that_family_and_discloses_it(
    clean_runtime: str, tmp_path,
) -> None:
    deployment = _publish(clean_runtime, tmp_path)
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        session = open_session(conn, deployment_name=deployment)
        scope = _scope(conn, session, families=["documentation"])
        page = search(
            conn, session, TERM, method="lexical", rank_by="self", scope=scope, limit=50,
        )
        control = search(conn, session, TERM, method="lexical", rank_by="self", limit=50)
    assert sorted(_ids(page)) == sorted(DOCS_IDS)
    # Non-vacuous: the other families really did match the same query.
    assert set(CODE_IDS) <= set(_ids(control))
    assert page["source_families"] == ["documentation"]
    assert page["scoped_source_ids"] == [DOCS_PRODUCER]
    assert page["scoped_source_id_count"] == 1
    assert page["scoped_source_ids_truncated"] is False
    assert page["source_families_declared"] is True
    # Every card names its own authority.
    assert {row["source_family"] for row in page["results"]} == {
        "documentation",
    }


def test_selection_never_changes_a_score_or_an_order(
    clean_runtime: str, tmp_path,
) -> None:
    """The recorded stance, checked rather than asserted in prose.

    `read_affordances.py:20-24` forbids an unqualified authority PRIOR. So the
    rows a family filter keeps must carry the same raw BM25 scores, in the same
    relative order, that the unfiltered search gives them.
    """
    deployment = _publish(clean_runtime, tmp_path)
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        session = open_session(conn, deployment_name=deployment)
        scope = _scope(conn, session, families=["code"])
        page = search(
            conn, session, TERM, method="lexical", rank_by="self", scope=scope, limit=50,
        )
        control = search(conn, session, TERM, method="lexical", rank_by="self", limit=50)
    control_scores = {row["node_id"]: row["score"] for row in control["results"]}
    for row in page["results"]:
        assert row["score"] == control_scores[row["node_id"]], row["node_id"]
    kept = _ids(page)
    assert kept == [
        node_id for node_id in _ids(control) if node_id in set(CODE_IDS)
    ]


def test_an_authority_profile_selects_docs_and_excludes_tests(
    clean_runtime: str, tmp_path,
) -> None:
    """The issue's headline case, and the one that needs BOTH controls.

    `engineering` selects the code AND documentation families, so the family
    filter alone cannot remove the test files — they share the code family's
    producer, exactly as a git source emits both. Only the path exclusion can,
    and it must be deterministic.
    """
    deployment = _publish(clean_runtime, tmp_path)
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        session = open_session(conn, deployment_name=deployment)
        scope = _scope(conn, session, profile="engineering")
        first = search(
            conn, session, TERM, method="lexical", rank_by="self", scope=scope, limit=50,
        )
        second = search(
            conn, session, TERM, method="lexical", rank_by="self", scope=scope, limit=50,
        )
        plain = search(
            conn, session, TERM, method="lexical", rank_by="self",
            scope=_scope(conn, session, profile="plain"), limit=50,
        )
    ids = _ids(first)
    assert _ids(second) == ids, "the exclusion must be deterministic"
    # Non-vacuous control: the same profile WITHOUT the authority keys keeps
    # the test rows, so their absence is the exclusion and not the corpus.
    assert set(TEST_PATH_IDS) <= set(_ids(plain))
    assert not set(TEST_PATH_IDS) & set(ids), ids
    assert set(DOCS_IDS) <= set(ids)
    assert "chunk:src:reader" in ids
    # The operational record carries NO `path`, and it is out because of the
    # FAMILY, not the glob — `engineering` does not select `operations`.
    assert not set(OPS_IDS) & set(ids)
    assert first["excluded_path_globs"] == ["tests/**", "**/conftest.py"]
    assert sorted(first["source_families"]) == ["code", "documentation"]


def test_a_path_exclusion_never_removes_a_row_it_did_not_name(
    clean_runtime: str, tmp_path,
) -> None:
    """A row with no `path` attribute survives every exclusion.

    An exclusion is a statement about paths. Dropping a path-less row would
    make the control delete material nobody asked it to.
    """
    deployment = _publish(clean_runtime, tmp_path)
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        session = open_session(conn, deployment_name=deployment)
        # `operations` is the only path-less family; the `authority` profile
        # excludes tests, so pair its globs with the ops family explicitly.
        scope = resolve_scope(
            conn,
            catalog_version_id=session.catalog_version_id,
            config=CONFIG,
            requested_profile="engineering",
            requested_families=["code"],
            deployment_name=session.deployment_name,
        )
        code_only = search(
            conn, session, TERM, method="lexical", rank_by="self", scope=scope, limit=50,
        )
        ops = search(
            conn, session, TERM, method="lexical", rank_by="self",
            scope=_scope(conn, session, families=["operations"]), limit=50,
        )
    assert sorted(_ids(ops)) == sorted(OPS_IDS)
    assert not set(TEST_PATH_IDS) & set(_ids(code_only))
    assert "chunk:src:reader" in _ids(code_only)


def test_profile_all_is_unscoped_in_every_axis(
    clean_runtime: str, tmp_path,
) -> None:
    """`all` remains the escape hatch after this change, not just before it.

    A deployment default that carried families and exclusions into `all` would
    trap a caller inside an authority scope with no way out — the exact thing
    the reserved name exists to prevent.
    """
    deployment = _publish(clean_runtime, tmp_path)
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        session = open_session(conn, deployment_name=deployment)
        scope = _scope(conn, session, profile=RESERVED_ALL)
        page = search(
            conn, session, TERM, method="lexical", rank_by="self", scope=scope, limit=50,
        )
    assert scope.subtypes is None
    assert scope.source_ids is None
    assert scope.excluded_path_globs == ()
    assert scope.max_per_parent is None
    assert sorted(_ids(page)) == sorted(ALL_CHUNK_IDS)
    assert page["search_profile"] == RESERVED_ALL
    assert page["source_families"] is None
    assert page["excluded_path_globs"] is None


def test_a_caller_family_still_applies_under_all(
    clean_runtime: str, tmp_path,
) -> None:
    """`all` drops the deployment's authority scope, not the caller's own.

    The caller asked for the narrowing; refusing it would be a different kind
    of surprise from the one `all` exists to prevent.
    """
    deployment = _publish(clean_runtime, tmp_path)
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        session = open_session(conn, deployment_name=deployment)
        scope = _scope(
            conn, session, profile=RESERVED_ALL, families=["documentation"],
        )
        page = search(
            conn, session, TERM, method="lexical", rank_by="self", scope=scope, limit=50,
        )
    assert sorted(_ids(page)) == sorted(DOCS_IDS)
    assert page["source_families"] == ["documentation"]


def test_the_family_filter_applies_to_alias_resolution_too(
    clean_runtime: str, tmp_path,
) -> None:
    """A globally-scoped alias must not be a side door past a family filter."""
    deployment = make_search_deployment(tmp_path, dsn=clean_runtime)
    publish_nodes(
        clean_runtime,
        deployment,
        subtypes=SUBTYPES,
        narrowings=NARROWINGS,
        nodes=[],
        node_groups=node_groups(),
        aliases=[("prov", "chunk:test:core", None)],
    )
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        session = open_session(conn, deployment_name=deployment)
        unfiltered = search(conn, session, "prov", method="alias")
        scope = _scope(conn, session, families=["documentation"])
        filtered = search(conn, session, "prov", method="alias", scope=scope)
    assert [row["canonical"] for row in unfiltered["results"]] == [
        "chunk:test:core",
    ]
    assert filtered["results"] == []
    assert filtered["source_families"] == ["documentation"]


# ---------- req.authority.declared-families (refusals) ----------

def test_an_unknown_family_is_refused_and_names_the_declared_ones(
    clean_runtime: str, tmp_path,
) -> None:
    deployment = _publish(clean_runtime, tmp_path)
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        session = open_session(conn, deployment_name=deployment)
        with pytest.raises(UnknownSourceFamily) as exc:
            _scope(conn, session, families=["documentaton"])
    message = str(exc.value)
    assert "documentaton" in message
    for declared in ("code", "documentation", "operations"):
        assert declared in message
    assert exc.value.code == "unknown_source_family"


def test_an_empty_intersection_is_refused_not_answered_empty(
    clean_runtime: str, tmp_path,
) -> None:
    """The `SubtypeOutsideProfile` argument, applied to families.

    An empty page cannot be told apart from "your query matched nothing", so a
    family outside the profile's own set raises instead.
    """
    deployment = _publish(clean_runtime, tmp_path)
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        session = open_session(conn, deployment_name=deployment)
        with pytest.raises(SourceFamilyEmptyIntersection) as exc:
            _scope(conn, session, profile="authority", families=["code"])
    assert exc.value.code == "source_family_empty_intersection"
    assert "authority" in str(exc.value)
    assert RESERVED_ALL in str(exc.value)


def test_a_family_naming_an_unregistered_producer_is_refused(
    clean_runtime: str, tmp_path,
) -> None:
    """Fail-loud where an authority to check against EXISTS.

    With the deployment's producer registry populated, a family naming a
    producer it never registered would select nothing; that empty page is
    exactly what a typo produces, so the resolution raises instead.
    """
    deployment = _publish(clean_runtime, tmp_path)
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        session = open_session(conn, deployment_name=deployment)
        register_producer_profiles(
            clean_runtime,
            session.deployment_name,
            [CODE_PRODUCER, DOCS_PRODUCER, OPS_PRODUCER],
        )
        # A family whose producer was never registered.
        config = parse_search_config({
            "source_families": {"ghost": ["source:typo"]},
            "profiles": {"p": {"subtypes": ["chunk"]}},
        })
        with pytest.raises(SourceFamilySourceAbsent) as exc:
            resolve_scope(
                conn,
                catalog_version_id=session.catalog_version_id,
                config=config,
                requested_profile=None,
                requested_families=["ghost"],
                deployment_name=session.deployment_name,
            )
        # And the registered families resolve, reporting the validated state.
        scope = _scope(conn, session, families=["documentation"])
        page = search(
            conn, session, TERM, method="lexical", rank_by="self", scope=scope, limit=50,
        )
    assert exc.value.code == "source_family_source_absent"
    assert "source:typo" in str(exc.value)
    assert scope.family_validation == "producer_profiles"
    assert page["source_family_validation"] == "producer_profiles"


def test_an_unpopulated_registry_is_reported_not_treated_as_a_pass(
    clean_runtime: str, tmp_path,
) -> None:
    """The state that cannot be validated is NAMED, never presented as checked."""
    deployment = _publish(clean_runtime, tmp_path)
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        session = open_session(conn, deployment_name=deployment)
        scope = _scope(conn, session, families=["documentation"])
        page = search(
            conn, session, TERM, method="lexical", rank_by="self", scope=scope, limit=50,
        )
    assert scope.family_validation == "registry_unpopulated"
    assert page["source_family_validation"] == "registry_unpopulated"


def test_an_empty_family_page_says_whether_the_family_holds_anything(
    clean_runtime: str, tmp_path,
) -> None:
    """Two causes of an empty page, told apart by measurement.

    "Nothing in this family matched" and "this family holds no node here" are
    indistinguishable from the page itself, and the second is what a typo'd
    producer id gives on a deployment whose registry cannot check it.
    """
    deployment = _publish(clean_runtime, tmp_path)
    config = parse_search_config({
        "source_families": {"ghost": ["source:absent"],
                            "documentation": [DOCS_PRODUCER]},
        "profiles": {"p": {"subtypes": ["chunk"]}},
    })
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        session = open_session(conn, deployment_name=deployment)
        ghost = search(
            conn, session, TERM, method="lexical", rank_by="self", limit=50,
            scope=resolve_scope(
                conn, catalog_version_id=session.catalog_version_id,
                config=config, requested_profile=None,
                requested_families=["ghost"],
                deployment_name=session.deployment_name,
            ),
        )
        # The control: a real family whose query genuinely matches nothing.
        populated = search(
            conn, session, "unrelatedterm", method="lexical", rank_by="self", limit=50,
            scope=resolve_scope(
                conn, catalog_version_id=session.catalog_version_id,
                config=config, requested_profile=None,
                requested_families=["documentation"],
                deployment_name=session.deployment_name,
            ),
        )
    assert ghost["results"] == []
    assert ghost["source_family_holds_no_nodes"] is True
    assert ghost["source_family_node_counts"] == {"source:absent": 0}
    assert populated["results"] == []
    assert populated["source_family_holds_no_nodes"] is False
    assert populated["source_family_node_counts"][DOCS_PRODUCER] > 0


def test_a_method_that_cannot_carry_the_predicate_refuses_an_explicit_family(
) -> None:
    """Pure argument validation; no database. Mirrors `assert_method_can_scope`.

    A profile-DECLARED family on such a method simply does not apply and the
    envelope says so; only an EXPLICIT request raises.
    """
    with pytest.raises(SourceFamilyNotApplicable) as exc:
        assert_method_can_filter_family("attrs", "documentation")
    message = str(exc.value)
    assert "attrs" in message
    for method in ("lexical", "semantic", "alias"):
        assert method in message
    assert exc.value.code == "source_family_not_applicable"
    # No family named is always allowed.
    assert assert_method_can_filter_family("attrs", None) is None


# ---------- #1444 review: BLOCKER 4 and MAJOR 6/7/8, end to end ----------

def test_a_profile_declared_bound_does_not_break_alias_search(
    clean_runtime: str, tmp_path,
) -> None:
    """BLOCKER 4, through the real search rather than the helper.

    The `diverse` profile declares `max_per_parent: 1`. Before the per-control
    split, resolving it and running ANY alias search raised
    `diversity_not_applicable` — naming a parameter the caller never passed.
    """
    deployment = make_search_deployment(tmp_path, dsn=clean_runtime)
    publish_nodes(
        clean_runtime,
        deployment,
        subtypes=SUBTYPES,
        narrowings=NARROWINGS,
        nodes=[],
        node_groups=node_groups(),
        aliases=[("prov", "chunk:doc:guide", None)],
    )
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        session = open_session(conn, deployment_name=deployment)
        scope = restrict_scope_to_method(
            _scope(conn, session, profile="diverse"), "alias",
        )
        page = search(conn, session, "prov", method="alias", scope=scope)

    assert scope.max_per_parent is None
    assert [row["canonical"] for row in page["results"]] == ["chunk:doc:guide"]
    assert page["diversity_applicable"] is False
    # `diversity_applied` is the lexical path's OUTCOME key. Absent here is
    # correct and unambiguous precisely because `diversity_applicable: false`
    # is present on every method to explain the absence.
    assert "diversity_applied" not in page


def test_every_ranked_method_emits_the_complete_disclosure_set(
    clean_runtime: str, tmp_path,
) -> None:
    """MAJOR 8, measured on real envelopes rather than on a helper."""
    deployment = make_search_deployment(tmp_path, dsn=clean_runtime)
    publish_nodes(
        clean_runtime,
        deployment,
        subtypes=SUBTYPES,
        narrowings=NARROWINGS,
        nodes=[],
        node_groups=node_groups(),
        aliases=[("prov", "chunk:doc:guide", None)],
    )
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        session = open_session(conn, deployment_name=deployment)
        scope = _scope(conn, session, profile="engineering")
        lexical = search(
            conn, session, TERM, method="lexical", rank_by="self", scope=scope, limit=50,
        )
        alias = search(
            conn, session, "prov", method="alias",
            scope=restrict_scope_to_method(scope, "alias"),
        )
    for page in (lexical, alias):
        assert set(FAMILY_DISCLOSURE_KEYS) <= set(page), sorted(
            set(FAMILY_DISCLOSURE_KEYS) - set(page)
        )
    # MAJOR 6/7: the label says WHERE the predicate ran, per method.
    assert lexical["source_family_application"] == APPLICATION_RANKED_SQL
    assert lexical["excluded_path_globs_application"] == APPLICATION_RANKED_SQL
    assert alias["source_family_application"] == APPLICATION_RESIDUAL
    assert alias["excluded_path_globs_application"] == APPLICATION_RESIDUAL


def test_a_null_path_attribute_is_kept_by_sql_and_residual_alike(
    clean_runtime: str, tmp_path,
) -> None:
    """MINOR 11. One row must not get two answers depending on which ran.

    `attrs = {"path": null}` makes `attrs ? 'path'` true while `->>'path'` is
    SQL NULL, so without the `IS NOT NULL` guard the exclusion predicate
    evaluated to NULL and DROPPED the row — while the Python residual, which
    requires a `str`, KEPT it.
    """
    from okg.substrate.library.sources.base import NodeFact

    deployment = make_search_deployment(tmp_path, dsn=clean_runtime)
    null_path = NodeFact(
        node_id="chunk:docs:nullpath",
        subtype="chunk",
        attrs={"text": f"{TERM} {TERM} record with an explicitly null path",
               "path": None},
        source_record_id={"id": "chunk:docs:nullpath"},
        source_revision={"v": 1},
    )
    groups = node_groups()
    groups = [
        (producer, facts + ([null_path] if producer == DOCS_PRODUCER else []))
        for producer, facts in groups
    ]
    publish_nodes(
        clean_runtime,
        deployment,
        subtypes=SUBTYPES,
        narrowings=NARROWINGS,
        nodes=[],
        node_groups=groups,
    )
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        session = open_session(conn, deployment_name=deployment)
        page = search(
            conn, session, TERM, method="lexical", rank_by="self", limit=50,
            scope=_scope(conn, session, profile="engineering"),
        )
    # An exclusion names PATHS; a row whose path is null has not been named.
    assert "chunk:docs:nullpath" in _ids(page)
