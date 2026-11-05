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


# ---------- a passage's record path under exclude_path_globs (#4227) ----------
#
# PACT unit-grep-exclude-globs-4227. The usual shape: a document record
# carries the `path`, its chunks carry none. A profile's `exclude_path_globs`
# was checked on each row's own path only, so a chunk of an excluded document
# (no path of its own) was still matched, counted and served by grep's unit
# form, while the record form (which reads the record) left it out.

EXCLUDED_DOC = "doc:excluded"
KEPT_DOC = "doc:kept"
EX_TERM = "zircaloy"
#: Chunks of the excluded document (no own path).
EXCLUDED_CHUNKS = ["chunk:excluded:0", "chunk:excluded:1"]
KEPT_CHUNKS = ["chunk:kept:0"]
#: A chunk with no record at all: never excluded (it names no path).
LOOSE_CHUNK = "chunk:loose"
GLOB_CONFIG = parse_search_config({
    "profiles": {
        "no_tests": {"subtypes": ["chunk"], "exclude_path_globs": ["tests/**"]},
        "chunks": {"subtypes": ["chunk"]},
        "records_no_tests": {"subtypes": ["doc", "chunk"], "exclude_path_globs": ["tests/**"]},
    },
})


def _glob_facts() -> list:
    from okg.substrate.library.sources.base import EdgeFact, NodeFact

    def node(node_id: str, subtype: str, attrs: dict) -> NodeFact:
        return NodeFact(
            node_id=node_id, subtype=subtype, attrs=attrs,
            source_record_id={"id": node_id}, source_revision={"v": 1},
        )

    def edge(src: str, dst: str) -> EdgeFact:
        return EdgeFact(
            src=src, dst=dst, edge_type="contains",
            source_record_id={"id": f"{src}|contains|{dst}"}, source_revision={"v": 1},
        )

    facts: list = [
        node(EXCLUDED_DOC, "doc", {"path": "tests/fixtures/rods.py"}),
        node(KEPT_DOC, "doc", {"path": "src/okg/rods.py"}),
        node(LOOSE_CHUNK, "chunk", {"text": f"{EX_TERM} loose note"}),
    ]
    for i, chunk in enumerate(EXCLUDED_CHUNKS):
        facts += [node(chunk, "chunk", {"text": f"{EX_TERM} cladding test {i}"}),
                  edge(EXCLUDED_DOC, chunk)]
    for i, chunk in enumerate(KEPT_CHUNKS):
        facts += [node(chunk, "chunk", {"text": f"{EX_TERM} cladding source {i}"}),
                  edge(KEPT_DOC, chunk)]
    return facts


@pytest.fixture
def globbed(clean_runtime: str, tmp_path) -> str:
    from okg.substrate.library.sources.base import EdgeFact, NodeFact

    deployment = make_search_deployment(tmp_path, dsn=clean_runtime)
    facts = _glob_facts()
    publish_nodes(
        clean_runtime, deployment, subtypes=SUBTYPES, narrowings=NARROWINGS,
        nodes=[f for f in facts if isinstance(f, NodeFact)],
        edges=[f for f in facts if isinstance(f, EdgeFact)],
    )
    return deployment


def _glob_search(dsn: str, deployment: str, *, profile: str, **kwargs):
    with psycopg.connect(dsn, autocommit=True) as conn:
        session = open_session(conn, deployment_name=deployment)
        scope = resolve_scope(
            conn, catalog_version_id=session.catalog_version_id, config=GLOB_CONFIG,
            requested_profile=profile, deployment_name=session.deployment_name,
        )
        return search(conn, session, EX_TERM, scope=scope, **{"limit": 50, **kwargs})


def _doc_of(node_id: str) -> str:
    return node_id.rsplit(":", 1)[0].replace("chunk:", "doc:")


def test_unit_grep_leaves_out_passages_of_an_excluded_record(
    clean_runtime: str, globbed: str,
) -> None:
    """Must-fail on dev: the excluded document's chunks were matched,
    counted and served by grep's unit form. Now they are not, the count and
    the complete claim match what is served, and `grep_searched` says the
    exclusion reads each passage's record path."""
    from okg.substrate.mcp.envelope import with_read_coverage

    page = _glob_search(clean_runtime, globbed, profile="no_tests", method="grep")
    ids = _ids(page)
    assert not set(ids) & set(EXCLUDED_CHUNKS), ids
    assert set(ids) == {*KEPT_CHUNKS, LOOSE_CHUNK}
    assert page["total_count"] == len(ids) and page["total_count_basis"] == "exact"
    assert with_read_coverage({**page, "operator_name": "search"})["coverage"][
        "delivery"]["complete"] is True
    searched = page["grep_searched"]
    assert searched["excluded_path_globs"] == ["tests/**"]


@pytest.mark.parametrize("view", ["cards", "list"])
def test_unit_grep_views_and_counts_agree_on_the_exclusion(
    clean_runtime: str, globbed: str, view: str,
) -> None:
    """Every delivery of the unit form (cards, list) serves and counts the
    same rows."""
    page = _glob_search(
        clean_runtime, globbed, profile="no_tests", method="grep", delivery_view=view,
    )
    assert set(_ids(page)) == {*KEPT_CHUNKS, LOOSE_CHUNK}
    assert page["total_count"] == 2


def test_lexical_passages_of_an_excluded_record_are_left_out(
    clean_runtime: str, globbed: str,
) -> None:
    """The same class in lexical search: a chunk is judged by its record's
    path too, ranked one by one or folded into records."""
    passages = _glob_search(
        clean_runtime, globbed, profile="no_tests", method="lexical", rank_by="self",
    )
    assert set(_ids(passages)) == {*KEPT_CHUNKS, LOOSE_CHUNK}
    records = _glob_search(clean_runtime, globbed, profile="no_tests", method="lexical")
    served = {_doc_of(i) if i.startswith("chunk:") and i != LOOSE_CHUNK else i
              for i in _ids(records)}
    assert EXCLUDED_DOC not in served and not set(_ids(records)) & set(EXCLUDED_CHUNKS)


def test_without_an_exclusion_every_passage_is_served(
    clean_runtime: str, globbed: str,
) -> None:
    """Control: a profile with no `exclude_path_globs` serves the excluded
    document's chunks in every form (passes on dev)."""
    unit = _glob_search(clean_runtime, globbed, profile="chunks", method="grep")
    assert set(_ids(unit)) == {*EXCLUDED_CHUNKS, *KEPT_CHUNKS, LOOSE_CHUNK}
    lexical = _glob_search(
        clean_runtime, globbed, profile="chunks", method="lexical", rank_by="self",
    )
    assert set(EXCLUDED_CHUNKS) <= set(_ids(lexical))


def test_the_record_form_already_leaves_out_an_excluded_record(
    clean_runtime: str, globbed: str,
) -> None:
    """Control: grep's record form reads the record's path (passes on dev)."""
    page = _glob_search(
        clean_runtime, globbed, profile="records_no_tests", method="grep", within="record",
    )
    assert _ids(page) == [KEPT_DOC]


def test_semantic_passages_of_an_excluded_record_are_left_out(
    clean_runtime: str, globbed: str,
) -> None:
    """The same class in semantic search: ranked chunks of an excluded
    record are not served; without the exclusion they are (control)."""
    from okg.substrate.embeddings import HashEmbedder
    from okg.substrate.mcp.semantic_readiness import resolve_node_segment

    embedder = HashEmbedder()
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        segment = resolve_node_segment(conn)
        rows = conn.execute(
            f"SELECT node_id, attrs->>'text' FROM {segment} "
            "WHERE deleted = FALSE AND attrs ? 'text'"
        ).fetchall()
        for node_id, text in rows:
            vec = embedder.embed(text)
            conn.execute(
                f"UPDATE {segment} SET embedding = %s::vector, embedding_model = %s "
                "WHERE node_id = %s",
                ("[" + ",".join(f"{v:.6f}" for v in vec) + "]", embedder.model_id, node_id),
            )
    excluded = _glob_search(
        clean_runtime, globbed, profile="no_tests", method="semantic", embedder=embedder,
    )
    assert _ids(excluded) and not set(_ids(excluded)) & set(EXCLUDED_CHUNKS)
    plain = _glob_search(
        clean_runtime, globbed, profile="chunks", method="semantic", embedder=embedder,
    )
    assert set(EXCLUDED_CHUNKS) <= set(_ids(plain))


def test_an_alias_to_a_passage_of_an_excluded_record_is_left_out(
    clean_runtime: str, tmp_path,
) -> None:
    """The alias method checks the canonical node against the profile: a
    passage of an excluded record is left out too; without the exclusion it
    is served (control), and an alias to a kept passage is served."""
    from okg.substrate.library.sources.base import EdgeFact, NodeFact

    deployment = make_search_deployment(tmp_path, dsn=clean_runtime)
    facts = _glob_facts()
    publish_nodes(
        clean_runtime, deployment, subtypes=SUBTYPES, narrowings=NARROWINGS,
        nodes=[f for f in facts if isinstance(f, NodeFact)],
        edges=[f for f in facts if isinstance(f, EdgeFact)],
        aliases=[("rodex", EXCLUDED_CHUNKS[0], None), ("rodkept", KEPT_CHUNKS[0], None)],
    )

    def canonicals(alias: str, profile: str) -> list[str]:
        with psycopg.connect(clean_runtime, autocommit=True) as conn:
            session = open_session(conn, deployment_name=deployment)
            scope = resolve_scope(
                conn, catalog_version_id=session.catalog_version_id, config=GLOB_CONFIG,
                requested_profile=profile, deployment_name=session.deployment_name,
            )
            page = search(
                conn, session, alias, method="alias", limit=50,
                scope=restrict_scope_to_method(scope, "alias"),
            )
        return [row["canonical"] for row in page["results"]]

    assert canonicals("rodex", "no_tests") == []
    assert canonicals("rodex", "chunks") == [EXCLUDED_CHUNKS[0]]
    assert canonicals("rodkept", "no_tests") == [KEPT_CHUNKS[0]]


# ---------- review of 0946d59ad: in the SQL, under access rules, two records ----------

#: Excluded passages that sort before the kept ones: more than grep's
#: 2,000-candidate scan bound and lexical's 200-row rank cap.
BULK_EX = 3_000
BULK_KEPT = 3_000


def _bulk_facts() -> list:
    from okg.substrate.library.sources.base import EdgeFact, NodeFact

    def node(node_id: str, subtype: str, attrs: dict) -> NodeFact:
        return NodeFact(
            node_id=node_id, subtype=subtype, attrs=attrs,
            source_record_id={"id": node_id}, source_revision={"v": 1},
        )

    def edge(src: str, dst: str) -> EdgeFact:
        return EdgeFact(
            src=src, dst=dst, edge_type="contains",
            source_record_id={"id": f"{src}|contains|{dst}"}, source_revision={"v": 1},
        )

    facts: list = [
        node(EXCLUDED_DOC, "doc", {"path": "tests/fixtures/rods.py"}),
        node(KEPT_DOC, "doc", {"path": "src/okg/rods.py"}),
    ]
    for i in range(BULK_EX):
        chunk = f"chunk:a:{i:05d}"
        facts += [node(chunk, "chunk", {"text": f"{EX_TERM} test {i}"}), edge(EXCLUDED_DOC, chunk)]
    for i in range(BULK_KEPT):
        chunk = f"chunk:b:{i:05d}"
        facts += [node(chunk, "chunk", {"text": f"{EX_TERM} source {i}"}), edge(KEPT_DOC, chunk)]
    return facts


@pytest.fixture
def bulk_globbed(clean_runtime: str, tmp_path) -> str:
    from okg.substrate.library.sources.base import EdgeFact, NodeFact

    deployment = make_search_deployment(tmp_path, dsn=clean_runtime)
    facts = _bulk_facts()
    publish_nodes(
        clean_runtime, deployment, subtypes=SUBTYPES, narrowings=NARROWINGS,
        nodes=[f for f in facts if isinstance(f, NodeFact)],
        edges=[f for f in facts if isinstance(f, EdgeFact)],
    )
    return deployment


def _kept_bulk(ids: list[str]) -> bool:
    return all(i.startswith("chunk:b:") for i in ids)


def test_many_excluded_passages_never_fill_the_bounds(
    clean_runtime: str, bulk_globbed: str,
) -> None:
    """Must-fail on dev and on 0946d59ad: 3,000 excluded chunks sort before
    3,000 kept ones. They filled grep's 2,000-row scan bound (2 rows served)
    and lexical's 200-row rank cap (2 rows, `truncated: false`). The rule now
    runs in the SQL that bounds the read: full pages of kept chunks, the
    count of kept chunks, `truncated` true, and the reply says the exclusion
    ran in the ranked SQL."""
    grep = _glob_search(clean_runtime, bulk_globbed, profile="no_tests", method="grep",
                        limit=20)
    assert len(_ids(grep)) == 20 and _kept_bulk(_ids(grep)), _ids(grep)[:5]
    assert grep["total_count"] == BULK_KEPT and grep["total_count_basis"] == "exact"
    assert grep["truncated"] is True and grep["next_offset"] == 20
    assert grep["excluded_path_globs_application"] == "ranked_sql"
    second = _glob_search(clean_runtime, bulk_globbed, profile="no_tests", method="grep",
                          limit=20, offset=20)
    assert len(_ids(second)) == 20 and _kept_bulk(_ids(second))
    assert not set(_ids(second)) & set(_ids(grep))
    lexical = _glob_search(clean_runtime, bulk_globbed, profile="no_tests",
                           method="lexical", rank_by="self", limit=20)
    assert len(_ids(lexical)) == 20 and _kept_bulk(_ids(lexical)), _ids(lexical)[:5]
    assert lexical["truncated"] is True
    assert lexical["excluded_path_globs_application"] == "ranked_sql"
    records = _glob_search(clean_runtime, bulk_globbed, profile="records_no_tests",
                           method="lexical", limit=20)
    served = _ids(records)
    assert served and EXCLUDED_DOC not in served
    assert not any(i.startswith("chunk:a:") for i in served)


def test_the_exclusion_costs_no_extra_statements_per_row(
    clean_runtime: str, bulk_globbed: str, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Measured on 6,000 matches: a grep under the excluding profile runs
    about as many statements as one under the same profile without
    exclusions: the count stays in SQL, and the reader-view check adds two
    statements per decided batch (about 25 more here; 0946d59ad ran 1,015
    against 129)."""
    counts: dict[str, int] = {}
    original = psycopg.Cursor.execute

    def measure(profile: str) -> int:
        seen = [0]

        def counted(self, *args, **kwargs):
            seen[0] += 1
            return original(self, *args, **kwargs)

        monkeypatch.setattr(psycopg.Cursor, "execute", counted)
        try:
            _glob_search(clean_runtime, bulk_globbed, profile=profile, method="grep",
                         limit=20)
        finally:
            monkeypatch.setattr(psycopg.Cursor, "execute", original)
        return seen[0]

    counts["chunks"] = measure("chunks")
    counts["no_tests"] = measure("no_tests")
    assert counts["no_tests"] <= counts["chunks"] + 40, counts


def _two_record_facts() -> list:
    from okg.substrate.library.sources.base import EdgeFact, NodeFact

    def node(node_id: str, subtype: str, attrs: dict) -> NodeFact:
        return NodeFact(
            node_id=node_id, subtype=subtype, attrs=attrs,
            source_record_id={"id": node_id}, source_revision={"v": 1},
        )

    def edge(src: str, dst: str) -> EdgeFact:
        return EdgeFact(
            src=src, dst=dst, edge_type="contains",
            source_record_id={"id": f"{src}|contains|{dst}"}, source_revision={"v": 1},
        )

    return [
        node(EXCLUDED_DOC, "doc", {"path": "tests/fixtures/rods.py"}),
        node(KEPT_DOC, "doc", {"path": "src/okg/rods.py"}),
        node("doc:nopath", "doc", {"title": "no path"}),
        node("chunk:shared", "chunk", {"text": f"{EX_TERM} shared"}),
        edge(EXCLUDED_DOC, "chunk:shared"), edge(KEPT_DOC, "chunk:shared"),
        node("chunk:ownpath", "chunk", {"text": f"{EX_TERM} own", "path": "src/x.py"}),
        edge(EXCLUDED_DOC, "chunk:ownpath"),
        node("chunk:nopathdoc", "chunk", {"text": f"{EX_TERM} nopath"}),
        edge("doc:nopath", "chunk:nopathdoc"),
        node("chunk:only", "chunk", {"text": f"{EX_TERM} only"}),
        edge(EXCLUDED_DOC, "chunk:only"),
    ]


def test_any_record_that_is_not_excluded_keeps_a_passage(
    clean_runtime: str, tmp_path,
) -> None:
    """The rule for a passage with several records, in SQL and as read: it
    is left out only when every record declaring it is excluded. A record
    with a kept path, or with no path, keeps it (as any visible lending
    record admits a unit under `path_prefix`). A passage's own kept path does
    not keep it when every record is excluded (`chunk:ownpath`)."""
    from okg.substrate.library.sources.base import EdgeFact, NodeFact

    deployment = make_search_deployment(tmp_path, dsn=clean_runtime)
    facts = _two_record_facts()
    publish_nodes(
        clean_runtime, deployment, subtypes=SUBTYPES, narrowings=NARROWINGS,
        nodes=[f for f in facts if isinstance(f, NodeFact)],
        edges=[f for f in facts if isinstance(f, EdgeFact)],
    )
    expected = {"chunk:shared", "chunk:nopathdoc"}
    for kwargs in ({"method": "grep"}, {"method": "lexical", "rank_by": "self"}):
        page = _glob_search(clean_runtime, deployment, profile="no_tests", **kwargs)
        assert set(_ids(page)) == expected, (kwargs, _ids(page))


#: Producers for the access-rule cases.
VIS = "connector:vis"
HID = "connector:hid"
MSK = "connector:msk"


def _nomos_glob_groups() -> list[tuple[str, list]]:
    from okg.substrate.library.sources.base import EdgeFact, NodeFact

    def node(node_id: str, subtype: str, attrs: dict) -> NodeFact:
        return NodeFact(
            node_id=node_id, subtype=subtype, attrs=attrs,
            source_record_id={"id": node_id}, source_revision={"v": 1},
        )

    def edge(src: str, dst: str) -> EdgeFact:
        return EdgeFact(
            src=src, dst=dst, edge_type="contains",
            source_record_id={"id": f"{src}|contains|{dst}"}, source_revision={"v": 1},
        )

    visible = [
        node("doc:vis:ex", "doc", {"path": "tests/fixtures/a.py"}),
        node("doc:vis:kept", "doc", {"path": "src/a.py"}),
        node("chunk:visex", "chunk", {"text": f"{EX_TERM} visible excluded"}),
        node("chunk:viskept", "chunk", {"text": f"{EX_TERM} visible kept"}),
        node("chunk:hidrec", "chunk", {"text": f"{EX_TERM} hidden record"}),
        node("chunk:hidedge", "chunk", {"text": f"{EX_TERM} hidden edge"}),
        node("chunk:masked", "chunk", {"text": f"{EX_TERM} masked record"}),
        edge("doc:vis:ex", "chunk:visex"),
        edge("doc:vis:kept", "chunk:viskept"),
    ]
    hidden = [
        node("doc:hid:ex", "doc", {"path": "tests/fixtures/b.py"}),
        edge("doc:hid:ex", "chunk:hidrec"),
        # A visible excluded record reached through a hidden edge.
        edge("doc:vis:ex", "chunk:hidedge"),
    ]
    masked = [
        node("doc:msk", "doc", {"path": "src/masked.py"}),
        edge("doc:msk", "chunk:masked"),
    ]
    return [(VIS, visible), (HID, hidden), (MSK, masked)]


def test_under_access_rules_hidden_records_decide_nothing_and_masked_paths_fail_closed(
    clean_runtime: str, tmp_path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Nomos `enforce`. A record the reader may not see, or one reached only
    through an edge the reader may not see, decides nothing: its passage is
    judged on its own path and served, so a hidden record's path is never
    revealed. A record whose path the reader reads masked cannot be judged:
    its passage is left out (fail closed). A visible excluded record leaves
    its passage out. The count matches the rows served, in grep and lexical,
    and the reply says the rule ran after the read."""
    from tests.substrate.mcp.test_search_records import READER, _activate, _allow

    deployment = make_search_deployment(tmp_path, dsn=clean_runtime)
    publish_nodes(
        clean_runtime, deployment, subtypes=SUBTYPES, narrowings=NARROWINGS,
        nodes=[], node_groups=_nomos_glob_groups(),
    )
    _activate(clean_runtime, deployment, [_allow(VIS), {
        "rule_id": "redact_msk", "effect": "redact", "actions": ["graph.read.mcp"],
        "subjects": [READER], "match": {"source_id": MSK}, "priority": 100,
        "obligations": [{"type": "redaction"}, {"type": "audit"}],
    }])
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        conn.execute(
            "UPDATE okg.subtypes SET metadata = coalesce(metadata, '{}'::jsonb)"
            " || '{\"pii_classes\": {\"path\": \"free_text\"}}'::jsonb"
            " WHERE name = 'doc'"
        )
    monkeypatch.setenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", "1")
    expected = {"chunk:viskept", "chunk:hidrec", "chunk:hidedge"}

    def page(**kwargs) -> dict:
        with psycopg.connect(clean_runtime, autocommit=True) as conn:
            session = open_session(conn, deployment_name=deployment, nomos_subject=READER)
            scope = resolve_scope(
                conn, catalog_version_id=session.catalog_version_id, config=GLOB_CONFIG,
                requested_profile="no_tests", deployment_name=session.deployment_name,
            )
            return search(conn, session, EX_TERM, scope=scope, **{"limit": 50, **kwargs})

    grep = page(method="grep")
    assert set(_ids(grep)) == expected, _ids(grep)
    # Counts may be withheld under access rules; when given, they match.
    assert grep["total_count"] in (None, len(expected)), grep["total_count"]
    assert grep["excluded_path_globs_application"] == "post_fetch_residual"
    lexical = page(method="lexical", rank_by="self")
    assert set(_ids(lexical)) == expected, _ids(lexical)
    assert lexical["excluded_path_globs_application"] == "post_fetch_residual"
    assert "tests/fixtures/b.py" not in str(grep) + str(lexical)


@pytest.mark.parametrize("regime", ["enforce", "audit"])
def test_under_access_rules_many_excluded_passages_never_leave_a_false_page(
    clean_runtime: str, bulk_globbed: str, monkeypatch: pytest.MonkeyPatch, regime: str,
) -> None:
    """Must-fail on 743bce14f: under access rules the record-path rule runs
    after the read, and 3,000 excluded chunks ranked first filled lexical's
    rank cap: 0-4 rows served with `truncated: false` and `rank_capped:
    false`, and the folded records lost `doc:kept` without saying so. Grep
    continues its scan and serves kept rows. Lexical counts the rows the
    profile dropped toward the cap, ranks once deeper (1,000 rows), and when
    excluded rows still fill it the page says it is cut (`truncated`,
    `rank_capped`), never complete. The policy allows every row, so nothing
    is hidden and only the profile decides."""
    from tests.substrate.mcp.test_search_records import MAIN, READER, _activate, _allow

    _activate(clean_runtime, bulk_globbed, [_allow(MAIN)])
    monkeypatch.setenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", "1" if regime == "enforce" else "audit")

    def page(profile: str, **kwargs) -> dict:
        with psycopg.connect(clean_runtime, autocommit=True) as conn:
            session = open_session(conn, deployment_name=bulk_globbed, nomos_subject=READER)
            scope = resolve_scope(
                conn, catalog_version_id=session.catalog_version_id, config=GLOB_CONFIG,
                requested_profile=profile, deployment_name=session.deployment_name,
            )
            return search(conn, session, EX_TERM, scope=scope, **{"limit": 20, **kwargs})

    grep = page("no_tests", method="grep")
    assert len(_ids(grep)) == 20 and _kept_bulk(_ids(grep)), _ids(grep)[:5]
    assert grep["truncated"] is True
    assert grep["excluded_path_globs_application"] == "post_fetch_residual"
    lexical = page("no_tests", method="lexical", rank_by="self")
    assert _kept_bulk(_ids(lexical))
    assert lexical["truncated"] is True and lexical["rank_capped"] is True
    assert lexical["excluded_path_globs_application"] == "post_fetch_residual"
    for kwargs in ({"method": "lexical"}, {"method": "grep"}):
        got = page("records_no_tests", **kwargs)
        ids = _ids(got)
        assert EXCLUDED_DOC not in ids and not any(i.startswith("chunk:a:") for i in ids)
        if KEPT_DOC not in ids and not any(i.startswith("chunk:b:") for i in ids):
            # The kept record is past the cap: the page must say it is cut.
            assert got["truncated"] is True, (kwargs, ids)
