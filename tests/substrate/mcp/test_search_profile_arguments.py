"""No search method may accept a profile and quietly discard it (issue #797).

Pure argument validation — no connection, no manifest — so these run in the
`fast` lane and do not need the MCP server's role-aware pool.

The behaviour under test is a refusal. Accepting a profile on a method that
cannot honour a subtype scope, and returning an unscoped page, would hand the
caller exactly the bookkeeping-heavy result the profile exists to prevent while
letting them believe it had been scoped.
"""
from __future__ import annotations

from unittest import mock

import pytest

from okg.substrate.mcp.search_profiles import (
    APPLICATION_RANKED_SQL,
    APPLICATION_RESIDUAL,
    DIVERSITY_APPLICABLE_METHODS,
    FAMILY_APPLICABLE_METHODS,
    FAMILY_DISCLOSURE_KEYS,
    PROFILE_APPLICABLE_METHODS,
    DiversityBoundInvalid,
    ProfileNotApplicable,
    ResolvedScope,
    SourceFamilyNotApplicable,
    UnknownSourceFamily,
    assert_method_can_filter_family,
    assert_method_can_scope,
    family_disclosure,
    normalize_requested_families,
    restrict_scope_to_method,
    validate_max_per_parent,
)


@pytest.mark.parametrize("method", PROFILE_APPLICABLE_METHODS)
def test_methods_that_return_node_rows_accept_a_profile(method: str) -> None:
    assert_method_can_scope(method, "analyst")


def test_inapplicable_method_refuses_a_profile_and_names_the_supported_ones(
) -> None:
    with pytest.raises(ProfileNotApplicable) as exc:
        assert_method_can_scope("identifier_mentions", "analyst")
    message = str(exc.value)
    assert "identifier_mentions" in message
    for method in PROFILE_APPLICABLE_METHODS:
        assert method in message
    assert exc.value.data["supported_methods"] == list(
        PROFILE_APPLICABLE_METHODS
    )


def test_no_profile_named_is_always_allowed() -> None:
    """The deployment default must not turn into a hard error everywhere.

    A caller who named no profile is fine on any method; the envelope reports
    `profile_applicable: false` for the methods that cannot scope, so the
    non-application is a stated state rather than an inference.
    """
    assert_method_can_scope("identifier_mentions", None)


def test_a_broken_manifest_never_degrades_to_an_unscoped_search() -> None:
    """Only "no such deployment" may become UNSCOPED. Everything else raises.

    The sharp case is self-inflicted: a malformed `search:` block raises
    `DeploymentConfigError` at manifest load, so a surface that swallowed every
    exception would answer a typo'd `default_profle:` by silently running every
    search unscoped — defeating the load-time strictness that exists to catch
    it, and handing back exactly the bookkeeping-heavy page issue #797 reports.
    """
    from okg.substrate.cli import _read_side
    from okg.substrate.deployments import (
        DeploymentConfigError,
        DeploymentResolutionError,
    )
    from okg.substrate.mcp import server as mcp_server
    from okg.substrate.mcp.search_profiles import UNSCOPED

    surfaces = (
        lambda **kw: _read_side._resolve_search_scope(
            None, None, deployment="d", method="lexical", **kw
        ),
        lambda **kw: mcp_server._resolve_mcp_search_scope(
            None, None, deployment_name="d", method="lexical", **kw
        ),
    )

    for resolve in surfaces:
        for raised in (DeploymentConfigError("bad search block"), OSError("EACCES")):
            with mock.patch(
                "okg.substrate.deployments.resolve_deployment",
                side_effect=raised,
            ):
                with pytest.raises(type(raised)):
                    resolve(requested_profile=None, subtype_filter=None)

        # ...while a genuinely absent deployment has no `search:` block to
        # honour, so an unscoped search is the complete answer.
        with mock.patch(
            "okg.substrate.deployments.resolve_deployment",
            side_effect=DeploymentResolutionError("no such deployment"),
        ):
            assert resolve(
                requested_profile=None, subtype_filter=None,
            ) is UNSCOPED
            # But a profile the caller NAMED cannot be satisfied, so it raises.
            with pytest.raises(DeploymentResolutionError):
                resolve(requested_profile="analyst", subtype_filter=None)


def test_the_reserved_all_profile_works_without_a_readable_manifest() -> None:
    """The escape hatch must exist in the situation that needs it.

    `all` is defined as "always resolves to the unscoped search". A broken
    manifest is precisely when an operator reaches for it, so resolving it MUST
    NOT depend on reading that manifest.
    """
    from okg.substrate.cli import _read_side
    from okg.substrate.deployments import DeploymentConfigError
    from okg.substrate.mcp import server as mcp_server
    from okg.substrate.mcp.search_profiles import RESERVED_ALL

    for resolve in (
        lambda: _read_side._resolve_search_scope(
            None, None, deployment="d", requested_profile=RESERVED_ALL,
            subtype_filter=None, method="lexical",
        ),
        lambda: mcp_server._resolve_mcp_search_scope(
            None, None, deployment_name="d", requested_profile=RESERVED_ALL,
            subtype_filter=None, method="lexical",
        ),
    ):
        with mock.patch(
            "okg.substrate.deployments.resolve_deployment",
            side_effect=DeploymentConfigError("bad search block"),
        ):
            scope = resolve()
        assert scope.profile == RESERVED_ALL
        assert scope.subtypes is None


def test_the_attrs_scope_predicate_survives_the_pushdown_renderer(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The scope clause must be an `in` node, not an `or` tree.

    The pushdown renderer DROPS any `or`: `_attr_free_pushdown_clauses` keeps
    only eq/in/exists (the effective-view renderer that had no `or` branch
    either went with the materialized read path, PACT
    lane-s-storage-cutover). An `or` scope clause would therefore vanish
    silently while the envelope still reported `scoped_subtypes` — the
    disclosure would be a lie, on the one method whose scope is not proven
    end-to-end in the Postgres lane.
    """
    from okg.substrate.mcp.server import (
        _PUSHDOWN_SAFE_OPS,
        _profile_scope_clause,
        build_server,
    )

    clause = _profile_scope_clause(["chunk", "entity"])
    assert clause == {
        "op": "in", "field": "subtype", "values": ["chunk", "entity"],
    }

    # Renderer 1: the conjunct filter. Asserting only against
    # `_PUSHDOWN_SAFE_OPS` would be tautological, so check the FIELD is a
    # stable pushdown column too — both must hold for the clause to survive.
    assert clause["op"] in _PUSHDOWN_SAFE_OPS
    assert "or" not in _PUSHDOWN_SAFE_OPS

    # The real renderer, driven through the server's seam, so deleting its
    # `in` case reds this test. No database is contacted -- the builder is
    # pure predicate assembly.
    # A localhost DSN that is never connected to: build_server only derives
    # role DSNs at construction, and it refuses to guess them for a non-local
    # host. Nothing here opens a socket.
    # The start-time capability probe (#3151) is the one build step that
    # needs a database, so it is stubbed with the bare-server fixture.
    from okg.substrate.mcp.capability_probe import CAPABILITY_FIXTURES

    monkeypatch.setattr(
        "okg.substrate.mcp.server.probe_capabilities_from_pool",
        lambda *_args, **_kwargs: CAPABILITY_FIXTURES["none"](),
    )
    server = build_server(
        dsn="postgresql://postgres@127.0.0.1:1/none", deployment_name="d",
    )
    render = server._okg_attr_scan_pushdown_predicate

    assert render(kind="nodes", input={}, predicates=(clause,)) == clause

    # The `or` form means the same thing to a reader and renders to NOTHING.
    # Pinned so nobody reintroduces it.
    assert render(kind="nodes", input={}, predicates=({
        "op": "or",
        "clauses": [
            {"op": "eq", "field": "subtype", "value": "chunk"},
            {"op": "eq", "field": "subtype", "value": "entity"},
        ],
    },)) is None


def test_mcp_search_tool_refuses_a_profile_on_an_inapplicable_method() -> None:
    """The refusal is reachable through the tool's own pre-`_open` validation.

    Proves the wiring, not just the helper: this raises before any connection
    is acquired, which is why it can run without a database at all.
    """
    from okg.substrate.mcp.server import _assert_method_can_scope

    with pytest.raises(ProfileNotApplicable):
        _assert_method_can_scope("identifier_mentions", "analyst")
    _assert_method_can_scope("lexical", "analyst")
    _assert_method_can_scope("identifier_mentions", None)


# ---------- mcp-authority-diversity-budgets (issue #1444, part 2) ----------
#
# Same discipline, applied to the two controls this change adds: a method that
# cannot carry the predicate REFUSES an explicit request rather than accepting
# it and quietly returning an unfiltered page.

@pytest.mark.parametrize("method", FAMILY_APPLICABLE_METHODS)
def test_methods_whose_sql_projects_the_producer_id_accept_a_family(
    method: str,
) -> None:
    assert_method_can_filter_family(method, "documentation")


@pytest.mark.parametrize("method", ["attrs", "identifier_mentions"])
def test_a_method_that_cannot_page_on_the_producer_id_refuses_a_family(
    method: str,
) -> None:
    """`attrs` is the sharp one, and its absence is MEASURED.

    The typed attrs lookup pages in Postgres over the effective-attribute
    relation and only hydrates the producer id afterwards, so a family filter
    there would run after pagination and silently under-fill the page.
    """
    with pytest.raises(SourceFamilyNotApplicable) as exc:
        assert_method_can_filter_family(method, "documentation")
    message = str(exc.value)
    assert method in message
    for supported in FAMILY_APPLICABLE_METHODS:
        assert supported in message
    assert exc.value.data["supported_methods"] == list(
        FAMILY_APPLICABLE_METHODS
    )


def test_no_family_named_is_always_allowed() -> None:
    """A profile-declared family must not become a hard error everywhere.

    The envelope reports `source_family_applicable: false` on those methods, so
    the non-application is a stated state rather than an inference.
    """
    assert assert_method_can_filter_family("attrs", None) is None


def test_the_family_argument_shape_is_validated() -> None:
    assert normalize_requested_families(None) is None
    assert normalize_requested_families("docs") == ("docs",)
    assert normalize_requested_families(["docs", "code", "docs"]) == (
        "docs", "code",
    )
    for bad in (17, {"a": 1}, [], [""], "  "):
        with pytest.raises(UnknownSourceFamily):
            normalize_requested_families(bad)


def test_the_diversity_bound_is_refused_rather_than_coerced() -> None:
    assert validate_max_per_parent(None, origin="x") is None
    assert validate_max_per_parent(4, origin="x") == 4
    for bad in (0, -3, "2", 2.0, True):
        with pytest.raises(DiversityBoundInvalid) as exc:
            validate_max_per_parent(bad, origin="search: max_per_parent")
        assert exc.value.code == "invalid_max_per_parent"


# ---------- #1444 review: per-method applicability and truthful disclosure ----

#: A scope with every control switched on, as a profile would resolve it.
def _loaded_scope() -> ResolvedScope:
    return ResolvedScope(
        profile="authority",
        subtypes=("chunk",),
        source_families=("documentation",),
        source_ids=("source:docs",),
        excluded_path_globs=("tests/**",),
        max_per_parent=2,
        family_validation="producer_profiles",
        declared_family_pairs=(("source:docs", "documentation"),),
    )


ALL_SEARCH_METHODS = (
    "lexical", "semantic", "alias", "attrs", "identifier_mentions",
)


@pytest.mark.parametrize("method", ALL_SEARCH_METHODS)
def test_every_method_discloses_the_same_key_set(method: str) -> None:
    """MAJOR 8. A PARTIAL disclosure is worse than none.

    A payload carrying `source_family_applicable` while omitting
    `excluded_path_globs` reads as "no exclusion was requested" — when in truth
    a profile declared one and this method discarded it.
    """
    keys = set(family_disclosure(_loaded_scope(), method=method))
    assert keys == set(FAMILY_DISCLOSURE_KEYS)


def test_disclosure_names_where_each_predicate_actually_ran() -> None:
    """MAJOR 6/7. "Applied" and "applied in the ranked query" differ.

    A residual runs over the fetched window, so the page can UNDER-FILL. A
    caller told only that the exclusion "applied" reads the short page as
    "that is all there is".
    """
    scope = _loaded_scope()
    lexical = family_disclosure(scope, method="lexical")
    semantic = family_disclosure(scope, method="semantic")
    alias = family_disclosure(scope, method="alias")

    assert lexical["source_family_application"] == APPLICATION_RANKED_SQL
    assert lexical["excluded_path_globs_application"] == APPLICATION_RANKED_SQL
    # The semantic ranking binds the family ids inside the lowered program, but
    # its path exclusion is a residual.
    assert semantic["source_family_application"] == APPLICATION_RANKED_SQL
    assert semantic["excluded_path_globs_application"] == APPLICATION_RESIDUAL
    # `okg.aliases` has no `source` column, so BOTH are residuals there. The
    # earlier comment claiming otherwise was simply wrong.
    assert alias["source_family_application"] == APPLICATION_RESIDUAL
    assert alias["excluded_path_globs_application"] == APPLICATION_RESIDUAL


def test_application_is_null_when_the_control_did_not_apply() -> None:
    """An application label on a control nobody requested would be noise."""
    disclosure = family_disclosure(ResolvedScope(), method="lexical")
    assert disclosure["source_family_application"] is None
    assert disclosure["excluded_path_globs_application"] is None
    assert disclosure["source_families"] is None
    assert disclosure["excluded_path_globs"] is None
    assert disclosure["source_families_declared"] is False


@pytest.mark.parametrize("method", ["attrs", "identifier_mentions"])
def test_dropped_controls_are_reported_inapplicable(method: str) -> None:
    disclosure = family_disclosure(_loaded_scope(), method=method)
    assert disclosure["source_family_applicable"] is False
    assert disclosure["path_exclusion_applicable"] is False
    assert disclosure["diversity_applicable"] is False


@pytest.mark.parametrize("method", ["semantic", "alias"])
def test_diversity_is_inapplicable_off_the_lexical_path(method: str) -> None:
    assert method not in DIVERSITY_APPLICABLE_METHODS
    assert family_disclosure(_loaded_scope(), method=method)[
        "diversity_applicable"
    ] is False


# ---------- BLOCKER 4: a profile default must not hard-fail a whole method ----

@pytest.mark.parametrize("method", ["semantic", "alias"])
def test_a_profile_declared_bound_is_dropped_not_raised(method: str) -> None:
    """The regression this split exists for.

    Before it, a deployment that declared `max_per_parent` on ANY profile made
    every `semantic` and `alias` search under that profile raise
    `diversity_not_applicable` — an error naming a parameter the caller never
    passed, on a profile a `default_profile` may have selected for them.
    """
    restricted = restrict_scope_to_method(_loaded_scope(), method)
    assert restricted.max_per_parent is None
    # The family control is unaffected: these methods CAN honour it.
    assert restricted.source_ids == ("source:docs",)
    assert restricted.excluded_path_globs == ("tests/**",)
    # And the drop is stated, not silent.
    assert family_disclosure(restricted, method=method)[
        "diversity_applicable"
    ] is False


@pytest.mark.parametrize("method", ["attrs", "identifier_mentions"])
def test_every_profile_control_is_dropped_where_none_can_apply(
    method: str,
) -> None:
    restricted = restrict_scope_to_method(_loaded_scope(), method)
    assert restricted.max_per_parent is None
    assert restricted.source_ids is None
    assert restricted.source_families is None
    assert restricted.excluded_path_globs == ()
    assert restricted.family_validation is None
    # The subtype scope and the family MAP survive: the first still applies on
    # `attrs`, and the second only labels rows.
    assert restricted.subtypes == ("chunk",)
    assert restricted.declared_family_pairs is not None


def test_the_lexical_path_keeps_every_control() -> None:
    scope = _loaded_scope()
    assert restrict_scope_to_method(scope, "lexical") == scope


def test_an_explicit_bound_on_a_non_lexical_method_still_raises() -> None:
    """The other half of the split: an explicit request is a stated belief."""
    from okg.substrate.mcp.result_diversity import (
        DiversityNotApplicable,
        assert_diversity_rank_mode,
    )

    for method in ("semantic", "alias"):
        with pytest.raises(DiversityNotApplicable) as exc:
            assert_diversity_rank_mode(2, rank_by="self", method=method)
        assert exc.value.code == "diversity_not_applicable"
        assert method in str(exc.value)
    # And on the lexical path it is accepted.
    assert assert_diversity_rank_mode(2, rank_by="self", method="lexical") is None


# The lexical supplement's graph read (the only read path since PACT
# lane-s-storage-cutover) must get every argument its caller takes. In S2 it
# once returned before it used `reader_sql` (hidden rows filled its 200-row
# cap) and `budget_ms` (the literal-read budget never applied). The check is
# the caller's whole signature: every parameter must reach
# `_graph_lexical_candidates` unchanged, unless it is listed here with the
# reason. A new caller parameter fails the test until someone classifies it.
_CALLER_ONLY = {"conn", "session"}  # the connection and session: passed positionally
#: caller parameter -> why it does not arrive under its own name.
_NOT_FORWARDED_BY_NAME = {
    # split into `terms` (the words of the query)
    "query": "terms",
    # given whole as `filter`; its `attr_filters` is also passed as `attr_filters`
    "filter": "filter and attr_filters",
}
#: The two arguments the S2 defect dropped, named so their removal from the
#: caller fails too.
_GUARDED = ("reader_sql", "budget_ms")


def _caller_parameters() -> list[str]:
    import importlib
    import inspect

    search_mod = importlib.import_module("okg.substrate.mcp.search")
    caller = inspect.signature(search_mod._effective_view_lexical_candidates).parameters
    return [name for name in caller if name not in _CALLER_ONLY]


def test_graph_lexical_candidates_accepts_every_forwarded_argument():
    import importlib
    import inspect

    search_mod = importlib.import_module("okg.substrate.mcp.search")

    graph = inspect.signature(search_mod._graph_lexical_candidates).parameters
    assert set(_GUARDED) <= set(_caller_parameters())
    for name in _caller_parameters():
        if name in _NOT_FORWARDED_BY_NAME:
            continue
        assert name in graph, f"graph branch does not accept {name}"


def test_graph_lexical_candidates_gets_every_argument_the_caller_takes(monkeypatch):
    import importlib

    search_mod = importlib.import_module("okg.substrate.mcp.search")

    seen: dict = {}

    def fake(conn, session, **kwargs):
        seen.update(kwargs)
        return None

    monkeypatch.setattr(search_mod, "_graph_lexical_candidates", fake)
    forwarded = [n for n in _caller_parameters() if n not in _NOT_FORWARDED_BY_NAME]
    sentinels = {name: object() for name in forwarded}
    attr_filters = {"k": "v"}
    result = search_mod._effective_view_lexical_candidates(
        object(), object(), query="alpha", filter={"attr_filters": attr_filters},
        **sentinels,
    )
    assert result is None
    for name, sentinel in sentinels.items():
        assert name in seen, f"the graph branch is not given {name}"
        assert seen[name] is sentinel, f"{name} changed on the way to the graph branch"
    # The special cases, by what they become.
    assert seen["terms"] == ["alpha"]
    assert seen["attr_filters"] == attr_filters
    assert seen["filter"] == {"attr_filters": attr_filters}
    for name in _GUARDED:
        assert seen[name] is sentinels[name], name
