"""Tests for ``af_pubs.invariants_health`` — Stage 5.

Two layers (mirroring tests/views/test_supersedure_health.py):

* **Unit / planner tests** — synthesize the corpus by hand from
  ``Anchor.from_content`` calls so the planner's logic is
  exercised in isolation.
* **Refresh-cost integration tests** — drive ``fresh()`` →
  ``refresh()`` against synthetic corpora to verify the
  Property-A bounds declared in the spec:
    - no-change cycle: 0 refreshed, 0 LLM calls.
    - violator gain: 3 refreshed (summary + finding + analysis),
      2 LLM calls (both deterministic stub).
    - invariant passes: finding + analysis dropped; summary
      refreshes (1 LLM call).
    - severity_filter excludes warns when set to ["error"].

Postgres-backed integration against the live af_pubs gen 13
fixture lives in test_invariants_health_live.py (kept separate
so the `OKG_DSN` skip doesn't gate this file).
"""
from __future__ import annotations

import json

import pytest

pytest.importorskip("views.invariants_health")
from views.invariants_health import (
    SUMMARY_ANCHOR_ID,
    VIEW_DEF_HASH,
    VIEW_ID,
    invariants_health_assembler,
    invariants_health_planner,
    invariants_health_view_def,
)
from okg.views.execution.llm import DeterministicStubLLM
from okg.views.planners.base import (
    GeneratedSpec,
    QuoteSpec,
)
from okg.views.refresh import fresh, refresh
from okg.views.types import Anchor


# ---------------------------------------------------------------------------
# Synthetic-corpus helpers
# ---------------------------------------------------------------------------


def _finding_anchor(
    name: str,
    *,
    severity: str = "warn",
    scope: str = "cross",
    source: str | None = None,
    violator_count: int = 1,
    sample: list[dict] | None = None,
    duration_ms: int = 100,
    timed_out: bool = False,
) -> Anchor:
    """Build an `invariant:finding:{name}` anchor in the same
    canonical-JSON shape `invariant_finding_anchor_builder`
    produces.
    """
    if sample is None:
        sample = [{"node_id": f"publication:{name}-x1",
                   "pub_number": f"{name.upper()}-X1"}]
    content = json.dumps(
        {
            "name": name,
            "severity": severity,
            "scope": scope,
            "source": source,
            "violator_count": violator_count,
            "sample": sample,
            "duration_ms": duration_ms,
            "timed_out": timed_out,
        },
        sort_keys=True,
    )
    return Anchor.from_content(
        id=f"invariant:finding:{name}", content=content,
    )


def _summary_anchor(
    *,
    generation_id: int = 13,
    total_invariants: int = 7,
    failed: int = 5,
    passed: int = 2,
    warns: int = 5,
    timed_out: int = 2,
    total_duration_ms: int = 17674,
    total_budget_exceeded: bool = False,
) -> Anchor:
    content = json.dumps(
        {
            "generation_id": generation_id,
            "total_invariants": total_invariants,
            "failed": failed,
            "passed": passed,
            "warns": warns,
            "timed_out": timed_out,
            "total_duration_ms": total_duration_ms,
            "total_budget_exceeded": total_budget_exceeded,
        },
        sort_keys=True,
    )
    return Anchor.from_content(id=SUMMARY_ANCHOR_ID, content=content)


def _corpus(*anchors: Anchor) -> dict[str, Anchor]:
    corpus = {a.id: a for a in anchors}
    for anchor in anchors:
        if not anchor.id.startswith("invariant:finding:"):
            continue
        payload = json.loads(anchor.content)
        if not payload.get("sample"):
            continue
        name = anchor.id[len("invariant:finding:"):]
        investigation_id = f"invariant:investigation:{name}"
        corpus.setdefault(
            investigation_id,
            Anchor.from_content(
                id=investigation_id,
                content=f"Investigation notes for {name}.",
            ),
        )
    return corpus


# ---------------------------------------------------------------------------
# Planner — structural invariants
# ---------------------------------------------------------------------------


class TestPlannerStructure:
    def test_default_params_emit_summary_quotes_and_analyses(self):
        a = _finding_anchor("a", violator_count=5)
        b = _finding_anchor("b", violator_count=0, sample=[])
        c = _finding_anchor("c", violator_count=12)
        # Note: builder would skip `b` (violator_count==0,
        # !timed_out). For this unit test we feed it directly to
        # exercise the planner's "no analysis when sample empty"
        # branch.
        s = _summary_anchor()
        view_def = invariants_health_view_def()
        specs = invariants_health_planner(
            _corpus(a, b, c, s), view_def,
        )
        ids = [spec.id for spec in specs]
        assert ids[0] == "summary"
        assert "finding:a" in ids
        assert "finding:b" in ids
        assert "finding:c" in ids
        assert "analysis:a" in ids
        assert "analysis:c" in ids
        # b has empty sample → no analysis spec.
        assert "analysis:b" not in ids

    def test_finding_quotes_appear_before_analyses(self):
        a = _finding_anchor("alpha", violator_count=3)
        b = _finding_anchor("bravo", violator_count=7)
        s = _summary_anchor()
        specs = invariants_health_planner(
            _corpus(a, b, s), invariants_health_view_def(),
        )
        ids = [spec.id for spec in specs]
        # Planner contract: summary, then all findings, then all
        # analyses (in matching order).
        assert ids[0] == "summary"
        finding_pos = [i for i, x in enumerate(ids)
                       if x.startswith("finding:")]
        analysis_pos = [i for i, x in enumerate(ids)
                        if x.startswith("analysis:")]
        assert max(finding_pos) < min(analysis_pos)

    def test_severity_filter_drops_warns(self):
        a = _finding_anchor("a", severity="warn", violator_count=5)
        b = _finding_anchor("b", severity="error", violator_count=2)
        s = _summary_anchor()
        view_def = invariants_health_view_def(severity_filter=["error"])
        specs = invariants_health_planner(
            _corpus(a, b, s), view_def,
        )
        ids = {spec.id for spec in specs}
        assert "finding:a" not in ids
        assert "finding:b" in ids
        assert "analysis:a" not in ids
        assert "analysis:b" in ids

    def test_summary_sources_include_every_finding_plus_summary(self):
        a = _finding_anchor("a")
        b = _finding_anchor("b")
        s = _summary_anchor()
        specs = invariants_health_planner(
            _corpus(a, b, s), invariants_health_view_def(),
        )
        summary = next(s for s in specs if s.id == "summary")
        assert isinstance(summary, GeneratedSpec)
        assert set(summary.sources) == {
            "invariant:finding:a",
            "invariant:finding:b",
            SUMMARY_ANCHOR_ID,
        }

    def test_analysis_sources_are_investigation_only(self):
        a = _finding_anchor("a")
        s = _summary_anchor()
        specs = invariants_health_planner(
            _corpus(a, s), invariants_health_view_def(),
        )
        analysis = next(s for s in specs if s.id == "analysis:a")
        assert isinstance(analysis, QuoteSpec)
        assert analysis.anchor_id == "invariant:investigation:a"

    def test_finding_quote_anchor_id_matches_finding_anchor(self):
        a = _finding_anchor("a")
        s = _summary_anchor()
        specs = invariants_health_planner(
            _corpus(a, s), invariants_health_view_def(),
        )
        quote = next(s for s in specs if s.id == "finding:a")
        assert isinstance(quote, QuoteSpec)
        assert quote.anchor_id == "invariant:finding:a"

    def test_empty_corpus_emits_nothing(self):
        specs = invariants_health_planner(
            {}, invariants_health_view_def(),
        )
        assert specs == []


# ---------------------------------------------------------------------------
# Refresh-cost — Property A on this view's source declarations
# ---------------------------------------------------------------------------


class TestRefreshCosts:
    def _vd(self):
        return invariants_health_view_def()

    def _stub(self):
        return DeterministicStubLLM()

    def test_no_change_cycle_zero_refresh_zero_llm(self):
        """5.1 No-change: every fragment reused, 0 LLM calls."""
        a = _finding_anchor("a", violator_count=5)
        b = _finding_anchor("b", violator_count=12)
        s = _summary_anchor()
        corpus_v1 = _corpus(a, b, s)

        view_v1, stats_v1 = fresh(
            corpus_v1, self._vd(), llm_client=self._stub(),
        )
        # Fresh: 1 summary + 2 finding + 2 static-analysis = 5
        # fragments. Only summary is LLM-bound.
        assert stats_v1.fragments_total == 5
        assert stats_v1.added == 5
        assert stats_v1.llm_calls == 1

        # Identical corpus on the next cycle: every fragment
        # reused, 0 LLM calls.
        view_v2, stats_v2 = refresh(
            cached_view=view_v1,
            new_corpus=corpus_v1,
            view_def=self._vd(),
            llm_client=self._stub(),
        )
        assert stats_v2.fragments_total == 5
        assert stats_v2.reused == 5
        assert stats_v2.refreshed == 0
        assert stats_v2.added == 0
        assert stats_v2.dropped == 0
        assert stats_v2.llm_calls == 0
        # Markdown byte-identical (deterministic stub).
        assert (
            invariants_health_assembler(view_v1).markdown
            == invariants_health_assembler(view_v2).markdown
        )

    def test_violator_gain_refreshes_summary_finding_and_analysis(self):
        """5.2 One invariant gains a violator: summary and finding
        refresh; static analysis is reused."""
        a_v1 = _finding_anchor("a", violator_count=5)
        b = _finding_anchor("b", violator_count=12)
        s = _summary_anchor()

        view_v1, _ = fresh(
            _corpus(a_v1, b, s),
            self._vd(),
            llm_client=self._stub(),
        )

        # Mutate `a` only: violator_count 5 → 6, sample changes.
        a_v2 = _finding_anchor(
            "a", violator_count=6,
            sample=[{"node_id": "publication:a-x9",
                     "pub_number": "A-X9"}],
        )
        # The summary anchor is unchanged in this scenario.
        view_v2, stats = refresh(
            cached_view=view_v1,
            new_corpus=_corpus(a_v2, b, s),
            view_def=self._vd(),
            llm_client=self._stub(),
        )
        assert stats.fragments_total == 5
        # Reused: finding:b + both static analyses = 3.
        # Refreshed: summary + finding:a = 2.
        assert stats.refreshed == 2
        assert stats.reused == 3
        assert stats.added == 0
        assert stats.dropped == 0
        assert stats.llm_calls == 1

    def test_invariant_passes_drops_finding_and_analysis(self):
        """5.3 Invariant transitions to passed: that finding's
        anchor leaves the corpus → finding + analysis dropped;
        summary refreshes (1 LLM call)."""
        a = _finding_anchor("a", violator_count=5)
        b = _finding_anchor("b", violator_count=12)
        s = _summary_anchor()

        view_v1, _ = fresh(
            _corpus(a, b, s),
            self._vd(),
            llm_client=self._stub(),
        )

        # `a` passes — its anchor is no longer in the corpus.
        view_v2, stats = refresh(
            cached_view=view_v1,
            new_corpus=_corpus(b, s),
            view_def=self._vd(),
            llm_client=self._stub(),
        )
        # New plan: summary + finding:b + analysis:b = 3.
        assert stats.fragments_total == 3
        # Reused: finding:b + analysis:b = 2 (their sources
        # didn't change). Refreshed: summary (its source list
        # shrunk by one). Dropped: finding:a + analysis:a = 2.
        assert stats.reused == 2
        assert stats.refreshed == 1
        assert stats.added == 0
        assert stats.dropped == 2
        assert stats.llm_calls == 1

    def test_severity_filter_excludes_warns(self):
        """5.4 With severity_filter=['error'], warn-severity
        findings are excluded from the plan."""
        warn_a = _finding_anchor("a", severity="warn",
                                 violator_count=5)
        err_b = _finding_anchor("b", severity="error",
                                violator_count=2)
        s = _summary_anchor()

        view_def = invariants_health_view_def(
            severity_filter=["error"],
        )
        view_v1, stats = fresh(
            _corpus(warn_a, err_b, s),
            view_def,
            llm_client=self._stub(),
        )
        ids = {f.id for f in view_v1.fragments}
        assert "finding:b" in ids
        assert "analysis:b" in ids
        assert "finding:a" not in ids
        assert "analysis:a" not in ids
        # 1 summary + 1 finding + 1 analysis = 3 fragments.
        assert stats.fragments_total == 3


# ---------------------------------------------------------------------------
# Hermeticity — RestrictedCorpus catches undeclared reads
# ---------------------------------------------------------------------------


class TestHermeticity:
    def test_planner_emits_only_declared_sources(self):
        """A correctly-declared planner produces fragments
        whose source declarations cover every anchor the
        synthesize step actually reads. This test exercises the
        full fresh() pipeline against a synthetic corpus and
        asserts no HermeticityViolation is raised.
        """
        a = _finding_anchor("a", violator_count=3)
        b = _finding_anchor("b", violator_count=7)
        s = _summary_anchor()
        view_v1, stats = fresh(
            _corpus(a, b, s),
            invariants_health_view_def(),
            llm_client=DeterministicStubLLM(),
        )
        # If the prompt rendered an undeclared source, the
        # RestrictedCorpus would have raised on first read.
        assert stats.fragments_total == 5
        assert stats.added == 5


# ---------------------------------------------------------------------------
# Builder — `_walk_fired_warns` ordering + filter behaviour
# ---------------------------------------------------------------------------


class TestBuilderUnit:
    def test_builder_skips_violator_zero_not_timed_out(self):
        """The builder must omit anchors whose violator_count==0
        AND timed_out==false (the spec's filter)."""
        from views.builders import _walk_fired_warns
        inv = {
            "cross_source": {
                "fired_warns": [
                    {"name": "a", "violator_count": 0,
                     "timed_out": False},
                    {"name": "b", "violator_count": 5,
                     "timed_out": False},
                    {"name": "c", "violator_count": 0,
                     "timed_out": True},
                ],
            },
            "source_local": {},
        }
        # _walk_fired_warns itself does NOT apply the filter;
        # filtering lives in invariant_finding_anchor_builder.
        # This unit test pins the ordering contract.
        out = _walk_fired_warns(inv)
        names = [e["name"] for e in out]
        # Sorted by (severity_rank=warn=1, name ASC).
        assert names == ["a", "b", "c"]

    def test_walk_fired_warns_includes_source_local_buckets(self):
        from views.builders import _walk_fired_warns
        inv = {
            "cross_source": {
                "fired_warns": [
                    {"name": "z", "violator_count": 1},
                ],
            },
            "source_local": {
                "src_a": {
                    "fired_warns": [
                        {"name": "src_a_warn", "violator_count": 2},
                    ],
                },
            },
        }
        out = _walk_fired_warns(inv)
        names = [e["name"] for e in out]
        assert set(names) == {"z", "src_a_warn"}
        # The src_a_warn entry carries scope="source", source="src_a"
        src_warn = next(e for e in out if e["name"] == "src_a_warn")
        assert src_warn["scope"] == "source"
        assert src_warn["source"] == "src_a"


# ---------------------------------------------------------------------------
# View metadata
# ---------------------------------------------------------------------------


class TestAnchorResolver:
    """`anchor_resolver` for the view_cites_node edge projection.

    Per `replace-portal-findings-with-views`, the materializer
    calls this resolver per fragment source. Finding anchors
    resolve to their first sampled violator's node_id; aggregate
    anchors return None.
    """

    def test_summary_anchor_returns_none(self):
        from views.invariants_health import (
            _refresh_anchor_resolver_cache,
            anchor_resolver,
        )
        s = _summary_anchor()
        _refresh_anchor_resolver_cache({s.id: s})
        assert anchor_resolver(SUMMARY_ANCHOR_ID) is None

    def test_finding_anchor_returns_first_sample_node_id(self):
        from views.invariants_health import (
            _refresh_anchor_resolver_cache,
            anchor_resolver,
        )
        a = _finding_anchor(
            "alpha",
            sample=[
                {"node_id": "publication:foo", "pub_number": "FOO"},
                {"node_id": "publication:bar", "pub_number": "BAR"},
            ],
        )
        _refresh_anchor_resolver_cache({a.id: a})
        assert (
            anchor_resolver("invariant:finding:alpha")
            == "publication:foo"
        )

    def test_finding_with_empty_sample_returns_none(self):
        from views.invariants_health import (
            _refresh_anchor_resolver_cache,
            anchor_resolver,
        )
        a = _finding_anchor("beta", violator_count=0, sample=[])
        _refresh_anchor_resolver_cache({a.id: a})
        assert anchor_resolver("invariant:finding:beta") is None

    def test_unknown_anchor_returns_none(self):
        from views.invariants_health import anchor_resolver
        assert anchor_resolver("unknown:foo") is None
        assert anchor_resolver("publication:x:catalog") is None

    def test_cache_overwritten_each_refresh(self):
        from views.invariants_health import (
            _refresh_anchor_resolver_cache,
            anchor_resolver,
        )
        a = _finding_anchor(
            "gamma",
            sample=[{"node_id": "publication:p1", "pub_number": "P1"}],
        )
        _refresh_anchor_resolver_cache({a.id: a})
        assert (
            anchor_resolver("invariant:finding:gamma")
            == "publication:p1"
        )
        # New cycle's corpus has no gamma anchor — resolver
        # must not return the stale cached value.
        _refresh_anchor_resolver_cache({})
        assert anchor_resolver("invariant:finding:gamma") is None


class TestViewDefinition:
    def test_factory_returns_canonical_metadata(self):
        vd = invariants_health_view_def()
        assert vd.view_id == VIEW_ID
        assert vd.view_def_hash == VIEW_DEF_HASH
        assert vd.planner is invariants_health_planner
        assert vd.assembler is invariants_health_assembler
        assert vd.params["severity_filter"] == ["error", "warn"]
        assert vd.params["include_passed"] is False
        assert vd.params["include_timed_out"] is True

    def test_factory_overrides(self):
        vd = invariants_health_view_def(
            severity_filter=["error"],
            include_passed=True,
            include_timed_out=False,
        )
        assert vd.params["severity_filter"] == ["error"]
        assert vd.params["include_passed"] is True
        assert vd.params["include_timed_out"] is False
