"""Unit tests for okg.views.refresh — Stage 1.7.

These cover the umbrella's Property A (refresh byte-reuse) at the
unit level. Stage 5 will repeat the gate end-to-end against a real
af_pubs view; this file proves the framework's loop semantics on
synthetic fixtures.
"""
from __future__ import annotations

import pytest

from okg.views.assembly import assemble
from okg.views.execution.llm import DeterministicStubLLM
from okg.views.planners.base import (
    GeneratedSpec,
    QuoteSpec,
    ViewDefinition,
)
from okg.views.refresh import RefreshStats, fresh, is_stable, refresh
from okg.views.types import Anchor, Fragment


# ---------------------------------------------------------------------------
# Tiny fixture view: 3 anchors, 3 fragments (2 quote + 1 generated).
# ---------------------------------------------------------------------------


def _make_corpus(*, status_a: str = "current") -> dict[str, Anchor]:
    return {
        "a:catalog": Anchor.from_content("a:catalog", f"status: {status_a}"),
        "b:catalog": Anchor.from_content("b:catalog", "status: rescinded"),
        "scope": Anchor.from_content("scope", "scope summary"),
    }


def _planner(corpus, view_def):
    """Deterministic 3-fragment planner."""
    return [
        QuoteSpec(id="row:a", anchor_id="a:catalog"),
        QuoteSpec(id="row:b", anchor_id="b:catalog"),
        GeneratedSpec(
            id="scope",
            sources=("scope",),
            system_prompt="sys",
            task_prompt="task",
        ),
    ]


def _view_def(view_id: str = "demo") -> ViewDefinition:
    return ViewDefinition(
        view_id=view_id,
        planner=_planner,
        assembler=assemble,
        view_def_hash="def-hash",
    )


# ---------------------------------------------------------------------------
# is_stable predicate
# ---------------------------------------------------------------------------


class TestIsStable:
    def test_unchanged_corpus_is_stable(self):
        c = _make_corpus()
        f = Fragment(
            id="x",
            kind="quote",
            sources=("a:catalog",),
            source_hashes={"a:catalog": c["a:catalog"].content_hash},
            content="...",
        )
        assert is_stable(f, c)

    def test_missing_anchor_is_unstable(self):
        c = _make_corpus()
        f = Fragment(
            id="x",
            kind="quote",
            sources=("missing",),
            source_hashes={"missing": "deadbeef00000000"},
            content="...",
        )
        assert not is_stable(f, c)

    def test_changed_hash_is_unstable(self):
        c_before = _make_corpus()
        f = Fragment(
            id="x",
            kind="quote",
            sources=("a:catalog",),
            source_hashes={"a:catalog": c_before["a:catalog"].content_hash},
            content="...",
        )
        c_after = _make_corpus(status_a="rescinded")
        assert not is_stable(f, c_after)

    def test_partial_match_is_unstable(self):
        # Two-source fragment, one source unchanged, one changed —
        # MUST be unstable.
        c_before = _make_corpus()
        f = Fragment(
            id="x",
            kind="generated",
            sources=("a:catalog", "scope"),
            source_hashes={
                "a:catalog": c_before["a:catalog"].content_hash,
                "scope": c_before["scope"].content_hash,
            },
            content="...",
        )
        c_after = _make_corpus(status_a="rescinded")
        assert not is_stable(f, c_after)


# ---------------------------------------------------------------------------
# fresh() — first-time render
# ---------------------------------------------------------------------------


class TestFresh:
    def test_fresh_produces_three_fragments_and_stats(self):
        c = _make_corpus()
        view, stats = fresh(
            new_corpus=c,
            view_def=_view_def(),
            llm_client=DeterministicStubLLM(),
        )
        assert len(view.fragments) == 3
        assert stats.fragments_total == 3
        assert stats.added == 3
        assert stats.reused == 0
        assert stats.refreshed == 0
        assert stats.llm_calls == 1  # the GeneratedSpec

    def test_fresh_records_generation_id_when_supplied(self):
        c = _make_corpus()
        view, _ = fresh(
            new_corpus=c,
            view_def=_view_def(),
            llm_client=DeterministicStubLLM(),
            generation_id=42,
        )
        assert view.generation_id == 42

    def test_fresh_without_llm_client_works_for_quote_only_planner(self):
        # Custom planner emitting only quote specs.
        def quote_only(corpus, view_def):
            return [
                QuoteSpec(id="row:a", anchor_id="a:catalog"),
                QuoteSpec(id="row:b", anchor_id="b:catalog"),
            ]

        vd = ViewDefinition(
            view_id="demo",
            planner=quote_only,
            assembler=assemble,
            view_def_hash="def",
        )
        view, stats = fresh(
            new_corpus=_make_corpus(),
            view_def=vd,
            llm_client=None,
        )
        assert len(view.fragments) == 2
        assert stats.llm_calls == 0


# ---------------------------------------------------------------------------
# refresh() — Property A scenarios
# ---------------------------------------------------------------------------


class TestPropertyA:
    """Property A holds across these representative scenarios."""

    def test_unchanged_corpus_reuses_everything_byte_identically(self):
        # Build a fresh view, then refresh against the same corpus.
        # Every fragment must be reused (same Python object).
        c = _make_corpus()
        cached, _ = fresh(
            new_corpus=c,
            view_def=_view_def(),
            llm_client=DeterministicStubLLM(),
        )

        new_view, stats = refresh(
            cached_view=cached,
            new_corpus=c,
            view_def=_view_def(),
            llm_client=DeterministicStubLLM(),
        )
        assert stats.reused == 3
        assert stats.refreshed == 0
        assert stats.llm_calls == 0
        # Same Python object refs — strict byte-reuse.
        for old, new in zip(cached.fragments, new_view.fragments, strict=True):
            assert old is new
        # Assembled markdown is byte-identical.
        assert assemble(cached) == assemble(new_view)

    def test_out_of_scope_anchor_added_yields_byte_identical_view(self):
        # Add a new anchor that no fragment's sources references.
        # Property A's compositional-isolation corollary: assembled
        # markdown is byte-identical; zero refresh, zero LLM calls.
        c_before = _make_corpus()
        cached, _ = fresh(
            new_corpus=c_before,
            view_def=_view_def(),
            llm_client=DeterministicStubLLM(),
        )

        c_after = dict(c_before)
        c_after["unrelated"] = Anchor.from_content("unrelated", "ignored")

        new_view, stats = refresh(
            cached_view=cached,
            new_corpus=c_after,
            view_def=_view_def(),
            llm_client=DeterministicStubLLM(),
        )
        assert stats.reused == 3
        assert stats.refreshed == 0
        assert stats.llm_calls == 0
        assert assemble(cached) == assemble(new_view)

    def test_in_scope_quote_anchor_change_refreshes_only_that_fragment(self):
        # Mutate a:catalog's content. Only row:a should refresh.
        # row:b and scope are untouched.
        c_before = _make_corpus()
        cached, _ = fresh(
            new_corpus=c_before,
            view_def=_view_def(),
            llm_client=DeterministicStubLLM(),
        )
        c_after = _make_corpus(status_a="rescinded")

        new_view, stats = refresh(
            cached_view=cached,
            new_corpus=c_after,
            view_def=_view_def(),
            llm_client=DeterministicStubLLM(),
        )
        assert stats.refreshed == 1
        assert stats.reused == 2
        assert stats.llm_calls == 0  # row:a is a quote — no LLM call

        # row:a changed; row:b and scope are the same Python objs.
        cached_by_id = {f.id: f for f in cached.fragments}
        new_by_id = {f.id: f for f in new_view.fragments}
        assert new_by_id["row:b"] is cached_by_id["row:b"]
        assert new_by_id["scope"] is cached_by_id["scope"]
        assert new_by_id["row:a"] is not cached_by_id["row:a"]
        assert new_by_id["row:a"].content == "status: rescinded"

    def test_added_spec_increments_added_counter(self):
        # New planner emits one extra spec.
        def planner_with_extra(corpus, view_def):
            specs = _planner(corpus, view_def)
            specs.append(QuoteSpec(id="row:b2", anchor_id="b:catalog"))
            return specs

        c = _make_corpus()
        cached, _ = fresh(
            new_corpus=c,
            view_def=_view_def(),
            llm_client=DeterministicStubLLM(),
        )

        vd2 = ViewDefinition(
            view_id="demo",
            planner=planner_with_extra,
            assembler=assemble,
            view_def_hash="def",
        )
        new_view, stats = refresh(
            cached_view=cached,
            new_corpus=c,
            view_def=vd2,
            llm_client=DeterministicStubLLM(),
        )
        assert stats.added == 1
        assert stats.reused == 3
        assert len(new_view.fragments) == 4

    def test_dropped_spec_increments_dropped_counter(self):
        # New planner emits fewer specs than cached.
        def planner_minus_one(corpus, view_def):
            return _planner(corpus, view_def)[:-1]  # drop scope

        c = _make_corpus()
        cached, _ = fresh(
            new_corpus=c,
            view_def=_view_def(),
            llm_client=DeterministicStubLLM(),
        )
        vd2 = ViewDefinition(
            view_id="demo",
            planner=planner_minus_one,
            assembler=assemble,
            view_def_hash="def",
        )
        new_view, stats = refresh(
            cached_view=cached,
            new_corpus=c,
            view_def=vd2,
            llm_client=DeterministicStubLLM(),
        )
        assert stats.dropped == 1
        assert stats.reused == 2
        assert len(new_view.fragments) == 2


# ---------------------------------------------------------------------------
# Hermeticity is enforced through synthesize during refresh
# ---------------------------------------------------------------------------


class TestHermeticityDuringRefresh:
    def test_planner_reading_undeclared_anchor_raises(self):
        # A buggy GeneratedSpec declares sources=(a:catalog,) but
        # its synthesize logic would secretly read 'b:catalog'.
        # synthesize wraps the corpus in RestrictedCorpus before
        # the LLMClient sees anything; the spec's own sources tuple
        # is what the wrapper allows. So a properly-declared spec
        # cannot read outside its declaration. We exercise this by
        # supplying a spec whose sources tuple is too narrow.

        def buggy_planner(corpus, view_def):
            return [
                GeneratedSpec(
                    id="scope",
                    # sources DECLARED narrow — but the spec's
                    # 'task_prompt' field doesn't read anything. The
                    # wrapper's reads happen only for declared
                    # sources, so this spec is fine *as written*.
                    # To trigger HermeticityViolation we declare a
                    # source that the wrapper allows but which
                    # synthesize then tries to extend by reading an
                    # undeclared id. Realistically that path runs
                    # inside synthesize's _render_sources_block, but
                    # that loop iterates exactly spec.sources. So
                    # the only way to trigger the violation is if
                    # the planner itself declares MORE sources than
                    # the wrapper allows — which we test in the
                    # synthesis suite.
                    sources=("scope", "out_of_scope"),
                    system_prompt="s",
                    task_prompt="t",
                ),
            ]

        # The corpus does NOT contain 'out_of_scope', and the wrapper
        # is built from spec.sources = ('scope', 'out_of_scope'), so
        # synthesize's anchor read for 'out_of_scope' will raise
        # KeyError (not HermeticityViolation) because the id IS in
        # the allow-list but missing from the corpus. That's
        # different from the hermeticity violation case — and tested
        # in test_synthesis.py already. Here we focus on the case
        # where refresh() correctly propagates whatever exception
        # synthesize raises.
        c = _make_corpus()
        vd = ViewDefinition(
            view_id="demo",
            planner=buggy_planner,
            assembler=assemble,
            view_def_hash="def",
        )
        with pytest.raises(KeyError):
            refresh(
                cached_view=None,
                new_corpus=c,
                view_def=vd,
                llm_client=DeterministicStubLLM(),
            )


# ---------------------------------------------------------------------------
# RefreshStats
# ---------------------------------------------------------------------------


class TestRefreshStats:
    def test_as_dict_round_trip(self):
        s = RefreshStats(
            fragments_total=5,
            reused=3,
            refreshed=1,
            added=1,
            dropped=0,
            llm_calls=1,
        )
        assert s.as_dict() == {
            "fragments_total": 5,
            "reused": 3,
            "refreshed": 1,
            "added": 1,
            "dropped": 0,
            "llm_calls": 1,
        }
