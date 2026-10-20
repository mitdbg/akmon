"""Tests for `af_pubs.recent_changes` — Stage 5 of
add-recent-changes-view.

Two layers:

* Unit / planner tests with synthetic corpora — the
  framework's `fresh()` and `refresh()` exercise the full
  pipeline against `DeterministicStubLLM`.
* Spec-bound refresh-cost scenarios:
    - no-change cycle: 0 refreshed, 0 LLM
    - single-bucket landing: ≤3 refreshed
      (summary + bucket quote + narrative), ≤2 LLM calls
    - non-listed-subtype landing: ≤2 refreshed
      (summary + bucket quote — no narrative), ≤1 LLM call
    - new generation published: ≤2 refreshed
      (summary + generations:table), ≤1 LLM call
"""
from __future__ import annotations

import json

import pytest

pytest.importorskip("views.recent_changes")
from views.recent_changes import (
    VIEW_DEF_HASH,
    VIEW_ID,
    anchor_resolver,
    recent_changes_assembler,
    recent_changes_planner,
    recent_changes_view_def,
)
from okg.views.execution.llm import DeterministicStubLLM
from okg.views.planners.base import GeneratedSpec, QuoteSpec
from okg.views.refresh import fresh, refresh
from okg.views.types import Anchor


# ---------------------------------------------------------------------------
# Synthetic-corpus helpers
# ---------------------------------------------------------------------------


def _bucket_anchor(
    subtype: str,
    op: str,
    *,
    count: int = 1,
    samples: list[dict] | None = None,
) -> Anchor:
    if samples is None:
        samples = [{
            "node_id": f"{subtype}:x1",
            "observed_at": "2026-04-27T00:00:00Z",
            "source": "af_pubs.catalog",
            "attrs_summary": {"pub_number": "X1"},
        }]
    content = json.dumps(
        {
            "subtype": subtype,
            "op": op,
            "count": count,
            "samples": samples,
        },
        sort_keys=True,
    )
    return Anchor.from_content(
        id=f"change:bucket:{subtype}:{op}", content=content,
    )


def _gen_anchor(generation_id: int) -> Anchor:
    content = json.dumps(
        {
            "generation_id": generation_id,
            "status": "published",
            "published_at": f"2026-04-{20 + generation_id:02d}T00:00:00Z",
            "fact_watermark": 100 * generation_id,
            "invariant_summary": {},
            "blocked_reason": {},
        },
        sort_keys=True,
    )
    return Anchor.from_content(
        id=f"change:gen:{generation_id}", content=content,
    )


def _gens_table_anchor(generation_ids: list[int]) -> Anchor:
    rows = [
        {
            "generation_id": g,
            "status": "published",
            "published_at": f"2026-04-{20 + g:02d}T00:00:00Z",
            "blocked_reason": {},
        }
        for g in generation_ids
    ]
    content = json.dumps({"rows": rows}, sort_keys=True)
    return Anchor.from_content(
        id="change:generations:table", content=content,
    )


def _corpus(*anchors: Anchor) -> dict[str, Anchor]:
    return {a.id: a for a in anchors}


# ---------------------------------------------------------------------------
# Planner structure
# ---------------------------------------------------------------------------


class TestPlanner:
    def test_default_params_emit_summary_quotes_narratives_and_table(
        self,
    ):
        """Default config: 2 buckets in `bucket_subtypes` plus
        a 3rd bucket of a non-listed subtype + 2 generations →
        1 summary + 3 quotes + 2 narratives + 1 generations
        table = 7 specs.
        """
        a = _bucket_anchor("publication", "I", count=12)
        b = _bucket_anchor("publication_family", "I", count=3)
        c = _bucket_anchor("section", "I", count=8)  # not listed
        g1 = _gen_anchor(13)
        g2 = _gen_anchor(14)
        gt = _gens_table_anchor([13, 14])

        view_def = recent_changes_view_def()
        specs = recent_changes_planner(
            _corpus(a, b, c, g1, g2, gt), view_def,
        )
        ids = [s.id for s in specs]
        assert ids[0] == "summary"
        assert "bucket:publication:I" in ids
        assert "bucket:publication_family:I" in ids
        assert "bucket:section:I" in ids
        # Narratives only for bucket_subtypes members.
        assert "narrative:publication:I" in ids
        assert "narrative:publication_family:I" in ids
        assert "narrative:section:I" not in ids
        assert "generations:table" in ids
        assert ids[-1] == "generations:table"

    def test_summary_sources_include_every_bucket_and_gen_anchor(self):
        a = _bucket_anchor("publication", "I", count=5)
        g = _gen_anchor(13)
        view_def = recent_changes_view_def()
        specs = recent_changes_planner(_corpus(a, g), view_def)
        summary = next(s for s in specs if s.id == "summary")
        assert isinstance(summary, GeneratedSpec)
        assert set(summary.sources) == {
            "change:bucket:publication:I",
            "change:gen:13",
        }

    def test_narrative_sources_are_bucket_only(self):
        a = _bucket_anchor("publication", "I", count=5)
        view_def = recent_changes_view_def()
        specs = recent_changes_planner(_corpus(a), view_def)
        narrative = next(
            s for s in specs if s.id == "narrative:publication:I"
        )
        assert isinstance(narrative, GeneratedSpec)
        assert narrative.sources == ("change:bucket:publication:I",)

    def test_quote_anchor_id_matches_bucket_anchor(self):
        a = _bucket_anchor("publication", "I", count=5)
        view_def = recent_changes_view_def()
        specs = recent_changes_planner(_corpus(a), view_def)
        quote = next(s for s in specs if s.id == "bucket:publication:I")
        assert isinstance(quote, QuoteSpec)
        assert quote.anchor_id == "change:bucket:publication:I"

    def test_empty_corpus_emits_nothing(self):
        specs = recent_changes_planner({}, recent_changes_view_def())
        assert specs == []


# ---------------------------------------------------------------------------
# Refresh-cost — Property A on this view's source declarations
# ---------------------------------------------------------------------------


class TestRefreshCosts:
    def _stub(self):
        return DeterministicStubLLM()

    def _vd(self):
        return recent_changes_view_def()

    def _baseline_corpus(self) -> dict[str, Anchor]:
        return _corpus(
            _bucket_anchor("publication", "I", count=10),
            _bucket_anchor("publication_family", "I", count=2),
            _gen_anchor(13),
            _gens_table_anchor([13]),
        )

    def test_no_change_cycle_zero_refresh_zero_llm(self):
        """Spec scenario: no-change cycle reuses every fragment."""
        corpus_v1 = self._baseline_corpus()
        view_v1, stats_v1 = fresh(
            corpus_v1, self._vd(), llm_client=self._stub(),
        )
        # 1 summary + 2 buckets + 2 narratives + 1 generations
        # table = 6 specs. 3 are LLM-bound (summary + 2
        # narratives).
        assert stats_v1.fragments_total == 6
        assert stats_v1.added == 6
        assert stats_v1.llm_calls == 3

        view_v2, stats_v2 = refresh(
            cached_view=view_v1,
            new_corpus=self._baseline_corpus(),
            view_def=self._vd(),
            llm_client=self._stub(),
        )
        assert stats_v2.fragments_total == 6
        assert stats_v2.reused == 6
        assert stats_v2.refreshed == 0
        assert stats_v2.added == 0
        assert stats_v2.llm_calls == 0

    def test_single_bucket_landing_refreshes_summary_quote_narrative(
        self,
    ):
        """Spec scenario: single-bucket landing on a listed
        subtype → 3 refreshed (summary + bucket quote +
        narrative), 2 LLM calls (summary + narrative).
        """
        corpus_v1 = self._baseline_corpus()
        view_v1, _ = fresh(
            corpus_v1, self._vd(), llm_client=self._stub(),
        )

        # Mutate the publication bucket: count 10 → 11, sample
        # changes.
        new_pub = _bucket_anchor(
            "publication", "I", count=11,
            samples=[{
                "node_id": "publication:newest",
                "observed_at": "2026-04-27T12:00:00Z",
                "source": "af_pubs.catalog",
                "attrs_summary": {"pub_number": "NEW"},
            }],
        )
        corpus_v2 = _corpus(
            new_pub,
            _bucket_anchor("publication_family", "I", count=2),
            _gen_anchor(13),
            _gens_table_anchor([13]),
        )
        view_v2, stats = refresh(
            cached_view=view_v1,
            new_corpus=corpus_v2,
            view_def=self._vd(),
            llm_client=self._stub(),
        )
        assert stats.fragments_total == 6
        # Reused: bucket:publication_family:I quote +
        # narrative:publication_family:I + generations:table = 3
        assert stats.reused == 3
        # Refreshed: summary + bucket:publication:I +
        # narrative:publication:I = 3
        assert stats.refreshed == 3
        # LLM calls: summary (its sources include the changed
        # bucket) + narrative:publication:I = 2
        assert stats.llm_calls == 2

    def test_non_listed_bucket_landing_refreshes_summary_only(self):
        """A bucket whose subtype is NOT in `bucket_subtypes`
        gets a quote but no narrative. Mutating it refreshes
        summary + quote (2 fragments), 1 LLM call (summary).
        """
        a = _bucket_anchor("publication", "I", count=10)
        c = _bucket_anchor("section", "I", count=5)
        g = _gen_anchor(13)
        gt = _gens_table_anchor([13])

        view_v1, _ = fresh(
            _corpus(a, c, g, gt), self._vd(),
            llm_client=self._stub(),
        )
        # 1 summary + 2 quotes + 1 narrative (publication
        # only) + 1 gens table = 5 fragments.

        c_new = _bucket_anchor("section", "I", count=6)
        view_v2, stats = refresh(
            cached_view=view_v1,
            new_corpus=_corpus(a, c_new, g, gt),
            view_def=self._vd(),
            llm_client=self._stub(),
        )
        assert stats.fragments_total == 5
        # Reused: bucket:publication:I + narrative:publication:I
        # + generations:table = 3
        assert stats.reused == 3
        # Refreshed: summary + bucket:section:I = 2
        assert stats.refreshed == 2
        # LLM calls: summary only (section has no narrative).
        assert stats.llm_calls == 1

    def test_new_generation_published_refreshes_table_only(self):
        """A new `change:gen:N+1` anchor enters the corpus +
        the synthetic generations:table anchor changes.

        Subtlety: the framework's `refresh()` reuses a cached
        fragment when ITS RECORDED sources are still stable —
        even if the spec's sources have grown. So:

          - summary's cached source_hashes = {b1, b2, g13}
            (the prior cycle's anchors). All still present
            with same hashes → reused.
          - generations:table's source = (change:generations:table,)
            whose content changed (now lists g13 + g14) →
            REFRESHED.

        Net: 1 fragment refreshed, 0 LLM calls (the
        generations table is a QuoteSpec). The summary
        narrative will be stale (doesn't mention g14) until a
        bucket-content change forces it to re-synthesize.
        That's the framework's reuse contract; the spec's
        upper bound of "≤ 2 refreshed, ≤ 1 LLM call" still
        holds.
        """
        view_v1, _ = fresh(
            self._baseline_corpus(),
            self._vd(),
            llm_client=self._stub(),
        )

        corpus_v2 = _corpus(
            _bucket_anchor("publication", "I", count=10),
            _bucket_anchor("publication_family", "I", count=2),
            _gen_anchor(13),
            _gen_anchor(14),
            _gens_table_anchor([13, 14]),
        )
        view_v2, stats = refresh(
            cached_view=view_v1,
            new_corpus=corpus_v2,
            view_def=self._vd(),
            llm_client=self._stub(),
        )
        assert stats.fragments_total == 6
        # Reused: summary (its recorded sources are still
        # stable, even though the new spec lists g14 too)
        # + 2 buckets + 2 narratives = 5
        assert stats.reused == 5
        # Refreshed: generations:table only.
        assert stats.refreshed == 1
        # No LLM calls (only quote refreshed).
        assert stats.llm_calls == 0


# ---------------------------------------------------------------------------
# Hermeticity
# ---------------------------------------------------------------------------


class TestHermeticity:
    def test_planner_emits_only_declared_sources(self):
        """End-to-end fresh() against a synthetic corpus —
        if the planner read an undeclared anchor the
        RestrictedCorpus would have raised on first read.
        """
        corpus = _corpus(
            _bucket_anchor("publication", "I", count=5),
            _gen_anchor(13),
            _gens_table_anchor([13]),
        )
        view, stats = fresh(
            corpus,
            recent_changes_view_def(),
            llm_client=DeterministicStubLLM(),
        )
        # 1 summary + 1 bucket quote + 1 narrative + 1 gens
        # table = 4 fragments.
        assert stats.fragments_total == 4
        assert stats.added == 4


# ---------------------------------------------------------------------------
# Anchor resolver — every anchor returns None
# ---------------------------------------------------------------------------


class TestAnchorResolver:
    def test_bucket_anchor_returns_none(self):
        assert anchor_resolver("change:bucket:publication:I") is None

    def test_generation_anchor_returns_none(self):
        assert anchor_resolver("change:gen:13") is None

    def test_generations_table_anchor_returns_none(self):
        assert anchor_resolver("change:generations:table") is None

    def test_unknown_anchor_returns_none(self):
        assert anchor_resolver("nonsense:foo") is None


# ---------------------------------------------------------------------------
# View metadata
# ---------------------------------------------------------------------------


class TestViewDefinition:
    def test_factory_returns_canonical_metadata(self):
        vd = recent_changes_view_def()
        assert vd.view_id == VIEW_ID
        assert vd.view_def_hash == VIEW_DEF_HASH
        assert vd.params["window_days"] == 30
        assert vd.params["top_k_per_bucket"] == 10
        assert vd.params["bucket_subtypes"] == [
            "publication", "publication_family",
        ]

    def test_factory_rejects_invalid_window_days(self):
        with pytest.raises(ValueError, match="window_days"):
            recent_changes_view_def(window_days=0)
        with pytest.raises(ValueError, match="window_days"):
            recent_changes_view_def(window_days=-1)

    def test_factory_overrides(self):
        vd = recent_changes_view_def(
            window_days=7,
            top_k_per_bucket=5,
            bucket_subtypes=["publication"],
        )
        assert vd.params["window_days"] == 7
        assert vd.params["top_k_per_bucket"] == 5
        assert vd.params["bucket_subtypes"] == ["publication"]


# ---------------------------------------------------------------------------
# Assembler smoke
# ---------------------------------------------------------------------------


class TestAssembler:
    def test_byte_clean_markdown_renders_sections(self):
        corpus = _corpus(
            _bucket_anchor("publication", "I", count=5),
            _gen_anchor(13),
            _gens_table_anchor([13]),
        )
        view, _ = fresh(
            corpus,
            recent_changes_view_def(),
            llm_client=DeterministicStubLLM(),
        )
        result = recent_changes_assembler(view)
        md = result.markdown
        assert "# `af_pubs.recent_changes`" in md
        assert "## Summary" in md
        assert "## What landed, by bucket" in md
        assert "### publication I" in md
        assert "## Generations published in the window" in md

    def test_byte_clean_does_not_contain_markers(self):
        corpus = _corpus(
            _bucket_anchor("publication", "I", count=5),
            _gen_anchor(13),
            _gens_table_anchor([13]),
        )
        view, _ = fresh(
            corpus,
            recent_changes_view_def(),
            llm_client=DeterministicStubLLM(),
        )
        result = recent_changes_assembler(view)
        # The `markdown` field never contains <!--frag:...-->.
        assert "<!--frag:" not in result.markdown
        # The `markered` field DOES.
        assert "<!--frag:" in result.markered

    def test_offsets_address_byte_clean_output(self):
        corpus = _corpus(
            _bucket_anchor("publication", "I", count=5),
            _gen_anchor(13),
            _gens_table_anchor([13]),
        )
        view, _ = fresh(
            corpus,
            recent_changes_view_def(),
            llm_client=DeterministicStubLLM(),
        )
        result = recent_changes_assembler(view)
        for off in result.fragment_offsets:
            chunk = result.markdown[off.start:off.start + off.length]
            assert chunk  # non-empty
