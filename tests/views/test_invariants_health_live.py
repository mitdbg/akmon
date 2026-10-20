"""Stage 6 — live-substrate gate for `af_pubs.invariants_health`.

Exercises the view against the LIVE af_pubs published
generation. Two checks:

1. **Live finding count**: at the latest published gen, the
   view emits one finding per fired-warn invariant in the
   af_pubs precision-floor set (post `prune-af-pubs-low-confidence-
   invariants`, 2026-04-28). The test does NOT pin violator
   counts — those drift across publish cycles — only that the
   named invariants are present and that each has a non-empty
   source list once filtered by the spec's
   `violator_count > 0 OR timed_out` rule.

2. **Property A on a no-op cycle**: building the corpus a
   second time at the same generation_id and refreshing
   produces 0 LLM calls and every fragment reused.

Read-only against the live substrate; never writes.
"""
from __future__ import annotations

from okg.views.execution.llm import DeterministicStubLLM
from okg.views.refresh import fresh, refresh
import psycopg
import pytest
from views.invariants_health import (
    build_corpus,
    invariants_health_assembler,
    invariants_health_view_def,
)

pytestmark = pytest.mark.postgres


# Invariants kept after `prune-af-pubs-low-confidence-invariants`
# that fire on the af_pubs corpus. `orphan_supersedes` and
# `r3_two_currents_on_chain` are also in the kept set but
# genuinely return 0 violators on the live data, so they don't
# appear in `fired_warns`.
_EXPECTED_FIRING_INVARIANTS = {
    "af_pubs_gm_predates_publication",
    "af_pubs_interim_change_overdue",
    "af_pubs_stale_only_contact_updates",
    "af_pubs_stale_without_review",
}


def test_live_finding_anchors_match_audit(live_af_pubs_generation):
    """The corpus at the live af_pubs published gen contains
    one finding anchor per fired-warn invariant audited
    2026-04-27. New invariants showing up is acceptable
    (operator added one); missing audited invariants fails the
    test.
    """
    dsn, gen_id = live_af_pubs_generation
    with psycopg.connect(dsn, autocommit=True) as conn:
        corpus = build_corpus(conn, generation_id=gen_id)
    finding_names = {
        aid[len("invariant:finding:"):]
        for aid in corpus
        if aid.startswith("invariant:finding:")
    }
    missing = _EXPECTED_FIRING_INVARIANTS - finding_names
    assert not missing, (
        f"audited invariants missing from live gen {gen_id}: "
        f"{sorted(missing)}. Live finding set: {sorted(finding_names)}"
    )
    # Sanity: summary anchor is also present.
    assert "invariant:summary" in corpus


def test_live_no_change_cycle_property_a(live_af_pubs_generation):
    """Property A on a no-op cycle: read the corpus twice at
    the same generation, refresh, expect every fragment
    reused and 0 LLM calls.
    """
    dsn, gen_id = live_af_pubs_generation
    stub = DeterministicStubLLM()
    view_def = invariants_health_view_def()

    with psycopg.connect(dsn, autocommit=True) as conn:
        corpus_v1 = build_corpus(conn, generation_id=gen_id)
    view_v1, stats_v1 = fresh(corpus_v1, view_def, llm_client=stub)
    assert stats_v1.added == stats_v1.fragments_total
    assert stats_v1.fragments_total > 0  # something fired

    # Read the corpus a second time at the same pin. By the
    # substrate's MVCC + bitemporal invariants, a published
    # generation's `invariant_summary` is immutable, so the
    # corpus is byte-identical.
    with psycopg.connect(dsn, autocommit=True) as conn:
        corpus_v2 = build_corpus(conn, generation_id=gen_id)
    view_v2, stats_v2 = refresh(
        cached_view=view_v1,
        new_corpus=corpus_v2,
        view_def=view_def,
        llm_client=stub,
    )
    assert stats_v2.fragments_total == stats_v1.fragments_total
    assert stats_v2.reused == stats_v2.fragments_total, (
        f"Property A violation: expected all "
        f"{stats_v2.fragments_total} fragments reused, got "
        f"{stats_v2.reused}. Live cycle's anchors must be "
        f"byte-identical at a stable generation pin."
    )
    assert stats_v2.refreshed == 0
    assert stats_v2.llm_calls == 0
    # Markdown byte-identical.
    assert (
        invariants_health_assembler(view_v1).markdown
        == invariants_health_assembler(view_v2).markdown
    )


def test_live_assembled_markdown_renders_each_audited_invariant(
    live_af_pubs_generation,
):
    """Each audited invariant gets a heading in the assembled
    markdown."""
    dsn, gen_id = live_af_pubs_generation
    stub = DeterministicStubLLM()
    view_def = invariants_health_view_def()

    with psycopg.connect(dsn, autocommit=True) as conn:
        corpus = build_corpus(conn, generation_id=gen_id)
    view, _ = fresh(corpus, view_def, llm_client=stub)
    md = invariants_health_assembler(view).markdown
    for name in _EXPECTED_FIRING_INVARIANTS:
        assert f"`{name}`" in md, (
            f"audited invariant {name!r} not rendered in "
            f"assembled markdown"
        )
