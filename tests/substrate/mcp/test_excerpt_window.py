"""Unit tests for the excerpt window rule (PACT search-result-shape, D4).

Database-free: the matcher is built directly, as the one SQL call per page
would build it (`lex:<lexeme>` per stemmed query word, exact short words).
The database path (`build_matcher` against PostgreSQL's english
configuration) is exercised by tests/substrate/mcp/test_search_records.py.
"""
from __future__ import annotations

from okg.substrate.mcp.excerpt_window import (
    BASIS_FALLBACK,
    BASIS_LEXIZED,
    CANDIDATE_WORD_CAP,
    ExcerptMatcher,
    best_window_start,
    build_matcher,
    fallback_matcher,
    window_excerpt,
)


def _matcher(words: dict[str, str], exact: tuple[str, ...] = ()) -> ExcerptMatcher:
    """`words` maps a text word to the lexeme it lexizes to."""
    return ExcerptMatcher(
        basis=BASIS_LEXIZED,
        term_of={w: frozenset({f"lex:{lex}"}) for w, lex in words.items()},
        exact_words=frozenset(exact),
        lexemes=frozenset(words.values()),
    )


FILLER = "lorem ipsum dolor sit amet " * 40  # 1,080 characters, no match


def test_the_window_holding_both_answer_words_wins_over_the_first_hit() -> None:
    text = (
        "pump " + FILLER + "the pump failed. " + FILLER
        + "pump pressure dropped below the valve threshold. " + FILLER
    )
    matcher = _matcher({"pump": "pump", "pressure": "pressur", "valve": "valv"})
    excerpt = window_excerpt(text, matcher, max_chars=280)
    assert "pressure" in excerpt and "valve" in excerpt
    assert excerpt.startswith("...")


def test_a_stopword_never_anchors_the_window() -> None:
    # "the" yields no lexeme under the english configuration, so the matcher
    # has no term for it; the window goes to the content word, not the
    # first "the".
    text = "the " + FILLER + "zirconium cladding " + FILLER
    matcher = _matcher({"zirconium": "zirconium"})
    excerpt = window_excerpt(text, matcher, max_chars=200)
    assert "zirconium" in excerpt
    assert not excerpt.startswith("the ")


def test_a_fragment_inside_a_longer_word_never_anchors() -> None:
    # "id" as a short query word matches only the whole word "id", not the
    # "id" inside "identifier" or "valid".
    text = "identifier valid " + FILLER + "the id is 42 " + FILLER
    matcher = _matcher({}, exact=("id",))
    excerpt = window_excerpt(text, matcher, max_chars=120)
    assert "the id is 42" in excerpt


def test_a_short_non_stopword_counts_as_a_query_word() -> None:
    text = FILLER + "5g modem " + FILLER + "5g modem firmware " + FILLER
    matcher = _matcher({"modem": "modem", "firmware": "firmwar"}, exact=("5g",))
    excerpt = window_excerpt(text, matcher, max_chars=120)
    assert "5g modem firmware" in excerpt


def test_ties_go_to_the_earliest_window() -> None:
    text = FILLER + "alpha " + FILLER + "alpha " + FILLER
    matcher = _matcher({"alpha": "alpha"})
    first = text.index("alpha")
    start = best_window_start(text, matcher, 100)
    assert start <= first < start + 100


def test_no_match_keeps_the_start_and_a_short_text_is_returned_whole() -> None:
    matcher = _matcher({"absent": "absent"})
    excerpt = window_excerpt(FILLER, matcher, max_chars=100)
    assert excerpt.startswith("lorem") and excerpt.endswith("...")
    assert len(excerpt) == 100
    assert window_excerpt("short text", matcher, max_chars=100) == "short text"


def test_the_excerpt_never_exceeds_max_chars() -> None:
    text = FILLER + "needle " + FILLER
    matcher = _matcher({"needle": "needl"})
    for budget in (64, 100, 280, 1000):
        assert len(window_excerpt(text, matcher, max_chars=budget)) <= budget


def test_the_fallback_matches_whole_words_without_stopwords() -> None:
    matcher = fallback_matcher("the pump of valves")
    assert matcher.basis == BASIS_FALLBACK
    assert matcher.terms("pump") and matcher.terms("valves")
    assert not matcher.terms("the") and not matcher.terms("of")
    # Whole words only: no stemming in the fallback.
    assert not matcher.terms("pumps")


def test_past_the_candidate_cap_the_page_falls_back_without_a_query() -> None:
    """Failure case: too many candidate words never reach the database; the
    page names the fallback basis instead."""

    class _Refuse:
        def cursor(self):  # pragma: no cover - must not be called
            raise AssertionError("lexized past the cap")

    texts = [" ".join(f"pum{i}x" for i in range(CANDIDATE_WORD_CAP + 1))]
    matcher = build_matcher(_Refuse(), "pumps", texts)
    assert matcher.basis == BASIS_FALLBACK


def test_without_a_connection_the_page_falls_back() -> None:
    assert build_matcher(None, "pump", ["pump"]).basis == BASIS_FALLBACK


def test_a_match_at_the_very_end_is_not_cut_by_the_ellipsis() -> None:
    """Review S1: the leading "..." must not push the match out of view."""
    excerpt = window_excerpt("a" * 1000 + " zebra", fallback_matcher("zebra"), max_chars=50)
    assert excerpt.endswith("zebra"), excerpt
    assert len(excerpt) <= 50


def test_dense_matches_choose_the_window_in_linear_time() -> None:
    """Review: the window search is linear, not quadratic, on dense matches
    (1 MB of matching words at max_chars=280 took 78 s before).

    Proved by scaling, not by a wall-clock bound: an absolute limit failed
    on a loaded CI runner (3.67 s against 3.0 s). Four times the words must
    cost well under sixteen times the time; linear code measures about 4x,
    the old quadratic scan about 16x. CPU time of this process, the fastest
    of interleaved runs, keeps other load on the host out of the ratio.

    Issue #3245: on a CI worker that earlier tests left with a large heap,
    one cyclic garbage collection during the large run cost over a second
    of this process's CPU time (ratios of 112-135). The collector is run
    first and paused while timing, and every run repeats: the old early
    stop kept a single GC-inflated large run as the best."""
    import gc
    import time

    matcher = _matcher({"pump": "pump"})
    small, large = "pump " * 4_000, "pump " * 16_000

    def cpu_seconds(text: str) -> float:
        started = time.process_time()
        window_excerpt(text, matcher, max_chars=280)
        return time.process_time() - started

    small_best = large_best = float("inf")
    was_enabled = gc.isenabled()
    gc.collect()
    gc.disable()
    try:
        for _ in range(7):
            small_best = min(small_best, cpu_seconds(small))
            large_best = min(large_best, cpu_seconds(large))
    finally:
        if was_enabled:
            gc.enable()
    ratio = large_best / max(small_best, 1e-4)
    assert ratio < 8, (small_best, large_best, ratio)


def test_a_cut_text_always_says_so_and_never_drops_its_tail() -> None:
    """Re-review: when the window ends within three characters of the end,
    the tail was dropped with no trailing "...". Either the excerpt reaches
    the end of the text, or it ends with "..."."""
    text = "a" * 30 + " zebra cccccccc"
    excerpt = window_excerpt(text, fallback_matcher("zebra"), max_chars=20)
    assert excerpt.endswith("cccccccc") or excerpt.endswith("..."), excerpt
    assert len(excerpt) <= 20
    for size in range(40, 80):
        for budget in (12, 20, 33):
            body = "x " * size + "zebra" + " y" * (size // 3)
            cut = window_excerpt(body, fallback_matcher("zebra"), max_chars=budget)
            assert len(cut) <= budget
            core = cut.removeprefix("...").removesuffix("...")
            assert cut.endswith("...") or body.endswith(core), (size, budget, cut)
