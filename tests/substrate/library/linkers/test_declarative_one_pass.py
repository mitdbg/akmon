"""The one-pass `word_boundary` and `subject_prefix` matchers.

Both matchers used to compare every source with every destination in
Python, which ran for hours on a large deployment. The rewrite must find
exactly the pairs the old loops found. The old loops are kept verbatim in
`_one_pass_reference` as the reference; the tests compare the two on generated and
hand-picked inputs, check the new code at a size the old code could not
finish, and check the pair-limit guard and the strict graph reads.
"""
from __future__ import annotations

import ast
import random
import time
from pathlib import Path

import psycopg
import pytest
from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st

from okg.substrate.library.linkers import _chronos
from okg.substrate.library.linkers import declarative as module
from okg.substrate.library.linkers.declarative import (
    DEFAULT_MAX_PAIR_COMPARISONS,
    CrossLinkPairLimitExceeded,
    DeclarativeLinker,
)
from okg.substrate.library.linkers.rules import (
    CrossLinkRule,
    DeriveFromPath,
    RuleEndpoint,
)
from tests.substrate.library.linkers._one_pass_reference import (
    WORDS as _WORDS,
    new_subject_prefix,
    new_word_boundary,
    old_pairs_subject_prefix as _old_pairs_subject_prefix,
    old_pairs_word_boundary as _old_pairs_word_boundary,
    synthetic_case,
)


# Characters chosen to hit every branch: word characters (letters, digits,
# hyphen), the separators paths use, spaces and colons (subject breaks), a
# newline (the old regex's `$` also matches before a final newline), regex
# metacharacters (the old code escaped them), and non-ASCII letters and
# digits (not word characters for the `[a-zA-Z0-9-]` class).
_ALPHABET = list("aAzZ09-_/.: \n") + list("()[]{}*+?|^$\\") + ["é", "ß", "١", "日"]


def _random_string(rng: random.Random, max_len: int) -> str:
    return "".join(rng.choice(_ALPHABET) for _ in range(rng.randint(0, max_len)))


def _random_case(
    rng: random.Random,
) -> tuple[list[tuple[str, str]], list[tuple[str, str]]]:
    """A small random case whose slugs often occur in the texts."""
    texts = [_random_string(rng, 30) for _ in range(rng.randint(0, 8))]
    slugs: list[str] = []
    for _ in range(rng.randint(0, 10)):
        roll = rng.random()
        if roll < 0.45 and texts:
            # A substring of a text, so a match is likely.
            text = rng.choice(texts)
            if text:
                i = rng.randrange(len(text))
                j = rng.randint(i, min(len(text), i + 8))
                slugs.append(text[i:j])
                continue
        if roll < 0.65 and slugs:
            # A prefix or suffix of another slug.
            other = rng.choice(slugs)
            cut = rng.randint(0, len(other))
            slugs.append(other[:cut] if rng.random() < 0.5 else other[cut:])
            continue
        slugs.append(_random_string(rng, 6))
    # Some texts are built from slugs and separators, to get matches at the
    # very start and end and repeated matches.
    for _ in range(rng.randint(0, 3)):
        parts = [rng.choice(slugs) if slugs and rng.random() < 0.6
                 else _random_string(rng, 3) for _ in range(rng.randint(1, 5))]
        texts.append(rng.choice(["", " ", ":", "/", "-", "a"]).join(parts))
    srcs = [(f"s{i % 7}", text) for i, text in enumerate(texts)]
    # Destination ids repeat on purpose: one destination can carry two slugs
    # and two destinations can carry one slug.
    dsts = [(f"d{i % 5}", slug) for i, slug in enumerate(slugs)]
    return srcs, dsts


_HAND_PICKED_SLUGS = [
    "", "a", "-", "/", ".", " ", ":", "\n", "src", "src/", "src/okg",
    "src/okg/x.py", "okg/x.py", ".py", "x.py", "x", "a-b", "a_b", "a b",
    "a b:", "foo:", "foo bar", "OKG-12", "OKG-1", "(a)", "a+b", "[x]",
    "\\d", "$", "^a", "é", "日本", "١٢", "a.b.c", "b.c", "x/y/z", "y/z",
    "path with spaces/file.txt", "with spaces", "a\nb",
]
_HAND_PICKED_TEXTS = [
    "", "a", "-a-", "src/okg/x.py", "see src/okg/x.py.", "src/okg/x.pyc",
    "xsrc/okg/x.py", "-src/okg/x.py", "_src/okg/x.py", "/src/okg/x.py/",
    "src/okg/x.py\n", "\nsrc/okg/x.py", "OKG-12 fixed; OKG-1 too",
    "OKG-123", "a-b a_b a b", "foo: bar", "foo bar baz", "foo",
    "foo:bar", " foo", "(a) a+b [x] \\d $ ^a", "é日本١٢é", "a.b.c b.c",
    "x/y/z and y/z", "open path with spaces/file.txt now", "a\nb",
    "src src/ src/okg", "x.py x.py x.py", ".py", "a b: c",
]


def _hand_picked() -> tuple[list[tuple[str, str]], list[tuple[str, str]]]:
    srcs = [(f"s{i}", text) for i, text in enumerate(_HAND_PICKED_TEXTS)]
    dsts = [(f"d{i}", slug) for i, slug in enumerate(_HAND_PICKED_SLUGS)]
    return srcs, dsts


# ---------------------------------------------------------------------------
# Equivalence
# ---------------------------------------------------------------------------


# `word_boundary` has two code paths: the one-pass token walk, and a direct
# per-slug scan when a rule has few slugs. Every equivalence check runs both.
_PATHS = ("one_pass", "few_slugs")


def _word_boundary_via(
    path: str,
    srcs: list[tuple[str, str]],
    dsts: list[tuple[str, str]],
) -> set[tuple[str, str]]:
    saved = module._FEW_SLUGS
    module._FEW_SLUGS = -1 if path == "one_pass" else 10**9
    try:
        return new_word_boundary(srcs, dsts)
    finally:
        module._FEW_SLUGS = saved


def test_the_default_picks_the_path_by_slug_count(monkeypatch) -> None:
    calls: list[str] = []
    real = module._WordBoundaryIndex

    def spy(dsts):
        calls.append("one_pass")
        return real(dsts)

    monkeypatch.setattr(module, "_WordBoundaryIndex", spy)
    few = [(f"d{i}", f"slug{i}") for i in range(module._FEW_SLUGS)]
    many = [(f"d{i}", f"slug{i}") for i in range(module._FEW_SLUGS + 1)]
    texts = [("s", "see slug3 and slug30")]
    assert new_word_boundary(texts, few) == _old_pairs_word_boundary(texts, few)
    assert calls == []
    assert new_word_boundary(texts, many) == _old_pairs_word_boundary(texts, many)
    assert calls == ["one_pass"]


@pytest.mark.parametrize("path", _PATHS)
def test_hand_picked_cases_match_the_old_loops(path) -> None:
    srcs, dsts = _hand_picked()
    old_wb = _old_pairs_word_boundary(srcs, dsts)
    old_sp = _old_pairs_subject_prefix(srcs, dsts)
    assert _word_boundary_via(path, srcs, dsts) == old_wb
    assert new_subject_prefix(srcs, dsts) == old_sp
    # Not vacuous: both matchers find pairs, and both reject near misses.
    assert ("s3", "d11") in old_wb  # src/okg/x.py in itself
    assert ("s5", "d11") not in old_wb  # src/okg/x.pyc
    assert ("s6", "d11") not in old_wb  # xsrc/okg/x.py
    assert ("s8", "d11") in old_wb  # _src/... ('_' is a boundary)
    assert ("s15", "d20") in old_sp  # "foo: bar" starts with "foo:" + " "
    assert ("s17", "d20") not in old_sp  # "foo" vs slug "foo:"
    assert len(old_wb) > 40 and len(old_sp) > 5


_SEEDED_CASES = 4000


def test_seeded_random_cases_match_the_old_loops() -> None:
    """Thousands of generated cases; the pair sets must be identical."""
    rng = random.Random(20261101)
    total_wb = total_sp = 0
    for case in range(_SEEDED_CASES):
        srcs, dsts = _random_case(rng)
        old_wb = _old_pairs_word_boundary(srcs, dsts)
        old_sp = _old_pairs_subject_prefix(srcs, dsts)
        for path in _PATHS:
            assert _word_boundary_via(path, srcs, dsts) == old_wb, (
                path, case, srcs, dsts,
            )
        assert new_subject_prefix(srcs, dsts) == old_sp, (case, srcs, dsts)
        total_wb += len(old_wb)
        total_sp += len(old_sp)
    # The generator produces real matches, not only empty sets.
    assert total_wb > 3000
    assert total_sp > 300


_text = st.text(alphabet=st.sampled_from(_ALPHABET), max_size=40)
_slug = st.text(alphabet=st.sampled_from(_ALPHABET), max_size=8)


@settings(
    max_examples=600,
    deadline=None,
    suppress_health_check=[HealthCheck.too_slow],
)
@given(
    texts=st.lists(_text, max_size=6),
    slugs=st.lists(_slug, max_size=8),
    data=st.data(),
)
def test_hypothesis_cases_match_the_old_loops(texts, slugs, data) -> None:
    # Draw extra slugs out of the texts, so matches are common.
    for text in texts:
        if text and data.draw(st.booleans()):
            i = data.draw(st.integers(0, len(text) - 1))
            j = data.draw(st.integers(i, len(text)))
            slugs.append(text[i:j])
    srcs = [(f"s{i % 4}", t) for i, t in enumerate(texts)]
    dsts = [(f"d{i % 3}", s) for i, s in enumerate(slugs)]
    old = _old_pairs_word_boundary(srcs, dsts)
    for path in _PATHS:
        assert _word_boundary_via(path, srcs, dsts) == old
    assert new_subject_prefix(srcs, dsts) == _old_pairs_subject_prefix(srcs, dsts)


def test_empty_inputs_give_no_pairs() -> None:
    for fn in (new_word_boundary, new_subject_prefix):
        assert fn([], []) == set()
        assert fn([("s", "text")], []) == set()
        assert fn([], [("d", "slug")]) == set()
        assert fn([("s", "text")], [("d", "")]) == set()


# ---------------------------------------------------------------------------
# Scale: the size class the old loops could not finish
# ---------------------------------------------------------------------------

# The in-suite sizes are kept small enough for the fast lane; the large
# sizes (50,000 x 20,000 and 230,000 x 56,000) are timed by the PACT's
# evidence/scale_bench.py. At 10,000 x 20,000 the old loops make 2e8 regex
# searches (about an hour on a lab host); the new code takes about a second.
_SCALE_CEILING_SECONDS = 60.0


def test_word_boundary_scales_with_text_size_not_pair_count() -> None:
    srcs, dsts = synthetic_case(10_000, 20_000)
    started = time.perf_counter()
    pairs = new_word_boundary(srcs, dsts)
    elapsed = time.perf_counter() - started
    assert elapsed < _SCALE_CEILING_SECONDS, elapsed
    # The links are real: every embedded path was found.
    by_slug = {slug: dst_id for dst_id, slug in dsts}
    expected = {
        (src_id, by_slug[word])
        for src_id, text in srcs[:300]
        for word in text.rstrip(".").split(" ")
        if word in by_slug
    }
    assert expected
    assert {p for p in pairs if int(p[0].split(":")[1]) < 300} == expected


def test_word_boundary_with_one_changed_destination_is_cheap() -> None:
    """The incremental case: every text against one new path."""
    srcs, dsts = synthetic_case(20_000, 2_000)
    one = [dsts[17]]
    started = time.perf_counter()
    pairs = new_word_boundary(srcs, one)
    elapsed = time.perf_counter() - started
    assert elapsed < 10.0, elapsed
    assert pairs == {
        (src_id, one[0][0]) for src_id, text in srcs
        if (" " + one[0][1] + " ") in (" " + text.rstrip(".") + " ")
    }


def test_word_boundary_scale_sample_matches_the_old_loops() -> None:
    srcs, dsts = synthetic_case(40, 1_000)
    old = _old_pairs_word_boundary(srcs, dsts)
    assert len(old) > 10
    for path in _PATHS:
        assert _word_boundary_via(path, srcs, dsts) == old


def test_subject_prefix_scales_with_text_size_not_pair_count() -> None:
    rng = random.Random(3)
    n_srcs = 30_000
    dsts = [(f"change:{i}", f"change-{i}") for i in range(20_000)]
    srcs = [
        (
            f"commit:{i}",
            f"{dsts[rng.randrange(len(dsts))][1]}: " + " ".join(
                rng.choice(_WORDS) for _ in range(12)
            ) if i % 3 == 0 else " ".join(rng.choice(_WORDS) for _ in range(12)),
        )
        for i in range(n_srcs)
    ]
    started = time.perf_counter()
    pairs = new_subject_prefix(srcs, dsts)
    elapsed = time.perf_counter() - started
    assert elapsed < 30.0, elapsed
    assert len(pairs) == sum(1 for i in range(n_srcs) if i % 3 == 0)
    sample_srcs = srcs[:100]
    assert {p for p in pairs if int(p[0].split(":")[1]) < 100} == (
        _old_pairs_subject_prefix(sample_srcs, dsts)
    )


# ---------------------------------------------------------------------------
# Pair-limit guard
# ---------------------------------------------------------------------------

def _patch_field_rows(monkeypatch, rows: list[tuple[str, str]]) -> None:
    monkeypatch.setattr(
        DeclarativeLinker,
        "_fetch_subtype_field",
        staticmethod(lambda cur, **kwargs: list(rows)),
    )


def test_containment_filter_refuses_above_the_pair_limit(monkeypatch) -> None:
    """A field the SQL cannot embed is filtered in Python, every row
    against every value; above the limit it refuses before working."""
    _patch_field_rows(monkeypatch, [("n:1", "alpha beta"), ("n:2", "gamma")])
    with pytest.raises(CrossLinkPairLimitExceeded) as caught:
        DeclarativeLinker._fetch_subtype_field_containing_values(
            object(),
            subtypes=("doc",),
            field="meta.path",
            values={"alpha", "gamma"},
            shape_skips=[],
            rule_id="doc_paths",
            max_pair_comparisons=3,
        )
    err = caught.value
    assert err.code == "cross_link_pair_limit_exceeded"
    assert err.data == {
        "rule_id": "doc_paths",
        "step": "derive_from_path containment filter on field 'meta.path'",
        "n_left": 2,
        "n_right": 2,
        "limit": 3,
    }
    assert "doc_paths" in str(err) and "2 x 2 = 4" in str(err)


def test_containment_filter_runs_at_the_pair_limit(monkeypatch) -> None:
    _patch_field_rows(monkeypatch, [("n:1", "alpha beta"), ("n:2", "gamma")])
    rows = DeclarativeLinker._fetch_subtype_field_containing_values(
        object(),
        subtypes=("doc",),
        field="meta.path",
        values={"alpha", "zeta"},
        shape_skips=[],
        rule_id="doc_paths",
        max_pair_comparisons=4,
    )
    assert rows == [("n:1", "alpha beta")]


def test_pair_limit_is_registered_and_configurable() -> None:
    registry = Path(__file__).resolve().parents[4] / "docs/reference/error_codes.yaml"
    assert "code: cross_link_pair_limit_exceeded" in registry.read_text()
    assert DeclarativeLinker(rules=[]).max_pair_comparisons == (
        DEFAULT_MAX_PAIR_COMPARISONS
    )
    assert DeclarativeLinker(rules=[], max_pair_comparisons=10).max_pair_comparisons == 10


@pytest.mark.parametrize("bad", [0, -1, 1.5, "10", True])
def test_pair_limit_must_be_a_positive_integer(bad) -> None:
    with pytest.raises(ValueError, match="max_pair_comparisons"):
        DeclarativeLinker(rules=[], max_pair_comparisons=bad)


def test_linker_passes_its_pair_limit_to_derive_from_path(monkeypatch) -> None:
    seen: dict[str, object] = {}

    def fake_match(cur, rule, **kwargs):
        seen.update(kwargs)
        return set()

    monkeypatch.setattr(
        DeclarativeLinker, "_match_derive_from_path", staticmethod(fake_match),
    )
    linker = DeclarativeLinker(rules=[], max_pair_comparisons=77)
    rule = CrossLinkRule(
        id="r",
        src=RuleEndpoint(subtypes=("a",), field="f"),
        dst=RuleEndpoint(
            subtypes=("b",),
            field="g",
            derive_from_path=DeriveFromPath(
                attr="f", regex=r"^(\w+)", capture_field="g",
            ),
        ),
        matcher="derive_from_path",
        edge="references",
        bidirectional=False,
    )
    linker._dispatch_matcher(
        object(),
        rule=rule,
        cv=1,
        dirty_node_ids=None,
        dirty_edge_ids=frozenset(),
        dirty_node_subtypes=None,
        dirty_node_attr_changes_by_subtype=None,
        endpoint_cache={},
        shape_skips=[],
    )
    assert seen["max_pair_comparisons"] == 77


# ---------------------------------------------------------------------------
# Strict graph reads: a failed or timed-out read raises, never "no rows"
# ---------------------------------------------------------------------------

def _timed_out_reads(monkeypatch) -> list[bool]:
    """Stand in for the shared read wrapper: a lenient read swallows the
    timeout and returns no rows (what `_chronos.query` does), a strict read
    raises it."""
    calls: list[bool] = []

    def fake_query(cur, sql, params, *, strict):
        calls.append(strict)
        if strict:
            raise psycopg.errors.QueryCanceled(
                "canceling statement due to statement timeout"
            )
        return []

    monkeypatch.setattr(_chronos, "_query", fake_query)
    return calls


def test_a_timed_out_field_read_raises(monkeypatch) -> None:
    calls = _timed_out_reads(monkeypatch)
    with pytest.raises(psycopg.errors.QueryCanceled):
        DeclarativeLinker._fetch_subtype_field(
            object(), subtypes=("a",), field="f",
        )
    with pytest.raises(psycopg.errors.QueryCanceled):
        DeclarativeLinker._fetch_subtype_fields_concat(
            object(), subtypes=("a",), fields=("f", "g"),
        )
    with pytest.raises(psycopg.errors.QueryCanceled):
        DeclarativeLinker._fetch_subtype_field_matching_values(
            object(), subtypes=("a",), field="f", values={"v"},
        )
    with pytest.raises(psycopg.errors.QueryCanceled):
        DeclarativeLinker._fetch_subtype_field_containing_values(
            object(), subtypes=("a",), field="f", values={"v"}, shape_skips=[],
        )
    with pytest.raises(psycopg.errors.QueryCanceled):
        DeclarativeLinker._any_live_node(object(), subtypes=("a",))
    assert calls and all(calls)


@pytest.mark.parametrize("matcher", ["word_boundary", "subject_prefix", "exact"])
def test_a_timed_out_read_fails_the_matcher(monkeypatch, matcher) -> None:
    _timed_out_reads(monkeypatch)
    rule = CrossLinkRule(
        id="r",
        src=RuleEndpoint(subtypes=("a",), field="f"),
        dst=RuleEndpoint(subtypes=("b",), field="g"),
        matcher=matcher,
        edge="references",
        bidirectional=False,
    )
    with pytest.raises(psycopg.errors.QueryCanceled):
        DeclarativeLinker(rules=[rule])._dispatch_matcher(
            object(),
            rule=rule,
            cv=1,
            dirty_node_ids=None,
            dirty_edge_ids=frozenset(),
            dirty_node_subtypes=None,
            dirty_node_attr_changes_by_subtype=None,
            endpoint_cache={},
            shape_skips=[],
        )


def test_declarative_linker_reads_the_graph_only_through_the_strict_reader() -> None:
    """The module reaches the graph wrapper only as `_chronos.query_strict`,
    so no read (`query`, `nodes`, `edges`, or one added later) can quietly
    turn a failure into "no rows"."""
    tree = ast.parse(Path(module.__file__).read_text())
    other_uses = [
        (node.lineno, node.attr)
        for node in ast.walk(tree)
        if isinstance(node, ast.Attribute)
        and isinstance(node.value, ast.Name)
        and node.value.id == "_chronos"
        and node.attr != "query_strict"
    ]
    direct_imports = [
        node.lineno
        for node in ast.walk(tree)
        if isinstance(node, ast.ImportFrom)
        and (node.module or "").endswith("_chronos")
    ]
    assert other_uses == []
    assert direct_imports == []
