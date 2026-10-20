"""Deployment-declared concept groups and lexical query expansion (issue #1218).

Hermetic: declaration validation, the matching rule, and the two properties the
change stands on -- exactness (a declared string matches itself and nothing
else) and "no declaration, no change" (a deployment that declares no group
ranks the byte-for-byte query it always did).

The corpus vocabulary here is deliberately neutral (`throughput`, `\tput`,
`items/s`). The issue is about a physics corpus, but no HEP term belongs in
`src/okg` or in a substrate test: concept groups are DEPLOYMENT-declared, and a
fixture that reached for physics would read as the substrate knowing physics.
"""

from __future__ import annotations

import pytest

from okg.substrate.mcp.concept_expansion import (
    ConceptCanonicalShadowed,
    ConceptGroupConfigInvalid,
    ConceptTermDuplicated,
    expand_query,
    expansion_rank,
    parse_concept_groups,
    tokenize,
)
from okg.substrate.mcp.search import _expansion_rank, _resolve_expansion
from okg.substrate.mcp.search_profiles import (
    ResolvedScope,
    parse_search_config,
    restrict_scope_to_method,
)
from okg.substrate.mcp.passage_rollup import RANK_BY_PASSAGES


GROUPS = parse_concept_groups(
    {
        "throughput": ["\\tput", "items/s"],
        "latency": {"variants": ["\\lat"]},
    }
)


# ---------- (a) declaration validation: one test per refusal ----------


def test_absent_block_declares_no_groups() -> None:
    assert parse_concept_groups(None) == ()


def test_block_must_be_a_mapping() -> None:
    with pytest.raises(ConceptGroupConfigInvalid) as excinfo:
        parse_concept_groups(["throughput"])
    assert excinfo.value.code == "concept_group_config_invalid"


def test_group_must_list_its_variants() -> None:
    with pytest.raises(ConceptGroupConfigInvalid):
        parse_concept_groups({"throughput": "\\tput"})


def test_group_of_one_is_refused_not_kept_as_a_no_op() -> None:
    with pytest.raises(ConceptGroupConfigInvalid) as excinfo:
        parse_concept_groups({"throughput": []})
    assert "can never expand" in str(excinfo.value)


def test_unknown_key_under_a_group_is_refused() -> None:
    with pytest.raises(ConceptGroupConfigInvalid) as excinfo:
        parse_concept_groups({"throughput": {"varients": ["\\tput"]}})
    assert excinfo.value.data["unknown_keys"] == ["varients"]


@pytest.mark.parametrize("nested_canonical", ["rate", "latency", 7])
def test_nested_canonical_is_refused_instead_of_ignored(nested_canonical) -> None:
    with pytest.raises(ConceptGroupConfigInvalid) as excinfo:
        parse_concept_groups(
            {"rate": {"canonical": nested_canonical, "variants": ["speed"]}}
        )
    assert excinfo.value.data["unknown_keys"] == ["canonical"]
    assert excinfo.value.data["accepted_keys"] == ["variants"]


def test_mixed_type_unknown_keys_raise_the_stable_configuration_error() -> None:
    with pytest.raises(ConceptGroupConfigInvalid) as excinfo:
        parse_concept_groups({"rate": {1: "x", "wrong": "x", "variants": ["speed"]}})
    assert excinfo.value.code == "concept_group_config_invalid"
    assert set(excinfo.value.data["unknown_keys"]) == {1, "wrong"}


def test_non_string_term_is_refused() -> None:
    with pytest.raises(ConceptGroupConfigInvalid):
        parse_concept_groups({"throughput": ["\\tput", 7]})


@pytest.mark.parametrize("term", ["\\tp*", "item?"])
def test_glob_syntax_is_refused_never_matched_literally(term: str) -> None:
    with pytest.raises(ConceptGroupConfigInvalid) as excinfo:
        parse_concept_groups({"throughput": [term]})
    assert excinfo.value.code == "concept_group_config_invalid"
    assert excinfo.value.data["unsupported"]


def test_latex_notation_is_not_mistaken_for_a_pattern() -> None:
    """`^`, `_`, `\\`, `{}`, `|` and `+` are ordinary notation, not globs."""
    groups = parse_concept_groups(
        {
            "transverse momentum": ["p_T^\\mu", "\\pt^{\\ell}", "|p|+1"],
        }
    )
    assert groups[0].variants == ("p_T^\\mu", "\\pt^{\\ell}", "|p|+1")


def test_a_term_repeated_inside_one_group_is_refused() -> None:
    with pytest.raises(ConceptTermDuplicated):
        parse_concept_groups({"throughput": ["\\tput", "\\tput"]})


def test_a_term_declared_in_two_groups_is_refused() -> None:
    with pytest.raises(ConceptTermDuplicated) as excinfo:
        parse_concept_groups(
            {
                "throughput": ["\\tput"],
                "rate": ["\\tput"],
            }
        )
    assert excinfo.value.code == "concept_term_duplicated"
    assert excinfo.value.data["term"] == "\\tput"


def test_a_variant_equal_to_another_groups_canonical_is_refused() -> None:
    with pytest.raises(ConceptCanonicalShadowed) as excinfo:
        parse_concept_groups(
            {
                "throughput": ["\\tput"],
                "rate": ["throughput"],
            }
        )
    assert excinfo.value.code == "concept_canonical_shadowed"
    assert excinfo.value.data["shadowed_canonical"] == "throughput"


@pytest.mark.parametrize(
    "raw",
    [
        {"flow rate": ["one"], "FLOW RATE": ["two"]},
        {"flow rate": ["one"], "other": ["flow  rate"]},
        {"throughput": ["flow."], "flow": ["three"]},
        {"flow rate": ["FLOW RATE"]},
    ],
)
def test_terms_colliding_under_query_normalization_are_refused(raw):
    with pytest.raises((ConceptTermDuplicated, ConceptCanonicalShadowed)):
        parse_concept_groups(raw)


def test_punctuation_only_term_is_not_a_silent_noop():
    with pytest.raises(ConceptGroupConfigInvalid):
        parse_concept_groups({"...": ["rate"]})


def test_the_manifest_search_block_accepts_and_validates_the_key() -> None:
    config = parse_search_config(
        {
            "profiles": {"analyst": {"subtypes": ["chunk"]}},
            "concept_groups": {"throughput": ["\\tput"]},
        }
    )
    assert config is not None
    assert config.concept_groups[0].canonical == "throughput"
    with pytest.raises(ConceptTermDuplicated):
        parse_search_config(
            {
                "concept_groups": {"a": ["x"], "b": ["x"]},
            }
        )


# ---------- (b) the matching rule ----------


def test_tokenize_splits_on_whitespace_and_trims_sentence_punctuation() -> None:
    assert tokenize("measure throughput, then p_T^\\mu.") == (
        "measure",
        "throughput",
        "then",
        "p_T^\\mu",
    )


def test_a_plain_word_matches_case_insensitively() -> None:
    expanded = expand_query("Throughput regression", GROUPS)
    assert expanded.applied
    assert expanded.groups[0]["canonical"] == "throughput"
    assert expanded.groups[0]["added_terms"] == ["\\tput", "items/s"]


def test_notation_matches_case_sensitively() -> None:
    groups = parse_concept_groups({"momentum": ["p_T"]})
    assert expand_query("p_T spectrum", groups).applied
    assert not expand_query("p_t spectrum", groups).applied


def test_expansion_needs_a_whole_token_never_a_substring() -> None:
    # `throughputs` and `xthroughput` contain the term; neither IS the term.
    assert not expand_query("throughputs regressed", GROUPS).applied
    assert not expand_query("xthroughput", GROUPS).applied


def test_alpha_s_never_reaches_the_alpha_group() -> None:
    """The exactness property the issue names as worse-than-nothing if lost."""
    groups = parse_concept_groups({"\\alpha": ["fine structure constant"]})
    assert expand_query("\\alpha running", groups).applied
    expanded = expand_query("\\alpha_s running", groups)
    assert not expanded.applied
    assert expanded.reason == "no_group_matched"
    assert expanded.query == "\\alpha_s running"


def test_a_multi_word_term_matches_a_contiguous_token_run() -> None:
    groups = parse_concept_groups({"transverse momentum": ["\\pt"]})
    assert expand_query("the transverse momentum tail", groups).applied
    assert not expand_query("momentum transverse", groups).applied


def test_a_variant_reaches_the_canonical_not_only_the_other_way() -> None:
    expanded = expand_query("items/s at peak", GROUPS)
    assert expanded.applied
    assert expanded.groups[0]["added_terms"] == ["throughput", "\\tput"]


def test_two_groups_in_one_query_both_report() -> None:
    expanded = expand_query("throughput and latency", GROUPS)
    assert [group["canonical"] for group in expanded.groups] == [
        "latency",
        "throughput",
    ]


def test_no_declared_groups_leaves_the_query_byte_for_byte() -> None:
    expanded = expand_query("throughput \\tput items/s", ())
    assert expanded.query == "throughput \\tput items/s"
    assert not expanded.applied
    assert expanded.reason == "no_groups_declared"
    assert expanded.groups == ()


def test_a_query_already_holding_every_member_is_not_rewritten() -> None:
    expanded = expand_query("throughput \\tput items/s", GROUPS)
    assert expanded.query == "throughput \\tput items/s"
    assert not expanded.applied
    assert expanded.reason == "all_terms_already_present"


def test_added_terms_are_appended_to_the_callers_own_string() -> None:
    expanded = expand_query("peak throughput", GROUPS)
    assert expanded.query == "peak throughput \\tput items/s"


# ---------- exact beats expanded, and the request/method switches ----------


def test_expansion_rank_demotes_only_rows_without_a_typed_term() -> None:
    terms = expand_query("peak throughput", GROUPS).original_terms
    assert expansion_rank("the throughput was fine", terms) == 0
    assert expansion_rank("measured \\tput only", terms) == 1
    # Substring is not a token: `throughputs` does not make a row exact.
    assert expansion_rank("throughputs \\tput", terms) == 1


def test_an_alias_promoted_row_is_never_demoted_by_expansion() -> None:
    """Promotion (issue #797) puts a row on the page because the caller's OWN
    query matched an alias naming it, so its body text must not demote it."""
    expansion = expand_query("peak throughput", GROUPS)
    promoted = {"attrs": {"text": "\\tput only"}, "match_kind": "catalog_alias"}
    assert _expansion_rank(promoted, expansion) == 0
    assert _expansion_rank({"attrs": promoted["attrs"]}, expansion) == 1


def _scope(groups=GROUPS) -> ResolvedScope:
    return ResolvedScope(concept_groups=tuple(groups))


def test_expand_concepts_false_disables_expansion() -> None:
    expanded = _resolve_expansion(
        "peak throughput",
        scope=_scope(),
        rank_by="self",
        expand_concepts=False,
    )
    assert expanded.query == "peak throughput"
    assert not expanded.applied
    assert expanded.reason == "disabled_by_request"


def test_expansion_is_on_by_default_when_groups_are_declared() -> None:
    expanded = _resolve_expansion(
        "peak throughput",
        scope=_scope(),
        rank_by="self",
        expand_concepts=True,
    )
    assert expanded.applied
    assert expanded.disclosure()["expanded_query"] == ("peak throughput \\tput items/s")


def test_passage_rollup_refuses_to_expand_and_says_so() -> None:
    expanded = _resolve_expansion(
        "peak throughput",
        scope=_scope(),
        rank_by=RANK_BY_PASSAGES,
        expand_concepts=True,
    )
    assert not expanded.applied
    assert expanded.reason == "rank_by_passages_unsupported"


def test_an_unscoped_search_expands_nothing() -> None:
    expanded = _resolve_expansion(
        "peak throughput",
        scope=None,
        rank_by="self",
        expand_concepts=True,
    )
    assert expanded.query == "peak throughput"
    assert expanded.reason == "no_groups_declared"


@pytest.mark.parametrize("method", ["semantic", "alias"])
def test_a_method_that_cannot_expand_carries_no_groups(method: str) -> None:
    restricted = restrict_scope_to_method(_scope(), method)
    assert restricted.concept_groups == ()
    assert restrict_scope_to_method(_scope(), "lexical").concept_groups


def test_the_disclosure_block_is_always_shaped_the_same() -> None:
    for expanded in (
        expand_query("peak throughput", GROUPS),
        expand_query("peak throughput", ()),
    ):
        assert set(expanded.disclosure()) == {
            "applied",
            "groups",
            "expanded_query",
            "reason",
        }
