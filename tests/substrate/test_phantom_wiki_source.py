"""PhantomWiki source — the derived-rule boundary, proven rather than asserted.

PACT okg-bench-graph-benchmarks / req.pw.corpus, task.pw.source.

The refusal is the whole leakage question for this benchmark, so every test
here that claims a refusal also shows the check FAILING on an input that
should trip it. A refusal test that only ever sees clean input reports
verified having verified nothing — the defect class this program exists to
remove.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from okg.substrate.errors import SubstrateError
from okg.substrate.library.sources.base import EdgeFact, NodeFact
from okg.substrate.library.sources.phantom_wiki import (
    ARTICLE_ATTRIBUTE_KINDS,
    ARTICLE_RELATIONS,
    PhantomWikiSource,
    census_prolog_kb,
    split_prolog_terms,
)

# The composed vocabulary PhantomWiki's QUESTIONS are phrased in, and which
# its `rules_derived.pl` defines. None of it may become an edge type.
DERIVED_VOCABULARY = (
    "niece", "nephew", "grandparent", "grandmother", "grandfather",
    "great_aunt", "great_uncle", "grandchild", "granddaughter", "grandson",
    "great_grandparent", "great_grandmother", "great_grandfather",
    "great_grandchild", "great_granddaughter", "great_grandson",
    "second_aunt", "second_uncle", "aunt", "uncle", "cousin",
    "female_cousin", "male_cousin", "female_second_cousin",
    "male_second_cousin", "female_first_cousin_once_removed",
    "male_first_cousin_once_removed", "mother_in_law", "father_in_law",
    "son_in_law", "daughter_in_law", "sister_in_law", "brother_in_law",
)

# A miniature but SHAPE-FAITHFUL knowledge base: the ground facts, the
# base rules, the derived rules, and the SWI-Prolog internals that
# `save_all_clauses_to_file` leaks into a real dump.
KB = """\
:- dynamic prolog_load_file/2.
:- multifile prolog_load_file/2.

female(X) :-
  gender(X, "female").

sibling(X, Y) :-
  parent(X, A),
  parent(Y, A),
  X \\= Y.

mother(X, Y) :-
  parent(X, Y),
  female(Y).

great_granddaughter(X, Y) :-
    great_grandchild(X, Y),
    female(Y).

female_cousin(X, Y) :-
    cousin(X, Y),
    female(Y).

niece(X, Y) :-
    sibling(X, A),
    daughter(A, Y).

file_search_path(library, Dir) :-
    library_directory(Dir).

file_search_path(swi, "/opt/homebrew").
prolog_file_type(pl, prolog).

parent("Vicki Hackworth", "Dino Beltran").
parent("Vicki Hackworth", "Shelli Beltran").
gender("Dino Beltran", "male").
gender("Shelli Beltran", "female").
gender("Vicki Hackworth", "female").
dob("Dino Beltran", "0268-08-09").
dob("Shelli Beltran", "0269-01-02").
dob("Vicki Hackworth", "0291-04-05").
job("Dino Beltran", "broadcast engineer").
job("Shelli Beltran", "osteopath").
job("Vicki Hackworth", "geologist").
hobby("Dino Beltran", "meteorology").
hobby("Shelli Beltran", "biology").
hobby("Vicki Hackworth", "geocaching").
friend_("Dino Beltran", "Shelli Beltran").
attribute("meteorology").
attribute("broadcast engineer").
type("Dino Beltran", person).
type("Shelli Beltran", person).
type("Vicki Hackworth", person).
"""

ARTICLES = {
    "Dino Beltran": """\
# Dino Beltran

## Family
The daughters of Dino Beltran are Vicki Hackworth.
The wife of Dino Beltran is Shelli Beltran.

## Friends
The friends of Dino Beltran are Shelli Beltran.

## Attributes
The date of birth of Dino Beltran is 0268-08-09.
The occupation of Dino Beltran is broadcast engineer.
The hobby of Dino Beltran is meteorology.
The gender of Dino Beltran is male.
""",
    "Shelli Beltran": """\
# Shelli Beltran

## Family
The daughters of Shelli Beltran are Vicki Hackworth.
The husband of Shelli Beltran is Dino Beltran.

## Friends
The friends of Shelli Beltran are Dino Beltran.

## Attributes
The date of birth of Shelli Beltran is 0269-01-02.
The occupation of Shelli Beltran is osteopath.
The hobby of Shelli Beltran is biology.
The gender of Shelli Beltran is female.
""",
    "Vicki Hackworth": """\
# Vicki Hackworth

## Family
The mother of Vicki Hackworth is Shelli Beltran.
The father of Vicki Hackworth is Dino Beltran.

## Friends


## Attributes
The date of birth of Vicki Hackworth is 0291-04-05.
The occupation of Vicki Hackworth is geologist.
The hobby of Vicki Hackworth is geocaching.
The gender of Vicki Hackworth is female.
""",
}

TRIPLE = {
    "universe_seed": 1,
    "universe_family_trees": 1,
    "universe_max_family_tree_size": 25,
    "universe_question_depth": 20,
}


def _instance(tmp_path: Path, *, articles=None, kb=KB) -> tuple[Path, Path]:
    """Lay out one generated instance. The KB sits OUTSIDE `articles/`,
    which is where the real snapshot must keep it too."""
    root = tmp_path / "instance"
    art = root / "articles"
    art.mkdir(parents=True, exist_ok=True)
    for name, text in (articles if articles is not None else ARTICLES).items():
        (art / f"{name}.txt").write_text(text, encoding="utf-8")
    kb_path = root / "kb" / "facts.pl"
    kb_path.parent.mkdir(parents=True, exist_ok=True)
    kb_path.write_text(kb, encoding="utf-8")
    return art, kb_path


def _source(tmp_path: Path, **over) -> PhantomWikiSource:
    art, kb = _instance(
        tmp_path,
        articles=over.pop("articles", None),
        kb=over.pop("kb", KB),
    )
    params = dict(
        repo="kilian-group/phantom-wiki",
        articles_dir=art,
        kb_facts_path=kb,
        **TRIPLE,
    )
    params.update(over)
    return PhantomWikiSource(**params)


def _facts(source: PhantomWikiSource):
    return list(source.run("t", mode="scope_complete").facts)


# ----------------------------------------------------------------------
# the derived-rule boundary
# ----------------------------------------------------------------------


def test_derived_rules_are_refused(tmp_path):
    """A `rules_derived.pl` clause presented to the source is REFUSED, and
    the refusal is COUNTED — never a silent skip.

    Both halves matter. Absence of a `great_granddaughter` edge could just
    mean the parser never saw the clause; the count is what shows the
    clause was seen, classified, and turned away.
    """
    source = _source(tmp_path)
    facts = _facts(source)
    stats = source.last_run_stats

    heads = stats["kb_refused_clause_heads"]
    for name in ("great_granddaughter/2", "female_cousin/2", "niece/2"):
        assert heads[name] == 1, f"{name} was not refused: {heads}"
    # rules_base.pl is inlined into the SAME dump and its clauses are
    # refused too — the boundary is clause-vs-ground-fact, not
    # rules_derived.pl-vs-everything-else
    for name in ("mother/2", "sibling/2", "female/1"):
        assert heads[name] == 1, f"{name} was not refused: {heads}"
    # ...as are the SWI-Prolog internals the dump leaks
    assert heads["file_search_path/2"] == 1
    assert stats["kb_directives_refused"] == 2

    # and nothing from any clause reached the graph
    edge_types = {f.edge_type for f in facts if isinstance(f, EdgeFact)}
    assert edge_types <= set(ARTICLE_RELATIONS) | {"has_attribute"}
    for word in DERIVED_VOCABULARY:
        assert word not in edge_types


def test_the_refusal_check_can_fail(tmp_path):
    """Non-vacuity. Deleting the derived rules from the KB makes the
    assertion above go red, so it is measuring the input rather than
    passing on any input at all."""
    stripped = "\n".join(
        line for line in KB.splitlines()
        if not line.startswith(("great_granddaughter", "female_cousin", "niece"))
    )
    source = _source(tmp_path, kb=stripped)
    _facts(source)
    heads = source.last_run_stats["kb_refused_clause_heads"]
    assert "great_granddaughter/2" not in heads
    assert "niece/2" not in heads


def test_refusal_discriminates_measured_on_the_complement(tmp_path):
    """A predicate true of the whole file discriminates nothing.

    Both sides must be non-empty: clauses refused AND ground facts
    corroborated, with no term in both buckets.
    """
    source = _source(tmp_path)
    _facts(source)
    stats = source.last_run_stats
    assert stats["kb_clauses_refused"] > 0
    assert stats["kb_ground_person_facts_corroborated"] > 0
    refused = {k.split("/")[0] for k in stats["kb_refused_clause_heads"]}
    grounded = {
        k.split("/")[0] for k in stats["kb_ground_person_facts_by_predicate"]
    }
    # `mother` is a clause here and `parent` a ground fact; the KB genuinely
    # carries both kinds and the classifier separates them
    assert "mother" in refused
    assert "parent" in grounded
    assert not (refused & grounded), (
        "a predicate classified as both clause and ground fact means the "
        "split is not a split"
    )


def test_no_graph_fact_derives_from_the_knowledge_base(tmp_path):
    """The KB contributes evidence, never a node or an edge.

    Proof by construction: emitting with a KB whose ground facts are
    reduced to the minimum still yields the same graph, because the graph
    is built from prose alone.
    """
    full = _source(tmp_path / "full")
    graph_full = {
        (f.src, f.edge_type, f.dst) for f in _facts(full) if isinstance(f, EdgeFact)
    }
    # a KB with only what the corroborator needs, and no rules at all
    minimal = "\n".join(
        line for line in KB.splitlines()
        if line.startswith(("parent(", "gender(", "dob(", "job(", "hobby(",
                            "friend_(", "type(", "attribute("))
    )
    lean = _source(tmp_path / "lean", kb=minimal)
    graph_lean = {
        (f.src, f.edge_type, f.dst) for f in _facts(lean) if isinstance(f, EdgeFact)
    }
    assert graph_full == graph_lean
    assert lean.last_run_stats["kb_clauses_refused"] == 0
    assert full.last_run_stats["kb_clauses_refused"] > 0


# ----------------------------------------------------------------------
# what the articles produce
# ----------------------------------------------------------------------


def test_articles_parse_into_typed_person_and_kinship_nodes(tmp_path):
    source = _source(tmp_path)
    facts = _facts(source)
    nodes = [f for f in facts if isinstance(f, NodeFact)]
    edges = [f for f in facts if isinstance(f, EdgeFact)]

    people = {n.attrs["name"] for n in nodes if n.subtype == "person"}
    assert people == set(ARTICLES)
    kinds = {n.attrs["kind"] for n in nodes if n.subtype == "person_attribute"}
    assert kinds == {kind for kind, _ in ARTICLE_ATTRIBUTE_KINDS.values()}

    triples = {(f.src, f.edge_type, f.dst) for f in edges}
    pid = source.person_node_id
    assert (pid("Vicki Hackworth"), "mother", pid("Shelli Beltran")) in triples
    assert (pid("Vicki Hackworth"), "father", pid("Dino Beltran")) in triples
    assert (pid("Dino Beltran"), "daughter", pid("Vicki Hackworth")) in triples
    assert (pid("Dino Beltran"), "wife", pid("Shelli Beltran")) in triples
    assert (pid("Shelli Beltran"), "husband", pid("Dino Beltran")) in triples
    # friendship is symmetric upstream AND both articles state it, so both
    # directions are read off prose rather than one being synthesised
    assert (pid("Dino Beltran"), "friend", pid("Shelli Beltran")) in triples
    assert (pid("Shelli Beltran"), "friend", pid("Dino Beltran")) in triples

    # every node is reachable; the corpus is not a bag of people
    endpoints = {f.src for f in edges} | {f.dst for f in edges}
    assert {n.node_id for n in nodes} - endpoints == set()


def test_person_carries_the_generation_triple(tmp_path):
    source = _source(tmp_path)
    person = next(
        f for f in _facts(source)
        if isinstance(f, NodeFact) and f.subtype == "person"
    )
    for key, value in TRIPLE.items():
        assert person.attrs[key] == value
    assert person.attrs["universe_id"] == source.universe_id
    assert person.attrs["article_path"].endswith(".txt")


def test_universe_id_separates_nested_universes(tmp_path):
    """Two universes at one seed and different sizes are NESTED, and must
    not share an identity."""
    a = _source(tmp_path / "a")
    b = _source(tmp_path / "b", universe_family_trees=2000)
    assert a.universe_seed == b.universe_seed
    assert a.universe_id != b.universe_id


def test_universe_id_over_discriminates_on_question_depth(tmp_path):
    """MEASURED, and it corrects the obvious reading of the id.

    Regenerating the same seed and sizes at `--question-depth 10` instead
    of 20 produced a BYTE-IDENTICAL article corpus (100,628 files, one
    sha256 over the ordered set). Depth is a QA-budget knob and does not
    perturb the world, so two corpora with different universe_ids can be
    the same world. The three fields that DO identify the world — seed,
    trees, tree size — are carried separately on every node for exactly
    this reason.
    """
    deep = _source(tmp_path / "d20")
    shallow = _source(tmp_path / "d10", universe_question_depth=10)
    assert deep.universe_id != shallow.universe_id
    world = ("universe_seed", "universe_family_trees",
             "universe_max_family_tree_size")
    assert all(getattr(deep, k) == getattr(shallow, k) for k in world)

    def _world(source):
        person = next(
            f for f in _facts(source)
            if isinstance(f, NodeFact) and f.subtype == "person"
        )
        return tuple(person.attrs[k] for k in world)

    assert _world(deep) == _world(shallow)


def test_unknown_article_sentence_is_refused_not_skipped(tmp_path):
    """The article templates are fixed by the generator, so a sentence no
    template produces means the release changed shape. Skipping it would
    drop a relation from the graph silently."""
    articles = dict(ARTICLES)
    articles["Vicki Hackworth"] = articles["Vicki Hackworth"].replace(
        "The mother of Vicki Hackworth is Shelli Beltran.",
        "The great-grandmother of Vicki Hackworth is Shelli Beltran.",
    )
    source = _source(tmp_path, articles=articles)
    with pytest.raises(SubstrateError, match="no generated template produces"):
        _facts(source)


def test_unresolvable_relation_target_is_skipped_and_counted(tmp_path):
    """Never stubbed, never dangling. A person node invented for a name no
    article describes would sit in the retrieval candidate pool the
    benchmark scores."""
    articles = dict(ARTICLES)
    articles["Dino Beltran"] = articles["Dino Beltran"].replace(
        "The friends of Dino Beltran are Shelli Beltran.",
        "The friends of Dino Beltran are Shelli Beltran, Nobody Atall.",
    )
    source = _source(tmp_path, articles=articles)
    facts = _facts(source)
    assert source.last_run_stats["unresolved_relation_targets"] == 1
    node_ids = {f.node_id for f in facts if isinstance(f, NodeFact)}
    endpoints = {
        e for f in facts if isinstance(f, EdgeFact) for e in (f.src, f.dst)
    }
    assert endpoints <= node_ids, "an unresolved target must not dangle"
    assert source.person_node_id("Nobody Atall") not in node_ids


# ----------------------------------------------------------------------
# the isolation stamp
# ----------------------------------------------------------------------


HERMIT = """\
# Solo Person

## Family


## Friends


## Attributes
The date of birth of Solo Person is 0300-01-01.
The occupation of Solo Person is lexicographer.
The hobby of Solo Person is meditation.
The gender of Solo Person is male.
"""


def test_stranding_stamp_is_derived_from_the_article_not_the_emission(tmp_path):
    """A person with no relation sentence carries the reason; a person with
    relations does not.

    Measured on the COMPLEMENT, which is what tells a discriminating stamp
    from a blanket one: 1 stamped person with zero relation sentences, 3
    unstamped with relations, and nothing stamped-and-connected. A stamp
    read off `did an edge come out` would be true exactly when the node is
    isolated and would forgive the parsing regression the isolation clause
    exists to catch.
    """
    articles = dict(ARTICLES)
    articles["Solo Person"] = HERMIT
    source = _source(tmp_path, articles=articles)
    facts = _facts(source)
    people = [f for f in facts if isinstance(f, NodeFact) and f.subtype == "person"]
    stamped = {
        p.attrs["name"] for p in people if "no_relations_reason" in p.attrs
    }
    unstamped = {p.attrs["name"] for p in people} - stamped
    assert stamped == {"Solo Person"}
    assert unstamped == set(ARTICLES)
    assert len(unstamped) == 3, "an empty complement forgives everything"
    assert source.last_run_stats["people_without_relations"] == 1

    # ...and the stamp agrees with the graph in both directions: the
    # attribute edges keep Solo Person non-isolated, so the stamp is NOT a
    # restatement of "has no edges"
    kin = {
        f.src for f in facts
        if isinstance(f, EdgeFact) and f.edge_type in ARTICLE_RELATIONS
    }
    assert source.person_node_id("Solo Person") not in kin
    attr_srcs = {
        f.src for f in facts
        if isinstance(f, EdgeFact) and f.edge_type == "has_attribute"
    }
    assert source.person_node_id("Solo Person") in attr_srcs


def test_stamp_check_can_fail(tmp_path):
    """Non-vacuity for the stamp: with no hermit in the corpus the stamped
    set is EMPTY, so the assertion above is reading the input."""
    source = _source(tmp_path)
    people = [
        f for f in _facts(source)
        if isinstance(f, NodeFact) and f.subtype == "person"
    ]
    assert not [p for p in people if "no_relations_reason" in p.attrs]
    assert source.last_run_stats["people_without_relations"] == 0


# ----------------------------------------------------------------------
# corroboration against the released KB
# ----------------------------------------------------------------------


def test_kb_ground_fact_the_prose_does_not_state_is_refused(tmp_path):
    """A prose corpus weaker than the released KB is a parsing regression,
    not a smaller corpus, and the point of reading the KB is to notice."""
    extra = KB + '\nparent("Vicki Hackworth", "Ghost Parent").\n'
    source = _source(tmp_path, kb=extra)
    with pytest.raises(SubstrateError, match="ground facts the articles do not"):
        _facts(source)


def test_corroboration_passes_on_the_released_shape(tmp_path):
    """Complement of the test above: the unmodified KB corroborates, so the
    refusal is keyed on the discrepancy rather than firing always."""
    source = _source(tmp_path)
    _facts(source)
    assert source.last_run_stats["kb_ground_person_facts_corroborated"] == 18


# ----------------------------------------------------------------------
# config guards
# ----------------------------------------------------------------------


def test_kb_inside_the_article_snapshot_is_refused(tmp_path):
    art, _ = _instance(tmp_path)
    inside = art / "facts.pl"
    inside.write_text(KB, encoding="utf-8")
    source = PhantomWikiSource(
        repo="r", articles_dir=art, kb_facts_path=inside, **TRIPLE,
    )
    with pytest.raises(SubstrateError, match="INSIDE the article directory"):
        source.run("t")


@pytest.mark.parametrize(
    "missing",
    ["repo", "universe_seed", "universe_question_depth",
     "universe_family_trees", "universe_max_family_tree_size"],
)
def test_generation_triple_is_required(tmp_path, missing):
    source = _source(tmp_path, **{missing: None})
    with pytest.raises(SubstrateError, match="missing required parameters"):
        source.run("t")


def test_fact_sidecars_are_skipped_with_a_count(tmp_path):
    """The generator writes `<Name>_facts.txt` beside each article: the same
    content in Prolog form. Reading both would double-encode every fact."""
    art, kb = _instance(tmp_path)
    for name in ARTICLES:
        (art / f"{name}_facts.txt").write_text('mother("a", "b").\n')
    source = PhantomWikiSource(
        repo="r", articles_dir=art, kb_facts_path=kb, **TRIPLE,
    )
    _facts(source)
    assert source.last_run_stats["articles"] == 3
    assert source.last_run_stats["sidecar_files_skipped"] == 3


def test_duplicate_person_names_are_refused(tmp_path):
    art, kb = _instance(tmp_path)
    (art / "Dino Beltran (2).txt").write_text(
        ARTICLES["Dino Beltran"], encoding="utf-8"
    )
    source = PhantomWikiSource(
        repo="r", articles_dir=art, kb_facts_path=kb, **TRIPLE,
    )
    with pytest.raises(SubstrateError, match="file stem"):
        source.run("t")


# ----------------------------------------------------------------------
# the Prolog reader itself
# ----------------------------------------------------------------------


def test_term_splitter_is_quote_aware():
    """`.\\n` splitting is close but not close enough, and the census is
    the evidence the boundary held — evidence built on an approximate parse
    is not evidence."""
    terms = split_prolog_terms('job("a", "Ph.D. holder").\nhobby("a", "go").\n')
    assert terms == ['job("a", "Ph.D. holder")', 'hobby("a", "go")']
    assert split_prolog_terms("% a comment.\nparent(\"a\", \"b\").") == [
        'parent("a", "b")'
    ]


def test_unquoted_atom_arguments_are_read_not_bucketed():
    """`type("X", person)` mixes a quoted string and a bare atom. An
    argument reader that accepted only quoted atoms would push every
    `type/2` fact into the out-of-vocabulary bucket and report a
    vocabulary drift that is not there."""
    census = census_prolog_kb('type("Dino Beltran", person).')
    assert census.ground_person_facts == {"type/2": 1}
    assert census.ground_out_of_vocabulary == {}


# ----------------------------------------------------------------------
# the catalog is the enforcement point
# ----------------------------------------------------------------------


def test_kinship_module_admits_exactly_what_the_source_emits(tmp_path):
    """The narrowing catalog is where the boundary is ENFORCED.

    An edge whose (src_subtype, edge_type, dst_subtype) has no narrowing is
    refused at insert, so a future source that ingested `rules_derived.pl`
    would block the publish rather than quietly win the experiment. The
    assertion is a SET, not a count: a count can report that two numbers
    differ but cannot name the triple that appeared.
    """
    import yaml

    from okg.substrate.catalog.modules_compose import compose_catalog

    deployment = tmp_path / "scratch"
    deployment.mkdir()
    (deployment / "deployment.yaml").write_text(
        yaml.safe_dump(
            {"name": "scratch", "schema_dir": "schemas", "modules": ["kinship"]}
        )
    )
    (deployment / "schemas").mkdir()

    composed = compose_catalog(deployment)
    for subtype in ("person", "person_attribute"):
        assert subtype in composed.subtypes, sorted(composed.subtypes)
        assert composed.subtype_origin[subtype] == "kinship"

    declared = {
        (n.parent_subtype, n.edge_archetype, n.child_subtype)
        for n in composed.narrowings
    }
    expected = {("person", et, "person") for et in ARTICLE_RELATIONS}
    expected.add(("person", "has_attribute", "person_attribute"))
    assert declared == expected

    # ...and nothing composed is admissible, including `parent`/`sibling`,
    # which ARE base predicates in the KB but which no article states and
    # which the questions do use
    for word in DERIVED_VOCABULARY + ("parent", "sibling", "child", "spouse"):
        assert not [t for t in declared if t[1] == word], word


def test_census_emits_nothing():
    census = census_prolog_kb(KB, retain_person_facts=True)
    assert census.refused_clause_count == 7
    assert census.refused_directives == 2
    assert census.ground_person_fact_count == 18
    assert census.ground_registry_facts == {"attribute/1": 2}
    assert census.unparseable_terms == 0
    preds = {pred for pred, _ in census.retained_person_facts}
    assert preds == {"parent", "gender", "dob", "job", "hobby", "friend_", "type"}
