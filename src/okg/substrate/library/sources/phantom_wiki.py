"""PhantomWiki source adapter — generated biographical articles + a refused KB.

Profile: `reference_catalog`.

PhantomWiki (MIT, code AND data) generates a fictional universe from a seed:
one markdown-ish article per person, a Prolog knowledge base of the same
world, and multi-hop QA whose answers are entity SETS. Because a universe
regenerated at a new seed has never existed, contamination is zero by
construction — which is the reason it is in the okg-bench program at all.

WHAT THIS ADAPTER EMITS, and where each fact comes from.

  `person` nodes, one per article, node id `person:<repo>:<name>`.
  `person_attribute` nodes, one per distinct (kind, value) pair, SHARED
      across the people who hold them, node id
      `person_attribute:<repo>:<kind>:<value>`.
  eight family relations + `friend` + `has_attribute` edges.

All of it is read off the ARTICLE PROSE, sentence by sentence, against the
generator's own fixed templates ("The mother of <subject> is <name>.").
A line that matches no template is a REFUSAL, not a skip: the templates are
generated, so an unrecognised sentence means the release changed shape and
the corpus is no longer the thing the contract was measured on.

WHAT THIS ADAPTER REFUSES, which is the whole leakage question.

The released `facts.pl` is not an answer key — it holds the same world the
articles describe — but it is also NOT just ground facts. Measured on a
generated instance: 49,387 ground facts and 67 RULE CLAUSES over 52
predicate heads, all in the one file. Those clauses define
`great_granddaughter`, `female_second_cousin`, `niece`, `mother_in_law` —
the exact vocabulary the QUESTIONS are phrased in — while the articles
state only the eight base relations. Admitting them would hand the graph
arm the reasoning schema the prose arm must infer.

So the KB is read and NOTHING from it is emitted. Every term is classified:

  * a CLAUSE (`Head :- Body`) or a directive (`:- dynamic ...`) is REFUSED
    and counted by head predicate. This is not limited to the
    `rules_derived.pl` half — `rules_base.pl` is inlined into the same dump
    and its `sister/2`, `parent/2`-consuming clauses are refused too, as
    are the SWI-Prolog internals (`file_search_path/2`, `prolog_file_type/2`,
    `pyrun/2`) that `save_all_clauses_to_file` leaks into the dump.
  * a GROUND FACT of a known person predicate is CORROBORATED against what
    the prose already produced. That is strictly stronger than ingesting
    it: it proves the emitted graph carries the release's ground facts
    rather than asserting it. A ground fact the prose does not entail is a
    corpus defect and raises.
  * a ground fact of any other predicate is counted as out-of-vocabulary,
    so an upstream vocabulary change is a stated finding rather than a
    silent ingest.

The refusal is measurable in both directions on a real instance: 67 clauses
refused, 49,387 ground facts corroborated. An "exemption" that held of the
whole file would discriminate nothing.

PACT okg-bench-graph-benchmarks / req.pw.corpus, task.pw.source.
"""
from __future__ import annotations

import hashlib
import sys
from dataclasses import dataclass, field, replace
from pathlib import Path
from typing import Any, Iterator, Optional

from okg.substrate.errors import SubstrateError
from okg.substrate.library.sources.base import (
    EdgeFact,
    EdgeKey,
    NodeFact,
    RecordEmission,
    RecordNode,
    RecordSet,
    SourceRun,
)

__all__ = [
    "ARTICLE_ATTRIBUTE_KINDS",
    "ARTICLE_RELATIONS",
    "KB_PERSON_PREDICATES",
    "KB_REGISTRY_PREDICATES",
    "KbCensus",
    "PhantomWikiSource",
    "census_prolog_kb",
    "split_prolog_terms",
]


#: The eight family relations plus friendship, exactly as PhantomWiki's
#: article generator words them. Keyed by edge type; the two strings are
#: the singular and plural sentence stems the generator substitutes the
#: subject into (`phantom_wiki.facts.family.constants.FAMILY_FACT_TEMPLATES`
#: and `..._PL`, and the friends module's pair).
#:
#: `parent`, `child` and `sibling` are ABSENT because the article generator
#: drops them by construction (`core/article.py` filters them out of
#: FAMILY_RELATION_EASY). They are ground/base predicates in the KB and
#: they appear in questions, so a source that quietly added them would be
#: putting question vocabulary into the graph that no article states.
ARTICLE_RELATIONS: dict[str, tuple[str, str]] = {
    "mother": ("The mother of {subject} is ", "The mothers of {subject} are "),
    "father": ("The father of {subject} is ", "The fathers of {subject} are "),
    "sister": ("The sister of {subject} is ", "The sisters of {subject} are "),
    "brother": ("The brother of {subject} is ", "The brothers of {subject} are "),
    "son": ("The son of {subject} is ", "The sons of {subject} are "),
    "daughter": ("The daughter of {subject} is ", "The daughters of {subject} are "),
    "wife": ("The wife of {subject} is ", "The wives of {subject} are "),
    "husband": ("The husband of {subject} is ", "The husbands of {subject} are "),
    "friend": ("The friend of {subject} is ", "The friends of {subject} are "),
}

#: Attribute sentence stems, keyed by the KB predicate that carries the same
#: fact. The article words `dob` as "date of birth" and `job` as
#: "occupation"; the graph keeps the ARTICLE's wording as the node's `kind`
#: because that is the vocabulary a question uses ("the person whose
#: occupation is ...").
ARTICLE_ATTRIBUTE_KINDS: dict[str, tuple[str, str]] = {
    "dob": ("date of birth", "The date of birth of {subject} is "),
    "job": ("occupation", "The occupation of {subject} is "),
    "hobby": ("hobby", "The hobby of {subject} is "),
    "gender": ("gender", "The gender of {subject} is "),
}

#: KB ground predicates that state a fact ABOUT A PERSON, and must therefore
#: be entailed by what the articles said. `parent/2` is entailed by the
#: union of `mother` and `father`; `friend_/2` by a `friend` edge in either
#: direction (the KB's `friend/2` rule is the symmetric closure of
#: `friend_/2`, and both articles state the pair).
KB_PERSON_PREDICATES: frozenset[tuple[str, int]] = frozenset({
    ("parent", 2), ("gender", 2), ("friend_", 2),
    ("dob", 2), ("job", 2), ("hobby", 2), ("type", 2),
})

#: KB ground predicates that are NOT facts about a person and are therefore
#: counted rather than corroborated. `attribute/1` is the value POOL the
#: generator sampled hobbies and jobs from — it lists values no person in
#: the universe holds, and materialising them would publish attribute nodes
#: nothing points at.
KB_REGISTRY_PREDICATES: frozenset[tuple[str, int]] = frozenset({
    ("attribute", 1),
})

#: Bound on one article's byte size. Generated articles run to a few
#: kilobytes; anything far past that is a different artifact.
ARTICLE_MAX_BYTES = 1_000_000

_SECTION_HEADINGS = ("## Family", "## Friends", "## Attributes")


# ----------------------------------------------------------------------
# Prolog KB census
# ----------------------------------------------------------------------


def split_prolog_terms(text: str) -> list[str]:
    """Split a Prolog source file into terms.

    Quote- and comment-aware: a term ends at a `.` that is followed by
    whitespace or end-of-input, and that is not inside a quoted atom or a
    `%` line comment. Naively splitting on `.\\n` is close but not close
    enough — the census this feeds is the evidence that derived rules were
    refused, and evidence built on an approximate parse is not evidence.
    """
    terms: list[str] = []
    buf: list[str] = []
    quote: str | None = None
    in_comment = False
    i = 0
    n = len(text)
    while i < n:
        ch = text[i]
        if in_comment:
            if ch == "\n":
                in_comment = False
                buf.append(ch)
            i += 1
            continue
        if quote is not None:
            buf.append(ch)
            if ch == "\\" and i + 1 < n:
                buf.append(text[i + 1])
                i += 2
                continue
            if ch == quote:
                quote = None
            i += 1
            continue
        if ch in ("'", '"'):
            quote = ch
            buf.append(ch)
            i += 1
            continue
        if ch == "%":
            in_comment = True
            i += 1
            continue
        if ch == "." and (i + 1 >= n or text[i + 1] in " \t\r\n"):
            term = "".join(buf).strip()
            if term:
                terms.append(term)
            buf = []
            i += 1
            continue
        buf.append(ch)
        i += 1
    tail = "".join(buf).strip()
    if tail:
        terms.append(tail)
    return terms


def _head_of(term: str) -> tuple[str, int] | None:
    """Functor name and arity of a term's head, or None when unparseable."""
    head = term.split(":-", 1)[0].strip() if ":-" in term else term.strip()
    if not head or not (head[0].islower() or head[0] == "_"):
        return None
    idx = 0
    while idx < len(head) and (head[idx].isalnum() or head[idx] == "_"):
        idx += 1
    name = head[:idx]
    if not name:
        return None
    rest = head[idx:].lstrip()
    if not rest.startswith("("):
        return (name, 0)
    return (name, _arity(rest))


def _arity(args: str) -> int:
    """Top-level comma count + 1 inside a leading parenthesised argument
    list. Quote- and nesting-aware so `job(X, "a, b")` reads as arity 2."""
    depth = 0
    quote: str | None = None
    count = 1
    for i, ch in enumerate(args):
        if quote is not None:
            if ch == quote:
                quote = None
            continue
        if ch in ("'", '"'):
            quote = ch
        elif ch in "([{":
            depth += 1
        elif ch in ")]}":
            depth -= 1
            if depth == 0:
                return count
        elif ch == "," and depth == 1:
            count += 1
    return count


def _ground_args(term: str) -> list[str] | None:
    """Argument values of a ground fact, or None when any argument is
    neither a quoted string nor a plain atom.

    Both forms occur in a released instance and BOTH are needed:
    `gender("Dino Beltran", "male")` quotes its value while
    `type("Dino Beltran", person)` does not, and an argument reader that
    accepted only quoted atoms would push every `type/2` fact into the
    out-of-vocabulary bucket — reporting a vocabulary drift that is not
    there, and skipping the corroboration that every named person has an
    article. Deliberately still narrow: compound and variable arguments
    return None and are counted as unrecognised rather than guessed at.
    """
    idx = term.find("(")
    if idx < 0 or not term.rstrip().endswith(")"):
        return None
    body = term.rstrip()[idx + 1: -1]
    args: list[str] = []
    i = 0
    n = len(body)
    while i < n:
        while i < n and body[i] in " \t\r\n":
            i += 1
        if i >= n:
            break
        if body[i] == '"':
            i += 1
            chars: list[str] = []
            while i < n and body[i] != '"':
                if body[i] == "\\" and i + 1 < n:
                    chars.append(body[i + 1])
                    i += 2
                    continue
                chars.append(body[i])
                i += 1
            if i >= n:
                return None
            i += 1
            args.append("".join(chars))
        else:
            start = i
            while i < n and body[i] not in ",()[]{}\"'":
                i += 1
            atom = body[start:i].strip()
            if not atom or not (atom[0].islower() or atom[0].isdigit()):
                return None
            args.append(atom)
        while i < n and body[i] in " \t\r\n":
            i += 1
        if i < n:
            if body[i] != ",":
                return None
            i += 1
    return args


@dataclass
class KbCensus:
    """What a Prolog KB contains, split at the clause/ground-fact line.

    `refused_clause_heads` and the ground counters are keyed `name/arity`.
    Nothing in this object is emitted; it is the evidence that the
    derived-rule boundary held.
    """

    refused_clause_heads: dict[str, int] = field(default_factory=dict)
    refused_directives: int = 0
    ground_person_facts: dict[str, int] = field(default_factory=dict)
    ground_registry_facts: dict[str, int] = field(default_factory=dict)
    ground_out_of_vocabulary: dict[str, int] = field(default_factory=dict)
    unparseable_terms: int = 0
    #: retained ground person facts, only when `retain_person_facts=True`.
    #: A universe large enough to exceed a context window holds millions of
    #: them, so the default is to stream past and count.
    retained_person_facts: list[tuple[str, tuple[str, ...]]] = field(
        default_factory=list
    )

    @property
    def refused_clause_count(self) -> int:
        return sum(self.refused_clause_heads.values())

    @property
    def ground_person_fact_count(self) -> int:
        return sum(self.ground_person_facts.values())

    @property
    def ground_fact_count(self) -> int:
        return (
            self.ground_person_fact_count
            + sum(self.ground_registry_facts.values())
            + sum(self.ground_out_of_vocabulary.values())
        )


def census_prolog_kb(
    text: str,
    *,
    on_person_fact: Any = None,
    retain_person_facts: bool = False,
) -> KbCensus:
    """Classify every term of a Prolog KB. Emits nothing.

    A term is refused when it is a clause or a directive; the head
    predicate is recorded so the refusal can be shown to have covered the
    named vocabulary rather than merely to have been nonzero.

    `on_person_fact(pred, args)` is called for each ground fact about a
    person, so the caller can corroborate it against the graph it built
    without the census holding millions of tuples.
    """
    census = KbCensus()
    for term in split_prolog_terms(text):
        if term.startswith(":-") or term.startswith("?-"):
            census.refused_directives += 1
            continue
        head = _head_of(term)
        if head is None:
            census.unparseable_terms += 1
            continue
        key = f"{head[0]}/{head[1]}"
        if ":-" in term:
            census.refused_clause_heads[key] = (
                census.refused_clause_heads.get(key, 0) + 1
            )
            continue
        if head in KB_PERSON_PREDICATES:
            args = _ground_args(term)
            if args is None or len(args) != head[1]:
                census.ground_out_of_vocabulary[key] = (
                    census.ground_out_of_vocabulary.get(key, 0) + 1
                )
                continue
            census.ground_person_facts[key] = (
                census.ground_person_facts.get(key, 0) + 1
            )
            if retain_person_facts:
                census.retained_person_facts.append((head[0], tuple(args)))
            if on_person_fact is not None:
                on_person_fact(head[0], tuple(args))
            continue
        if head in KB_REGISTRY_PREDICATES:
            census.ground_registry_facts[key] = (
                census.ground_registry_facts.get(key, 0) + 1
            )
            continue
        census.ground_out_of_vocabulary[key] = (
            census.ground_out_of_vocabulary.get(key, 0) + 1
        )
    return census


# ----------------------------------------------------------------------
# article parsing
# ----------------------------------------------------------------------


@dataclass
class _Article:
    """One parsed article, before any fact is emitted."""

    name: str
    path: str
    #: edge_type -> target names, in the order the article states them
    relations: dict[str, list[str]]
    #: article-worded kind -> value
    attributes: dict[str, str]

    @property
    def relation_line_count(self) -> int:
        return sum(1 for targets in self.relations.values() if targets)


def _split_targets(tail: str) -> list[str]:
    """Split a relation sentence's object list. The generator joins names
    with ", " and terminates with a period."""
    body = tail.strip()
    if body.endswith("."):
        body = body[:-1]
    return [part.strip() for part in body.split(",") if part.strip()]


class _Corroborator:
    """Checks each KB ground fact against what the articles already said.

    Nothing is INGESTED from the KB. Corroboration is strictly stronger
    than ingestion for this corpus: it proves the graph built from prose
    carries every ground fact the release states, so the two arms are
    reading one world. A ground fact the prose does not entail is a
    parsing regression, and refusing is the only honest response — a count
    would let the graph arm quietly read a smaller world than the prose arm.
    """

    __slots__ = ("_by_name", "_parents", "_friends", "_adapter", "unstated")

    def __init__(self, by_name: dict[str, _Article], *, adapter: str) -> None:
        self._by_name = by_name
        self._adapter = adapter
        self.unstated: list[str] = []
        self._parents: set[tuple[str, str]] = set()
        self._friends: set[tuple[str, str]] = set()
        for article in by_name.values():
            for target in article.relations.get("mother", []):
                self._parents.add((article.name, target))
            for target in article.relations.get("father", []):
                self._parents.add((article.name, target))
            for target in article.relations.get("friend", []):
                self._friends.add((article.name, target))
                self._friends.add((target, article.name))

    def __call__(self, pred: str, args: tuple[str, ...]) -> None:
        if len(self.unstated) >= 5:
            return
        if pred == "parent":
            ok = (args[0], args[1]) in self._parents
        elif pred == "friend_":
            ok = (args[0], args[1]) in self._friends
        elif pred == "type":
            ok = args[0] in self._by_name
        else:
            kind = ARTICLE_ATTRIBUTE_KINDS.get(pred, (pred, ""))[0]
            article = self._by_name.get(args[0])
            ok = article is not None and article.attributes.get(kind) == args[1]
        if not ok:
            rendered = ", ".join(f'"{a}"' for a in args)
            self.unstated.append(f"{pred}({rendered})")

    def raise_if_unstated(self) -> None:
        if not self.unstated:
            return
        raise SubstrateError(
            f"source {self._adapter!r}: the released knowledge base states "
            f"ground facts the articles do not: {self.unstated}",
            data={"adapter": self._adapter, "examples": self.unstated},
            fix_hint=(
                "the prose corpus must carry every ground fact the KB "
                "states, or the graph arm and the prose arm are reading "
                "different worlds. Check the article templates against "
                "ARTICLE_RELATIONS / ARTICLE_ATTRIBUTE_KINDS"
            ),
        )


@dataclass
class PhantomWikiSource:
    """PhantomWiki adapter over one GENERATED instance.

    Pass all of:
    - `repo` — the corpus identity, namespacing node ids.
    - `articles_dir` — the instance's `articles/` directory. Only
      `<Name>.txt` is read; the generator's `<Name>_facts.txt` sidecars are
      the same content in Prolog form and are skipped WITH A COUNT, because
      reading both would double-encode every fact.
    - `kb_facts_path` — the instance's `facts.pl`. Read, censused, and
      never emitted. It is deliberately NOT expected to live inside the
      arm-visible snapshot: a snapshot holding the KB would give the prose
      arm the structured encoding that is the treatment under test.
    - the generation triple: `universe_seed`, `universe_family_trees`,
      `universe_max_family_tree_size`, `universe_question_depth`. A
      universe that cannot be regenerated from its recorded parameters is
      not a corpus, so these are required rather than defaulted.
    """

    name: str = "phantom_wiki"
    profile: str = "reference_catalog"
    repo: Optional[str] = None
    articles_dir: Optional[Path | str] = None
    kb_facts_path: Optional[Path | str] = None
    universe_seed: Optional[int] = None
    universe_family_trees: Optional[int] = None
    universe_max_family_tree_size: Optional[int] = None
    universe_question_depth: Optional[int] = None

    last_run_stats: dict[str, Any] = field(default_factory=dict, init=False)

    # Incremental-source contract (issue #323): a frozen generated instance
    # -> the content-hash floor over the article bytes and the KB bytes.
    change_probe_kind = "content_hash"

    def __post_init__(self) -> None:
        self.change_probe = self._build_change_probe()
        # The run consumes exactly the hashed bytes; a matching token
        # proves identical emission.
        self.probe_short_circuit_safe = True
        self._unresolved = 0
        self._no_relations = 0

    # ---------- change probe ----------

    def _build_change_probe(self) -> Any:
        from okg.substrate.library.sources.content_hash_probe import (
            ContentHashProbe,
        )
        from okg.substrate.library.sources.emission_identity import (
            emission_identity_targets,
        )

        def _items() -> list[tuple[str, Path]]:
            items: list[tuple[str, Path]] = []
            if self.articles_dir is not None:
                root = Path(self.articles_dir)
                if root.is_dir():
                    items.extend(
                        (str(p), p) for p in sorted(self._article_paths(root))
                    )
            if self.kb_facts_path is not None:
                kb = Path(self.kb_facts_path)
                if kb.is_file():
                    items.append((str(kb), kb))
            return items

        return ContentHashProbe(
            content_items=_items,
            config={
                "repo": self.repo,
                "articles_dir": (
                    str(self.articles_dir) if self.articles_dir is not None else None
                ),
                "kb_facts_path": (
                    str(self.kb_facts_path)
                    if self.kb_facts_path is not None else None
                ),
                "universe_seed": self.universe_seed,
                "universe_family_trees": self.universe_family_trees,
                "universe_max_family_tree_size": self.universe_max_family_tree_size,
                "universe_question_depth": self.universe_question_depth,
            },
            emit_targets=emission_identity_targets(
                self.name,
                sys.modules[__name__],
            ),
        )

    # ---------- identity ----------

    @property
    def universe_id(self) -> str:
        """Digest of the full generation triple.

        Two corpora sharing this value were generated identically. It
        OVER-discriminates, and the measurement says so: regenerating at
        `--question-depth 10` instead of 20, seed and sizes held, produced
        a BYTE-IDENTICAL article corpus (100,628 files, same sha256 over
        the ordered set). Question depth is a QA-budget knob and does not
        perturb the world, so two corpora with different ids can still be
        the same world with different question sets. The world itself is a
        function of (seed, trees, tree size), all three carried separately
        on every node — which is what makes NESTING detectable: two corpora
        at one seed and different sizes begin with the same people and
        identical family sections.
        """
        payload = (
            f"seed={self.universe_seed};trees={self.universe_family_trees};"
            f"size={self.universe_max_family_tree_size};"
            f"depth={self.universe_question_depth}"
        )
        return hashlib.sha256(payload.encode("utf-8")).hexdigest()[:16]

    def person_node_id(self, name: str) -> str:
        return f"person:{self.repo}:{name}"

    def attribute_node_id(self, kind: str, value: str) -> str:
        return f"person_attribute:{self.repo}:{kind}:{value}"

    # ---------- run ----------

    def run(
        self,
        run_id: str,
        *,
        mode: str = "scope_complete",
        cursor: Optional[dict[str, Any]] = None,
    ) -> SourceRun:
        self._require_config()
        articles_root = Path(self.articles_dir)  # type: ignore[arg-type]
        kb_path = Path(self.kb_facts_path)  # type: ignore[arg-type]

        articles, sidecars_skipped = self._parse_articles(articles_root)
        by_name = {article.name: article for article in articles}
        # The KB is read AFTER the articles so each of its ground facts can
        # be corroborated as it streams past, without holding millions of
        # tuples for a universe sized to exceed a context window.
        kb_text = self._read_text(kb_path, "kb_facts_path")
        checker = _Corroborator(by_name, adapter=self.name)
        census = census_prolog_kb(kb_text, on_person_fact=checker)
        checker.raise_if_unstated()

        completed_scope = mode in {"reconcile", "scope_complete", "release_new"}
        # TWO PASSES over `_emit`, never a materialised list. The list this
        # replaces held every fact resident — ~1.1M objects on a 100k-person
        # universe — while the comment above it claimed the opposite. Measured
        # cost of that: the build scales linearly to ~5k articles and turns
        # over to roughly n^1.27 beyond it, which is what allocation and GC
        # pressure look like.
        #
        # A second pass is cheap and safe: `_emit` is a generator over
        # `articles`, which is already parsed and in memory, so re-running it
        # re-reads nothing from disk. It resets its own counters at entry and
        # is deterministic over the same inputs, so the stats captured below
        # are the same either pass. The record set is unavoidably O(people) —
        # that is what it IS — but it no longer sits alongside a full copy of
        # the fact stream.
        record_set = (
            self._record_set_from_facts(self._emit(articles, by_name))
            if completed_scope else None
        )

        self.last_run_stats = {
            "articles": len(articles),
            "sidecar_files_skipped": sidecars_skipped,
            "kb_clauses_refused": census.refused_clause_count,
            "kb_directives_refused": census.refused_directives,
            "kb_refused_clause_heads": dict(
                sorted(census.refused_clause_heads.items())
            ),
            "kb_ground_person_facts_corroborated":
                census.ground_person_fact_count,
            "kb_ground_person_facts_by_predicate":
                dict(sorted(census.ground_person_facts.items())),
            "kb_ground_registry_facts": dict(census.ground_registry_facts),
            "kb_ground_out_of_vocabulary": dict(census.ground_out_of_vocabulary),
            "kb_unparseable_terms": census.unparseable_terms,
            # None, NOT 0, when no pass has run yet. These two counters are
            # written inside `_emit`, and `_emit` has run by this point only
            # on the record-set path above. Under `cursor` / `scope_incomplete`
            # / `release_unchanged` they would otherwise be reported as their
            # __init__ zeros -- and "0 unresolved relation targets" is exactly
            # what a clean run looks like, so a never-measured counter would
            # read as a measured, perfect one. `_stream` fills them in below
            # once the emitting pass has actually happened.
            "unresolved_relation_targets": (
                self._unresolved if record_set is not None else None
            ),
            "people_without_relations": (
                self._no_relations if record_set is not None else None
            ),
            "universe_id": self.universe_id,
            "universe_seed": self.universe_seed,
            "universe_family_trees": self.universe_family_trees,
            "universe_max_family_tree_size": self.universe_max_family_tree_size,
            "universe_question_depth": self.universe_question_depth,
        }

        def _stream() -> Iterator[NodeFact | EdgeFact]:
            # The emitting pass. Nothing is held: the runner consumes each
            # fact and drops it.
            yield from self._emit(articles, by_name)
            # `_emit` has now run, so the two counters it writes are real.
            # Publish them, replacing the `None` placeholders above — an
            # operator reading `source_stats` after the run gets a measured
            # number instead of a never-measured zero.
            self.last_run_stats["unresolved_relation_targets"] = self._unresolved
            self.last_run_stats["people_without_relations"] = self._no_relations

        return SourceRun(
            facts=_stream(),
            completed_scope=completed_scope,
            applied_raw_token=self.change_probe.token_from_item_hashes(
                self._item_hashes(articles_root, kb_path)
            ),
            applied_token_scope="global",
            applied_token_authority="exact",
            run_mode=mode,
            record_set=record_set,
        )

    # ---------- config ----------

    def _require_config(self) -> None:
        missing = [
            key for key in (
                "repo", "articles_dir", "kb_facts_path", "universe_seed",
                "universe_family_trees", "universe_max_family_tree_size",
                "universe_question_depth",
            )
            if getattr(self, key) is None
        ]
        if missing:
            raise SubstrateError(
                f"source {self.name!r}: missing required parameters {missing}",
                data={"adapter": self.name, "missing": missing},
                fix_hint=(
                    "pass repo, articles_dir, kb_facts_path and the full "
                    "generation triple (universe_seed, universe_family_trees, "
                    "universe_max_family_tree_size, universe_question_depth); "
                    "a universe that cannot be regenerated from its recorded "
                    "parameters is not a corpus"
                ),
            )
        root = Path(self.articles_dir)  # type: ignore[arg-type]
        if not root.is_dir():
            raise SubstrateError(
                f"source {self.name!r}: articles_dir {root} is not a directory",
                data={"adapter": self.name, "articles_dir": str(root)},
                fix_hint="point articles_dir at the generated instance's "
                         "articles/ directory",
            )
        kb = Path(self.kb_facts_path)  # type: ignore[arg-type]
        if not kb.is_file():
            raise SubstrateError(
                f"source {self.name!r}: kb_facts_path {kb} is not a file",
                data={"adapter": self.name, "kb_facts_path": str(kb)},
                fix_hint="point kb_facts_path at the generated instance's "
                         "facts.pl",
            )
        try:
            kb.resolve().relative_to(root.resolve())
        except ValueError:
            return
        raise SubstrateError(
            f"source {self.name!r}: kb_facts_path {kb} sits INSIDE the "
            f"article directory {root}",
            data={"adapter": self.name, "kb_facts_path": str(kb)},
            fix_hint=(
                "keep facts.pl out of the arm-visible article snapshot; a "
                "snapshot holding the KB gives the prose arm the structured "
                "encoding that is the treatment under test"
            ),
        )

    # ---------- read ----------

    @staticmethod
    def _article_paths(root: Path) -> list[Path]:
        return [
            p for p in root.iterdir()
            if p.is_file() and p.suffix == ".txt"
            and not p.name.endswith("_facts.txt")
        ]

    def _item_hashes(self, root: Path, kb: Path) -> dict[str, str]:
        items = {
            str(p): hashlib.sha256(p.read_bytes()).hexdigest()
            for p in sorted(self._article_paths(root))
        }
        items[str(kb)] = hashlib.sha256(kb.read_bytes()).hexdigest()
        return items

    def _read_text(self, path: Path, label: str) -> str:
        try:
            raw = path.read_bytes()
        except OSError as exc:
            raise SubstrateError(
                f"source {self.name!r}: {label} {path} could not be read",
                data={"adapter": self.name, label: str(path)},
                fix_hint=f"make {label} readable",
            ) from exc
        try:
            return raw.decode("utf-8")
        except UnicodeDecodeError as exc:
            raise SubstrateError(
                f"source {self.name!r}: {path} is not UTF-8 text",
                data={"adapter": self.name, label: str(path)},
                fix_hint="regenerate the instance",
            ) from exc

    # ---------- parse ----------

    def _parse_articles(self, root: Path) -> tuple[list[_Article], int]:
        paths = sorted(self._article_paths(root))
        sidecars = sum(
            1 for p in root.iterdir()
            if p.is_file() and p.name.endswith("_facts.txt")
        )
        if not paths:
            raise SubstrateError(
                f"source {self.name!r}: {root} contains no article files",
                data={"adapter": self.name, "articles_dir": str(root)},
                fix_hint="check the generation wrote articles/<Name>.txt",
            )
        articles: list[_Article] = []
        seen: set[str] = set()
        for path in paths:
            if path.stat().st_size > ARTICLE_MAX_BYTES:
                raise SubstrateError(
                    f"source {self.name!r}: article {path.name} is "
                    f"{path.stat().st_size} bytes, past the "
                    f"{ARTICLE_MAX_BYTES}-byte bound",
                    data={"adapter": self.name, "article": str(path)},
                    fix_hint="a generated article is a few kilobytes; this "
                             "is a different artifact",
                )
            article = self._parse_article(
                self._read_text(path, "article"),
                rel_path=f"{root.name}/{path.name}",
                file_name=path.name,
            )
            if article.name in seen:
                raise SubstrateError(
                    f"source {self.name!r}: two articles name "
                    f"{article.name!r}",
                    data={"adapter": self.name, "name": article.name},
                    fix_hint="regenerate without --duplicate-names; a "
                             "universe with two people of one name has no "
                             "stable person identity",
                )
            seen.add(article.name)
            articles.append(article)
        return articles, sidecars

    def _parse_article(
        self, text: str, *, rel_path: str, file_name: str
    ) -> _Article:
        lines = [ln.strip() for ln in text.splitlines()]
        if not lines or not lines[0].startswith("# "):
            raise SubstrateError(
                f"source {self.name!r}: {rel_path} does not open with a "
                "'# <name>' heading",
                data={"adapter": self.name, "article": rel_path},
                fix_hint="the article template is generated; an article "
                         "without its heading means the release changed shape",
            )
        name = lines[0][2:].strip()
        if not name:
            raise SubstrateError(
                f"source {self.name!r}: {rel_path} has an empty name heading",
                data={"adapter": self.name, "article": rel_path},
                fix_hint="regenerate the instance",
            )
        stem = file_name[: -len(".txt")]
        if stem != name:
            raise SubstrateError(
                f"source {self.name!r}: {rel_path} names {name!r} but its "
                f"file stem is {stem!r}",
                data={"adapter": self.name, "article": rel_path},
                fix_hint="the article path is the person's identity; a "
                         "mismatch means the file cannot be addressed from "
                         "the node id",
            )

        relations: dict[str, list[str]] = {}
        attributes: dict[str, str] = {}
        for line in lines[1:]:
            if not line or line in _SECTION_HEADINGS:
                continue
            if self._consume_relation(line, name, relations):
                continue
            if self._consume_attribute(line, name, attributes):
                continue
            raise SubstrateError(
                f"source {self.name!r}: {rel_path} states a sentence no "
                f"generated template produces: {line!r}",
                data={"adapter": self.name, "article": rel_path, "line": line},
                fix_hint=(
                    "the article templates are fixed by the generator, so an "
                    "unrecognised sentence means the release changed shape "
                    "and the corpus is no longer what the contract was "
                    "measured on. Refused rather than skipped: a skipped "
                    "sentence is a relation silently missing from the graph"
                ),
            )
        return _Article(
            name=name, path=rel_path, relations=relations, attributes=attributes,
        )

    def _consume_relation(
        self, line: str, subject: str, out: dict[str, list[str]]
    ) -> bool:
        for edge_type, (singular, plural) in ARTICLE_RELATIONS.items():
            for stem in (plural.format(subject=subject),
                         singular.format(subject=subject)):
                if line.startswith(stem):
                    targets = _split_targets(line[len(stem):])
                    if not targets:
                        raise SubstrateError(
                            f"source {self.name!r}: {subject!r} states a "
                            f"{edge_type} sentence with no object: {line!r}",
                            data={"adapter": self.name, "line": line},
                            fix_hint="regenerate the instance",
                        )
                    out.setdefault(edge_type, []).extend(targets)
                    return True
        return False

    def _consume_attribute(
        self, line: str, subject: str, out: dict[str, str]
    ) -> bool:
        for _pred, (kind, template) in ARTICLE_ATTRIBUTE_KINDS.items():
            stem = template.format(subject=subject)
            if line.startswith(stem):
                value = line[len(stem):].strip()
                if value.endswith("."):
                    value = value[:-1]
                value = value.strip()
                if not value:
                    raise SubstrateError(
                        f"source {self.name!r}: {subject!r} states an empty "
                        f"{kind}: {line!r}",
                        data={"adapter": self.name, "line": line},
                        fix_hint="regenerate the instance",
                    )
                if kind in out and out[kind] != value:
                    raise SubstrateError(
                        f"source {self.name!r}: {subject!r} states two "
                        f"different values for {kind}",
                        data={"adapter": self.name, "kind": kind},
                        fix_hint="regenerate the instance",
                    )
                out[kind] = value
                return True
        return False

    # ---------- emit ----------

    def _emit(
        self, articles: list[_Article], by_name: dict[str, _Article]
    ) -> Iterator[NodeFact | EdgeFact]:
        repo = self.repo
        revision = {
            "repo": repo,
            "universe_id": self.universe_id,
            "universe_seed": self.universe_seed,
            "universe_family_trees": self.universe_family_trees,
            "universe_max_family_tree_size": self.universe_max_family_tree_size,
            "universe_question_depth": self.universe_question_depth,
        }
        self._unresolved = 0
        self._no_relations = 0
        emitted_attributes: set[str] = set()

        for article in articles:
            record_id = {"repo": repo, "name": article.name}
            attrs: dict[str, Any] = {
                "name": article.name,
                "article_path": article.path,
                "universe_id": self.universe_id,
                "universe_seed": self.universe_seed,
                "universe_family_trees": self.universe_family_trees,
                "universe_max_family_tree_size":
                    self.universe_max_family_tree_size,
                "universe_question_depth": self.universe_question_depth,
                "relation_line_count": article.relation_line_count,
            }
            # The stranding stamp is computed from what the ARTICLE STATES —
            # zero relation sentences — and never from whether an edge came
            # out. A stamp read off emission would be true exactly when the
            # node is isolated, which forgives the parsing regression the
            # isolation clause exists to catch.
            if article.relation_line_count == 0:
                attrs["no_relations_reason"] = "article_states_no_relations"
                self._no_relations += 1
            yield NodeFact(
                node_id=self.person_node_id(article.name),
                subtype="person",
                attrs=attrs,
                source_record_id=record_id,
                source_revision=revision,
                op="I",
            )
            for kind, value in sorted(article.attributes.items()):
                node_id = self.attribute_node_id(kind, value)
                if node_id not in emitted_attributes:
                    emitted_attributes.add(node_id)
                    yield NodeFact(
                        node_id=node_id,
                        subtype="person_attribute",
                        attrs={"kind": kind, "value": value},
                        source_record_id={
                            "repo": repo, "kind": kind, "value": value,
                        },
                        source_revision=revision,
                        op="I",
                    )

        for article in articles:
            record_id = {"repo": repo, "name": article.name}
            src = self.person_node_id(article.name)
            for edge_type in ARTICLE_RELATIONS:
                for target in article.relations.get(edge_type, []):
                    if target not in by_name:
                        # Skipped and counted, never stubbed and never
                        # dangled: a person node invented for a name no
                        # article describes would sit in the retrieval
                        # candidate pool the benchmark scores.
                        self._unresolved += 1
                        continue
                    yield EdgeFact(
                        src=src,
                        dst=self.person_node_id(target),
                        edge_type=edge_type,
                        attrs={},
                        provenance="explicit",
                        source_record_id=record_id,
                        source_revision=revision,
                        op="I",
                    )
            for kind, value in sorted(article.attributes.items()):
                yield EdgeFact(
                    src=src,
                    dst=self.attribute_node_id(kind, value),
                    edge_type="has_attribute",
                    attrs={"kind": kind},
                    provenance="explicit",
                    source_record_id=record_id,
                    source_revision=revision,
                    op="I",
                )

    # ---------- record set ----------

    def _record_set_from_facts(
        self, facts: Iterator[NodeFact | EdgeFact],
    ) -> RecordSet:
        """One logical record per person, plus one per attribute value.

        Attribute values are universe-level vocabulary shared across people,
        so they carry their own record; that is what lets the runner retract
        an occupation when the last person holding it leaves the universe.
        """
        records: dict[str, RecordEmission] = {}

        def _put(key: str, *, primary: Optional[RecordNode] = None) -> None:
            existing = records.get(key)
            if existing is None:
                records[key] = RecordEmission(primary=primary)
            elif primary is not None and existing.primary is None:
                records[key] = replace(existing, primary=primary)

        for fact in facts:
            if isinstance(fact, NodeFact):
                _put(
                    fact.node_id,
                    primary=RecordNode(
                        node_id=fact.node_id,
                        subtype=fact.subtype,
                        source_record_id=fact.source_record_id,
                        source_revision=fact.source_revision,
                    ),
                )
                continue
            key = self.person_node_id(fact.source_record_id["name"])
            _put(key)
            records[key].edges.append(
                EdgeKey(
                    src=fact.src,
                    dst=fact.dst,
                    edge_type=fact.edge_type,
                    attrs=fact.attrs,
                    provenance=fact.provenance,
                    confidence=fact.confidence,
                    source_record_id=fact.source_record_id,
                    source_revision=fact.source_revision,
                )
            )
        return RecordSet(records=records)
