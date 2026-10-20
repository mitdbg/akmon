"""Deployment-declared concept groups for lexical query expansion (#1218).

A deployment declares, under `search.concept_groups:`, closed groups of terms
naming one concept in different vocabularies -- a word, a macro, a unit.
`search(method="lexical")` expands a query with the other members of any group
a query token names. Exactness: every comparison is whole-token equality
against a term the deployment WROTE DOWN, nothing stemmed and no pattern
interpreted, so `\\lat_p99` never reaches a group declaring `\\lat`. Exact
beats expanded: `expansion_rank` marks rows found only through an added term
and `_search_lexical` sorts on it before the BM25 score.

Groups live in the manifest `search:` block, not `aliases.json`: `okg.aliases`
maps an alias to a canonical NODE ID, naming an entity, while groups map a term
to other TERMS. They are retrieval vocabulary, and `search:` is where this
deployment's other retrieval controls already are.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Iterable, Mapping, Sequence

from okg.substrate.errors import SubstrateError


__all__ = [
    "ConceptCanonicalShadowed",
    "ConceptGroup",
    "ConceptGroupConfigInvalid",
    "ConceptTermDuplicated",
    "ExpandedQuery",
    "expand_query",
    "expansion_rank",
    "parse_concept_groups",
    "tokenize",
]


class ConceptGroupConfigInvalid(SubstrateError):
    """A `search.concept_groups:` declaration has the wrong shape, no variants,
    a non-string term, or a term carrying glob syntax."""

    code = "concept_group_config_invalid"


class ConceptTermDuplicated(SubstrateError):
    """One term is declared in two groups (or twice in one). Refused rather
    than resolved by order: which group won would depend on mapping order."""

    code = "concept_term_duplicated"


class ConceptCanonicalShadowed(SubstrateError):
    """A group declares a variant that is another group's canonical. The
    canonical is the name the envelope reports, so a shadowed one would
    attribute an expansion to a group that did not cause it."""

    code = "concept_canonical_shadowed"


#: Characters that make a term a PATTERN rather than a string. Only these two:
#: `^`, `_`, `\`, `{`, `}`, `+`, `|` and `$` are ordinary LaTeX notation, so
#: banning regex metacharacters wholesale would refuse most of the vocabulary
#: this exists to serve.
_GLOB_CHARS = ("*", "?")

#: Stripped from a token's ends. Sentence punctuation only -- parentheses and
#: brackets carry meaning in notation (`x[i]`), so stripping them would turn
#: one term into another.
_EDGE_PUNCTUATION = ",;:.!?\"'"

_CONCEPT_BLOCK_KEYS = frozenset({"variants"})


def _is_notation(term: str) -> bool:
    """Whether `term` is notation (case-SENSITIVE) or a plain word: notation as
    soon as it carries a non-ASCII-letter, non-hyphen character. `Q_s` and `q_s`
    are different quantities; `Throughput` and `throughput` are one word."""
    return any(
        not (character.isascii() and (character.isalpha() or character == "-"))
        for character in term
    )


def _key(term: str) -> str:
    """The comparison key: the term itself for notation, casefolded for a word."""
    return term if _is_notation(term) else term.casefold()


def _phrase_key(term: str) -> str:
    """The index key for a possibly multi-word term."""
    return " ".join(_key(part) for part in tokenize(term))


def tokenize(text: Any) -> tuple[str, ...]:
    """Whitespace-delimited tokens, sentence punctuation trimmed off the ends.

    Whitespace ALONE is the point: splitting on punctuation would cut
    `q_s^\\max` into three tokens, and a declared string must match only itself."""
    if not isinstance(text, str):
        return ()
    tokens = []
    for raw in text.split():
        token = raw.strip(_EDGE_PUNCTUATION)
        if token:
            tokens.append(token)
    return tuple(tokens)


@dataclass(frozen=True)
class ConceptGroup:
    """One declared group: a canonical term and its closed list of variants."""

    canonical: str
    variants: tuple[str, ...]

    @property
    def members(self) -> tuple[str, ...]:
        """Canonical first, then variants as declared -- the order the envelope
        reports, so an expansion is reproducible from the manifest."""
        return (self.canonical, *self.variants)


@dataclass(frozen=True)
class ExpandedQuery:
    """The query a lexical search actually ranked, and why it differs. `query`
    is the string handed to BM25, IDENTICAL to the caller's when `applied` is
    false; `original_terms` are the caller's own tokens, which decide which
    rows are exact (rank 0)."""

    query: str
    applied: bool
    groups: tuple[Mapping[str, Any], ...] = ()
    original_terms: tuple[str, ...] = ()
    reason: str | None = None

    def disclosure(self) -> dict[str, Any]:
        """The always-present `concept_expansion` envelope block."""
        return {
            "applied": self.applied,
            "groups": [dict(group) for group in self.groups],
            "expanded_query": self.query if self.applied else None,
            "reason": self.reason,
        }


def parse_concept_groups(raw: Any, *, where: str = "") -> tuple[ConceptGroup, ...]:
    """Validate `search.concept_groups:`; absent or empty parses to `()`.

    Accepted shape is a mapping of canonical term to its variant list, or to
    `{variants: [...]}` -- `throughput: ["\\tput", "items/s"]`.
    """
    if raw is None:
        return ()
    if not isinstance(raw, Mapping):
        raise ConceptGroupConfigInvalid(
            f"deployment.yaml `search.concept_groups` must be a mapping of "
            f"canonical term -> variants; got {type(raw).__name__}.{where}",
            data={"got": type(raw).__name__},
            fix_hint="declare `search.concept_groups` as canonical -> [variants]",
        )
    groups = tuple(
        _parse_group(canonical, body, where=where) for canonical, body in raw.items()
    )
    _assert_terms_are_disjoint(groups, where=where)
    return groups


def _parse_group(canonical: Any, body: Any, *, where: str) -> ConceptGroup:
    canonical = _validated_term(canonical, role="canonical", where=where)
    if isinstance(body, Mapping):
        unknown = sorted(set(body) - _CONCEPT_BLOCK_KEYS, key=repr)
        if unknown:
            raise ConceptGroupConfigInvalid(
                f"deployment.yaml `search.concept_groups.{canonical}` has "
                f"unknown key(s) {', '.join(repr(k) for k in unknown)}; "
                f"accepted keys are {sorted(_CONCEPT_BLOCK_KEYS)}.{where}",
                data={
                    "canonical": canonical,
                    "unknown_keys": unknown,
                    "accepted_keys": sorted(_CONCEPT_BLOCK_KEYS),
                },
                fix_hint=f"fix the key spelling under `{canonical}`",
            )
        raw_variants = body.get("variants")
    else:
        raw_variants = body
    if not isinstance(raw_variants, Sequence) or isinstance(raw_variants, str):
        raise ConceptGroupConfigInvalid(
            f"deployment.yaml `search.concept_groups.{canonical}` must list "
            f"its variants; got {type(raw_variants).__name__}.{where}",
            data={"canonical": canonical, "got": type(raw_variants).__name__},
            fix_hint=f"declare `{canonical}: [variant, ...]`",
        )
    variants = tuple(
        _validated_term(value, role="variant", where=where, canonical=canonical)
        for value in raw_variants
    )
    if not variants:
        # A group of one expands nothing. Refused rather than kept as a no-op:
        # the author believed they had declared a synonym.
        raise ConceptGroupConfigInvalid(
            f"deployment.yaml `search.concept_groups.{canonical}` declares no "
            f"variants, so it can never expand a query.{where}",
            data={"canonical": canonical},
            fix_hint=f"list at least one variant under `{canonical}`, or drop it",
        )
    seen: dict[str, str] = {}
    for term in (canonical, *variants):
        key = _phrase_key(term)
        previous = seen.get(key)
        if previous is None:
            seen[key] = term
            continue
        raise ConceptTermDuplicated(
            f"deployment.yaml `search.concept_groups.{canonical}` declares "
            f"{term!r} twice (matching {previous!r}).{where}",
            data={"canonical": canonical, "term": term, "duplicate_of": previous},
            fix_hint=f"remove the repeated term from `{canonical}`",
        )
    return ConceptGroup(canonical=canonical, variants=variants)


def _validated_term(
    value: Any,
    *,
    role: str,
    where: str,
    canonical: str | None = None,
) -> str:
    owner = (
        f"`search.concept_groups.{canonical}`"
        if canonical
        else "`search.concept_groups`"
    )
    if not isinstance(value, str) or not value.strip():
        raise ConceptGroupConfigInvalid(
            f"deployment.yaml {owner} {role} must be a non-empty string; got "
            f"{value!r}.{where}",
            data={"role": role, "value": repr(value), "canonical": canonical},
            fix_hint=f"write the {role} as a quoted, non-empty string",
        )
    term = value.strip()
    if not tokenize(term):
        raise ConceptGroupConfigInvalid(
            "a concept term must contain a searchable token",
            data={"role": role, "term": term, "canonical": canonical},
            fix_hint="declare a term containing more than sentence punctuation",
        )
    globs = sorted({character for character in _GLOB_CHARS if character in term})
    if globs:
        raise ConceptGroupConfigInvalid(
            f"deployment.yaml {owner} {role} {term!r} carries glob syntax "
            f"{globs}; concept terms match by exact string equality and no "
            f"pattern is interpreted.{where}",
            data={
                "role": role,
                "term": term,
                "unsupported": globs,
                "canonical": canonical,
            },
            fix_hint="list each literal term instead of a pattern",
        )
    return term


def _assert_terms_are_disjoint(
    groups: Sequence[ConceptGroup],
    *,
    where: str,
) -> None:
    """No term may belong to two groups, and no variant may be a canonical."""
    canonicals = {_phrase_key(group.canonical): group.canonical for group in groups}
    owner: dict[str, str] = {}
    for group in groups:
        for term in group.members:
            key = _phrase_key(term)
            shadowed = canonicals.get(key)
            if shadowed is not None and term != group.canonical:
                raise ConceptCanonicalShadowed(
                    f"deployment.yaml `search.concept_groups.{group.canonical}` "
                    f"declares variant {term!r}, which is the canonical of "
                    f"another group ({shadowed!r}).{where}",
                    data={
                        "canonical": group.canonical,
                        "variant": term,
                        "shadowed_canonical": shadowed,
                    },
                    fix_hint=(
                        f"merge the two groups, or rename the variant in "
                        f"`{group.canonical}`"
                    ),
                )
            previous = owner.setdefault(key, group.canonical)
            if previous != group.canonical:
                raise ConceptTermDuplicated(
                    f"deployment.yaml `search.concept_groups` declares {term!r} "
                    f"in two groups ({previous!r} and {group.canonical!r})."
                    f"{where}",
                    data={"term": term, "groups": [previous, group.canonical]},
                    fix_hint="declare each term in exactly one concept group",
                )


def expand_query(
    query: str,
    groups: Sequence[ConceptGroup] | None,
) -> ExpandedQuery:
    """The query BM25 should rank, plus the report of what expanded it.

    Whole-token only: a group is reached when a token of `query` -- or a
    contiguous run, for a multi-word term -- EQUALS a declared term under the
    case rule. No group declared or matched returns the caller's own string."""
    tokens = tokenize(query)
    original_terms = tuple(dict.fromkeys(_key(token) for token in tokens))
    if not groups:
        return ExpandedQuery(
            query=query,
            applied=False,
            original_terms=original_terms,
            reason="no_groups_declared",
        )
    index: dict[str, ConceptGroup] = {}
    longest = 1
    for group in groups:
        for term in group.members:
            index[_phrase_key(term)] = group
            longest = max(longest, len(tokenize(term)))

    matched: dict[str, list[str]] = {}
    by_canonical: dict[str, ConceptGroup] = {}
    keys = [_key(token) for token in tokens]
    for size in range(min(longest, len(keys)), 0, -1):
        for start in range(0, len(keys) - size + 1):
            run = " ".join(keys[start : start + size])
            group = index.get(run)
            if group is None:
                continue
            by_canonical[group.canonical] = group
            hits = matched.setdefault(group.canonical, [])
            if run not in hits:
                hits.append(run)
    if not matched:
        return ExpandedQuery(
            query=query,
            applied=False,
            original_terms=original_terms,
            reason="no_group_matched",
        )

    present = set(original_terms) | {run for runs in matched.values() for run in runs}
    added: list[str] = []
    report: list[Mapping[str, Any]] = []
    for canonical in sorted(matched):
        group_added = [
            term
            for term in by_canonical[canonical].members
            if _phrase_key(term) not in present
        ]
        added.extend(group_added)
        report.append(
            {
                "canonical": canonical,
                "matched_terms": sorted(matched[canonical]),
                "added_terms": list(group_added),
            }
        )
    if not added:
        # Every member was already in the query: report the groups -- they DID
        # match -- but leave the query byte-for-byte alone.
        return ExpandedQuery(
            query=query,
            applied=False,
            groups=tuple(report),
            original_terms=original_terms,
            reason="all_terms_already_present",
        )
    return ExpandedQuery(
        query=" ".join((query, *added)),
        applied=True,
        groups=tuple(report),
        original_terms=original_terms,
    )


def expansion_rank(text: Any, original_terms: Iterable[str]) -> int:
    """`0` when `text` carries a term the caller typed, `1` when it does not.

    The guarantee that an exact match outranks an expanded one: a row is
    demoted only when NONE of the query's own tokens appears in it as a whole
    token, i.e. it is a candidate only because expansion added a term."""
    terms = set(original_terms)
    if not terms:
        return 0
    for token in tokenize(text):
        if _key(token) in terms:
            return 0
    return 1
