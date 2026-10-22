"""The keys `aggregate` groups matched text by (PACT read-aggregate-variants-3666).

`aggregate(by=["match"])` counts each distinct text a text condition
matched, as written. Two more keys merge the variants of one value:

* ``norm(match)``: letter case, spacing and punctuation do not count:
  "Owner: Eng-Platform" and "owner  eng platform" are one group,
  ``owner eng platform``;
* ``words(match)``: ``norm(match)`` with the words in sorted order, so word
  order does not count either: "Platform Eng" and "Eng Platform" are one
  group, ``eng platform``.

And a regex with one capture group counts the group's text, not the whole
match: ``re:/Owner team: (?:Eng )?([A-Za-z ]+)/`` counts the team name.
Every merged group lists the texts it holds (`variants`), so the merge can
be checked.

No database here: the SQL that computes a key, and the reply's words.
"""
from __future__ import annotations

from typing import Iterable, Optional, Sequence

__all__ = [
    "GROUP_KEYS",
    "MATCH_KEY",
    "NORM_KEY",
    "VARIANTS_SHOWN",
    "WORDS_KEY",
    "is_match_key",
    "key_sql",
    "may_be_one",
    "merged_label",
    "variants_text",
]

#: The text as matched (letter case kept).
MATCH_KEY = "match"
#: Lower case, every run of characters that are not letters or digits one space.
NORM_KEY = "norm(match)"
#: `NORM_KEY` with its words sorted.
WORDS_KEY = "words(match)"
GROUP_KEYS = (MATCH_KEY, NORM_KEY, WORDS_KEY)
#: Variants listed per merged group; the rest are counted ("+N more").
VARIANTS_SHOWN = 4
#: Pairs of groups the reply names as possibly one value.
MAY_BE_ONE_SHOWN = 3


def is_match_key(key: object) -> bool:
    return isinstance(key, str) and key in GROUP_KEYS


def key_sql(key: str, column: str) -> str:
    """SQL computing the group key of the matched text in `column`.

    `norm(match)`: lower case; every run of characters that are neither
    letters nor digits becomes one space; no space at either end. A text of
    punctuation only has the empty key and is counted in no group, as an
    empty match is not. `words(match)`: those words sorted (byte order), so
    the key does not depend on word order."""
    if key == MATCH_KEY:
        return column
    norm = f"btrim(regexp_replace(lower({column}), '[^[:alnum:]]+', ' ', 'g'))"
    if key == NORM_KEY:
        return norm
    if key == WORDS_KEY:
        return (
            "array_to_string(ARRAY(SELECT w FROM regexp_split_to_table("
            f"{norm}, ' ') AS w WHERE w <> '' ORDER BY w COLLATE \"C\"), ' ')"
        )
    raise ValueError(f"key_sql: {key!r} is not a match key")


def variants_text(variants: Sequence[tuple[str, int]], *, shown: int = VARIANTS_SHOWN) -> str:
    """`Eng Platform (22); Platform (18); +2 more`: a merged group's texts
    as matched, most occurrences first, each with its occurrences. No
    quotes, so a table cell prints it as it is."""
    parts = [f"{' '.join(text.split())} ({count})" for text, count in variants[:shown]]
    if len(variants) > shown:
        parts.append(f"+{len(variants) - shown} more")
    return "; ".join(parts)


def may_be_one(keys: Iterable[str], *, shown: int = MAY_BE_ONE_SHOWN) -> list[tuple[str, str]]:
    """Pairs `(longer, shorter)` of merged keys where every word of the
    shorter is a word of the longer ("eng platform" holds "platform"): two
    groups the caller may mean as one value, which no normalization can
    tell. Keys with more words first, at most `shown`; never merged here."""
    ordered = list(dict.fromkeys(k for k in keys if k))
    words = {k: frozenset(k.split(" ")) for k in ordered}
    pairs: list[tuple[str, str]] = []
    for longer in sorted(ordered, key=lambda k: (-len(words[k]), ordered.index(k))):
        for shorter in ordered:
            if shorter != longer and words[shorter] < words[longer]:
                pairs.append((longer, shorter))
                if len(pairs) >= shown:
                    return pairs
    return pairs


def merged_label(key: Optional[str]) -> str:
    """What a key merges, for the reply."""
    if key == WORDS_KEY:
        return "letter case, spacing, punctuation and word order"
    return "letter case, spacing and punctuation"
