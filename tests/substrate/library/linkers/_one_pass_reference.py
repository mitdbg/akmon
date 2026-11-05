"""Shared reference for the one-pass link matcher tests.

The old every-pair `subject_prefix` and `word_boundary` loops, kept verbatim
as they shipped before the one-pass rewrite, the new matchers under short
names, and a generator of large synthetic inputs. The unit tests, the
Postgres tests and the PACT's scale bench all compare against these.

This module names no repository path, so a Postgres test can import it
without widening what its lane reads.
"""
from __future__ import annotations

import random
import re

from okg.substrate.library.linkers.declarative import DeclarativeLinker


def old_pairs_subject_prefix(
    srcs: list[tuple[str, str]],
    dsts: list[tuple[str, str]],
) -> set[tuple[str, str]]:
    out: set[tuple[str, str]] = set()
    for src_id, subj in srcs:
        for dst_id, slug in dsts:
            if not slug:
                continue
            if (
                subj == slug
                or subj.startswith(slug + ":")
                or subj.startswith(slug + " ")
            ):
                out.add((src_id, dst_id))
    return out


def old_pairs_word_boundary(
    srcs: list[tuple[str, str]],
    dsts: list[tuple[str, str]],
) -> set[tuple[str, str]]:
    pat_cache: dict[str, re.Pattern] = {}
    out: set[tuple[str, str]] = set()
    for src_id, text in srcs:
        for dst_id, slug in dsts:
            if not slug:
                continue
            pat = pat_cache.get(slug)
            if pat is None:
                pat = re.compile(
                    r"(?:^|[^a-zA-Z0-9-])"
                    + re.escape(slug)
                    + r"(?:[^a-zA-Z0-9-]|$)"
                )
                pat_cache[slug] = pat
            if pat.search(text):
                out.add((src_id, dst_id))
    return out


new_subject_prefix = DeclarativeLinker._pairs_subject_prefix
new_word_boundary = DeclarativeLinker._pairs_word_boundary


WORDS = (
    "the build failed on a fresh checkout because the parser read the "
    "wrong field fix it and add a regression test for the case before "
    "release see the notes in the ticket and the review thread"
).split()


def synthetic_case(
    n_srcs: int,
    n_dsts: int,
    *,
    text_chars: int = 600,
    seed: int = 7,
) -> tuple[list[tuple[str, str]], list[tuple[str, str]]]:
    """`n_srcs` issue-like texts of about `text_chars` characters and
    `n_dsts` file paths; about one word in a hundred is a path."""
    rng = random.Random(seed)
    dsts = [
        (
            f"file:{i}",
            f"src/pkg{i % 40}/sub{(i // 40) % 25}/module_{i}.py",
        )
        for i in range(n_dsts)
    ]
    srcs: list[tuple[str, str]] = []
    for i in range(n_srcs):
        words: list[str] = []
        size = 0
        while size < text_chars:
            if rng.random() < 0.01:
                word = dsts[rng.randrange(n_dsts)][1]
            else:
                word = rng.choice(WORDS)
            words.append(word)
            size += len(word) + 1
        srcs.append((f"issue:{i}", " ".join(words) + "."))
    return srcs, dsts
