"""Cross-snapshot comparability + snapshot usability, for the doc-corpus
(FreshQA) vertical.

FreshQA's claim is temporal: the same question, asked against two dated
snapshots of the same corpus, may have two different right answers. That
claim is only measurable if the two graphs can be RELATED. Two things make
them relatable, and they are deliberately different things:

    which passage is this, across snapshots?   (doc_path, chunk_ordinal)
    did this passage change between them?      the chunk node id differs

Chunk node identity is content-addressed — `chunk:<sha256(text)[:16]>` —
and stays that way (see the PACT's designs/chunk-identity-finding.md). Under
content-addressed identity alone the first question is unanswerable, because
an edited passage looks like a deletion plus an unrelated insertion. Under
positional identity the second would need a text comparison. Carrying the
positional KEY as queryable attributes while leaving IDENTITY content-
addressed answers both, and is what `req.fresh.comparable` requires.

The second half of this module is snapshot USABILITY. A document set the
operator supplies might simply not contain the answers the run will be
scored on, and a corpus that cannot answer anything still publishes,
still satisfies its structural clauses, and still scores zero — the
failure looking exactly like a bad model. The answer-coverage probe makes
that case a REJECTION with its numbers named. It lives here rather than in
the corpus contract because the answers live in the FreshQA sheet, not in
the graph, so it cannot be SQL over a published generation.

PACT okg-corpus-doc-corpus / req.fresh.comparable + req.fresh.contract.
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence

from .errors import CorpusBuildError


#: Chunks that carry the positional key. A chunk without one is not a
#: comparable passage; it is counted, never silently treated as matching.
_KEYED_CHUNKS_SQL = """
SELECT n.node_id,
       n.attrs->>'doc_path'       AS doc_path,
       n.attrs->>'chunk_ordinal'  AS chunk_ordinal
  FROM okg.graph_nodes n
 WHERE n.subtype = 'document_chunk'
   AND n.attrs ? 'doc_path'
   AND n.attrs ? 'chunk_ordinal'
"""

_CHUNK_TEXT_SQL = """
SELECT n.node_id, n.attrs->>'text' AS text
  FROM okg.graph_nodes n
 WHERE n.subtype = 'document_chunk'
   AND n.attrs ? 'text'
"""

_SNAPSHOT_DATES_SQL = """
SELECT DISTINCT n.attrs->>'snapshot_date' AS snapshot_date
  FROM okg.graph_nodes n
 WHERE n.subtype IN ('document', 'document_chunk')
"""


def _checkout(dsn: str, generation_id: str):
    from okg.substrate.graph_store import ChronosGraphStore

    store = ChronosGraphStore.connect(dsn, ensure_schema=False)
    return store, store.checkout_generation(generation_id)


def passage_keys(dsn: str, generation_id: str) -> dict[tuple[str, int], str]:
    """`(doc_path, chunk_ordinal) -> chunk node id` for one generation.

    The key is what two snapshots share; the value is what differs when the
    passage changed. A duplicated key would make the mapping ambiguous, so
    it is a loud failure rather than a last-write-wins collapse.
    """
    store, graph = _checkout(dsn, generation_id)
    try:
        out: dict[tuple[str, int], str] = {}
        collisions: list[str] = []
        for row in graph.query(_KEYED_CHUNKS_SQL):
            key = (str(row["doc_path"]), int(row["chunk_ordinal"]))
            if key in out and out[key] != str(row["node_id"]):
                collisions.append(f"{key} -> {out[key]} and {row['node_id']}")
                continue
            out[key] = str(row["node_id"])
        if collisions:
            raise CorpusBuildError(
                f"generation {generation_id} has {len(collisions)} duplicated "
                "passage key(s); the positional key must identify exactly one "
                "chunk per document position: " + "; ".join(collisions[:5])
            )
        return out
    finally:
        store.close()


def chunk_texts(dsn: str, generation_id: str) -> dict[str, str]:
    """`chunk node id -> text` for one generation."""
    store, graph = _checkout(dsn, generation_id)
    try:
        return {
            str(row["node_id"]): str(row["text"] or "")
            for row in graph.query(_CHUNK_TEXT_SQL)
        }
    finally:
        store.close()


def snapshot_dates(dsn: str, generation_id: str) -> list[str | None]:
    """Every distinct `snapshot_date` on a document/chunk node. More than
    one, or a NULL, means the build was not one dated observation."""
    store, graph = _checkout(dsn, generation_id)
    try:
        return sorted(
            (row["snapshot_date"] for row in graph.query(_SNAPSHOT_DATES_SQL)),
            key=lambda value: (value is None, value or ""),
        )
    finally:
        store.close()


# --- relating a successor snapshot to its predecessor -------------------


@dataclass(frozen=True)
class SnapshotLineage:
    """How a successor snapshot relates to the one it supersedes."""

    predecessor_corpus: str
    predecessor_deployment: str
    predecessor_generation: str
    predecessor_snapshot_date: str
    successor_snapshot_date: str
    shared_keys: int
    predecessor_keys: int
    successor_keys: int
    #: shared keys whose chunk node id is unchanged — the passage did not change
    unchanged_passages: int
    #: shared keys whose chunk node id differs — the passage CHANGED
    changed_passages: int
    #: keys present only in one side (a document gained/lost passages)
    added_passages: int
    removed_passages: int

    def as_dict(self) -> dict[str, Any]:
        return {
            "predecessor_corpus": self.predecessor_corpus,
            "predecessor_deployment": self.predecessor_deployment,
            "predecessor_generation": self.predecessor_generation,
            "predecessor_snapshot_date": self.predecessor_snapshot_date,
            "successor_snapshot_date": self.successor_snapshot_date,
            "shared_keys": self.shared_keys,
            "predecessor_keys": self.predecessor_keys,
            "successor_keys": self.successor_keys,
            "unchanged_passages": self.unchanged_passages,
            "changed_passages": self.changed_passages,
            "added_passages": self.added_passages,
            "removed_passages": self.removed_passages,
        }


def _identity(label: str, corpus: Mapping[str, Any]) -> str:
    return (
        f"{label}(vertical={corpus.get('vertical')!r} "
        f"deployment={corpus.get('deployment_name')!r} "
        f"snapshot_date={corpus.get('snapshot_date')!r} "
        f"generation={corpus.get('generation_id')!r})"
    )


def read_corpus_json(path: Path) -> dict[str, Any]:
    if not path.is_file():
        raise CorpusBuildError(
            f"predecessor corpus.json not found at {path}; a successor "
            "snapshot must name a real predecessor or none at all"
        )
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError) as e:
        raise CorpusBuildError(
            f"predecessor corpus.json at {path} is unreadable: {e}"
        ) from e


def relate_to_predecessor(
    *,
    predecessor_corpus_json: Path,
    vertical: str,
    snapshot_date: str,
    dsn: str,
    generation_id: str,
    deployment_name: str,
) -> SnapshotLineage:
    """Relate a freshly built snapshot to the one it supersedes, or FAIL.

    Every refusal names BOTH snapshot identities. An unrelatable successor
    must never be quietly accepted: a pair of graphs that share no passage
    scores as a freshness measurement while measuring nothing, and that is
    indistinguishable from a real answer change.
    """
    predecessor = read_corpus_json(predecessor_corpus_json)
    successor: dict[str, Any] = {
        "vertical": vertical,
        "deployment_name": deployment_name,
        "snapshot_date": snapshot_date,
        "generation_id": generation_id,
    }
    identities = (
        f"{_identity('predecessor', predecessor)} vs "
        f"{_identity('successor', successor)}"
    )

    if predecessor.get("vertical") != vertical:
        raise CorpusBuildError(
            "successor snapshot cannot be related to its recorded "
            "predecessor: they are different verticals. " + identities
        )

    prior_date_raw = predecessor.get("snapshot_date")
    if not prior_date_raw:
        raise CorpusBuildError(
            "successor snapshot cannot be related to its recorded "
            "predecessor: the predecessor records no snapshot_date, so the "
            "two observations cannot be ordered. " + identities
        )
    try:
        prior_date = date.fromisoformat(str(prior_date_raw))
        later_date = date.fromisoformat(str(snapshot_date))
    except ValueError as e:
        raise CorpusBuildError(
            "successor snapshot cannot be related to its recorded "
            f"predecessor: a snapshot_date is not an ISO date ({e}). "
            + identities
        ) from e
    if later_date <= prior_date:
        raise CorpusBuildError(
            "successor snapshot cannot be related to its recorded "
            "predecessor: a successor must be strictly LATER, otherwise "
            "'which answer is fresher' has no answer. " + identities
        )

    prior_dsn = str(predecessor.get("dsn") or "")
    prior_generation = str(predecessor.get("generation_id") or "")
    if not prior_dsn or not prior_generation:
        raise CorpusBuildError(
            "successor snapshot cannot be related to its recorded "
            "predecessor: the predecessor corpus.json records no dsn or "
            "generation_id to read its passages from. " + identities
        )

    prior_keys = passage_keys(prior_dsn, prior_generation)
    later_keys = passage_keys(dsn, generation_id)
    shared = set(prior_keys) & set(later_keys)
    if not shared:
        raise CorpusBuildError(
            "successor snapshot cannot be related to its recorded "
            f"predecessor: the two share ZERO passage keys "
            f"({len(prior_keys)} predecessor keys, {len(later_keys)} "
            "successor keys). Answer accuracy on A versus B would compare "
            "two unrelated graphs. " + identities
        )

    unchanged = sum(1 for key in shared if prior_keys[key] == later_keys[key])
    return SnapshotLineage(
        predecessor_corpus=str(predecessor_corpus_json),
        predecessor_deployment=str(predecessor.get("deployment_name") or ""),
        predecessor_generation=prior_generation,
        predecessor_snapshot_date=str(prior_date_raw),
        successor_snapshot_date=str(snapshot_date),
        shared_keys=len(shared),
        predecessor_keys=len(prior_keys),
        successor_keys=len(later_keys),
        unchanged_passages=unchanged,
        changed_passages=len(shared) - unchanged,
        added_passages=len(set(later_keys) - set(prior_keys)),
        removed_passages=len(set(prior_keys) - set(later_keys)),
    )


# --- snapshot usability: can the run's answers be found at all? ---------

_WS = re.compile(r"\s+")


def _normalize(text: str) -> str:
    return _WS.sub(" ", text.strip().casefold())


@dataclass(frozen=True)
class AnswerCoverage:
    """How much of a question sample the snapshot can actually support."""

    sampled: int
    covered: int
    minimum_fraction: float
    uncovered: tuple[str, ...]

    @property
    def fraction(self) -> float:
        return (self.covered / self.sampled) if self.sampled else 0.0

    @property
    def ok(self) -> bool:
        return self.sampled > 0 and self.fraction >= self.minimum_fraction

    def render(self) -> str:
        return (
            f"answer coverage {self.covered}/{self.sampled} "
            f"({self.fraction:.3f}) required >={self.minimum_fraction}"
        )


def answer_coverage(
    *,
    texts: Iterable[str],
    questions: Sequence[tuple[str, Sequence[str]]],
    minimum_fraction: float,
) -> AnswerCoverage:
    """Match a sample of ANSWERABLE questions against the snapshot's text.

    `questions` is `(question_id, gold_answers)`; a question counts as
    covered when any of its gold answers appears in some chunk. False-
    premise questions have no answer to find and are the caller's to
    exclude — counting them would inflate coverage with questions the
    corpus never had to support.

    `minimum_fraction` has NO default on purpose. The bar depends on which
    document set the operator chooses, and a default here would silently
    become that decision.
    """
    if not 0.0 < minimum_fraction <= 1.0:
        raise CorpusBuildError(
            "answer-coverage minimum_fraction must be in (0, 1]; got "
            f"{minimum_fraction!r}. A zero bar accepts every snapshot."
        )
    haystack = "\n".join(_normalize(text) for text in texts)
    covered = 0
    uncovered: list[str] = []
    for question_id, answers in questions:
        needles = [_normalize(a) for a in answers]
        needles = [n for n in needles if n]
        if not needles:
            raise CorpusBuildError(
                f"question {question_id!r} was sampled for the answer-"
                "coverage probe with no gold answer text; false-premise "
                "questions must be excluded by the caller, not scored as "
                "uncovered"
            )
        if any(needle in haystack for needle in needles):
            covered += 1
        else:
            uncovered.append(question_id)
    return AnswerCoverage(
        sampled=len(questions),
        covered=covered,
        minimum_fraction=minimum_fraction,
        uncovered=tuple(uncovered),
    )


def enforce_answer_coverage(
    coverage: AnswerCoverage, *, snapshot_date: str
) -> None:
    """REJECT a snapshot the run's questions cannot be answered from.

    The alternative — accept it and run — produces a full set of zero
    scores that reads as a model failure. The gutting check the FreshQA
    adapter already performs on the QUESTION set, applied to the corpus.
    """
    if coverage.sampled == 0:
        raise CorpusBuildError(
            f"snapshot {snapshot_date} was accepted against an EMPTY answer-"
            "coverage sample; a probe over no questions proves nothing"
        )
    if not coverage.ok:
        sample = ", ".join(coverage.uncovered[:10])
        raise CorpusBuildError(
            f"snapshot {snapshot_date} is not usable as a FreshQA corpus: "
            f"{coverage.render()}. No chunk contains the answer to "
            f"{len(coverage.uncovered)} sampled question(s) [{sample}]. "
            "Rejecting: an accepted snapshot that cannot answer its "
            "questions scores zero and looks like a model failure."
        )
