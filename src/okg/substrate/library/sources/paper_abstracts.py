"""Paper-abstract corpus source adapter — frozen local corpus.jsonl.

Profile: `reference_catalog`.

Ingests an operator-frozen scientific-literature corpus (the SciFact
``corpus.jsonl`` shape: one JSON object per line with ``doc_id``,
``title``, ``abstract`` as a list of sentences, and an optional
``structured`` flag) with NO live network calls. This is the
scientific-literature vertical's ingestion in miniature: the same
source feeds the okg-bench scifact arm and any deployment that mounts
a paper-abstract corpus.

WHAT IT EMITS, and why each piece exists:

- `paper` nodes (doc_id, title, sentence_count, the joined abstract
  truncated to ~4000 chars with a `truncated` flag, structured).
  Node ids use `paper:<repo>:<doc_id>` so the benchmark adapter's
  doc_ids resolve onto these nodes.
- `abstract_sentence` nodes — one per element of the RELEASED
  `abstract` array, carrying that element's zero-based
  `sentence_index`. SciFact's gold evidence is a set of
  `(doc_id, sentence_index)` pairs, so the sentence, not the paper,
  is the unit the benchmark scores; a graph that cannot name it
  cannot answer the question. Ids are `sentence:<repo>:<doc_id>:<i>`
  — stable across runs and derivable from the gold pair without a
  lookup. Ordering is the released array's, never a re-split of the
  joined text.
- `paper -contains-> abstract_sentence` edges, one per sentence.
- a deterministic CONCEPT relation over the corpus as a whole:
  `concept` nodes plus `abstract_sentence -mentions-> concept` and
  `paper -shares_concept-> paper`. Without it the corpus is a flat
  document store and "which papers address this claim" / "which
  papers disagree" degrade to a substring scan.

WHY THE CONCEPT RELATION LIVES IN THE SOURCE. It reads nothing but
the corpus bytes this adapter already parses — no LLM, no embeddings,
no claims file, no gold labels — so it is a pure function of the
frozen snapshot and reproduces exactly under the adapter's
content-hash change probe. Running it as a separate enricher would
re-read the same bytes through the graph to compute the same answer.
The derivation is document-frequency banded: a term is a concept iff
it appears in at least CONCEPT_MIN_DF papers (shared enough to link
two papers) and at most `ceil(CONCEPT_DF_FRACTION * papers)` papers
(rare enough to be about the science rather than about English).

Per `okg-bench-domain-adapters` (2026-07-27) and
`okg-corpus-literature` req.lit.sentence / req.lit.structure
(2026-07-28).
"""
from __future__ import annotations

import hashlib
import json
import math
import re
import sys
from collections import Counter
from dataclasses import dataclass, replace
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


__all__ = ["PaperAbstractsSource"]


#: joined-abstract truncation bound (chars). The paper node carries a
#: bounded excerpt for display; the sentence-indexed truth is now in
#: the graph as `abstract_sentence` nodes, not only in the corpus file.
ABSTRACT_MAX_CHARS = 4000

#: Per-sentence text bound (chars). A pathological single "sentence"
#: must not put an unbounded blob in a node's attrs.
SENTENCE_MAX_CHARS = 4000

#: A term is a concept iff it appears in at least this many PAPERS.
#: One paper cannot link two papers, so a df of 1 is not a relation.
CONCEPT_MIN_DF = 2

#: ...and in at most `ceil(CONCEPT_DF_FRACTION * paper_count)` papers,
#: floored at CONCEPT_MIN_DF so a tiny corpus still admits a band.
#: Above this the term is corpus-generic ("results", "patients") and
#: linking on it would connect everything to everything.
CONCEPT_DF_FRACTION = 0.02

#: Per-sentence fan-out bound on `mentions`: the N rarest concepts of
#: the sentence, ties broken by term string.
MAX_CONCEPTS_PER_SENTENCE = 8

#: `shares_concept` is only drawn through concepts held by at most
#: this many papers, so one concept contributes at most
#: N*(N-1)/2 = 28 pairs and the relation stays sparse and meaningful.
MAX_SHARED_CONCEPT_DF = 8

#: Candidate term shape: lowercase, four or more characters, starting
#: with a letter, hyphens allowed inside (e.g. `t-cell`, `covid-19`).
_TERM_RE = re.compile(r"[a-z][a-z0-9-]{3,}")

#: Punctuation stripped from a token's ends before matching. Trailing
#: hyphens and quotes are noise; interior hyphens are part of the term.
_TERM_EDGE_RE = re.compile(r"^[^a-z0-9]+|[^a-z0-9]+$")

#: Explicit, deliberately small English stopword list. Only words of
#: four or more characters can reach it (shorter tokens never match
#: `_TERM_RE`), so it holds function words, not content words — the
#: document-frequency ceiling, not this list, is what removes generic
#: scientific vocabulary.
STOPWORDS = frozenset({
    "about", "above", "after", "again", "against", "almost", "along",
    "already", "also", "although", "always", "among", "another",
    "around", "because", "been", "before", "being", "below",
    "between", "both", "cannot", "does", "done", "down", "during",
    "each", "either", "else", "enough", "even", "ever", "every",
    "from", "further", "have", "having", "hence", "here", "however",
    "into", "itself", "just", "less", "like", "made", "make", "many",
    "more", "moreover", "most", "much", "must", "neither", "none",
    "once", "only", "other", "others", "over", "perhaps", "rather",
    "same", "several", "should", "since", "some", "such", "than",
    "that", "their", "them", "then", "there", "therefore", "these",
    "they", "this", "those", "though", "through", "thus", "together",
    "toward", "under", "until", "upon", "very", "were", "what",
    "when", "where", "whether", "which", "while", "whom", "whose",
    "will", "with", "within", "without", "would", "your",
})


@dataclass(frozen=True)
class _Paper:
    """One parsed corpus row, before any fact is emitted."""

    doc_id: str
    title: str
    sentences: tuple[str, ...]
    structured: bool


def _candidate_terms(text: str) -> list[str]:
    """Ordered-unique candidate terms of one sentence.

    Order is first-occurrence order so the derivation never depends on
    set iteration; the strings are interned because the same few tens
    of thousands of terms recur across a 5,183-abstract corpus and the
    per-sentence lists are held for the whole run.
    """
    out: list[str] = []
    seen: set[str] = set()
    for raw_token in text.lower().split():
        token = _TERM_EDGE_RE.sub("", raw_token)
        if not _TERM_RE.fullmatch(token) or token in STOPWORDS:
            continue
        if token in seen:
            continue
        seen.add(token)
        out.append(sys.intern(token))
    return out


@dataclass
class PaperAbstractsSource:
    """Paper-abstract adapter over one frozen local corpus.jsonl.

    Pass both:
    - `repo='owner/name'` — the corpus identity (e.g. 'allenai/scifact'),
      namespacing the node ids.
    - `corpus_jsonl=Path(...)` — the frozen corpus file. There is no
      live mode.
    """

    name: str = "paper_abstracts"
    profile: str = "reference_catalog"
    repo: Optional[str] = None
    corpus_jsonl: Optional[Path | str] = None

    # Incremental-source contract (issue #323): one frozen local file
    # -> the content-hash floor over its bytes.
    change_probe_kind = "content_hash"

    def __post_init__(self) -> None:
        self.change_probe = self._build_change_probe()
        # The run consumes exactly the hashed bytes of the one corpus
        # file, so a matching token proves identical emission. The
        # concept derivation is a pure function of those same bytes,
        # so it does not weaken this claim.
        self.probe_short_circuit_safe = True

    def _build_change_probe(self) -> Any:
        from okg.substrate.library.sources.content_hash_probe import (
            ContentHashProbe,
        )
        from okg.substrate.library.sources.emission_identity import (
            emission_identity_targets,
        )

        def _items() -> list[tuple[str, Path]]:
            if self.corpus_jsonl is None:
                return []
            path = Path(self.corpus_jsonl)
            return [(str(path), path)] if path.is_file() else []

        return ContentHashProbe(
            content_items=_items,
            config={
                "repo": self.repo,
                "corpus_jsonl": (
                    str(self.corpus_jsonl)
                    if self.corpus_jsonl is not None else None
                ),
            },
            emit_targets=emission_identity_targets(
                self.name,
                sys.modules[__name__],
            ),
        )

    def run(
        self,
        run_id: str,
        *,
        mode: str = "scope_complete",
        cursor: Optional[dict[str, Any]] = None,
    ) -> SourceRun:
        if self.repo is None or self.corpus_jsonl is None:
            raise SubstrateError(
                f"source {self.name!r}: pass repo and corpus_jsonl",
                fix_hint="repo='allenai/scifact', "
                         "corpus_jsonl=Path('.../corpus.jsonl')",
            )
        path = Path(self.corpus_jsonl)
        raw = self._read_corpus_bytes(path)
        facts = list(self._normalize(raw, path=path))

        completed_scope = mode in {"reconcile", "scope_complete", "release_new"}
        record_set = (
            self._record_set_from_facts(facts) if completed_scope else None
        )
        applied_raw_token = self.change_probe.token_from_item_hashes({
            str(path): hashlib.sha256(raw).hexdigest(),
        })

        def _stream() -> Iterator[NodeFact | EdgeFact]:
            yield from facts

        return SourceRun(
            facts=_stream(),
            completed_scope=completed_scope,
            applied_raw_token=applied_raw_token,
            applied_token_scope="global",
            applied_token_authority="exact",
            run_mode=mode,
            record_set=record_set,
        )

    # ---------- node ids ----------

    def paper_node_id(self, doc_id: str) -> str:
        return f"paper:{self.repo}:{doc_id}"

    def sentence_node_id(self, doc_id: str, index: int) -> str:
        """Addressable from a SciFact gold `(doc_id, sentence_index)`
        pair alone — no lookup, no re-split, stable across runs."""
        return f"sentence:{self.repo}:{doc_id}:{index}"

    def concept_node_id(self, term: str) -> str:
        return f"concept:{self.repo}:{term}"

    # ---------- read + parse ----------

    def _read_corpus_bytes(self, path: Path) -> bytes:
        try:
            return path.read_bytes()
        except FileNotFoundError as exc:
            raise SubstrateError(
                f"source {self.name!r}: corpus_jsonl {path} not found",
                data={"adapter": self.name, "corpus_jsonl": str(path)},
                fix_hint="point corpus_jsonl at the frozen corpus.jsonl",
            ) from exc
        except OSError as exc:
            raise SubstrateError(
                f"source {self.name!r}: corpus_jsonl {path} could not "
                f"be read",
                data={"adapter": self.name, "corpus_jsonl": str(path)},
                fix_hint="make corpus_jsonl readable",
            ) from exc

    def _normalize(
        self, raw: bytes, *, path: Path,
    ) -> Iterator[NodeFact | EdgeFact]:
        try:
            text = raw.decode("utf-8")
        except UnicodeDecodeError as exc:
            raise SubstrateError(
                f"source {self.name!r}: {path} is not UTF-8 text",
                data={"adapter": self.name, "corpus_jsonl": str(path)},
                fix_hint="re-download the corpus snapshot",
            ) from exc

        papers = self._parse(text, path=path)
        revision = {"repo": self.repo, "corpus": path.name}
        yield from self._emit(papers, revision=revision)

    def _parse(self, text: str, *, path: Path) -> list[_Paper]:
        """Parse every row up front.

        The concept relation is corpus-wide (a term's document
        frequency is not knowable until the last row is read), so the
        adapter materializes the parsed rows rather than streaming.
        The corpus is one frozen file the run already holds in memory.
        """
        papers: list[_Paper] = []
        seen: set[str] = set()
        for lineno, line in enumerate(text.splitlines(), start=1):
            if not line.strip():
                continue
            try:
                row = json.loads(line)
            except json.JSONDecodeError as exc:
                raise SubstrateError(
                    f"source {self.name!r}: {path} line {lineno} is not "
                    f"valid JSON: {exc}",
                    data={
                        "adapter": self.name,
                        "corpus_jsonl": str(path),
                        "line": lineno,
                    },
                    fix_hint="re-download the corpus snapshot",
                ) from exc
            doc_id, title, sentences, structured = self._validate_row(
                row, path=path, lineno=lineno,
            )
            if doc_id in seen:
                raise SubstrateError(
                    f"source {self.name!r}: {path} line {lineno} repeats "
                    f"doc_id {doc_id!r}",
                    data={
                        "adapter": self.name,
                        "corpus_jsonl": str(path),
                        "line": lineno,
                        "doc_id": doc_id,
                    },
                    fix_hint="deduplicate the corpus snapshot",
                )
            seen.add(doc_id)
            papers.append(
                _Paper(
                    doc_id=doc_id,
                    title=title,
                    sentences=tuple(sentences),
                    structured=structured,
                )
            )
        if not papers:
            raise SubstrateError(
                f"source {self.name!r}: {path} contains zero papers",
                data={"adapter": self.name, "corpus_jsonl": str(path)},
                fix_hint="check the download is the full corpus.jsonl, "
                         "not a fragment",
            )
        return papers

    def _validate_row(
        self, row: Any, *, path: Path, lineno: int,
    ) -> tuple[str, str, list[str], bool]:
        def _malformed(reason: str) -> SubstrateError:
            return SubstrateError(
                f"source {self.name!r}: {path} line {lineno} is "
                f"malformed: {reason}",
                data={
                    "adapter": self.name,
                    "corpus_jsonl": str(path),
                    "line": lineno,
                },
                fix_hint="re-download the corpus snapshot",
            )

        if not isinstance(row, dict):
            raise _malformed(f"expected a JSON object, got {type(row).__name__}")
        doc_id = row.get("doc_id")
        if not isinstance(doc_id, (int, str)) or isinstance(doc_id, bool):
            raise _malformed(f"doc_id must be an int or string, got {doc_id!r}")
        title = row.get("title")
        if not isinstance(title, str) or not title.strip():
            raise _malformed("title must be a non-empty string")
        sentences = row.get("abstract")
        if not isinstance(sentences, list) or not all(
            isinstance(s, str) for s in sentences
        ):
            raise _malformed("abstract must be a list of sentence strings")
        # req.lit.sentence: the sentence is the answer unit, so a paper
        # with no sentences is a corpus defect, not a paper with an
        # empty attribute. Emitting it would publish a node the gold
        # can address but the graph cannot answer for.
        if not sentences:
            raise _malformed(
                "abstract must be a non-empty list of sentence strings "
                "(the sentence is the answer unit; a paper with zero "
                "sentences cannot be evidence)"
            )
        structured = row.get("structured", False)
        if not isinstance(structured, bool):
            raise _malformed(f"structured must be a boolean, got {structured!r}")
        return str(doc_id), title, sentences, structured

    # ---------- emit ----------

    def _emit(
        self, papers: list[_Paper], *, revision: dict[str, Any],
    ) -> Iterator[NodeFact | EdgeFact]:
        """Emit papers, sentences, and the derived concept relation.

        Single pass over the corpus for tokenization, then dict work
        keyed by term — never a pass over paper pairs, which would be
        O(n^2) on 5,183 abstracts.
        """
        repo = self.repo
        # papers x sentences x terms, in released order throughout.
        sentence_terms: list[list[list[str]]] = []
        paper_terms: list[list[str]] = []

        for paper in papers:
            yield NodeFact(
                node_id=self.paper_node_id(paper.doc_id),
                subtype="paper",
                attrs=self._paper_attrs(paper),
                source_record_id={"repo": repo, "doc_id": paper.doc_id},
                source_revision=revision,
                op="I",
            )
            per_sentence: list[list[str]] = []
            paper_seen: set[str] = set()
            paper_unique: list[str] = []
            for index, sentence in enumerate(paper.sentences):
                sentence_id = self.sentence_node_id(paper.doc_id, index)
                yield NodeFact(
                    node_id=sentence_id,
                    subtype="abstract_sentence",
                    attrs={
                        "doc_id": paper.doc_id,
                        "sentence_index": index,
                        "text": sentence[:SENTENCE_MAX_CHARS],
                        "truncated": len(sentence) > SENTENCE_MAX_CHARS,
                    },
                    source_record_id={
                        "repo": repo,
                        "doc_id": paper.doc_id,
                        "sentence_index": index,
                    },
                    source_revision=revision,
                    op="I",
                )
                yield EdgeFact(
                    src=self.paper_node_id(paper.doc_id),
                    dst=sentence_id,
                    edge_type="contains",
                    attrs={"sentence_index": index},
                    provenance="explicit",
                    source_record_id={"repo": repo, "doc_id": paper.doc_id},
                    source_revision=revision,
                    op="I",
                )
                terms = _candidate_terms(sentence)
                per_sentence.append(terms)
                for term in terms:
                    if term not in paper_seen:
                        paper_seen.add(term)
                        paper_unique.append(term)
            sentence_terms.append(per_sentence)
            paper_terms.append(paper_unique)

        concept_df = self._concept_document_frequencies(paper_terms)

        for term in sorted(concept_df):
            yield NodeFact(
                node_id=self.concept_node_id(term),
                subtype="concept",
                attrs={
                    "term": term,
                    "document_frequency": concept_df[term],
                },
                source_record_id={"repo": repo, "term": term},
                source_revision=revision,
                op="I",
            )

        yield from self._mentions_edges(
            papers, sentence_terms, concept_df, revision=revision,
        )
        yield from self._shares_concept_edges(
            papers, paper_terms, concept_df, revision=revision,
        )

    def _paper_attrs(self, paper: _Paper) -> dict[str, Any]:
        joined = " ".join(paper.sentences)
        return {
            "doc_id": paper.doc_id,
            "title": paper.title,
            "sentence_count": len(paper.sentences),
            "abstract": joined[:ABSTRACT_MAX_CHARS],
            "truncated": len(joined) > ABSTRACT_MAX_CHARS,
            "structured": paper.structured,
        }

    @staticmethod
    def _concept_document_frequencies(
        paper_terms: list[list[str]],
    ) -> dict[str, int]:
        """Terms whose PAPER document frequency sits inside the band.

        Below CONCEPT_MIN_DF the term links nothing; above the ceiling
        it links everything. The ceiling scales with the corpus so the
        band means the same thing on a fixture and on 5,183 abstracts.
        """
        paper_count = len(paper_terms)
        df: Counter[str] = Counter()
        for terms in paper_terms:
            df.update(terms)
        ceiling = max(
            CONCEPT_MIN_DF,
            math.ceil(CONCEPT_DF_FRACTION * paper_count),
        )
        return {
            term: count
            for term, count in df.items()
            if CONCEPT_MIN_DF <= count <= ceiling
        }

    def _mentions_edges(
        self,
        papers: list[_Paper],
        sentence_terms: list[list[list[str]]],
        concept_df: dict[str, int],
        *,
        revision: dict[str, Any],
    ) -> Iterator[EdgeFact]:
        """`abstract_sentence -mentions-> concept`, rarest first.

        Capped per sentence so one long sentence cannot dominate the
        edge budget; the cap keeps the RAREST concepts because those
        are the ones that discriminate between papers.
        """
        repo = self.repo
        for paper, per_sentence in zip(papers, sentence_terms):
            for index, terms in enumerate(per_sentence):
                selected = sorted(
                    (
                        (concept_df[term], term)
                        for term in terms
                        if term in concept_df
                    )
                )[:MAX_CONCEPTS_PER_SENTENCE]
                for _df, term in selected:
                    yield EdgeFact(
                        src=self.sentence_node_id(paper.doc_id, index),
                        dst=self.concept_node_id(term),
                        edge_type="mentions",
                        attrs={},
                        provenance="derived_deterministic",
                        source_record_id={
                            "repo": repo,
                            "doc_id": paper.doc_id,
                        },
                        source_revision=revision,
                        op="I",
                    )

    def _shares_concept_edges(
        self,
        papers: list[_Paper],
        paper_terms: list[list[str]],
        concept_df: dict[str, int],
        *,
        revision: dict[str, Any],
    ) -> Iterator[EdgeFact]:
        """`paper -shares_concept-> paper`, deduped, pair-ordered.

        Only concepts held by at most MAX_SHARED_CONCEPT_DF papers
        draw the relation, so a single concept contributes at most
        28 pairs and the whole pass is bounded by
        sum(df^2) over a sparse band — never a walk over paper pairs.
        """
        repo = self.repo
        doc_ids = {
            self.paper_node_id(paper.doc_id): paper.doc_id
            for paper in papers
        }
        members: dict[str, list[str]] = {
            term: []
            for term, count in concept_df.items()
            if count <= MAX_SHARED_CONCEPT_DF
        }
        for paper, terms in zip(papers, paper_terms):
            node_id = self.paper_node_id(paper.doc_id)
            for term in terms:
                bucket = members.get(term)
                if bucket is not None:
                    bucket.append(node_id)

        pair_counts: dict[tuple[str, str], int] = {}
        for term in sorted(members):
            ordered = sorted(members[term])
            for position, src in enumerate(ordered):
                for dst in ordered[position + 1:]:
                    pair_counts[(src, dst)] = pair_counts.get((src, dst), 0) + 1

        for (src, dst), shared in sorted(pair_counts.items()):
            yield EdgeFact(
                src=src,
                dst=dst,
                edge_type="shares_concept",
                attrs={"shared_concept_count": shared},
                provenance="derived_deterministic",
                source_record_id={"repo": repo, "doc_id": doc_ids[src]},
                source_revision=revision,
                op="I",
            )

    # ---------- record set ----------

    def _record_set_from_facts(
        self,
        facts: list[NodeFact | EdgeFact],
    ) -> RecordSet:
        """One logical record per paper, plus one per concept.

        A paper's record footprint is the paper node, its sentence
        nodes (descendants — they have no life of their own), and
        every edge the paper originates. Concepts are corpus-level
        vocabulary shared across papers, so they carry their own
        record; that is what lets the runner retract a concept when
        the last paper holding it leaves the snapshot.
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
                node = RecordNode(
                    node_id=fact.node_id,
                    subtype=fact.subtype,
                    source_record_id=fact.source_record_id,
                    source_revision=fact.source_revision,
                )
                if fact.subtype == "abstract_sentence":
                    key = self.paper_node_id(fact.source_record_id["doc_id"])
                    _put(key)
                    records[key].descendants.append(node)
                else:
                    _put(fact.node_id, primary=node)
                continue
            key = self.paper_node_id(fact.source_record_id["doc_id"])
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
