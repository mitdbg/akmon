"""LLM extraction for the synth benchmark's prose lane.

The LLM-free lane reads documents with the deterministic label parser
(``claim_corpus_source.parse_document_claims``), which matches ``Label: value``
line by line. Prose has no such line, so the prose lane needs an extractor that
reads sentences — this module.

THE CONTRACT IT MUST REPRODUCE
Buckets form on ``(predicate, normalized_key)`` and the gold manifest keys
corruptions the same way, so this extractor emits claims IDENTICAL in shape to
the deterministic one: predicate is the slug of the pack LABEL, normalized_key
is the document entity, scope is the markdown section path at the value's
offset. A free-form attribute ("Weight" rather than "body_weight") would key a
bucket the gold cannot find, and recall would silently fall — so the model is
given the CLOSED label vocabulary and must choose from it.

GROUNDING
Each fact must come back with the literal ``quote`` the value was read from.
The quote is located in the document to derive the offset (hence the scope) and
to prove the value was read rather than invented: a quote that is not a
verbatim substring is a LOUD failure, not a dropped fact.

REASONING BUDGET — the trap this module exists to survive
``qwen3.6-27b`` is a reasoning model. Measured on a trivial two-fact prompt:
212 of 293 completion tokens were reasoning. With a naive ``max_tokens`` the
call returns ``finish_reason='length'``, EMPTY ``content``, the answer stranded
in ``reasoning_content`` — and NO exception. An extractor that shrugged at that
would return zero facts per document, and every recall metric would report a
clean zero denominator while the run completed. Empty content is therefore an
error here, and the token budget is sized for reasoning plus answer.
"""

from __future__ import annotations

import os

import json
import re
import threading
from dataclasses import dataclass, field
from typing import Any, Iterable, Iterator, Mapping

from okg.bench.injection.claim_corpus_source import (
    MarkdownScopeIndex,
    _slug,
)
from okg.substrate.library.sources.base import NodeFact

#: The reading model. Overridable by ``OKG_SYNTH_LLM_MODEL`` so a run can be
#: pointed at a different model WITHOUT a code edit — and the model that
#: actually served a run is recorded in :meth:`ExtractionCost.as_dict`, so a
#: reading result always names the model that produced it. An extraction
#: number whose model is only knowable from the code that was checked out when
#: it ran is not reproducible.
DEFAULT_MODEL = os.environ.get("OKG_SYNTH_LLM_MODEL") or "openrouter/qwen/qwen3.6-27b"
#: Reasoning plus answer, and sized from MEASUREMENT rather than intuition.
#: On real smoke documents the reasoning share is 79.8% of completion tokens
#: (23,644 of 29,632 over 8 documents), averaging ~3.7k completion tokens per
#: document and peaking well above that on a referral letter carrying ~19
#: facts. At 4000 the two largest documents truncated mid-array. 12000 leaves
#: roughly 3x headroom over the observed mean; unused budget costs nothing,
#: while an exhausted one costs a whole document.
DEFAULT_MAX_TOKENS = 12000

_H1_RE = re.compile(r"^#\s+(?P<title>.+?)\s*$", re.MULTILINE)
_FENCE_RE = re.compile(r"^\s*```(?:json)?\s*|\s*```\s*$", re.MULTILINE)


class LlmExtractionError(RuntimeError):
    """Extraction failed in a way that must not be scored as 'no facts'."""


class QuoteScopeError(LlmExtractionError):
    """A per-FACT grounding failure: the quote cannot be scoped to a single
    section, so this ONE fact must not become a claim.

    Two causes, both fail-closed and both about a single quote: the quote is
    not a verbatim substring (the value was invented or altered, so no scope
    exists), or it occurs across MORE THAN ONE section (the extractor cannot
    tell which occurrence the value was read from, so keeping it would silently
    mis-key the bucket). Either way the guard's judgment is unchanged — never
    scope by guessing — but the blast radius is the fact, not the document: the
    lane drops this claim and records a fact-scope ledger row while every other
    fact of the document lands. A subclass of ``LlmExtractionError`` so any
    existing catch still holds; ``_claims_from_response`` catches it
    SPECIFICALLY to collect-and-continue rather than propagate."""


@dataclass
class ExtractionCost:
    """Per-run token and dollar accounting, so a scaled run can be costed
    from a small one rather than guessed.

    Thread-safe: the LLM lane extracts documents concurrently and every worker
    reports into this one accumulator, so mutation happens under a lock —
    read-modify-write on these counters is not atomic, and a torn update would
    silently under-report spend.
    """

    calls: int = 0
    prompt_tokens: int = 0
    completion_tokens: int = 0
    reasoning_tokens: int = 0
    cost_usd: float = 0.0
    #: Facts the model returned under an attribute outside the closed
    #: vocabulary. Dropping them is correct, but a silent drop would look like
    #: the model finding fewer facts rather than naming them differently.
    off_vocabulary: int = 0
    #: The model that served this run's calls, recorded so a reading
    #: result is attributable to the reader that produced it.
    model: str = ""
    _lock: threading.Lock = field(
        default_factory=threading.Lock, repr=False, compare=False
    )

    def observe(self, response: Any) -> None:
        usage = getattr(response, "usage", None)
        with self._lock:
            self.calls += 1
            if usage is None:
                return
            self.prompt_tokens += int(getattr(usage, "prompt_tokens", 0) or 0)
            self.completion_tokens += int(getattr(usage, "completion_tokens", 0) or 0)
            details = getattr(usage, "completion_tokens_details", None)
            self.reasoning_tokens += int(getattr(details, "reasoning_tokens", 0) or 0)
            self.cost_usd += float(getattr(usage, "cost", 0.0) or 0.0)

    def bump_off_vocabulary(self) -> None:
        with self._lock:
            self.off_vocabulary += 1

    def as_dict(self) -> dict[str, Any]:
        return {
            "model": self.model,
            "calls": self.calls,
            "prompt_tokens": self.prompt_tokens,
            "completion_tokens": self.completion_tokens,
            "reasoning_tokens": self.reasoning_tokens,
            "reasoning_share": (
                round(self.reasoning_tokens / self.completion_tokens, 3)
                if self.completion_tokens else 0.0
            ),
            "cost_usd": round(self.cost_usd, 6),
            "off_vocabulary": self.off_vocabulary,
        }


def _system_prompt(labels: Iterable[str]) -> str:
    vocabulary = "\n".join(f"  - {label}" for label in sorted(labels))
    return (
        "You extract clinical facts from a note.\n\n"
        "Return ONLY a JSON array. Each element is an object with exactly:\n"
        '  "attribute" — one of the allowed attributes below, copied EXACTLY\n'
        '  "value"     — the value as written in the note, including any unit\n'
        '  "quote"     — the shortest VERBATIM span of the note containing '
        "the value\n\n"
        "Allowed attributes:\n" + vocabulary + "\n\n"
        "Rules:\n"
        "- Use only attributes from the list. Skip anything that does not fit.\n"
        "- Copy values and quotes exactly; never normalise, convert or correct "
        "them.\n"
        "- The quote must appear character-for-character in the note.\n"
        "- Report what the note says even when it looks wrong.\n"
        "- One element per stated fact. Return [] if the note states none."
    )


def _content(response: Any, *, max_tokens: int) -> str:
    """The model's answer, or a LOUD failure that names the RIGHT cause.

    A reasoning model exhausts its budget in two distinguishable ways, and
    reporting the wrong one sends the next reader hunting the wrong bug:

    * budget gone before any answer  -> empty content, answer stranded in
      reasoning_content;
    * budget gone mid-answer         -> non-empty but TRUNCATED content, which
      then fails JSON parsing and looks like a malformed model rather than a
      short budget.

    Both raise, and both say "raise max_tokens", because scoring either as
    "no facts" would report a clean zero denominator instead of a failure.
    """
    choice = response.choices[0]
    finish = getattr(choice, "finish_reason", None)
    text = (getattr(choice.message, "content", None) or "").strip()

    if finish == "length":
        reasoning = getattr(choice.message, "reasoning_content", None) or ""
        where = "before answering" if not text else "mid-answer"
        raise LlmExtractionError(
            f"model hit the token ceiling {where} (finish_reason='length', "
            f"max_tokens={max_tokens}, {len(reasoning)} chars of reasoning, "
            f"{len(text)} chars of content). This is a REASONING model — "
            "measured reasoning share is ~80% of completion tokens — so raise "
            "max_tokens. Treating this as 'no facts' would silently shrink the "
            "recall denominator."
        )
    if not text:
        raise LlmExtractionError(
            f"model returned EMPTY content with finish_reason={finish!r}; "
            "nothing to parse, and an empty extraction must not be scored as "
            "a document that legitimately states no facts."
        )
    return text


def _quote_offsets(text: str, quote: str) -> list[int]:
    """Every start offset where ``quote`` occurs in ``text`` (all occurrences).

    text.find returns only the FIRST occurrence, so a quote string that appears
    twice in one document would silently scope the claim to whichever section
    the first hit lands in. Enumerating every occurrence is what lets the caller
    tell an unambiguous repeat (same section) from a genuine ambiguity (two
    sections) and fail loudly on the latter — req.attrib.llm-lane's
    extraction-integrity edge.
    """
    if not quote:
        return []
    offsets: list[int] = []
    start = text.find(quote)
    while start != -1:
        offsets.append(start)
        start = text.find(quote, start + 1)
    return offsets


def _parse(text: str) -> list[dict[str, str]]:
    body = _FENCE_RE.sub("", text).strip()
    start, end = body.find("["), body.rfind("]")
    if start == -1 or end == -1:
        raise LlmExtractionError(f"no JSON array in model output: {body[:200]!r}")
    try:
        rows = json.loads(body[start : end + 1])
    except json.JSONDecodeError as exc:
        raise LlmExtractionError(
            f"model output is not valid JSON ({exc}): {body[:200]!r}"
        ) from exc
    if not isinstance(rows, list):
        raise LlmExtractionError(f"expected a JSON array, got {type(rows).__name__}")
    return [r for r in rows if isinstance(r, dict)]


@dataclass
class LlmExtractor:
    """Reads prose documents into claims shaped like the deterministic ones."""

    labels: tuple[str, ...]
    model: str = DEFAULT_MODEL
    max_tokens: int = DEFAULT_MAX_TOKENS
    source_class: str = "clinical"
    cost: ExtractionCost = field(default_factory=ExtractionCost)

    def _complete(self, text: str) -> str:
        import litellm

        if not self.cost.model:
            self.cost.model = self.model

        litellm.suppress_debug_info = True
        response = litellm.completion(
            model=self.model,
            messages=[
                {"role": "system", "content": _system_prompt(self.labels)},
                {"role": "user", "content": text},
            ],
            temperature=0.0,
            max_tokens=self.max_tokens,
        )
        self.cost.observe(response)
        return _content(response, max_tokens=self.max_tokens)

    def _locate_quote(
        self,
        relpath: str,
        attribute: str,
        quote: str,
        text: str,
        index: "MarkdownScopeIndex",
        consumed: dict[str, int],
    ) -> int:
        """The offset a quote was read from, or a per-FACT LOUD failure.

        Two distinct failure modes, both fail-closed and both PER-FACT
        (:class:`QuoteScopeError` — the fact is dropped and recorded, the
        document is not lost):

        * the quote is not a verbatim substring — the value was invented or
          altered, so no scope can be located (the existing grounding check);
        * the quote occurs MORE THAN ONCE in DIFFERENT sections — the extractor
          cannot tell which occurrence the value was read from, so keeping the
          first would silently scope (and thereby mis-key) the claim. This is
          the extraction-integrity edge req.attrib.llm-lane names: disambiguate
          or fail, never mis-key silently.

        A quote that repeats WITHIN one section is unambiguous (every occurrence
        yields the same scope path), so it is scoped without complaint and each
        repeat consumes the next occurrence in order.
        """
        offsets = _quote_offsets(text, quote)
        if not offsets:
            raise QuoteScopeError(
                f"{relpath}: quote for {attribute!r} is not a verbatim "
                f"substring of the document: {quote[:120]!r}. The value was "
                "invented or altered, so its scope cannot be located."
            )
        scopes = {index.section_path_at(offset) for offset in offsets}
        if len(offsets) > 1 and len(scopes) > 1:
            raise QuoteScopeError(
                f"{relpath}: quote for {attribute!r} occurs {len(offsets)} times "
                f"across {len(scopes)} sections ({sorted(scopes)}); the extractor "
                "cannot tell which occurrence the value was read from, so its "
                "scope and bucket would be silently mis-keyed. Disambiguate the "
                f"occurrence or fail — never guess. Quote: {quote[:120]!r}"
            )
        taken = consumed.get(quote, 0)
        consumed[quote] = taken + 1
        return offsets[taken if taken < len(offsets) else len(offsets) - 1]

    def _claims_from_response(
        self,
        relpath: str,
        text: str,
        response_text: str,
        *,
        digest: str = "0",
        fact_failures: list[dict[str, str]] | None = None,
    ) -> Iterator[NodeFact]:
        """The parsed response as claims, dropping the facts whose quote cannot
        be scoped (:class:`QuoteScopeError`) rather than losing the document.

        ``fact_failures`` is the opt-in per-fact tolerance seam. When a list is
        given, a quote-scope failure appends a row (``doc``/``attribute``/
        ``quote``/``cause``) and the loop CONTINUES — the other facts of the
        document still land, and the caller surfaces the row into the
        extraction-failures ledger with ``scope: "fact"``. When it is ``None``
        the failure RE-RAISES: a caller that ignores per-fact failures gets the
        loud signal rather than a silently dropped claim. Document-fatal causes
        (missing H1, unparseable output) always raise regardless — they run
        before the row loop and cannot be scoped to a single fact.
        """
        h1 = _H1_RE.search(text)
        if h1 is None:
            raise LlmExtractionError(f"document {relpath!r} has no H1 title")
        entity = _slug(h1.group("title"))
        index = MarkdownScopeIndex(text)
        label_by_slug = {_slug(label): label for label in self.labels}

        occurrences: dict[tuple[str, str], int] = {}
        consumed_quotes: dict[str, int] = {}
        for row in _parse(response_text):
            attribute = str(row.get("attribute", "")).strip()
            value = str(row.get("value", "")).strip()
            quote = str(row.get("quote", "")).strip()
            if not attribute or not value:
                continue
            predicate = _slug(attribute)
            if predicate not in label_by_slug:
                # Off-vocabulary attributes would key a bucket the gold cannot
                # find. Dropping is right (the model was told to skip), but a
                # silent drop reads as "found fewer facts" rather than "named
                # them differently", so it is counted.
                self.cost.bump_off_vocabulary()
                continue
            try:
                offset = self._locate_quote(
                    relpath, attribute, quote, text, index, consumed_quotes
                )
            except QuoteScopeError as exc:
                # PER-FACT: an ambiguous or non-verbatim quote drops THIS claim,
                # never the document. Recorded (never silent) as a fact-scope
                # row; deterministic given the response, so it must not trigger a
                # document retry. With no sink the caller opted out of tolerance,
                # so the fault surfaces loudly instead of vanishing.
                if fact_failures is None:
                    raise
                fact_failures.append(
                    {
                        "doc": relpath,
                        "attribute": attribute,
                        "quote": quote,
                        "cause": str(exc),
                    }
                )
                continue
            occurrence = occurrences.get((predicate, entity), 0)
            occurrences[(predicate, entity)] = occurrence + 1
            node_id = f"claim:{relpath}:{predicate}:{entity}:{occurrence}"
            yield NodeFact(
                node_id,
                "consistency_claim",
                {
                    "predicate": predicate,
                    "normalized_key": entity,
                    "value": value,
                    "scope": index.section_path_at(offset),
                    "scope_state": "known",
                    "claim_provenance": "derived",
                    "source_id": relpath,
                    "source_class": self.source_class,
                },
                {"path": relpath, "claim": node_id},
                {"sha": digest},
            )

    def claims(
        self,
        relpath: str,
        text: str,
        *,
        digest: str = "0",
        fact_failures: list[dict[str, str]] | None = None,
    ) -> Iterator[NodeFact]:
        """Fetch + parse one document into claims (the single-shot convenience).

        The runner's LLM lane instead drives ``_complete`` and
        ``_claims_from_response`` separately so it can record the verbatim
        model response and apply the bounded per-document retry; this method
        keeps the two together for the shape witnesses. ``fact_failures``
        forwards the same per-fact tolerance seam: pass a list to collect the
        dropped-fact rows, omit it to have a quote-scope failure re-raise.

        The H1 guard runs BEFORE the model call: a document this method can
        never yield claims for must not cost a completion. (The guard also
        lives in ``_claims_from_response`` for the runner path, where the
        response is deliberately fetched first so it can be recorded.)
        """
        if _H1_RE.search(text) is None:
            raise LlmExtractionError(f"document {relpath!r} has no H1 title")
        response_text = self._complete(text)
        yield from self._claims_from_response(
            relpath, text, response_text, digest=digest, fact_failures=fact_failures
        )


def pack_labels(pack: Mapping[str, Any], world_kinds: Iterable[str]) -> tuple[str, ...]:
    """The closed attribute vocabulary, expanded for prefix label rules.

    Prefix kinds ("medication:", "allergy_status:") produce one label per key
    in the world, matching _label_for so the model's choices slug to the same
    predicates the gold uses.
    """
    labels: Mapping[str, str] = pack["labels"]
    out: set[str] = set()
    for kind in world_kinds:
        if kind in labels:
            out.add(labels[kind])
            continue
        for prefix, base in labels.items():
            if prefix.endswith(":") and kind.startswith(prefix):
                suffix = kind[len(prefix):].replace("_", " ").title()
                out.add(f"{base} {suffix}")
                break
    return tuple(sorted(out))
